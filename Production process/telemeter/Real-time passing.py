import socket
import ctypes
import json
import time
import os
import threading
import queue
from f1_pasing import * 
UDP_IP = "127.0.0.1"
UDP_PORT = 20777

PACKET_CLASSES = {
    0: PacketMotionData, 1: PacketSessionData, 2: PacketLapData,
    4: PacketParticipantsData, 5: PacketCarSetupData, 6: PacketCarTelemetryData,
    7: PacketCarStatusData, 8: PacketFinalClassificationData, 9: PacketLobbyInfoData,
    10: PacketCarDamageData, 11: PacketSessionHistoryData, 12: PacketTyreSetsData,
    13: PacketMotionExData, 14: PacketTimeTrialData, 15: PacketLapPositionsData
}

EVENT_MAP = {
    b'FTLP': 'FastestLap', b'RTMT': 'Retirement', b'DRSD': 'DRSDisabled',
    b'TMPT': 'TeamMateInPits', b'RCWN': 'RaceWinner', b'PENA': 'Penalty',
    b'SPTP': 'SpeedTrap', b'STLG': 'StartLights', b'DTSV': 'DriveThroughPenaltyServed',
    b'SGSV': 'StopGoPenaltyServed', b'FLBK': 'Flashback', b'BUTN': 'Buttons',
    b'OVTK': 'Overtake', b'SCAR': 'SafetyCar', b'COLL': 'Collision',
    b'RDFL': None, b'SSTA': None, b'SEND': None, b'CHQF': None, b'LGOT': None
}

PACKET_NAMES = {
    0: "motion", 1: "session", 2: "lap", 3: "event", 4: "participants",
    5: "setup", 6: "telemetry", 7: "status", 8: "final_classification",
    9: "lobby", 10: "damage", 11: "session_history", 12: "tyre_sets",
    13: "motion_ex", 14: "time_trial", 15: "lap_positions"
}

data_queue = queue.Queue(maxsize=100000)
is_running = True

def ctypes_to_dict(data):
    if isinstance(data, ctypes.Array):
        return [ctypes_to_dict(item) for item in data]
    elif hasattr(data, "_fields_"):
        result_dict = {}
        for field, _ in data._fields_:
            result_dict[field] = ctypes_to_dict(getattr(data, field))
        return result_dict
    else:
        if isinstance(data, bytes):
            return data.decode('utf-8', errors='ignore').rstrip('\x00')
        if isinstance(data, float):
            return round(data, 4)
        return data

def trim_files_on_flashback(file_handlers, flashback_time):
    print(f"\n[Sleipnir] Flashback Detected: {flashback_time:.3f}s. Rolling back all files...")
    for h in file_handlers.values():
        h.close()

    for pid, name in PACKET_NAMES.items():
        filename = f"record_{name}.jsonl"
        if not os.path.exists(filename): continue
        
        temp_filename = f"{filename}.tmp"
        try:
            with open(filename, 'r', encoding='utf-8') as f_in, \
                 open(temp_filename, 'w', encoding='utf-8') as f_out:
                for line in f_in:
                    try:
                        record = json.loads(line)
                        if record.get('m_header', {}).get('m_sessionTime', 0) <= flashback_time:
                            f_out.write(line)
                    except json.JSONDecodeError:
                        continue
            os.replace(temp_filename, filename)
        except Exception as e:
            print(f"Error trimming {filename}: {e}")

    file_handlers.clear()
    for pid, name in PACKET_NAMES.items():
        file_handlers[pid] = open(f"record_{name}.jsonl", 'a', encoding='utf-8', buffering=65536)
    
    print("[Sleipnir] Rollback complete. Resuming...")
    return file_handlers

def init_storage():
    print("[Sleipnir] 초기화: 새로운 세션을 위해 이전 기록을 정리합니다.")
    for name in PACKET_NAMES.values():
        filename = f"record_{name}.jsonl"
        if os.path.exists(filename):
            try:
                os.remove(filename)
            except PermissionError:
                pass

def receiver_thread():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((UDP_IP, UDP_PORT))
    sock.settimeout(1.0)
    while is_running:
        try:
            data, _ = sock.recvfrom(2048)
            data_queue.put_nowait(data)
        except (socket.timeout, queue.Full):
            continue
        except Exception:
            break
    sock.close()

def processor_thread():
    init_storage()
    file_handlers = {pid: open(f"record_{name}.jsonl", 'a', encoding='utf-8', buffering=65536) 
                     for pid, name in PACKET_NAMES.items()}
    
    packet_count = 0
    last_flush_time = time.time()

    while is_running or not data_queue.empty():
        try:
            data = data_queue.get(timeout=1.0)
        except queue.Empty:
            continue

        if len(data) < 29: 
            continue

        header = PacketHeader.from_buffer_copy(data)
        packet_id = header.m_packetId
        packet_dict = None

        if packet_id == 3: # Event
            if len(data) == ctypes.sizeof(PacketEventData):
                packet = PacketEventData.from_buffer_copy(data)
                event_code = bytes(packet.m_eventStringCode).replace(b'\x00', b'')
                
                target_attr = EVENT_MAP.get(event_code)
                details = {}
                if target_attr:
                    details = ctypes_to_dict(getattr(packet.m_eventDetails, target_attr))
                
                if event_code == b'FLBK':
                    exact_target_time = details.get('flashbackSessionTime', 0)
                    if exact_target_time > 0:
                        file_handlers = trim_files_on_flashback(file_handlers, exact_target_time)

                packet_dict = {
                    "m_header": ctypes_to_dict(packet.m_header),
                    "event_info": {
                        "event_code": event_code.decode('utf-8', errors='ignore'),
                        "details": details
                    }
                }

        elif packet_id in PACKET_CLASSES:
            packet_class = PACKET_CLASSES[packet_id]
            if len(data) == ctypes.sizeof(packet_class):
                packet_dict = ctypes_to_dict(packet_class.from_buffer_copy(data))

        if packet_dict and packet_id in file_handlers:
            file_handlers[packet_id].write(json.dumps(packet_dict, ensure_ascii=False) + '\n')
            packet_count += 1
            
            if time.time() - last_flush_time > 5.0:
                for h in file_handlers.values(): h.flush()
                last_flush_time = time.time()

            if packet_id == 2: # Lap Data
                p_idx = header.m_playerCarIndex
                if p_idx < len(packet_dict['m_lapData']):
                    my_lap = packet_dict['m_lapData'][p_idx]
                    print(f"\r[Sleipnir] Lap: {my_lap['m_currentLapNum']} | Packets: {packet_count} | Queue: {data_queue.qsize()}", end="")

    for h in file_handlers.values():
        h.close()

def run_recorder():
    global is_running
    t_recv = threading.Thread(target=receiver_thread)
    t_proc = threading.Thread(target=processor_thread)
    t_recv.start()
    t_proc.start()
    
    print(f"📡 Sleipnir Recorder 시작 ({UDP_IP}:{UDP_PORT})")
    try:
        while True:
            time.sleep(0.1)
    except KeyboardInterrupt:
        print("\n[Sleipnir] 종료 중... 모든 데이터를 저장합니다.")
        is_running = False
        t_recv.join()
        t_proc.join()
        print("[Sleipnir] 기록이 안전하게 종료되었습니다.")

if __name__ == "__main__":
    run_recorder()
