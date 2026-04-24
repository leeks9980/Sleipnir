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
    b'OVTK': 'Overtake', b'SCAR': 'SafetyCar', b'COLL': 'Collision'
}

PACKET_NAMES = {
    0: "motion", 1: "session", 2: "lap", 3: "event", 4: "participants",
    5: "setup", 6: "telemetry", 7: "status", 8: "final_classification",
    9: "lobby", 10: "damage", 11: "session_history", 12: "tyre_sets",
    13: "motion_ex", 14: "time_trial", 15: "lap_positions"
}

data_queue = queue.Queue(maxsize=50000)
is_running = True

def ctypes_to_dict(data):
    if isinstance(data, ctypes.Array):
        return [ctypes_to_dict(i) for i in data]
    elif hasattr(data, "_fields_"):
        result = {}
        for field, _ in data._fields_:
            result[field] = ctypes_to_dict(getattr(data, field))
        return result
    else:
        if isinstance(data, bytes):
            return data.decode('utf-8', errors='ignore').rstrip('\x00')
        if isinstance(data, float):
            return round(data, 4)
        return data

def trim_files_on_flashback(file_handlers, flashback_time):
    print(f"\nFlashback Detected: {flashback_time:.3f}s. Rolling back all files...")
    for h in file_handlers.values():
        h.close()

    for packet_id, name in PACKET_NAMES.items():
        filename = f"record_{name}.jsonl"
        if not os.path.exists(filename):
            continue
        
        temp_filename = f"{filename}.tmp"
        try:
            with open(filename, 'r', encoding='utf-8') as f_in, \
                 open(temp_filename, 'w', encoding='utf-8') as f_out:
                for line in f_in:
                    try:
                        record = json.loads(line)
                        record_time = record.get('m_header', {}).get('m_sessionTime', 0)
                        if record_time <= flashback_time:
                            f_out.write(line)
                    except json.JSONDecodeError:
                        continue
            os.replace(temp_filename, filename)
        except Exception:
            pass

    file_handlers.clear()
    for packet_id in list(PACKET_NAMES.keys()):
        filename = f"record_{PACKET_NAMES[packet_id]}.jsonl"
        if os.path.exists(filename):
            file_handlers[packet_id] = open(filename, 'a', encoding='utf-8', buffering=65536)

    print("Rollback complete. Resuming processing...")
    return file_handlers

def receiver_thread():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((UDP_IP, UDP_PORT))
    sock.settimeout(1.0)
    
    while is_running:
        try:
            data, _ = sock.recvfrom(2048)
            if len(data) >= ctypes.sizeof(PacketHeader):
                try:
                    data_queue.put_nowait(data)
                except queue.Full:
                    pass
        except socket.timeout:
            continue
        except Exception:
            break
    sock.close()

def processor_thread():
    file_handlers = {}
    packet_count = 0
    last_flush_time = time.time()
    highest_session_time = 0.0

    for pid, name in PACKET_NAMES.items():
        filename = f"record_{name}.jsonl"
        file_handlers[pid] = open(filename, 'a', encoding='utf-8', buffering=65536)

    while is_running or not data_queue.empty():
        try:
            data = data_queue.get(timeout=1.0)
        except queue.Empty:
            continue

        header = PacketHeader.from_buffer_copy(data)
        packet_id = header.m_packetId
        current_time = header.m_sessionTime
        packet_dict = None
        current_packet_obj = None

        if highest_session_time > 0 and current_time < highest_session_time - 0.5:
            file_handlers = trim_files_on_flashback(file_handlers, current_time)
            highest_session_time = current_time
        else:
            if current_time > highest_session_time:
                highest_session_time = current_time

        if packet_id == 3:
            if len(data) == ctypes.sizeof(PacketEventData):
                packet = PacketEventData.from_buffer_copy(data)
                event_code = bytes(packet.m_eventStringCode).replace(b'\x00', b'')
                if event_code in EVENT_MAP:
                    target_class_name = EVENT_MAP[event_code]
                    event_detail = getattr(packet.m_eventDetails, target_class_name)
                    packet_dict = {
                        "m_header": ctypes_to_dict(packet.m_header),
                        "event_info": {
                            "type": "EVENT",
                            "event_code": event_code.decode('utf-8', errors='ignore'),
                            "details": ctypes_to_dict(event_detail)
                        }
                    }
        elif packet_id in PACKET_CLASSES:
            packet_class = PACKET_CLASSES[packet_id]
            if len(data) == ctypes.sizeof(packet_class):
                current_packet_obj = packet_class.from_buffer_copy(data)
                packet_dict = ctypes_to_dict(current_packet_obj)

        if packet_dict is not None and packet_id in file_handlers:
            file_handlers[packet_id].write(json.dumps(packet_dict, ensure_ascii=False) + '\n')
            packet_count += 1
            
            if time.time() - last_flush_time > 5.0:
                for h in file_handlers.values():
                    h.flush()
                last_flush_time = time.time()

            if packet_id == 2 and current_packet_obj:
                p_idx = header.m_playerCarIndex
                my_lap = current_packet_obj.m_lapData[p_idx]
                print(f"\rLap: {my_lap.m_currentLapNum} | Packets: {packet_count} | Queue: {data_queue.qsize()}", end="")

    for h in file_handlers.values():
        h.close()

def run_recorder():
    global is_running
    
    t_recv = threading.Thread(target=receiver_thread)
    t_proc = threading.Thread(target=processor_thread)
    
    t_recv.start()
    t_proc.start()
    
    try:
        while True:
            time.sleep(0.1)
    except KeyboardInterrupt:
        is_running = False
        t_recv.join()
        t_proc.join()

if __name__ == "__main__":
    run_recorder()
