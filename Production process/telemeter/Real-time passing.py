import socket
import ctypes
import json
import time
from f1_pasing import * # 파싱용 구조체들이 정의되어 있어야 합니다.

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

def ctypes_to_dict(data):
    """ctypes 구조체를 JSON으로 변환 가능한 딕셔너리로 변환"""
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

def run_recorder():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((UDP_IP, UDP_PORT))
    
    file_handlers = {}
    packet_count = 0
    last_flush_time = time.time()

    print(f"📡 F1 UDP Recorder 시작 ({UDP_IP}:{UDP_PORT})")
    print("기록을 중단하려면 터미널을 종료하거나 Ctrl+C를 누르세요.")

    try:
        while True:
            data, _ = sock.recvfrom(2048)
            if len(data) < ctypes.sizeof(PacketHeader):
                continue

            header = PacketHeader.from_buffer_copy(data)
            packet_id = header.m_packetId
            packet_dict = None
            current_packet_obj = None # 화면 출력용 객체 저장

            # 1. 파일 핸들러 관리
            if packet_id not in file_handlers and packet_id in PACKET_NAMES:
                filename = f"record_{PACKET_NAMES[packet_id]}.jsonl"
                file_handlers[packet_id] = open(filename, 'a', encoding='utf-8', buffering=1)

            # 2. 패킷 파싱
            if packet_id == 3:  # Event
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
                                "event_code": event_code.decode('utf-8'),
                                "details": ctypes_to_dict(event_detail)
                            }
                        }
            elif packet_id in PACKET_CLASSES:
                packet_class = PACKET_CLASSES[packet_id]
                if len(data) == ctypes.sizeof(packet_class):
                    current_packet_obj = packet_class.from_buffer_copy(data)
                    packet_dict = ctypes_to_dict(current_packet_obj)

            # 3. 저장 및 출력
            if packet_dict is not None and packet_id in file_handlers:
                file_handlers[packet_id].write(json.dumps(packet_dict, ensure_ascii=False) + '\n')
                packet_count += 1
                
                # 5초 간격 강제 쓰기 (안전장치)
                if time.time() - last_flush_time > 5.0:
                    for h in file_handlers.values():
                        h.flush()
                    last_flush_time = time.time()

                # 화면 UI 출력 (Lap Data 기준)
                if packet_id == 2 and current_packet_obj:
                    p_idx = header.m_playerCarIndex
                    my_lap = current_packet_obj.m_lapData[p_idx]
                    print(f"\r🏎️  Lap: {my_lap.m_currentLapNum} | Packets: {packet_count} | Event: {packet_id}", end="")

    except KeyboardInterrupt:
        print("\n🛑 사용자에 의해 중단되었습니다.")
    finally:
        for h in file_handlers.values():
            h.close()
        print("\n✅ 모든 파일이 안전하게 저장 및 종료되었습니다.")

if __name__ == "__main__":
    run_recorder()