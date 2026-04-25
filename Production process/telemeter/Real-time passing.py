import socket
import ctypes
import json
import time
import os
import threading
import queue
from f1_pasing import * # 기존 구조체 정의 파일 임포트

UDP_IP = "127.0.0.1"
UDP_PORT = 20777

# 패킷 클래스 및 이름 매핑
PACKET_CLASSES = {
    0: PacketMotionData, 1: PacketSessionData, 2: PacketLapData,
    4: PacketParticipantsData, 5: PacketCarSetupData, 6: PacketCarTelemetryData,
    7: PacketCarStatusData, 8: PacketFinalClassificationData, 9: PacketLobbyInfoData,
    10: PacketCarDamageData, 11: PacketSessionHistoryData, 12: PacketTyreSetsData,
    13: PacketMotionExData, 14: PacketTimeTrialData, 15: PacketLapPositionsData
}

# [수정 1] 모든 이벤트를 수용할 수 있도록 확장된 EVENT_MAP
# 상세 데이터가 없는 이벤트도 키 값으로 존재해야 파싱 로직에서 누락되지 않습니다.
EVENT_MAP = {
    b'FTLP': 'FastestLap', b'RTMT': 'Retirement', b'DRSD': 'DRSDisabled',
    b'TMPT': 'TeamMateInPits', b'RCWN': 'RaceWinner', b'PENA': 'Penalty',
    b'SPTP': 'SpeedTrap', b'STLG': 'StartLights', b'DTSV': 'DriveThroughPenaltyServed',
    b'SGSV': 'StopGoPenaltyServed', b'FLBK': 'Flashback', b'BUTN': 'Buttons',
    b'OVTK': 'Overtake', b'SCAR': 'SafetyCar', b'COLL': 'Collision',
    b'RDFL': None, b'SSTA': None, b'SEND': None # 레드플래그 및 세션 상태 추가
}

PACKET_NAMES = {
    0: "motion", 1: "session", 2: "lap", 3: "event", 4: "participants",
    5: "setup", 6: "telemetry", 7: "status", 8: "final_classification",
    9: "lobby", 10: "damage", 11: "session_history", 12: "tyre_sets",
    13: "motion_ex", 14: "time_trial", 15: "lap_positions"
}

data_queue = queue.Queue(maxsize=50000)
is_running = True

# --- [수정 2] 배열 데이터 누락 오류를 완전히 해결한 변환 함수 ---
def ctypes_to_dict(data):
    """
    ctypes 구조체 및 배열을 파이썬 딕셔너리와 리스트로 완벽하게 변환합니다.
    """
    # 1. 배열 타입 처리 (배열 내부의 구조체까지 하나씩 순회)
    if isinstance(data, ctypes.Array):
        result_list = []
        for item in data:
            # 배열의 각 요소에 대해 재귀적으로 변환 수행
            result_list.append(ctypes_to_dict(item))
        return result_list
    
    # 2. 구조체 타입 처리
    elif hasattr(data, "_fields_"):
        result_dict = {}
        for field, _ in data._fields_:
            field_value = getattr(data, field)
            # 필드 값에 대해 재귀적으로 변환 수행
            result_dict[field] = ctypes_to_dict(field_value)
        return result_dict
    
    # 3. 기본 데이터 타입 및 바이트 처리
    else:
        if isinstance(data, bytes):
            return data.decode('utf-8', errors='ignore').rstrip('\x00')
        if isinstance(data, float):
            return round(data, 4)
        return data

# --- 플래시백 처리 로직 (생략 없이 유지) ---
def trim_files_on_flashback(file_handlers, flashback_time):
    print(f"\n[Sleipnir] Flashback Detected: {flashback_time:.3f}s. Rolling back...")
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
                    record = json.loads(line)
                    # 헤더의 세션 타임을 기준으로 데이터 보존
                    if record.get('m_header', {}).get('m_sessionTime', 0) <= flashback_time:
                        f_out.write(line)
            os.replace(temp_filename, filename)
        except Exception: pass

    # 파일 핸들러 재오픈
    file_handlers.clear()
    for pid, name in PACKET_NAMES.items():
        file_handlers[pid] = open(f"record_{name}.jsonl", 'a', encoding='utf-8', buffering=65536)
    return file_handlers

# --- 수신 및 처리 스레드 ---
def receiver_thread():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((UDP_IP, UDP_PORT))
    sock.settimeout(1.0)
    while is_running:
        try:
            data, _ = sock.recvfrom(2048)
            data_queue.put_nowait(data)
        except (socket.timeout, queue.Full): continue
        except Exception: break
    sock.close()

def processor_thread():
    # 파일 핸들러 초기화
    file_handlers = {pid: open(f"record_{name}.jsonl", 'a', encoding='utf-8', buffering=65536) 
                     for pid, name in PACKET_NAMES.items()}
    
    packet_count = 0
    last_flush_time = time.time()
    highest_session_time = 0.0
    last_participants_hash = None # 참가자 중복 체크용 (루프 외부 유지)

    while is_running or not data_queue.empty():
        try:
            data = data_queue.get(timeout=1.0)
        except queue.Empty: continue

        header = PacketHeader.from_buffer_copy(data)
        packet_id = header.m_packetId
        current_time = header.m_sessionTime
        packet_dict = None

        # 세션 시간 역전 시 플래시백 처리
        if highest_session_time > 0 and current_time < highest_session_time - 0.5:
            file_handlers = trim_files_on_flashback(file_handlers, current_time)
            highest_session_time = current_time
        else:
            highest_session_time = max(highest_session_time, current_time)

        # 1. 이벤트 패킷(ID 3) 처리 - 맵에 없어도 코드명은 기록하도록 보강
        if packet_id == 3:
            if len(data) == ctypes.sizeof(PacketEventData):
                packet = PacketEventData.from_buffer_copy(data)
                event_code = bytes(packet.m_eventStringCode).replace(b'\x00', b'')
                
                target_attr = EVENT_MAP.get(event_code)
                details = {}
                if target_attr: # 상세 구조체가 정의된 이벤트라면 데이터 추출
                    details = ctypes_to_dict(getattr(packet.m_eventDetails, target_attr))
                
                packet_dict = {
                    "m_header": ctypes_to_dict(packet.m_header),
                    "event_info": {
                        "event_code": event_code.decode('utf-8', errors='ignore'),
                        "details": details
                    }
                }

        # 2. 참가자 패킷(ID 4) 처리 - 해시를 이용한 중복 저장 방지
        elif packet_id == 4:
            current_hash = hash(data)
            if current_hash != last_participants_hash:
                packet_dict = ctypes_to_dict(PacketParticipantsData.from_buffer_copy(data))
                last_participants_hash = current_hash

        # 3. 그 외 일반 패킷 처리
        elif packet_id in PACKET_CLASSES:
            packet_class = PACKET_CLASSES[packet_id]
            if len(data) == ctypes.sizeof(packet_class):
                packet_dict = ctypes_to_dict(packet_class.from_buffer_copy(data))

        # 데이터 파일 저장
        if packet_dict and packet_id in file_handlers:
            file_handlers[packet_id].write(json.dumps(packet_dict, ensure_ascii=False) + '\n')
            packet_count += 1
            
            # 5초마다 파일 버퍼 강제 쓰기
            if time.time() - last_flush_time > 5.0:
                for h in file_handlers.values(): h.flush()
                last_flush_time = time.time()

            # 콘솔에 플레이어 랩 정보 출력 (상태 확인용)
            if packet_id == 2:
                p_idx = header.m_playerCarIndex
                my_lap = packet_dict['m_lapData'][p_idx]
                print(f"\r[Sleipnir] Lap: {my_lap['m_currentLapNum']} | Total Packets: {packet_count} | Queue: {data_queue.qsize()}", end="")

    # 종료 시 모든 파일 핸들러 닫기
    for h in file_handlers.values(): h.close()

def run_recorder():
    global is_running
    t_recv = threading.Thread(target=receiver_thread)
    t_proc = threading.Thread(target=processor_thread)
    t_recv.start(); t_proc.start()
    try:
        while True: time.sleep(0.1)
    except KeyboardInterrupt:
        print("\n[Sleipnir] Stopping recorder...")
        is_running = False
        t_recv.join(); t_proc.join()

if __name__ == "__main__":
    run_recorder()
