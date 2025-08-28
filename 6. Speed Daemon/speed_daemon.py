import socket
import sys
from threading import Thread, Lock
import queue
import atexit
import string
from socketserver import BaseRequestHandler, ThreadingTCPServer
from collections import defaultdict
import signal
import time

def print_useage():
    print("ProtoHackers Speed Daemon server.")
    print("Useage:")
    print("python speed_daemon.py <<PORT>>")
    print("i.e. python speed_daemon.py 5900")
    exit()

def consume_u8(buf):
    if len(buf) < 1:
        return None, 0, buf
    u8 = int.from_bytes(buf[0:1], 'big', signed=False)
    return u8, 1, buf[1:]

def consume_u16(buf):
    if len(buf) < 2:
        return None, 0, buf
    u16 = int.from_bytes(buf[0:2], 'big', signed=False)
    return u16, 2, buf[2:]

def consume_u32(buf):
    if len(buf) < 4:
        return None, 0, buf
    u16 = int.from_bytes(buf[0:4], 'big', signed=False)
    return u16, 4, buf[4:]

def consume_str(buf):
    str_len, consumed, buf = consume_u8(buf)
    if str_len == None:
        return None, 0, buf
    if len(buf) < str_len:
        return None, 0, buf
    data = buf[0:str_len]
    if data:
        data = data.decode()
    return data, consumed+str_len, buf[str_len:]

heartbeats = queue.Queue()

def handle_heartbeat():
    global heartbeats
    while True:
        deadline, conn, interval = heartbeats.get()
        if time.time() >= deadline:
            # send heartbeat to client
            if conn:
                try:
                    conn.send(b'\x41') # Heartbeat (Server->Client)
                    heartbeats.put((time.time()+interval, conn, interval))
                except:
                    pass
            heartbeats.task_done()
        else:
            heartbeats.put((deadline, conn, interval))
            heartbeats.task_done()

def convert_data_to_ticket(plate, road, mile1, ts1, mile2, ts2, speed):
    msg = b''
    msg += b'\x21' # Ticket
    msg += len(plate).to_bytes(1, 'big')
    msg += plate.encode()
    msg += road.to_bytes(2, 'big')
    if ts1 < ts2:
        msg += mile1.to_bytes(2, 'big')
        msg += ts1.to_bytes(4, 'big')
        msg += mile2.to_bytes(2, 'big')
        msg += ts2.to_bytes(4, 'big')
    else:
        msg += mile2.to_bytes(2, 'big')
        msg += ts2.to_bytes(4, 'big')
        msg += mile1.to_bytes(2, 'big')
        msg += ts1.to_bytes(4, 'big')
    msg += int(speed).to_bytes(2, 'big')
    return msg

speed_points = queue.Queue()

def speed_measurement_thread():
    global speed_points, tickets_to_send
    while True:
        plate, road, t1, m1, t2, m2, lim = speed_points.get()
        speed_points.task_done()
        speed = abs(m1 - m2) / (abs(t1 - t2) / 3600)
        speed *= 100
        if int(speed) > lim:
            msg = convert_data_to_ticket(plate, road, m1, t1, m2, t2, speed)
            # only one ticket per plate per day
            tickets_to_send.put((road, msg, plate, t1, t2))

tickets_to_send = queue.Queue()

def ticket_sender_thread():
    global tickets_to_send
    while True:
        road, msg, plate, t1, t2 = tickets_to_send.get()
        tickets_to_send.task_done()
        if plate not in SpeedServer.plates_ticketed_by_day[t1 // 86400] and plate not in SpeedServer.plates_ticketed_by_day[t2 // 86400]:
            if road not in SpeedServer.dispatchers_by_road or len(SpeedServer.dispatchers_by_road[road]) == 0:
                tickets_to_send.put((road, msg, plate, t1, t2))
            else:
                avail_dispatcher = list(SpeedServer.dispatchers_by_road[road])[0]
                avail_dispatcher.issue_ticket(msg)
                SpeedServer.plates_ticketed_by_day[t1 // 86400].add(plate)
                SpeedServer.plates_ticketed_by_day[t2 // 86400].add(plate)

class SpeedServer(BaseRequestHandler):
    """
    SpeedServer instances are created by ThreadingTCPServer.
    Each SpeedServer instance handles a single client conversation.
    """
    global heartbeats, speed_points
    valid_client_msg_types = {0x20:"Plate", 0x40:"Heartbeat", 0x80:"Camera", 0x81:"Dispatcher"}
    dispatchers_by_road = defaultdict(set)
    plates_ticketed_by_day = defaultdict(set)
    msgs_awaiting_dispatcher_by_road = defaultdict(list)
    observations_by_plate = defaultdict(dict)

    def handle_error(self):
        self.request.send(b'\x10' + b'\x03' + b'bad')
        self.request.close()
        raise RuntimeError("client sent illegal msg")

    def process_next_msg(self):
        while True:
            part = self.request.recv(1024)
            if not part:
                raise RuntimeError("socket connection broken")
            self.buf += part
            full_msg_on_buf = True
            while self.buf and full_msg_on_buf:
                if self.buf[0] not in self.valid_client_msg_types:
                    print("Invalid msg code", self.buf)
                    self.handle_error()
                else:
                    total_consumed = 0
                    working_buf = self.buf[:]
                    msg_type, delta, working_buf = consume_u8(working_buf)
                    total_consumed += delta
                    if msg_type == 0x20: # Plate (Client->Server)
                        if not self.is_camera:
                            self.handle_error()
                        plate, delta, working_buf = consume_str(working_buf)
                        if not plate:
                            full_msg_on_buf = False
                            break
                        total_consumed += delta
                        obs_time, delta, working_buf = consume_u32(working_buf)
                        if not obs_time:
                            full_msg_on_buf = False
                            break
                        total_consumed += delta
                        self.check_for_speeding(plate, obs_time)
                        self.buf = self.buf[total_consumed:] # message parsed, can clear
                        yield msg_type
                    elif msg_type == 0x40: # WantHeartbeat (Client->Server)
                        if self.heartbeat: # It is an error for a client to send multiple WantHeartbeat messages on a single connection.
                            self.handle_error()
                        interval, delta, working_buf = consume_u32(working_buf)
                        if interval == None:
                            full_msg_on_buf = False
                            break
                        total_consumed += delta
                        if interval >= 0:
                            self.heartbeat = float(interval / 10.0)
                        if self.heartbeat > 0:
                            heartbeats.put((time.time()+self.heartbeat, self.request, self.heartbeat))
                        print("Requested heartbeat at", self.heartbeat)
                        self.buf = self.buf[total_consumed:] # message parsed, can clear
                        yield msg_type
                    elif msg_type == 0x80: # IAmCamera (Client->Server)
                        if self.is_dispatcher or self.is_camera: # It is an error for a client that has already identified itself as either a camera or a ticket dispatcher to send an IAmCamera message.
                            self.handle_error()
                        road, delta, working_buf = consume_u16(working_buf)
                        if road == None:
                            full_msg_on_buf = False
                            break
                        total_consumed += delta
                        mile, delta, working_buf = consume_u16(working_buf)
                        if mile == None:
                            full_msg_on_buf = False
                            break
                        total_consumed += delta
                        limit, delta, working_buf = consume_u16(working_buf)
                        if limit == None:
                            full_msg_on_buf = False
                            break
                        total_consumed += delta
                        self.is_camera = True
                        print("ID as camera", road, mile, limit)
                        self.camera_loc = (road, mile)
                        self.speed_limit = limit * 100
                        self.buf = self.buf[total_consumed:] # message parsed, can clear
                        yield msg_type
                    elif msg_type == 0x81: # IAmDispatcher (Client->Server)
                        if self.is_camera: # It is an error for a client that has already identified itself as either a camera or a ticket dispatcher to send an IAmDispatcher message.
                            self.handle_error()
                        if self.is_dispatcher:
                            self.handle_error()
                        num_roads, delta, working_buf = consume_u8(working_buf)
                        if num_roads == None:
                            full_msg_on_buf = False
                            break
                        total_consumed += delta
                        if len(working_buf) < num_roads * 2:
                            full_msg_on_buf = False
                            break
                        roads = []
                        for r in range(num_roads):
                            road, delta, working_buf = consume_u16(working_buf)
                            total_consumed += delta
                            roads.append(road)
                        self.is_dispatcher = True
                        print("ID as dispatcher for roads: ", roads)
                        self.roads = roads
                        for road in roads:
                            SpeedServer.dispatchers_by_road[road].add(self)
                        self.buf = self.buf[total_consumed:] # message parsed, can clear
                        yield msg_type

    def issue_ticket(self, msg):
        self.request.send(msg)

    def check_for_speeding(self, plate, obs_time):
        print("Checking plate", plate)
        this_road, this_mile = self.camera_loc
        if this_road not in SpeedServer.observations_by_plate[plate]:
            SpeedServer.observations_by_plate[plate][this_road] = [ (obs_time, this_mile) ]
            return
        for time, mile in SpeedServer.observations_by_plate[plate][this_road]:
            speed_points.put((plate, this_road, obs_time, this_mile, time, mile, self.speed_limit))
        SpeedServer.observations_by_plate[plate][this_road].append((obs_time, this_mile))

    def handle(self):
        self.type_announced = False
        self.is_camera = False
        self.camera_loc = None
        self.speed_limit = None
        self.is_dispatcher = False
        self.roads = None
        self.heartbeat = None
        self.buf = b''

        try:
            while True:
                # wait for message from chat client or chat room proxy
                for msg in self.process_next_msg():
                    print("Processed msg", self.valid_client_msg_types[msg])
        except ConnectionError: # client terminated abruptly
            pass
        except RuntimeError: # client terminated abruptly
            pass
        finally:
            if self.is_dispatcher:
                for road in self.roads:
                    SpeedServer.dispatchers_by_road[road].discard(self)
            if self.request:
                self.request.close()

    def shutdown(self):
        if self.request:
            self.request.close()

@atexit.register
def shutdown_chat_server():
    print('shutting down server...')
    server.shutdown()

if __name__ == "__main__":

    if len(sys.argv) != 2:
        print_useage()

    port = int(sys.argv[1])

    try:
        # Allow server to rebind to previously used port number.
        for _ in range(2):
            ht = Thread(target=handle_heartbeat)
            ht.start()
        for _ in range(3):
            st = Thread(target=speed_measurement_thread)
            st.start()
        for _ in range(1):
            tt = Thread(target=ticket_sender_thread)
            tt.start()
        ThreadingTCPServer.allow_reuse_address = True
        server = ThreadingTCPServer(('', port), SpeedServer)
        print(f'Starting SpeedServer at port {port}...')
        server.serve_forever()
    except KeyboardInterrupt:  # catch Ctrl-c
        pass  # Interpreter will call atexit handler
