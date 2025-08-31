import socket
import sys
import time
from threading import Thread, Lock
import queue

def print_useage():
    print("ProtoHackers Line Reversal server.")
    print("Useage:")
    print("python line_reversal.py <<PORT>>")
    print("i.e. python line_reversal.py 5900")
    exit()

sessions = dict()

class Session():

    class_xmit_lock = Lock()

    def __init__(self, session_number, socket, addr):
        self.session_outbound = socket
        self.session_number = session_number
        self.is_open = False
        self.recv_counter = 0
        self.send_counter = 0
        self.max_send_ack = 0
        self.addr = addr # UDP
        self.buf = b''
        self.xmit_buf = b''
        self.xmit_queue = queue.Queue()
        self.xmit_thread = Thread(target=self.udp_xmit_thread)

    def check_for_lines(self):
        while b'\n' in self.buf:
            msg_end_idx = self.buf.index(b'\n')
            msg = self.buf[0:msg_end_idx]
            self.buf = self.buf[msg_end_idx+1:]
            msg = msg[::-1]
            start_idx = len(self.xmit_buf)
            self.xmit_buf += msg
            self.xmit_buf += b'\n'
            self.send_data_from_index(start_idx)

    def send_data_from_index(self, idx):
        self.xmit_queue.put((idx, len(self.xmit_buf)))

    def udp_xmit_thread(self):
        retry_counter = 0
        while self.is_open:
            starting_ack_counter = self.max_send_ack
            lo, hi = self.xmit_queue.get()
            frames = self.xmit_buf[lo:hi]
            base = lo
            while frames:
                frame = frames[0:800]
                frames = frames[800:]
                msg_len = len(frame)
                frame = escape_data(frame)
                if msg_len > 0:
                    packet = self.gen_data(frame, base)
                    self.transmit_packet(packet)
                    base += msg_len
                    self.send_counter = max(self.send_counter, base)
            self.xmit_queue.task_done()
            time.sleep(3)
            if self.max_send_ack == starting_ack_counter:
                self.send_data_from_index(starting_ack_counter)
                retry_counter += 1
            else:
                retry_counter = 0
            if retry_counter == 20:
                return

    def gen_data(self, payload, data_start_idx):
        msg = '/data/' + str(self.session_number) + '/' + str(data_start_idx) + '/'
        msg = msg.encode()
        msg += payload
        msg += b'/'
        return msg

    def gen_ack(self, count=0):
        msg = '/ack/' + str(self.session_number) + '/' + str(count) + '/'
        return msg.encode()

    def gen_close(self):
        msg = '/close/' + str(self.session_number) + '/'
        return msg.encode()

    def transmit_packet(self, packet):
        print("Transmitting", packet)
        with Session.class_xmit_lock:
            self.session_outbound.sendto(packet, self.addr)

    def consume_packet(self, packet):
        print("Session", self.session_number, "evaluating", packet)
        if packet[0] == b'connect':
            if not self.is_open:
                self.is_open = True
                self.xmit_thread.start()
            response = self.gen_ack()
            self.transmit_packet(response)
        elif packet[0] == b'data':
            if not self.is_open:
                response = self.gen_close()
                self.transmit_packet(response)
                return
            pos = int(packet[2])
            print("Now pos", pos)
            if pos < self.recv_counter:
                response = self.gen_ack(count = self.recv_counter)
                self.transmit_packet(response)
            elif pos == self.recv_counter:
                self.buf += packet[3]
                self.recv_counter += len(packet[3])
                response = self.gen_ack(count = self.recv_counter)
                self.transmit_packet(response)
                self.check_for_lines()
        elif packet[0] == b'ack':
            if not self.is_open:
                response = self.gen_close()
                self.transmit_packet(response)
                return
            length = int(packet[2])
            if length < self.max_send_ack:
                return
            if length > self.send_counter:
                response = self.gen_close()
                self.transmit_packet(response)
                return
            if length == self.send_counter:
                self.max_send_ack = self.send_counter
                return
            if length < self.send_counter:
                self.send_data_from_index(length)
        elif packet[0] == b'close':
            response = self.gen_close()
            self.is_open = False
            self.transmit_packet(response)

def valid_num(num):
    MAX_NUMBER = 2147483648
    if any(x not in b'0123456789' for x in num):
        return False
    value = int(num)
    if value < 0 or value > MAX_NUMBER:
        return False
    return True

def found_unescaped_fields(bytestream):
    for idx, char in enumerate(bytestream):
        if char == 47 and bytestream[idx - 1] != 92:
            return True
    return False

def unescape_data(bytestream):
    out = b''
    while bytestream:
        if bytestream[0:2] == chr(92).encode() + chr(92).encode():
            out += chr(92).encode()
            bytestream = bytestream[2:]
        elif bytestream[0:2] == chr(92).encode() + chr(47).encode():
            out += chr(47).encode()
            bytestream = bytestream[2:]
        else:
            out += bytestream[0:1]
            bytestream = bytestream[1:]
    return out

def escape_data(bytestream):
    out = b''
    while bytestream:
        if bytestream[0] == 92:
            out += chr(92).encode() + chr(92).encode()
        elif bytestream[0] == 47:
            out += chr(92).encode() + chr(47).encode()
        else:
            out += bytestream[0:1]
        bytestream = bytestream[1:]
    return out

'''
When the server receives an illegal packet it must silently ignore the packet instead of interpreting it as LRCP.
'''
def validate_and_pass_packet(packet, socket, addr):

    global sessions

    # Each message starts and ends with a forward slash character

    if len(packet) <= 1:
        return

    if not packet[0] == 47 or not packet[-1] == 47:
        return

    raw_packet = packet[:]
    packet = packet.split(b'/') # break fields by forward slash

    packet = packet[1:-1] # discard leading / trailing empty element
    print("Trimmed split packet", packet)

    if len(packet) < 2:
        return

    # see if message type is valid
    if packet[0] not in [b'connect', b'ack', b'data', b'close']:
        return

    # check messages by type
    if packet[0] == b'connect':
        if len(packet) != 2:
            return
        if not valid_num(packet[1]):
            return
        # message format was valid
        session = int(packet[1])
        if session not in sessions:
            sessions[session] = Session(session, socket, addr)
        sessions[session].consume_packet(packet)

    if packet[0] == b'data':
        if len(packet) < 4:
            return
        if not valid_num(packet[1]) or not valid_num(packet[2]):
            return
        # message format was valid
        session = int(packet[1])
        raw_data = raw_packet[len(packet[0]) + len(packet[1]) + len(packet[2]) + 4: -1]
        if found_unescaped_fields(raw_data):
            return
        data = unescape_data(raw_data)
        print("New data", data)
        if session not in sessions:
            sessions[session] = Session(session, socket, addr)
        sessions[session].consume_packet([packet[0], packet[1], packet[2], data])

    if packet[0] == b'ack':
        if len(packet) != 3:
            return
        if not valid_num(packet[1]) or not valid_num(packet[2]):
            return
        # message format was valid
        session = int(packet[1])
        if session not in sessions:
            sessions[session] = Session(session, socket, addr)
        sessions[session].consume_packet(packet)

    if packet[0] == b'close':
        if len(packet) != 2:
            return
        if not valid_num(packet[1]):
            return
        # message format was valid
        session = int(packet[1])
        if session not in sessions:
            sessions[session] = Session(session, socket, addr)
        sessions[session].consume_packet(packet)

if __name__ == "__main__":

    if len(sys.argv) != 2:
        print_useage()

    port = int(sys.argv[1])

    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    except socket.error as err:
        print("Socket creation failed with error.")
        print(err)
        exit()

    try:
        s.bind(('', port))
        print("Socket bound to port:",)
        print(port)
    except socket.error as err:
        print("Error listening.")
        print(err)
        exit()

    # loop forever
    while True:
       msg, addr = s.recvfrom(2500)
       print("Got ", msg, "From", addr)
       validate_and_pass_packet(msg, s, addr)

