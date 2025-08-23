import socket
import sys
import math
import struct
from _thread import start_new_thread
import threading


def print_useage():
    print("ProtoHackers Means to an End server.")
    print("Useage:")
    print("python means_to_an_end.py <<PORT>>")
    print("i.e. python means_to_an_end.py 5900")
    exit()


def recv_message(c):
    msg = b''
    while True:
        part = c.recv(9)
        if not part:
            break
        msg += part
        print("Got part", part)
        print("Len msg", len(msg))
        if len(msg) >= 9:
            yield msg[0:9]
            msg = msg[9:]

def manage_session(c, addr):
    db = dict()
    buf = b''
    while True:
        for msg in recv_message(c):
            print("Message", msg)
            if not msg or len(msg) != 9:
                c.close()
                break
            if msg[0] == 73: # 'I'
                timestamp = int.from_bytes(msg[1:5], 'big', signed=True)
                value     = int.from_bytes(msg[5:], 'big', signed=True)
                if timestamp in db: #undefined
                    c.close()
                    return
                db[timestamp] = value
            elif msg[0] == 81: # 'Q'
                total = 0
                entries = 0
                mintime = int.from_bytes(msg[1:5], 'big', signed=True)
                maxtime = int.from_bytes(msg[5:], 'big', signed=True)
                print("Query time:",mintime, maxtime)
                for k, v in db.items():
                    if mintime <= k <= maxtime:
                        total += v
                        entries += 1
                print("Total", total, "Entries", entries)
                if entries == 0 or maxtime < mintime:
                    total = 0
                    entries = 1
                total //= entries
                print("Final total was", total)
                try:
                    result = struct.pack(">l", total)
                    c.send(result)
                except struct.error as e:
                    print("Struct error", e)
                    c.close()
                    return
            else: # invalid
                print("Got invalid message.")
                c.close()
                return


if __name__ == "__main__":

    if len(sys.argv) != 2:
        print_useage()

    port = int(sys.argv[1])

    try:
        s = socket.socket()
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

    s.listen(5)
    print("Socket is listening...")

    # loop forever
    while True:
       c, addr = s.accept()
       print("Connection from", addr, c)
       start_new_thread(manage_session, (c, addr))
