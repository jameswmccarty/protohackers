import socket
import sys
import json
import math
from _thread import start_new_thread
import threading

lock = threading.Lock()

def print_useage():
    print("ProtoHackers PrimeTime server.")
    print("Useage:")
    print("python prime_time.py <<PORT>>")
    print("i.e. python prime_time.py 5900")
    exit()

def is_prime(a):
    if a <= 1:
        return False
    if not any(a%i==0 for i in range(2, int(math.sqrt(a)+1))):
        return True
    return False

def recv_message(c):
    msg = b''
    while True:
        part = c.recv(4096)
        if not part:
            break
        msg += part
        if msg.decode()[-1] == '\n':
            break
    return msg

def is_well_formed(msg):
    try:
        msg = json.loads(msg)
        print("Now loaded", msg)
    except json.JSONDecodeError as e:
        print("Invalid JSON syntax", e, msg)
        return False
    if "method" not in msg or "number" not in msg:
        return False
    if msg["method"] != "isPrime":
        return False
    try:
        num = int(msg["number"])
    except ValueError as e:
        return False
    except TypeError as e:
        return False
    if type(msg["number"]) == type(True):
        return False
    if type(msg["number"]) == type("string"):
        return False
    return True

def manage_session(c, addr):
    json_shell = dict()
    json_shell["method"] = "isPrime"
    json_shell["prime"] = None
    while True:
        msg = recv_message(c)
        print("Got message:", msg)
        if not msg:
            c.close()
            break
        for m in msg.decode().split('\n'):
            if is_well_formed(m):
                m = json.loads(m)
                num = int(m["number"])
                json_shell["prime"] = is_prime(num)
                response = json.dumps(json_shell).encode()
                response += b'\n'
                print("Response", response)
                c.send(response)
            elif len(m) > 0:
                c.send(b'invalid')
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
       #lock.acquire()
       start_new_thread(manage_session, (c, addr))
