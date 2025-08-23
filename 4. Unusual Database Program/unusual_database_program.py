import socket
import sys

db = dict()

def print_useage():
    print("ProtoHackers Unusual Database Program server.")
    print("Useage:")
    print("python unusual_database_program.py <<PORT>>")
    print("i.e. python unusual_database_program.py 5900")
    exit()

def db_add(key, value):
    global db
    if key == b'version':
        return
    db[key] = value

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

    db[b'version'] = b'FooBar 1.0'

    # loop forever
    while True:
       msg, addr = s.recvfrom(1000)
       print("Got ", msg, "From", addr)
       if b'=' in msg: # insert
           key = msg[0:msg.index(b'=')]
           value = msg[msg.index(b'=')+1:]
           db_add(key, value)
       else: # retrieve
           if msg in db:
               response = msg + b'=' + db[msg]
               print("Sending response", response)
               s.sendto(response, addr)
