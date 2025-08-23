import socket
import sys

def print_useage():
    print("A Python Echo server.")
    print("Useage:")
    print("python echo_server.py <<PORT>>")
    print("i.e. python echo_server.py 8007")
    exit()

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
       print("Connection from", addr)
       msg = b''
       while True:
           part = c.recv(4096)
           if not part:
               break
           msg += part
       print("Got message: ", str(msg))
       c.send(msg) # echo
       c.close()
