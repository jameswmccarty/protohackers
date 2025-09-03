import socket
import sys
from insecure_sockets_layer import InsecureSocketLayer
import time

if __name__ == "__main__":

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(("localhost", 5900))
    spec = InsecureSocketLayer.generate_random_cipher_spec()
    isl = InsecureSocketLayer(spec)
    s.send(spec)

    msg = b'3x rat,2x cat,1x doggo choggo\n'

    msg = isl.encode(msg)
    print("raw msg", msg)
    time.sleep(1)
    s.send(msg)
    response = s.recv(1000)
    print(isl.decode(response))
