import socket
import sys
from threading import Lock
import atexit
import string
from socketserver import BaseRequestHandler, ThreadingTCPServer
from insecure_sockets_layer import InsecureSocketLayer


def print_useage():
    print("ProtoHackers Insecure Sockets Layer server.")
    print("Useage:")
    print("python insecure_sockets_layer.py <<PORT>>")
    print("i.e. python insecure_sockets_layer.py 5900")
    exit()

class ApplicationServer(BaseRequestHandler):
    """
    ApplicationServer instances are created by ThreadingTCPServer.
    Each ApplicationServer instance handles a single client conversation.
    """

    def manage_request(self, decoded_msg):
        request = decoded_msg.decode().strip().split(',')
        highest_idx = 0
        max_qty = -1
        for idx, entry in enumerate(request):
            qty = entry.split(' ')[0].strip('x')
            qty = int(qty)
            if qty > max_qty:
                max_qty = qty
                highest_idx = idx
        return request[highest_idx].encode() + b'\n'

    def get_next_msg(self):
        buf = b''
        while True:
            part = self.request.recv(5000)
            if not part:
                break
            decoded = self.ISL.decode(part)
            buf += decoded
            print("Buffer as built", buf)
            while b'\n' in buf:
                msg_end_idx = buf.index(b'\n')
                msg = buf[0:msg_end_idx]
                yield msg
                buf = buf[msg_end_idx+1:]
        return None

    def handle(self):
        self.ISL = None
        print('Got connection from', self.client_address)
        # First requirement is to get a cipher spec
        try:
            msg = self.request.recv(80)
            self.ISL = InsecureSocketLayer(msg)
        except RuntimeError as e: # client terminated abruptly or invalid spec
            self.request.close()
            print(e)
            return
        try:
            while True:
                for msg in self.get_next_msg():
                    if not msg:
                        break
                    print(msg)
                    response = self.manage_request(msg)
                    print("MSG response", response)
                    encoded = self.ISL.encode(response)
                    self.request.send(encoded)
        except ConnectionError: # client terminated abruptly
            pass
        except RuntimeError: # client terminated abruptly
            pass
        finally:
            # the client closed the connection, so we'll remove the client's
            # socket from the chat_sockets set
            if self.request:
                self.request.close()

    def shutdown(cls):
        if self.request:
            self.request.close()

@atexit.register
def shutdown_app_server():
    print('shutting down app server...')
    server.shutdown()

if __name__ == "__main__":

    if len(sys.argv) != 2:
        print_useage()

    port = int(sys.argv[1])

    try:
        # Allow server to rebind to previously used port number.
        ThreadingTCPServer.allow_reuse_address = True
        server = ThreadingTCPServer(('', port), ApplicationServer)
        print(f'Starting ApplicationServer at port {port}...')
        server.serve_forever()
    except KeyboardInterrupt:  # catch Ctrl-c
        pass  # Interpreter will call atexit handler
