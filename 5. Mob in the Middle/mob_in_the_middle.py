import socket
import sys
from threading import Lock
import atexit
import string
from socketserver import BaseRequestHandler, ThreadingTCPServer


def print_useage():
    print("ProtoHackers Mob in the Middle server.")
    print("Useage:")
    print("python mob_in_the_middle.py <<PORT>>")
    print("i.e. python mob_in_the_middle.py 5900")
    exit()


class ChatProxy(BaseRequestHandler):
    """
    ChatProxy instances are created by ThreadingTCPServer.
    Each ChatProxy instance handles a single client conversation.
    """
    
    UPSTREAM_SERVER = "chat.protohackers.com"
    UPSTREAM_PORT = 16963

    #UPSTREAM_SERVER = "localhost"
    #UPSTREAM_PORT = 5900

    BOGO_ADDR = "7YWHMfk9JZe0LM0g1ZauHuiSxhI"

    def get_next_msg(self, conn):
        buf = b''
        while True:
            try:
                conn.settimeout(1)
                part = conn.recv(1024)
                if not part:
                    raise RuntimeError("socket connection broken")
                buf += part
                while b'\n' in buf:
                    msg = buf[0:buf.index(b'\n')+1]
                    buf = buf[buf.index(b'\n')+1:]
                    yield msg
            except TimeoutError:
                yield None

    def find_replace_boguscoin(self, msg):
        msg = msg.decode()
        msg = msg.split(' ')
        for idx, segment in enumerate(msg):
            print("Looking at", idx, segment)
            if len(segment.strip('\r\n')) >= 26 and \
            len(segment.strip('\r\n')) <= 35 and \
            segment[0] == "7" and \
            all(x in string.ascii_letters+string.digits for x in segment.strip('\r\n')):
                print("Match at idx", idx)
                msg[idx] = ChatProxy.BOGO_ADDR
                if idx == len(msg)-1:
                    msg[idx] += "\n"
        msg = ' '.join(msg)
        return msg.encode()

    def handle(self):

        self.upstream = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.upstream.connect((ChatProxy.UPSTREAM_SERVER, ChatProxy.UPSTREAM_PORT))

        self.upstream_reader = self.get_next_msg(self.upstream)
        self.downstream_reader = self.get_next_msg(self.request)

        try:
            while True:
                # wait for message from chat client or chat room proxy
                for msg in self.downstream_reader:
                    if msg:  # client closed socket
                        print("Raw message (downstream)", msg)
                        msg = self.find_replace_boguscoin(msg)
                        self.upstream.send(msg)
                    else:
                        break
                for msg in self.upstream_reader:
                    if msg:  # client closed socket
                        print("Raw message (upstream)", msg)
                        msg = self.find_replace_boguscoin(msg)
                        self.request.send(msg)
                    else:
                        break
        except ConnectionError: # client terminated abruptly
            pass
        except RuntimeError: # client terminated abruptly
            pass
        finally:
            # the client closed the connection
            self.shutdown()

    def shutdown(self):
        if self.request:
            self.request.close()
        if self.upstream:
            self.upstream.close()

@atexit.register
def shutdown_chat_server():
    print('shutting down chat server...')
    server.shutdown()

if __name__ == "__main__":

    if len(sys.argv) != 2:
        print_useage()

    port = int(sys.argv[1])

    try:
        # Allow server to rebind to previously used port number.
        ThreadingTCPServer.allow_reuse_address = True
        server = ThreadingTCPServer(('', port), ChatProxy)
        print(f'Starting ChatProxy at port {port}...')
        server.serve_forever()
    except KeyboardInterrupt:  # catch Ctrl-c
        pass  # Interpreter will call atexit handler
