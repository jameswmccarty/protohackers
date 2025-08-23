import socket
import sys
from threading import Lock
import atexit
import string
from socketserver import BaseRequestHandler, ThreadingTCPServer


def print_useage():
    print("ProtoHackers Budget Chat server.")
    print("Useage:")
    print("python budget_chat.py <<PORT>>")
    print("i.e. python budget_chat.py 5900")
    exit()


class ChatServer(BaseRequestHandler):
    """
    ChatServer instances are created by ThreadingTCPServer.
    Each ChatServer instance handles a single client conversation.
    """
    chat_sockets_lock = Lock()
    chat_sockets = set()
    chat_names = set()

    def broadcast_msg(self, msg):
        print("Broadcasting msg", msg)
        with ChatServer.chat_sockets_lock:
            for socket in ChatServer.chat_sockets:
                if socket != self.request:  # don't send msg to sender
                    socket.send(msg)


    def room_users_msg(self):
        msg = '* The room contains: '
        users = set(ChatServer.chat_names)
        users.discard(self.username)
        msg += ', '.join(sorted(users))
        msg += '\n'
        msg = msg.encode()
        self.request.send(msg)

    def get_next_msg(self):
        buf = b''
        while True:
            part = self.request.recv(1024)
            if not part:
                raise RuntimeError("socket connection broken")
            buf += part
            while b'\n' in buf:
                msg = buf[0:buf.index(b'\n')+1]
                buf = buf[buf.index(b'\n')+1:]
                yield msg

    def handle(self):
        self.username = None
        print('Got connection from', self.client_address)
        # First requirement is to get a username
        self.request.send(b"Welcome to budgetchat! What shall I call you?\n")
        try:
            msg = next(self.get_next_msg(), b'')
        except RuntimeError: # client terminated abruptly
            self.request.close()
            return # got empty message
        msg = msg.decode().strip()
        print("New request for username", msg)
        if not all(x in string.ascii_letters+string.digits for x in msg):
            print("Username was illegal", msg)
            self.request.send(b"Illegal Username\n")
            self.request.close()
            return
        self.username = msg # name selection was valid
        with ChatServer.chat_sockets_lock:
            ChatServer.chat_sockets.add(self.request)
            ChatServer.chat_names.add(self.username)
        self.room_users_msg()
        announce = '* '+self.username+' has entered the room\n'
        announce = announce.encode()
        self.broadcast_msg(announce)
        try:
            while True:
                # wait for message from chat client or chat room proxy
                for msg in self.get_next_msg():
                    if not msg:  # client closed socket
                        raise ConnectionError("No message.")
                    print("Raw message", msg)
                    prefix = '['+self.username+'] '
                    prefix = prefix.encode()
                    self.broadcast_msg(prefix+msg)
        except ConnectionError: # client terminated abruptly
            pass
        except RuntimeError: # client terminated abruptly
            pass
        finally:
            # the client closed the connection, so we'll remove the client's
            # socket from the chat_sockets set
            print("Got a leave for: ", self.username)
            with ChatServer.chat_sockets_lock:
                ChatServer.chat_sockets.discard(self.request)
                ChatServer.chat_names.discard(self.username)
                goodbye = '* '+self.username+' has left the room\n'
                goodbye = goodbye.encode()
                for socket in ChatServer.chat_sockets:
                    if socket != self.request:  # don't send msg to sender
                        socket.send(goodbye)
            if self.request:
                self.request.close()

    @classmethod
    def shutdown(cls):
        with cls.chat_sockets_lock:
            for socket in cls.chat_sockets:
                if socket:
                    socket.close()

@atexit.register
def shutdown_chat_server():
    print('Closing chat client sockets, ', end='')
    ChatServer.shutdown()
    print('shutting down chat server...')
    server.shutdown()

if __name__ == "__main__":

    if len(sys.argv) != 2:
        print_useage()

    port = int(sys.argv[1])

    try:
        # Allow server to rebind to previously used port number.
        ThreadingTCPServer.allow_reuse_address = True
        server = ThreadingTCPServer(('', port), ChatServer)
        print(f'Starting ChatServer at port {port}...')
        server.serve_forever()
    except KeyboardInterrupt:  # catch Ctrl-c
        pass  # Interpreter will call atexit handler
