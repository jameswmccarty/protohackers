import socket
import sys
import random
from threading import Lock
from socketserver import BaseRequestHandler, ThreadingTCPServer


class Cipher:

    kinds = { 0x01 : 'reversebits',
              0x02 : 'xorn',
              0x03 : 'xorpos',
              0x04 : 'addn',
              0x05 : 'addpos'}

    def __init__(self, kind, N=0):
        self.kind = Cipher.kinds[kind]
        self.N = N & 0xff
        print("Cipher of kind", self.kind, "with N", self.N)

    def reverse_bits(self, x):
        y = 0
        for i in range(8):
            y = y << 1
            y |= x & 1
            x = x >> 1
        return y & 0xff

    def apply_encode(self, x, pos):
        if self.kind == 'reversebits':
            return self.reverse_bits(x)
        if self.kind == 'xorn':
            return x ^ self.N
        if self.kind == 'xorpos':
            return (x ^ pos) % 256
        if self.kind == 'addn':
            return (x + self.N) % 256
        if self.kind == 'addpos':
            return (x + pos) % 256

    def apply_decode(self, x, pos):
        if self.kind == 'reversebits':
            return self.reverse_bits(x)
        if self.kind == 'xorn':
            return x ^ self.N
        if self.kind == 'xorpos':
            return (x ^ pos) % 256
        if self.kind == 'addn':
            return (x - self.N) % 256
        if self.kind == 'addpos':
            return (x - pos) % 256

class InsecureSocketLayer:

    def __init__(self, spec):
        self.ciphers = []
        self.recv_stream_pos = 0
        self.send_stream_pos = 0
        while spec:
            if spec[0] == 0x00:
                print("ISL extablished with ", len(self.ciphers), "ciphers")
                if len(spec) > 1:
                    print("Had additional data in this packet")
                return
            elif spec[0] in [0x01, 0x03, 0x05]:
                self.ciphers.append(Cipher(spec[0]))
                spec = spec[1:]
            elif spec[0] in [0x02, 0x04]:
                self.ciphers.append(Cipher(spec[0], N=spec[1]))
                spec = spec[2:]
            else:
                raise RuntimeError("invalid ISL spec")

    def generate_random_cipher_spec(n=15):
        spec = b''
        for i in range(n):
            c = random.choice([0x01, 0x02, 0x03, 0x04, 0x05])
            spec += c.to_bytes(1)
            if c in [0x02, 0x04]:
                spec += random.randint(0, 256).to_bytes(1)
            if len(spec) == 79:
                break
        spec += b'\x00'
        return spec

    def encode(self, bytestream):
        out = b''
        for b in bytestream:
            for cipher in self.ciphers:
                b = cipher.apply_encode(b, self.send_stream_pos) & 0xff
            out += int(b).to_bytes(1)
            self.send_stream_pos += 1
        if out == bytestream:
            raise RuntimeError("NOOP Cipher Detected for this connection")
        return out

    def decode(self, bytestream):
        out = b''
        for b in bytestream:
            for cipher in self.ciphers[::-1]:
                b = cipher.apply_decode(b, self.recv_stream_pos) & 0xff
            out += int(b).to_bytes(1)
            self.recv_stream_pos += 1
        if out == bytestream:
            raise RuntimeError("NOOP Cipher Detected for this connection")
        return out
