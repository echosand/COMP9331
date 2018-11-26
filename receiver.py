from socket import *
import sys
from pickle import *
import hashlib
import time
from sender import Segment


# class definition
class InputError(Exception):
    def __init__(self, err='Input error!!'):
        Exception.__init__(self, err)


class SocketError(Exception):
    def __init__(self, err='Udp start error!!'):
        Exception.__init__(self, err)


class Receiver:
    def __init__(self, receiver_port, filename):
        self.received = {}
        self.receiver_port = int(receiver_port)
        self.filename = filename
        self.initseq = 0
        self.buffersize = 4096
        self.buffer = {}
        self.nextack = 0
        self.nextseq = 0
        self.address = None
        self.corruption = 0
        self.duplicate = 0
        self.seg_recv = 0
        self.dup_ack = 0
        self.start_time = None
        self.address = None
        self.event = 'Waiting'
        self.length = 0
        self.udpstart()
        self.log_name = "Receiver_log.txt"
        open(self.log_name, 'w').close()

    def udpstart(self):
        try:
            self.socket = socket(AF_INET, SOCK_DGRAM)
            self.socket.bind(('', self.receiver_port))
            print('UDP socket established.')
        except SocketError:
            print('UDP establish failed')
            sys.exit()

    def gettype(self, reply):
        result = ''
        if reply.payload != b'':
            result += 'D'
        else:
            if reply.ack:
                result += 'A'
            if reply.syn:
                result += 'S'
            if reply.fin:
                result += 'F'
        return result

    def receive(self):
        recv, addr = self.socket.recvfrom(2048)
        self.seg_recv += 1
        if self.start_time is None:
            self.start_time = time.time()
            print(self.start_time)
        reply = loads(recv)
        print('recv seq', reply.seq_num,'ack',reply.ack_num)
        if self.address is None:
            self.address = addr
        self.length += len(reply.payload)
        return reply

    def send(self, seq_num, ack_num, syn, ack, fin, sent_time=None, if_retransmit=False):
        packet = Segment(seq_num,ack_num, b'', None, syn, ack, fin, if_retransmit=if_retransmit)
        packet.sent_time = sent_time
        packet = dumps(packet)
        self.socket.sendto(packet, self.address)
        print('send seq=', seq_num, 'ack=', ack_num)

    def handshake(self):
        reply = self.receive()
        self.log.write(
            f'rcv \t\t  {(time.time()-self.start_time) :.4f} \t S \t {reply.seq_num} \t 0 \t {reply.ack_num}\n')
        type = self.gettype(reply)
        print(type)
        if type == 'S':
            self.initack = reply.seq_num + 1
            self.send(self.initseq, self.initack, True, True, False)
            self.log.write(
                f'snd \t\t  {(time.time()-self.start_time) :.4f} \t A \t {self.initseq} \t 0 \t {self.initack}\n')
            reply = self.receive()
            if self.gettype(reply) == 'A' and reply.seq_num == self.initack and reply.ack_num == self.initseq + 1:
                self.log.write(
                    f'rcv \t\t  {(time.time()-self.start_time) :.4f} \t A \t {reply.seq_num} \t 0 \t {reply.ack_num}\n')
                self.nextseq = self.initseq + 1
                self.nextack = self.initack
                print('handshake successfully')
        self.event = 'Receiving'

    def receiving(self):
        while True:
            self.da = False
            reply = self.receive()
            type = self.gettype(reply)
            if type == 'D':
                checksum = hashlib.md5()
                checksum.update(reply.payload)
                if checksum.hexdigest() != reply.checksum:
                    self.log.write(
                        f'rcv/corr \t\t  {(time.time()-self.start_time) :.4f} \t D \t {reply.seq_num} \t {len(reply.payload)} \t {reply.ack_num}\n')
                    self.corruption += 1
                    print(self.corruption)
                
                else:
                    if reply.seq_num not in self.received:
                        self.received.update({reply.seq_num: reply.payload})
                        self.log.write(
                            f'rcv \t\t  {(time.time()-self.start_time) :.4f} \t D \t {reply.seq_num} \t {len(reply.payload)} \t {reply.ack_num}\n')
                    else:
                        self.duplicate += 1
                        self.log.write(
                            f'rcv/da \t\t  {(time.time()-self.start_time) :.4f} \t D \t {reply.seq_num} \t {len(reply.payload)} \t {reply.ack_num}\n')
                    if reply.seq_num == self.nextack:
                        check = 0
                        while self.nextack+check in self.received:
                            check += len(self.received[self.nextack+check])
                        self.nextack = check + self.nextack
                    else:
                        self.dup_ack += 1
                        self.da = True
                    print('seq=', self.nextseq)
                    sent_time = reply.sent_time
                    self.send(self.nextseq, self.nextack, False, True, False, sent_time,if_retransmit=reply.if_retransmit)
                    if self.da == True:
                        self.log.write(
                            f'snd/da \t\t  {(time.time()-self.start_time) :.4f} \t A \t {self.nextseq} \t 0 \t {self.nextack}\n')
                    else:
                        self.log.write(
                            f'snd \t\t  {(time.time()-self.start_time) :.4f} \t A \t {self.nextseq} \t 0 \t {self.nextack}\n')

            elif type == 'F':
                self.event = 'Wave_goodbye'
                self.nextack = reply.seq_num
                break

    def write_copy(self):
        content = b''
        sequence = sorted(self.received.keys())
        for key in sequence:
            content += self.received[key]
        with open(self.filename, 'wb') as file:
            file.write(content)

    def wave_goodbye(self):
        self.send(self.nextseq, self.nextack+1, False, True, True)
        self.log.write(
            f'snd \t\t  {(time.time()-self.start_time) :.4f} \t A \t {self.nextseq} \t 0 \t {self.nextack+1}\n')
        self.log.write(
            f'rcv \t\t  {(time.time()-self.start_time) :.4f} \t F \t {self.nextseq} \t 0 \t {self.nextack+1}\n')
        reply = self.receive()
        self.log.write(
            f'rcv \t\t  {(time.time()-self.start_time) :.4f} \t {self.gettype(reply)} \t {reply.seq_num} \t {len(reply.payload)} \t {reply.ack_num}\n')
        if  reply.syn == False and reply.ack == True and reply.fin == False:
            self.event = 'Waiting'
            print('end')
            self.write_copy()
            content = ''
            content += '=====================================================\n'
            content += f'Amount of Data Received (bytes) = {self.length:>}\n'
            content += f'Total segments received =  {self.seg_recv:>} \n'
            content += f'Data segments received =  {self.seg_recv-4:>} \n'
            content += f'Data Segments with bit errors = {self.corruption}\n'
            content += f'Duplicate data segments received = {self.duplicate}\n'
            content += f'Duplicate Ack sent = {self.dup_ack}\n'
            content += '====================================================\n'
            self.log.write(content)
            self.socket.close()

    def switch_events(self):
        self.event = 'Handshake'
        self.log = open('Receiver_log.txt', 'w')
        while self.event != 'Waiting':
            if self.event == 'Handshake':
                self.handshake()
            elif self.event == 'Receiving':
                self.receiving()
            elif self.event == 'Wave_goodbye':
                self.wave_goodbye()
            elif self.event == 'Waiting':
                break
            else:
                assert False


#===========================================
#main function
#===========================================

comment = sys.argv[:]
try:
    len(comment) == 3
except InputError:
    print('Input Error!!')
[receiver_port, newfile]=sys.argv[1:]
receive=Receiver(receiver_port, newfile)
receive.switch_events()






#python receiver.py 9999 file_r.pdf