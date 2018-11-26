import random
import threading
import time
import sys
from socket import *
from pickle import *
import hashlib

# class definition
class InputError(Exception):
    def __init__(self, err='Input error!!'):
        Exception.__init__(self, err)


class SocketError(Exception):
    def __init__(self, err='Udp start error!!'):
        Exception.__init__(self, err)


class Timer(object):
    def __init__(self, estimate_rtt, dev_rtt, gamma):
        self.timer = None
        self.started = False
        self.estimate_rtt = estimate_rtt
        self.dev_rtt = dev_rtt
        self.gamma = gamma
        self.interval = self.estimate_rtt + self.gamma * self.dev_rtt

    def cancel(self):
        self.started = False
        self.timer = None

    def start(self):
        self.timer = time.time()
        self.started = True

    def restart(self):
        self.timer = time.time()
        print('interval------------------->', self.interval)

    def expire(self):
        if self.started and time.time() - self.timer >= self.interval:
            return True
        return False

    def update_interval(self,sample_rtt):
        self.estimate_rtt = (1-0.125) * self.estimate_rtt + 0.125 *sample_rtt
        self.dev_rtt = (1-0.25) * self.dev_rtt + 0.25* abs(sample_rtt-self.estimate_rtt)
        self.interval = self.estimate_rtt + self.gamma * self.dev_rtt
        print('timeout interval', self.interval)

class Segment:
    def __init__(self, seq_num, ack_num, payload, checksum, syn=False, ack=False, fin=False, sent_time=None, if_retransmit=False):
        self.payload = payload
        self.fin = fin
        self.ack = ack
        self.syn = syn
        self.ack_num = ack_num
        self.seq_num = seq_num
        self.checksum = checksum
        self.sent_time = sent_time
        self.if_retransmit = if_retransmit

    def get_sent_time(self):
        self.sent_time = time.time()

    def get_sample_rtt(self):
        return time.time() - self.sent_time


class Sender:
    def __init__(self, receiver_host_ip, receiver_port, filename, mws, mss, gamma, pDrop, pDuplicate,
                 pCorrupt, pOrder, maxOrder, pDelay, maxDelay, seed):
        self.event = 'Waiting'
        self.address = (receiver_host_ip, int(receiver_port))
        self.filename = filename
        self.data = ''
        self.timer = None
        self.mws = int(mws)
        self.mss = int(mss)
        self.gamma = int(gamma)
        self.pDrop = float(pDrop)
        self.pDuplicate = float(pDuplicate)
        self.pCorrupt = float(pCorrupt)
        self.pOrder = float(pOrder)
        self.maxOrder = int(maxOrder)
        self.pDelay = float(pDelay)
        self.maxDelay = float(maxDelay)
        self.seed = int(seed)
        self.reorder_list=[]
        self.init_estimate_rtt = 0.5
        self.init_dev_rtt = 0.25
        # self.init_seq = random.randint(0, 1024)
        self.init_seq = 0
        self.LastByteAcked = self.init_seq + 1
        self.LastByteSent = self.init_seq
        self.udp_start()
        self.order_count = 0
        self.fast_retransmit = 0
        self.timeout_retransmit = 0
        self.delay = 0
        self.drop = 0
        self.corrupt = 0
        self.reorder = 0
        self.duplicate = 0
        self.plded = 0
        self.transmit = 0
        self.duplicate_ack = 0
        self.finishseq = None
        random.seed(self.seed)
        self.ackrecv = {}

    def get_types(self, segment):
        result = ''
        if len(segment.payload) != 0:
            result += 'D'
        else:
            if segment.ack:
                result += 'A'
            if segment.syn:
                result += 'S'
            if segment.fin:
                result += 'F'
        return result

    def handshake(self):
        self.start_time = time.time()
        self.send(self.init_seq, 0, True, False, False, b'')
        reply = self.receive()
        if reply.ack == True and reply.syn == True and reply.ack_num == self.init_seq + 1 and reply.fin == False:
            self.nextseq = reply.ack_num
            self.init_ack = reply.seq_num + 1
            self.send(self.nextseq,self.init_ack, False, True, False,b'')
            print('handshake successfully')
            self.event = 'Transferring'

    def PLD_module(self, packet):
        self.plded += 1
        self.transmit += 1
        type = self.get_types(packet)
        possibility = random.random()
        if possibility < self.pDrop:
            self.drop += 1
            self.log.write(
                f'snd/drop\t\t  {(time.time()-self.start_time) :.4f} \t {type} \t {packet.seq_num} \t {len(packet.payload)} \t {packet.ack_num}\n')
            print('dropped')
        else:
            possibility = random.random()
            if possibility < self.pDuplicate:
                self.duplicate += 1
                self.log.write(
                    f'snd/dup \t\t  {(time.time()-self.start_time) :.4f} \t {type} \t {packet.seq_num} \t {len(packet.payload)} \t {packet.ack_num}\n')
                self.socket.sendto(dumps(packet), self.address)
                self.log.write(
                    f'snd/dup \t\t  {(time.time()-self.start_time) :.4f} \t {type} \t {packet.seq_num} \t {len(packet.payload)} \t {packet.ack_num}\n')
                self.socket.sendto(dumps(packet), self.address)
                print('duplicated')
            else:
                possibility = random.random()
                if possibility < self.pCorrupt:
                    self.corrupt += 1
                    packet.payload = bytes.fromhex((packet.payload.hex()).replace('0', '2'))
                    self.log.write(
                        f'snd/corr \t\t  {(time.time()-self.start_time) :.4f} \t {type} \t {packet.seq_num} \t {len(packet.payload)} \t {packet.ack_num}\n')
                    self.socket.sendto(dumps(packet), self.address)
                    print('corrupted')
                else:
                    possibility = random.random()
                    if possibility < self.pOrder:
                        if self.reorder_list:
                            # print('reorder list full, send directly')
                            self.plded -= 1
                            self.socket.sendto(dumps(packet), self.address)
                        else:
                            self.reorder += 1
                            self.reorder_list.append(packet)
                            print('reordered')
                    else:
                        possibility = random.random()
                        if possibility < self.pDelay:
                            self.delay += 1
                            delay = random.random()*self.maxDelay/1000
                            t = threading.Timer(delay, self.delay_func, args=[packet])
                            t.start()
                            print('delayed', delay, 'ms')
                        else:
                            print('send normally')
                            self.log.write(
                                f'snd \t\t  {(time.time()-self.start_time) :.4f} \t {type} \t {packet.seq_num} \t {len(packet.payload)} \t {packet.ack_num}\n')
                            self.socket.sendto(dumps(packet), self.address)

    def delay_func(self,packet):
        type=self.get_types(packet)
        self.log.write(
            f'snd/dely \t\t  {(time.time()-self.start_time) :.4f} \t {type} \t {packet.seq_num} \t {len(packet.payload)} \t {packet.ack_num}\n')
        self.socket.sendto(dumps(packet), self.address)


    def receive(self):
        reply, addr = self.socket.recvfrom(2048)
        assert(addr == self.address)
        reply = loads(reply)
        type=self.get_types(reply)
        if self.event =='Transferring':
            if reply.ack_num in self.ackrecv:
                self.log.write(f'rcv/da \t\t  {(time.time()-self.start_time) :.4f} \t {type} \t {reply.seq_num} \t {len(reply.payload)} \t {reply.ack_num}\n')
            else:
                self.log.write(f'rcv \t\t  {(time.time()-self.start_time) :.4f} \t {type} \t {reply.seq_num} \t {len(reply.payload)} \t {reply.ack_num}\n')
        else:
            if type =='AF':
                self.log.write(
                    f'rcv \t\t  {(time.time()-self.start_time) :.4f} \t A \t {reply.seq_num} \t {len(reply.payload)} \t {reply.ack_num}\n')
                self.log.write(
                    f'rcv \t\t  {(time.time()-self.start_time) :.4f} \t F \t {reply.seq_num} \t {len(reply.payload)} \t {reply.ack_num}\n')
            else:
                self.log.write(
                    f'rcv \t\t  {(time.time()-self.start_time) :.4f} \t {type} \t {reply.seq_num} \t {len(reply.payload)} \t {reply.ack_num}\n')
        return reply

    def receiving_thread(self):
        print('receiving_thread start')
        while self.event == 'Transferring':
            reply = self.receive()
            type = self.get_types(reply)
            if type == 'A':
                '''
                if not reply.if_retransmit:
                    sample_rtt = reply.get_sample_rtt()
                    print('new sample rtt', sample_rtt)
                    self.timer.update_interval(sample_rtt)  # get interval via rtt
                    print('refresh rtt=', sample_rtt)
                '''
                ack = reply.ack_num
                if ack == self.finishseq and ack == self.finishseq:
                    self.event = 'Wave_goodbye'
                    break
                if ack not in self.ackrecv:
                    self.ackrecv.update({ack: 0})
                else:
                    self.ackrecv[ack] += 1
                    self.duplicate_ack += 1
                    if self.ackrecv[ack] >= 3:
                        self.log.write(
                            f'snd/rxt \t\t  {(time.time()-self.start_time) :.4f} \t D \t {ack} \t {len(self.dict[ack])} \t {self.init_ack}\n')
                        self.send(ack, self.init_ack, False, False, False, self.dict[ack], if_retransmit=True)
                        self.ackrecv[ack] = 0
                        self.fast_retransmit += 1
                        print('fast retransmit= ', self.fast_retransmit)


                acked_seq_index = self.seq_keys.index(ack) - 1
                if acked_seq_index != -1:
                    acked_seq = self.seq_keys[acked_seq_index]
                    if acked_seq > self.LastByteAcked:
                        self.LastByteAcked = acked_seq

                        if not reply.if_retransmit:
                            sample_rtt = reply.get_sample_rtt()
                            print('new sample rtt', sample_rtt)
                            self.timer.update_interval(sample_rtt)  # get interval via rtt
                            print('refresh rtt=', sample_rtt)

                        if self.LastByteAcked < self.LastByteSent:
                            self.timer.restart()

    def send(self, seq_num, ack_num, syn, ack, fin, payload, if_retransmit=False):
        print('send',seq_num, ack_num, syn, ack, fin)
        if payload != b'':
            checksum = hashlib.md5()
            checksum.update(payload)
            checksum = checksum.hexdigest()
        else:
            checksum = None
        packet = Segment(seq_num, ack_num, payload, checksum, syn, ack, fin, if_retransmit=if_retransmit)
        packet.get_sent_time()
        type = self.get_types(packet)
        if type == 'D':
            if self.reorder_list != [] and self.order_count == self.maxOrder:
                self.log.write(
                    f'snd/rord \t\t  {(time.time()-self.start_time) :.4f} \t {type} \t {self.reorder_list[0].seq_num} \t {len(self.reorder_list[0].payload)} \t {self.reorder_list[0].ack_num}\n')
                self.socket.sendto(dumps(self.reorder_list[0]), self.address)
                self.order_count = 0
            if self.reorder_list:
                self.order_count += 1
            #self.socket.sendto(dumps(packet), self.address)
            self.PLD_module(packet)
        else:
            self.log.write(
                f'snd \t\t  {(time.time()-self.start_time) :.4f} \t {type} \t {packet.seq_num} \t {len(packet.payload)} \t {packet.ack_num}\n')
            self.socket.sendto(dumps(packet), self.address)
            print('send seq=', seq_num, 'ack=', ack_num)

    def sending_thread(self):
        print('sending_thread start')
        while self.event == 'Transferring':
            if self.LastByteSent - self.LastByteAcked + self.mss <= self.mws:
                self.send(self.nextseq,self.init_ack, False, False, False, self.dict[self.nextseq])
                self.LastByteSent = self.nextseq
                self.nextseq += len(self.dict[self.nextseq])
                if not self.timer.started:
                    self.timer.start()
                if self.nextseq not in self.dict:
                    self.finishseq = self.nextseq
                    break

    def slice(self):
        file = open(self.filename, 'rb')
        self.data = file.read()
        self.dict = {}
        self.total_length = len(self.data)
        for i in range(0, self.total_length, self.mss):
            self.dict.update({self.init_seq+1+i:self.data[i:min(i+self.mss, self.init_seq+self.total_length)]})
        #print(self.dict)
        self.seq_keys = sorted(self.dict.keys())

    def timeout_thread(self):
        print('timer start')
        while self.event == 'Transferring':
            if self.timer.expire():
                self.timeout_retransmit += 1
                if self.LastByteAcked in self.dict:
                    self.log.write(
                        f'snd/rxt \t\t  {(time.time()-self.start_time) :.4f} \t D \t {self.LastByteAcked+self.mss} \t {len(self.dict[self.LastByteAcked+self.mss])} \t {self.init_ack}\n')
                    self.send(self.LastByteAcked + self.mss, self.init_ack, False, False, False,
                              self.dict[self.LastByteAcked + self.mss], if_retransmit=True)
                else:
                    self.log.write(
                        f'snd/rxt \t\t  {(time.time()-self.start_time) :.4f} \t D \t {self.LastByteAcked+1} \t {len(self.dict[self.LastByteAcked+1])} \t {self.init_ack}\n')
                    self.send(self.LastByteAcked + 1, self.init_ack, False, False, False,
                              self.dict[self.LastByteAcked + 1], if_retransmit=True)
                self.timer.restart()

    def transfer(self):
        self.slice()
        self.LastByteAcked = self.init_seq + 1
        self.LastByteSent= self.init_seq
        self.nextseq = self.init_seq + 1
        self.timer = Timer(self.init_estimate_rtt, self.init_dev_rtt, self.gamma)
        if self.dict != {}:
            threads = []
            threads.append(threading.Thread(target=self.timeout_thread))
            threads.append(threading.Thread(target=self.sending_thread))
            threads.append(threading.Thread(target=self.receiving_thread))
            for thread in threads:
                thread.setDaemon(True)
                thread.start()
            for thread in threads:
                thread.join()
        self.event = 'Wave_goodbye'


    def udp_start(self):
        try:
            self.socket = socket(AF_INET, SOCK_DGRAM)
            print('UDP socket established.')
        except SocketError:
            sys.exit()

    def wave_goodbye(self):
        self.send(self.finishseq, self.init_ack, False,False,True,b'')
        reply = self.receive()
        if  reply.syn == False and reply.ack == True and reply.fin == True:
            self.send(self.finishseq+1, self.init_ack+1, False, True, False, b'')
            self.event = 'Waiting'
            print('end')

    def switch_events(self):
        self.event = 'Handshake'
        self.log = open('Sender_log.txt', 'w')
        while self.event != 'Waiting':
            if self.event == 'Handshake':
                self.handshake()
            elif self.event == 'Transferring':
                self.transfer()
            elif self.event == 'Wave_goodbye':
                self.wave_goodbye()
            elif self.event == 'Waiting':
                break
            else:
                assert False

        content=''
        content+='======================================================================\n'
        content += f'Size of the file (in Bytes) = {self.total_length:>}\n'
        content += f'Segments transmitted (including drop & RXT) =  {self.plded+4:>} \n'
        content += f'Number of Segments handled by PLD =  {self.plded:>} \n'
        content += f'Number of Segments Dropped = {self.drop:>}\n'
        content += f'Number of Segments Corrupted = {self.corrupt:>}\n'
        content += f'Number of Segments Re-ordered = {self.reorder:>}\n'
        content += f'Number of Segments Duplicated = {self.duplicate:>}\n'
        content += f'Number of Segments Delayed = {self.delay:>}\n'
        content += f'Number of Retransmissions due to timeout = {self.timeout_retransmit:>}\n'
        content += f'Number of Fast Retransmissions = {self.fast_retransmit:>}\n'
        content += f'Number of Duplicate Acknowledgements received = {self.duplicate_ack:>}\n'
        content += '======================================================================\n'
        self.log.write(content)

        self.log.close()

# Main Function
if __name__ == '__main__':
    comment = sys.argv[:]
    assert(len(comment) == 15)
    [receiver_host_ip, receiver_port, filename, mws, mss, gamma, pDrop, pDuplicate,
     pCorrupt, pOrder, maxOrder, pDelay, maxDelay, seed] = sys.argv[1:]
    send = Sender(receiver_host_ip, receiver_port, filename, mws, mss, gamma, pDrop, pDuplicate,
                  pCorrupt, pOrder, maxOrder, pDelay, maxDelay, seed)

    send.switch_events()

#python sender.py 127.0.0.1 9999 test0.pdf 21 10 3 4 5 6 7 8 9 0 11