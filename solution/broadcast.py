from anysystem import Context, Message, Process
from typing import List


class BroadcastProcess(Process):
    def __init__(self, proc_id: str, processes: List[str]):
        self._id = proc_id
        self._processes = processes
        self._acks = {}
        self._sent = set()
        self._received = set()
        self._delivered = set()
        self._check = set()
        

    def on_local_message(self, msg: Message, ctx: Context):
        if msg.type == 'SEND':
            bcast_msg = Message('BCAST', {
                'text': msg['text']
            })
            self._sent.add(msg['text'])
            for proc in self._processes:
                ctx.send(bcast_msg, proc)

    def on_message(self, msg: Message, sender: str, ctx: Context):
        if msg.type == 'BCAST' and msg['text'] not in self._sent and msg['text'] not in self._received:

            self._received.add(msg['text'])
            msg = Message('ACK', {
                'text': msg['text']
            })
            ctx.send(msg, sender)

        elif msg.type == 'ACK':

            if msg['text'] not in self._acks:
                self._acks[msg['text']] = 0
            self._acks[msg['text']] += 1

            if self._acks[msg['text']] >= len(self._processes) // 2 and msg['text'] not in self._check:
                for proc in self._processes:
                    bcast_msg = Message('BCAST_DELIVER', {
                        'text': msg['text']
                    })
                    ctx.send(bcast_msg, proc)
                self._check.add(msg['text'])

        elif msg.type == "BCAST_DELIVER" and msg['text'] not in self._delivered:

            self._delivered.add(msg['text'])
            deliver_msg = Message('DELIVER', {
                'text': msg['text']
            })
            ctx.send_local(deliver_msg)

            for proc in self._processes:
                ctx.send(msg, proc)
            
            msg = Message('FINACK', {
                'text': msg['text']
            })
            ctx.send(msg, sender)


        elif msg.type == "FINACK":

            if msg['text'] not in self._delivered:
                self._delivered.add(msg['text'])
                deliver_msg = Message('DELIVER', {
                    'text': msg['text']
                })
                ctx.send_local(deliver_msg)

    def on_timer(self, timer_name: str, ctx: Context):
        pass