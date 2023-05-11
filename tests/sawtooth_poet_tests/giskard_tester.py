import concurrent
import copy
import threading
import time
import sys
from typing import List

import zmq
import socket
import json
import jsonpickle
from queue import Queue
from threading import Thread

from sawtooth_poet_engine.giskard_message import GiskardMessage
from sawtooth_poet_engine.giskard_global_state import GState
from sawtooth_poet_engine.giskard_global_trace import GTrace
from sawtooth_poet_engine.giskard_nstate import NState
from sawtooth_sdk.consensus.driver import Driver
from sawtooth_sdk.consensus.engine import StartupState
from sawtooth_sdk.consensus.engine import PeerMessage
from sawtooth_sdk.consensus.zmq_service import ZmqService
from sawtooth_sdk.consensus import exceptions
from sawtooth_sdk.messaging.stream import Stream
from sawtooth_sdk.protobuf import consensus_pb2
from sawtooth_sdk.protobuf.validator_pb2 import Message


class GiskardTester:
    def __init__(self, num_endpoints, k_peers):
        print("init tester")
        """ Create Streams for each engine in the net for exchanging nstates with each other """
        self.exit = False
        self.exited = False
        self.num_endpoints = num_endpoints
        self.k_peers = k_peers
        self.sockets = []
        self.context = zmq.Context()
        self.poller = zmq.Poller()
        for i in range(0, num_endpoints):
            self.poller, self.sockets = GiskardTester.create_socket(i, self.context, self.poller, self.sockets)
        print("tester bound sockets")
        self.file_name = "/mnt/c/repos/sawtooth-giskard/tests/sawtooth_poet_tests/tester_" + str(int(time.time())) + ".json"
        f = open(self.file_name, "w")
        f.write("")
        f.close()
        self.file_name2 = "/mnt/c/repos/sawtooth-giskard/tests/sawtooth_poet_tests/tester_nstates.json"
        f = open(self.file_name2, "w")
        f.write("")
        f.close()
        self.nstates_json = list()
        self.nodes = []
        self.init_gstate = GState()
        self.gtrace = GTrace()
        self.broadcast_msgs = []
        t1 = threading.Thread(target=self.tester_loop, args=())
        t1.start()

    @staticmethod
    def create_socket(i, context, poller, sockets):
        s = context.socket(zmq.SUB)
        s.bind(GiskardTester.tester_endpoint(i))
        s.setsockopt_string(zmq.SUBSCRIBE, '')
        poller.register(s, zmq.POLLIN)
        sockets.append(s)
        return [poller, sockets]

    @staticmethod
    def tester_endpoint(num):
        return 'tcp://127.0.0.1:{}'.format(3030+num)

    def tester_loop(self):
        while not self.exit:
            socks = dict(self.poller.poll(2000))
            for sock in self.sockets:
                if sock in socks and socks[sock] == zmq.POLLIN:
                    [nstate, lm] = sock.recv_pyobj(zmq.DONTWAIT)
                    self.broadcast_msgs = self.broadcast_msgs + lm
                    if nstate.node_id not in self.nodes:
                        self.nodes.append(nstate.node_id)
                        self.init_gstate.gstate.update({nstate.node_id: [nstate]})
                        self.init_gstate.broadcast_msgs = self.init_gstate.broadcast_msgs + lm
                        if len(self.nodes) == self.k_peers:
                            self.gtrace.gtrace[0] = self.init_gstate
                            self.gtrace.gtrace[-1].broadcast_msgs = self.broadcast_msgs
                    else:
                        latest_gstate = copy.deepcopy(self.gtrace.gtrace[-1])
                        if not latest_gstate:
                            latest_gstate = GState(self.nodes, {}, self.broadcast_msgs)
                        latest_gstate.gstate[nstate.node_id].append(nstate)
                        latest_gstate.broadcast_msgs = self.broadcast_msgs
                        self.gtrace.gtrace.append(latest_gstate)
                    #print("\n\n\nTester Msg received ", nstate.node_id, "\n\n\n")
                    self.nstates_json.append(jsonpickle.encode([nstate, self.broadcast_msgs], unpicklable=True))
                    f = open(self.file_name, "w")
                    f.write(json.dumps(self.nstates_json, indent=4))
                    f.close()
                    f = open(self.file_name2, "w")
                    f.write(json.dumps(self.nstates_json, indent=4))
                    f.close()
        # exit from loop, shutdown socket connections
        for sock in self.sockets:
            self.poller.unregister(sock)
            time.sleep(0.1)
            sock.close()
        self.context.destroy()
        self.exited = True
        return

    @staticmethod
    def create_final_GState_from_file() -> GState:
        file_name = "/mnt/c/repos/sawtooth-giskard/tests/sawtooth_poet_tests/tester_nstates.json"
        f = open(file_name)
        content = f.read()
        nstates = jsonpickle.decode(content)
        nodes = []
        broadcast_msgs = []
        gstate = GState()
        for nst in nstates:
            nstate, broadcast_msgs = jsonpickle.decode(nst, None, None, False, True, False, [NState, List[GiskardMessage]])
            if nstate.node_id not in nodes:
                nodes.append(nstate.node_id)
                gstate.gstate.update({nstate.node_id: [nstate]})
            else:
                gstate.gstate[nstate.node_id].append(nstate)
        f.close()
        gstate.broadcast_msgs = broadcast_msgs
        return gstate

    def _process(self, message):
        type_tag = message.message_type

        if type_tag == Message.CONSENSUS_NOTIFY_PEER_CONNECTED:
            notification = consensus_pb2.ConsensusNotifyPeerConnected()
            notification.ParseFromString(message.content)

            data = notification.peer_info

        elif type_tag == Message.CONSENSUS_NOTIFY_PEER_DISCONNECTED:
            notification = consensus_pb2.ConsensusNotifyPeerDisconnected()
            notification.ParseFromString(message.content)

            data = notification.peer_id

        elif type_tag == Message.CONSENSUS_NOTIFY_PEER_MESSAGE:
            notification = consensus_pb2.ConsensusNotifyPeerMessage()
            notification.ParseFromString(message.content)

            header = consensus_pb2.ConsensusPeerMessageHeader()
            header.ParseFromString(notification.message.header)

            peer_message = PeerMessage(
                header=header,
                header_bytes=notification.message.header,
                header_signature=notification.message.header_signature,
                content=notification.message.content)

            data = peer_message, notification.sender_id

        elif type_tag == Message.CONSENSUS_NOTIFY_BLOCK_NEW:
            notification = consensus_pb2.ConsensusNotifyBlockNew()
            notification.ParseFromString(message.content)

            data = notification.block

        elif type_tag == Message.CONSENSUS_NOTIFY_BLOCK_VALID:
            notification = consensus_pb2.ConsensusNotifyBlockValid()
            notification.ParseFromString(message.content)

            data = notification.block_id

        elif type_tag == Message.CONSENSUS_NOTIFY_BLOCK_INVALID:
            notification = consensus_pb2.ConsensusNotifyBlockInvalid()
            notification.ParseFromString(message.content)

            data = notification.block_id

        elif type_tag == Message.CONSENSUS_NOTIFY_BLOCK_COMMIT:
            notification = consensus_pb2.ConsensusNotifyBlockCommit()
            notification.ParseFromString(message.content)

            data = notification.block_id

        elif type_tag == Message.CONSENSUS_NOTIFY_ENGINE_DEACTIVATED:
            self.stop()
            data = None

        elif type_tag == Message.PING_REQUEST:
            data = None

        else:
            raise exceptions.ReceiveError(
                'Received unexpected message type: {}'.format(type_tag))

        self._stream.send_back(
            message_type=Message.CONSENSUS_NOTIFY_ACK,
            correlation_id=message.correlation_id,
            content=consensus_pb2.ConsensusNotifyAck().SerializeToString())

        return type_tag, data
