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
    def __init__(self, num_endpoints):
        """ Connect to each engine in the net for recording their nstates """
        print("init tester")
        self.exit = False  #  set to True from the outside to stop the GiskardTester
        self.exited = False
        self.num_endpoints = num_endpoints  # the number of participating nodes
        self.sockets = []
        self.context = zmq.Context()
        self.poller = zmq.Poller()
        for i in range(0, num_endpoints):
            self.poller, self.sockets = GiskardTester.create_socket(i, self.context, self.poller, self.sockets)
        print("tester bound sockets")
        self.nstates_json = list()  # this will be printed out, so we can later recreate/test the transitions
        self.nodes = []  # the node_id's of the participating nodes
        self.init_gstate = GState()  # the initial GState contains all nodes' initial nstate
        self.gtrace = GTrace()  # a gtrace is the record of all transitions
        self.broadcast_msgs = []  # a list of all sent messages by the participating nodes
        """ init the test files """
        self.file_name = self.file_name = "/mnt/c/repos/sawtooth-giskard/tests/sawtooth_poet_tests/tester_" + str(
            int(time.time())) + ".json"
        GiskardTester.init_nstate_test_file(self.file_name)
        self.file_name2 = "/mnt/c/repos/sawtooth-giskard/tests/sawtooth_poet_tests/tester_nstates.json"
        GiskardTester.init_nstate_test_file(self.file_name2)
        """ Start the message receiving loop """
        t1 = threading.Thread(target=self.tester_loop, args=())
        t1.start()

    @staticmethod
    def init_nstate_test_file(file_name: str):
        f = open(file_name, "w")
        f.write("")
        f.close()

    @staticmethod
    def update_nstate_test_file(file_name: str, nstates_json: str):
        f = open(file_name, "w")
        f.write(json.dumps(nstates_json, indent=4))
        f.close()

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
                    if nstate.node_id not in self.nodes:  # node not yet connected -> add init nstate to init_gstate
                        self.nodes.append(nstate.node_id)
                        self.init_gstate.gstate.update({nstate.node_id: [nstate]})
                        self.init_gstate.broadcast_msgs = self.init_gstate.broadcast_msgs + lm
                        if len(self.nodes) == self.num_endpoints:  # all participating nodes are connected
                            self.gtrace.gtrace[0] = self.init_gstate
                            self.gtrace.gtrace[-1].broadcast_msgs = self.broadcast_msgs
                    else:
                        """ Add the new nstate to a new gstate and append it to the gtrace """
                        latest_gstate = copy.deepcopy(self.gtrace.gtrace[-1])
                        if not latest_gstate:
                            latest_gstate = GState(self.nodes, {}, self.broadcast_msgs)
                        latest_gstate.gstate[nstate.node_id].append(nstate)
                        latest_gstate.broadcast_msgs = self.broadcast_msgs
                        self.gtrace.gtrace.append(latest_gstate)
                    """ Add the new nstate to the test files """
                    self.nstates_json.append(jsonpickle.encode([nstate, self.broadcast_msgs], unpicklable=True))
                    GiskardTester.update_nstate_test_file(self.file_name, self.nstates_json)
                    GiskardTester.update_nstate_test_file(self.file_name2, self.nstates_json)
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
