import concurrent
import copy
import threading
import time
import sys
import traceback
from typing import List

import zmq
import socket
import json
import jsonpickle
from queue import Queue
from threading import Thread
from tabulate import tabulate

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
        self.poller_empty = False
        for i in range(0, num_endpoints):
            self.poller, self.sockets = GiskardTester.create_socket(i, self.context, self.poller, self.sockets)
        print("tester bound sockets")
        self.nstates = []  # this will be printed out, so we can later recreate/test the transitions
        self.nodes = []  # the node_id's of the participating nodes
        self.init_gstate = GState()  # the initial GState contains all nodes' initial nstate
        self.gtrace = GTrace()  # a gtrace is the record of all transitions
        self.broadcast_msgs = []  # a list of all sent messages by the participating nodes
        self.msgs = []
        """ init the test files """
        self.file_name = self.file_name = "/home/repos/sawtooth-giskard/tests/sawtooth_poet_tests/tester_" + str(
            int(time.time())) + ".json"
        while not GiskardTester.init_nstate_test_file(self.file_name):
            continue
        self.file_name2 = "/home/repos/sawtooth-giskard/tests/sawtooth_poet_tests/tester_nstates.json"
        while not GiskardTester.init_nstate_test_file(self.file_name2):
            continue
        """ Start the message receiving loop """
        t1 = threading.Thread(target=self.tester_loop, args=())
        t1.start()

    @staticmethod
    def init_nstate_test_file(file_name: str):
        try:
            f = open(file_name, "w")
            f.write("")
            f.close()
        except OSError as e:
            traceback.print_exc()
            print(e)
            return False
        return True

    @staticmethod
    def update_nstate_test_file(file_name: str, nstates_json: str):
        try:
            f = open(file_name, "w")
            f.write(json.dumps(nstates_json, indent=4))
            f.close()
        except OSError as e:
            traceback.print_exc()
            print(e)
            return False
        return True


    @staticmethod
    def create_socket(i, context, poller, sockets):
        s = context.socket(zmq.PULL)
        s.bind(GiskardTester.tester_endpoint(i))
        #s.setsockopt_string(zmq.SUBSCRIBE, '')
        poller.register(s, zmq.POLLIN)
        sockets.append(s)
        return [poller, sockets]

    @staticmethod
    def tester_endpoint(num):
        return 'tcp://127.0.0.1:{}'.format(3030+num)

    def tester_loop(self):
        while not self.exit or not self.poller_empty:
            socks = dict(self.poller.poll(1000))
            self.poller_empty = True
            for sock in self.sockets:
                if sock in socks and socks[sock] == zmq.POLLIN:
                    self.poller_empty = False
                    [nstate, lm] = sock.recv_pyobj(zmq.NOBLOCK)
                    if nstate not in self.msgs:
                        self.msgs.append(nstate)
                    else:
                        continue
                    if len(nstate.in_messages) > 0:
                        print("\nGiskardTester handling msg with block_num: "+str(nstate.in_messages[0].block.block_num))
                    self.broadcast_msgs = self.broadcast_msgs + lm
                    if nstate.node_id not in self.nodes:  # node not yet connected -> add init nstate to init_gstate
                        self.nodes.append(nstate.node_id)
                        self.init_gstate.gstate.update({nstate.node_id: [nstate]})
                        self.init_gstate.broadcast_msgs = self.init_gstate.broadcast_msgs + lm
                        if len(self.nodes) == self.num_endpoints:  # all participating nodes are connected
                            self.gtrace.gtrace[0] = self.init_gstate
                            self.gtrace.gtrace[0].broadcast_msgs = self.broadcast_msgs
                    else:
                        """ Add the new nstate to a new gstate and append it to the gtrace """
                        latest_gstate = copy.deepcopy(self.gtrace.gtrace[-1])
                        if not latest_gstate:
                            latest_gstate = GState(self.nodes, {}, self.broadcast_msgs)
                        try:
                            latest_gstate.gstate[nstate.node_id].append(nstate)
                        except KeyError as e:
                            traceback.print_exc()
                            print(e)
                            latest_gstate.gstate.update({nstate.node_id: [nstate]})
                        latest_gstate.broadcast_msgs = self.broadcast_msgs
                        self.gtrace.gtrace.append(latest_gstate)
                    """ Add the new nstate to the test files """
                    self.nstates.append([nstate, self.broadcast_msgs])

        # exit from loop, shutdown socket connections
        nstates_json = []
        for nstate in self.nstates:
            nstates_json.append(jsonpickle.encode([nstate[0], nstate[1]], unpicklable=True))
        while not GiskardTester.update_nstate_test_file(self.file_name, nstates_json):
            print("write file1 failed, try again")
            continue
        while not GiskardTester.update_nstate_test_file(self.file_name2, nstates_json):
            print("write file2 failed, try again")
            continue
        for sock in self.sockets:
            self.poller.unregister(sock)
            time.sleep(0.1)
            sock.close()
        self.context.destroy()
        self.exited = True
        return

    @staticmethod
    def create_final_GState_from_file() -> GState:
        file_name = "/home/repos/repos/sawtooth-giskard/tests/sawtooth_poet_tests/tester_nstates.json"
        f = open(file_name)
        content = f.read()
        f.close()
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
        gstate.broadcast_msgs = broadcast_msgs
        return gstate

    @staticmethod
    def create_info_table():
        file_name = "/home/repos/repos/sawtooth-giskard/tests/sawtooth_poet_tests/tester_nstates.json"
        f = open(file_name)
        content = f.read()
        f.close()
        nstates = jsonpickle.decode(content)
        nodes = []
        table = [['block_num', 'view', 'block_index', 'block_id', 'proposer', 'votes_in_total', 'qc_in_total', 'honest', 'carryover'], []]
        blocks = []
        nodes_states = {}
        handled_msgs = []
        for nst in nstates:
            nstate, broadcast_msgs = jsonpickle.decode(nst, None, None, False, True, False,
                                                       [NState, List[GiskardMessage]])
            if nstate.node_id not in nodes:
                nodes.append(nstate.node_id)
                nodes_states.update({nstate.node_id: [nstate]})
            else:
                try:
                    nodes_states[nstate.node_id].append(nstate)
                except KeyError as e:
                    traceback.print_exc()
                    print(e)
                    nodes_states.update({nstate.node_id: [nstate]})
            for msg in broadcast_msgs:
                if msg.__str__() not in handled_msgs:
                    handled_msgs.append(msg.__str__())
                else:
                    continue
                if hasattr(msg.block.block_id, 'hex'):
                    msg.block.block_id = msg.block.block_id.hex()
                if msg.block not in blocks:
                    blocks.append(msg.block)
                    if msg.message_type == GiskardMessage.CONSENSUS_GISKARD_PREPARE_BLOCK \
                            or msg.message_type == GiskardMessage.CONSENSUS_GISKARD_PREPARE_VOTE:
                        sender = msg.sender
                        votes = 0
                        honest = "h"
                        if msg.block.payload == "Beware, I am a malicious block":
                            honest = "x"
                        carryover = "x"
                        if isinstance(msg.block.signer_id, str):
                            sender = msg.block.signer_id[0:6]
                        if hasattr(msg.block.signer_id, 'hex'):
                            sender = msg.block.signer_id.hex()[0:6]
                        if msg.message_type == GiskardMessage.CONSENSUS_GISKARD_PREPARE_VOTE:
                            votes = 1
                        table.append([msg.block.block_num, nstate.node_view, msg.block.block_index,
                                      msg.block.block_id[0:6], sender, votes, 0, honest, carryover])
                else:
                    pos = 0
                    view = msg.view
                    while pos == 0:
                        pos = GiskardTester.pos_block_in_table(table,
                                                               msg.block.block_id,
                                                               msg.block.payload,
                                                               view,
                                                               msg.message_type == GiskardMessage.CONSENSUS_GISKARD_VIEW_CHANGE_QC)
                        view -= view

                    if msg.message_type == GiskardMessage.CONSENSUS_GISKARD_PREPARE_VOTE:
                        table[pos][5] += 1  # update votes
                    if msg.message_type == GiskardMessage.CONSENSUS_GISKARD_PREPARE_QC:
                        table[pos][6] += 1  # update votes
                    if msg.message_type == GiskardMessage.CONSENSUS_GISKARD_VIEW_CHANGE_QC:
                        table[pos][8] = "c"  # carryover block from ViewChangeQC message

        print(tabulate(table, headers='firstrow', tablefmt='fancy_grid'))
        file_name3 = "/home/repos/repos/sawtooth-giskard/tests/sawtooth_poet_tests/info_table.txt"
        f = open(file_name3, 'w')
        f.write(tabulate(table, headers='firstrow', tablefmt='fancy_grid'))
        f.close()

    @staticmethod
    def pos_block_in_table(table, block_id, payload, view, carryover):
        for i in range(2, len(table)):
            if payload == "Beware, I am a malicious block":
                if table[i][7] == "x":
                    return i
            else:
                if table[i][3] == block_id[0:6] \
                        and (table[i][1] == view or carryover):
                    return i
            i += 1
        return 0

