# Copyright 2018 Intel Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# -----------------------------------------------------------------------------
import copy
import os
import logging
import pickle
import queue
import json
import struct
import time

import jsonpickle
import zmq
from collections import namedtuple

import sawtooth_signing as signing
from sawtooth_poet.journal.block_wrapper import LAST_BLOCK_INDEX_IDENTIFIER, NULL_BLOCK_IDENTIFIER
from sawtooth_signing import CryptoFactory
from sawtooth_signing.secp256k1 import Secp256k1PrivateKey

from sawtooth_sdk.messaging.stream import Stream
from sawtooth_sdk.consensus.engine import Engine
from sawtooth_sdk.consensus import exceptions
from sawtooth_sdk.protobuf.validator_pb2 import Message
import sawtooth_sdk.protobuf.consensus_pb2 as consensus_pb2

from sawtooth_poet_engine.oracle import PoetOracle, PoetBlock, _BlockCacheProxy, _BatchPublisherProxy, \
    _load_identity_signer
from sawtooth_poet_engine.giskard_block import GiskardBlock, GiskardGenesisBlock
from sawtooth_poet_engine.giskard_message import GiskardMessage
from sawtooth_poet_engine.giskard_nstate import NState
from sawtooth_poet_engine.giskard import Giskard
from sawtooth_poet_engine.pending import PendingForks
from sawtooth_poet_engine.giskard_node import GiskardNode

LOGGER = logging.getLogger(__name__)


class GiskardEngine(Engine):
    """The entrypoint for Giskard
        Keeps state
        Handles incoming messages, validates blocks
        Proposes new Blocks if it is Proposer in the current view"""

    def __init__(self, path_config, component_endpoint, validator_connect, dishonest=False, k_peers=2):
        # components
        self._path_config = path_config
        self._component_endpoint = component_endpoint
        self._validator_connect = validator_connect
        self._service = None
        self._oracle = None

        # state variables
        self._exit = False
        self._published = False
        self._building = False
        self._committing = False

        self._validating_blocks = set()
        self._pending_forks_to_resolve = PendingForks()

        """ Giskard stuff"""
        self.dishonest = dishonest
        self.all_initial_blocks_proposed = False
        self.genesis_proposed = False
        self.new_network = False
        self.peers = []
        self.k_peers = k_peers
        self.node = None
        # original NState from the formal specification
        self.nstate = None  # node identifier TODO get that from the registry service / the epoch protocol
        self.prepareQC_last_view = None
        self.hanging_prepareQC_new_proposer = False
        # connection to GiskardTester, to send state updates
        tester_endpoint = self.tester_endpoint(int(self._validator_connect[-1]))
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PUB)
        LOGGER.info("socket created " + tester_endpoint)
        self.socket.connect(tester_endpoint)
        self.in_buffer = []

        """ end Giskard stuff """

    # Ignore invalid override pylint issues
    # pylint: disable=invalid-overridden-method
    def name(self):
        return 'PoET'

    # Ignore invalid override pylint issues
    # pylint: disable=invalid-overridden-method
    def version(self):
        return '0.1'

    def additional_protocols(self):
        return [('poet', '0.1')]

    @staticmethod
    def tester_endpoint(num):
        return 'tcp://127.0.0.1:{}'.format(3030 + num)

    def stop(self):
        self._exit = True

    def _initialize_block(self):
        chain_head = self._get_chain_head()
        initialize = self._oracle.initialize_block(chain_head)

        if initialize:
            self._service.initialize_block(previous_id=chain_head.block_id)

        return initialize

    def _check_consensus(self, block):
        return self._oracle.verify_block(block)

    def _switch_forks(self, current_head, new_head):
        try:
            switch = self._oracle.switch_forks(current_head, new_head)
        # The PoET fork resolver raises TypeErrors in certain cases,
        # e.g. when it encounters non-PoET blocks.
        except TypeError as err:
            switch = False
            LOGGER.warning('PoET fork resolution error: %s', err)

        return switch

    def _check_block(self, block_id):
        self._service.check_blocks([block_id])

    def _fail_block(self, block_id):
        self._service.fail_block(block_id)

    def _get_chain_head(self):
        return PoetBlock(self._service.get_chain_head())

    def _get_block(self, block_id):
        return PoetBlock(self._service.get_blocks([block_id])[block_id])

    def _commit_block(self, block_id):
        self._service.commit_block(block_id)

    def _ignore_block(self, block_id):
        self._service.ignore_block(block_id)

    def _cancel_block(self):
        try:
            self._service.cancel_block()
        except exceptions.InvalidState:
            pass

    def _summarize_block(self):
        try:
            return self._service.summarize_block()
        except exceptions.InvalidState as err:
            LOGGER.warning(err)
            return None
        except exceptions.BlockNotReady:
            return None

    def _finalize_block(self):
        summary = self._summarize_block()

        if summary is None:
            LOGGER.debug('Block not ready to be summarized')
            return None

        consensus = self._oracle.finalize_block(summary)

        if consensus is None:
            return None

        try:
            block_id = self._service.finalize_block(consensus)
            LOGGER.info(
                'Finalized %s with %s',
                block_id.hex(),
                json.loads(consensus.decode()))
            return block_id
        except exceptions.BlockNotReady:
            LOGGER.debug('Block not ready to be finalized')
            return None
        except exceptions.InvalidState:
            LOGGER.warning('block cannot be finalized')
            return None

    def _check_publish_block(self):
        # Publishing is based solely on wait time, so just give it None.
        return self._oracle.check_publish_block(None)

    def start(self, updates, service, startup_state):
        """is called from main to start the Engine,
         so it starts receiving messages"""
        self._service = service

        self._oracle = PoetOracle(
            service=service,
            component_endpoint=self._component_endpoint,
            config_dir=self._path_config.config_dir,
            data_dir=self._path_config.data_dir,
            key_dir=self._path_config.key_dir)

        signer = _load_identity_signer(self._path_config.key_dir, 'validator')
        validator_id = signer.get_public_key().as_hex()
        self.peers.append(validator_id)
        stream = Stream(self._component_endpoint)
        block_cache = _BlockCacheProxy(self._service, stream)
        _batch_publisher = _BatchPublisherProxy(stream, signer)
        self.node = GiskardNode(validator_id, 0, self.dishonest, block_cache)
        self.nstate = NState(self.node)
        self.new_network = self._get_chain_head() == GiskardGenesisBlock()
        if self._get_chain_head().block_num >= 2:
            self.all_initial_blocks_proposed = True
        # send empty state to the GiskardTester
        state_msg = pickle.dumps([self.nstate, []], pickle.DEFAULT_PROTOCOL)
        tracker: zmq.MessageTracker = self.socket.send(state_msg, 0, False, True)
        while not tracker.done:
            continue

        # 1. Wait for an incoming message.
        # 2. Check for exit.
        # 3. Handle the message.
        # 4. Check for publishing.

        handlers = {
            Message.CONSENSUS_NOTIFY_BLOCK_NEW: self._handle_new_block,
            Message.CONSENSUS_NOTIFY_BLOCK_VALID: self._handle_valid_block,
            Message.CONSENSUS_NOTIFY_BLOCK_INVALID: self._handle_invalid_block,
            Message.CONSENSUS_NOTIFY_BLOCK_COMMIT: self._handle_committed_block,
            Message.CONSENSUS_NOTIFY_PEER_CONNECTED: self._handle_peer_connected,
            Message.CONSENSUS_NOTIFY_PEER_DISCONNECTED: self.handle_peer_disconnected,
            Message.CONSENSUS_NOTIFY_PEER_MESSAGE: self._handle_peer_msgs,
            GiskardMessage.CONSENSUS_GISKARD_PREPARE_BLOCK: self._handle_prepare_block,
            GiskardMessage.CONSENSUS_GISKARD_PREPARE_VOTE: self._handle_prepare_vote,
            GiskardMessage.CONSENSUS_GISKARD_VIEW_CHANGE: self._handle_view_change,
            GiskardMessage.CONSENSUS_GISKARD_PREPARE_QC: self._handle_prepare_qc,
            GiskardMessage.CONSENSUS_GISKARD_VIEW_CHANGE_QC: self._handle_view_change_qc
        }

        while True:
            if not self.genesis_proposed and Giskard.is_block_proposer(self.node, self.nstate.node_view, self.peers):
                """ chain head is the genesis block -> propose init block """
                self.node.block_cache.pending_blocks.append(copy.deepcopy(self._get_chain_head()))
                self.genesis_proposed = True
            if len(self.peers) >= self.k_peers and self.new_network and not self.all_initial_blocks_proposed:
                """ Propose chain head if it is the genesis block """
                # TODO check what checks are necessary here by poet
                if Giskard.is_block_proposer(self.node, self.nstate.node_view, self.peers):
                    if len(self.node.block_cache.pending_blocks) >= 3:
                        self.nstate, lm = Giskard.propose_block_init_set(
                            self.nstate, None, self.node.block_cache)
                        LOGGER.info("propose new block from while check: count msgs: " + str(len(lm)))
                        if self.node.block_cache.blocks_proposed_num == LAST_BLOCK_INDEX_IDENTIFIER:
                            self.all_initial_blocks_proposed = True
                            state_msg = pickle.dumps([self.nstate, lm], pickle.DEFAULT_PROTOCOL)
                            tracker: zmq.MessageTracker = self.socket.send(state_msg, 0, False, True)
                            while not tracker.done:
                                continue
                        self._send_out_msgs(lm)
                        for msg in lm:
                            if msg.block not in self.node.block_cache.block_store.uncommitted_blocks:
                                self.node.block_cache.block_store.uncommitted_blocks.append(msg.block)
                            self._handle_prepare_block(msg)
            if len(self.peers) >= self.k_peers and len(self.in_buffer) > 0 \
                    and not self.hanging_prepareQC_new_proposer:
                handlers_peer = {
                    GiskardMessage.CONSENSUS_GISKARD_PREPARE_BLOCK: self._handle_prepare_block,
                    GiskardMessage.CONSENSUS_GISKARD_PREPARE_VOTE: self._handle_prepare_vote,
                    GiskardMessage.CONSENSUS_GISKARD_VIEW_CHANGE: self._handle_view_change,
                    GiskardMessage.CONSENSUS_GISKARD_PREPARE_QC: self._handle_prepare_qc,
                    GiskardMessage.CONSENSUS_GISKARD_VIEW_CHANGE_QC: self._handle_view_change_qc
                }
                try:
                    gmsg = self.in_buffer.pop(0)
                    handle_peer_msg = handlers_peer[gmsg.message_type]
                except:
                    LOGGER.error('Unknown Messagetype: %s', gmsg.message_type)
                else:
                    handle_peer_msg(gmsg)
                    continue
            try:
                try:
                    type_tag, data = updates.get(timeout=0.1)
                except queue.Empty:
                    pass
                else:
                    LOGGER.debug('Received message: %s',
                                 Message.MessageType.Name(type_tag))
                    """f = open(file_name, "a")
                    f.write(str(type_tag) + "\n")
                    f.write(str(data) + "\n")
                    f.close()"""
                    try:
                        LOGGER.info('Received message: %s',
                                    Message.MessageType.Name(type_tag))
                        handle_message = handlers[type_tag]
                    except KeyError:
                        LOGGER.error('Unknown type tag: %s',
                                     Message.MessageType.Name(type_tag))
                    else:
                        handle_message(data)

                if self._exit:
                    self.socket.close()
                    self.context.destroy()
                    break

                self._try_to_publish()
            except Exception:  # pylint: disable=broad-except
                LOGGER.exception("Unhandled exception in message loop")

    def _try_to_publish(self):
        if self._published or self._validating_blocks:
            return

        if not self._building:
            if self._initialize_block():
                self._building = True

        if self._building:
            if self._check_publish_block():
                block_id = self._finalize_block()
                if block_id:
                    LOGGER.info("Published block %s", block_id.hex())
                    self._published = True
                    self._building = False
                else:
                    self._cancel_block()
                    self._building = False

    def _handle_new_block(self, block):
        block = PoetBlock(block)
        LOGGER.info('Received %s', block)
        self._check_block(block.block_id)
        self._validating_blocks.add(block.block_id)
        # Giskard -------------
        # TODO check if this block was already proposed
        self.node.block_cache.pending_blocks.append(block)
        #if block.block_num == 5:
            #import pdb; pdb.set_trace()
        if self.hanging_prepareQC_new_proposer \
                and len(self.node.block_cache.pending_blocks) >= 3:
            # TODO call all transitions with timeouts in mind
            self.hanging_prepareQC_new_proposer = False
            parent_block = self.prepareQC_last_view.block
            if parent_block is None:
                LOGGER.error("parent block of block_num: "+block.block_num+" is none in _handle_new_block, node: " +
                             self._validator_connect[-1])
            #LOGGER.info("\n\n\npending_blocks: "+self.node.block_cache.pending_blocks.__str__()+"\n\n\n")
            #lm = Giskard.make_PrepareBlocks(
            #    self.nstate,
            #    Giskard.adhoc_ParentBlock_msg(self.nstate, parent_block),
            #    self.node.block_cache)
            self.nstate, lm = \
                Giskard.process_PrepareQC_last_block_new_proposer_set(
                    self.nstate, self.nstate.in_messages[0], self.node.block_cache, self.peers)
            self.node.block_cache.blocks_proposed_num += len(lm)
            #LOGGER.info("\n\n\npending_blocks: " + self.node.block_cache.pending_blocks.__str__() + "\n\n\n")
            LOGGER.info("propose new block from handle_new_block: count msgs: " + str(len(lm)))
            self.node.block_cache.blocks_proposed_num += len(lm)
            state_msg = pickle.dumps([self.nstate, lm], pickle.DEFAULT_PROTOCOL)
            tracker: zmq.MessageTracker = self.socket.send(state_msg, 0, False, True)
            while not tracker.done:
                continue
            self.node.block_cache.blocks_reached_qc_current_view = []  # reset those as view change happened
            self._send_out_msgs(lm)
            for msg in lm:
                self._handle_prepare_block(msg)
        # Giskard End ---------

    def _handle_valid_block(self, block_id):
        self._validating_blocks.discard(block_id)
        block = self._get_block(block_id)
        LOGGER.info('Validated %s', block)

        if self._check_consensus(block):
            LOGGER.info('Passed consensus check: %s', block.block_id.hex())
            self._pending_forks_to_resolve.push(block)
            self._process_pending_forks()
        else:
            LOGGER.info('Failed consensus check: %s', block.block_id.hex())
            self._fail_block(block.block_id)

    def _handle_invalid_block(self, block_id):
        self._validating_blocks.discard(block_id)
        block = self._get_block(block_id)
        LOGGER.info('Block invalid: %s', block)
        self._fail_block(block.block_id)

    def _handle_peer_connected(self, msg):
        self.peers.append(msg.peer_id.hex())
        self.peers.sort(key=lambda h: int(h, 16))

    def handle_peer_disconnected(self, msg):
        self.peers.remove(msg.peer_id.hex())
        self.peers.sort(key=lambda h: int(h, 16))

    def _handle_peer_msgs(self, msg):
        # PoET does not care about peer notifications
        # LOGGER.info("Peer msg: " + jsonpickle.decode(msg[0].content,None,None,False,True,False,GiskardMessage).__str__())
        LOGGER.info("Peer msg: " + str(msg[0].content))
        gmsg = jsonpickle.decode(msg[0].content, None, None, False, True, False, GiskardMessage)
        if self.hanging_prepareQC_new_proposer:
            return  # view change all further messages of this view can be discarded
        # TODO handle recovery viewchange msgs if timeout
        if len(self.peers) < self.k_peers or len(self.in_buffer) > 0:  # when not all peers are connected yet, collect msgs in a buffer
            self.in_buffer.append(gmsg)
            return
        handlers = {
            GiskardMessage.CONSENSUS_GISKARD_PREPARE_BLOCK: self._handle_prepare_block,
            GiskardMessage.CONSENSUS_GISKARD_PREPARE_VOTE: self._handle_prepare_vote,
            GiskardMessage.CONSENSUS_GISKARD_VIEW_CHANGE: self._handle_view_change,
            GiskardMessage.CONSENSUS_GISKARD_PREPARE_QC: self._handle_prepare_qc,
            GiskardMessage.CONSENSUS_GISKARD_VIEW_CHANGE_QC: self._handle_view_change_qc
        }

        try:
            handle_msg = handlers[gmsg.message_type]
        except KeyError:
            LOGGER.error('Unknown Messagetype: %s', gmsg.message_type)
        else:
            handle_msg(gmsg)

    def _process_pending_forks(self):
        while not self._committing:
            block = self._pending_forks_to_resolve.pop()
            if block is None:
                break

            self._resolve_fork(block)

    def _resolve_fork(self, block):
        chain_head = self._get_chain_head()

        LOGGER.info(
            'Choosing between chain heads -- current: %s -- new: %s',
            chain_head.block_id.hex(),
            block.block_id.hex())

        if self._switch_forks(chain_head, block):
            LOGGER.info('Committing %s', block.block_id.hex())
            self._commit_block(block.block_id)
            self._committing = True
        else:
            LOGGER.info('Ignoring %s', block.block_id.hex())
            self._ignore_block(block.block_id)

    def _handle_committed_block(self, block_id):
        LOGGER.info(
            'Chain head updated to %s, abandoning block in progress',
            block_id.hex())

        self._cancel_block()

        self._building = False
        self._published = False
        self._committing = False

        self._process_pending_forks()
        # Giskard -------------
        #self.node.block_cache.remove_pending_block(block_id)
        #self.node.block_cache.block_store.remove_uncommitted_block(block_id) # TODO check if also need to do this at _handle_valid_block
        # Giskard End ---------

    def _handle_prepare_block(self, msg: GiskardMessage):
        LOGGER.info("Handle PrepareBlock")
        if msg.block.block_num >= 2:
            self.all_initial_blocks_proposed = True
        if msg.block not in self.node.block_cache.block_store.uncommitted_blocks:
            self.node.block_cache.block_store.uncommitted_blocks.append(msg.block)
        self.node.block_cache.remove_pending_block(msg.block.block_id)
        self.nstate = Giskard.add(self.nstate, msg)
        state_msg = pickle.dumps([self.nstate, []], pickle.DEFAULT_PROTOCOL)
        tracker: zmq.MessageTracker = self.socket.send(state_msg, 0, False, True)
        while not tracker.done:
            continue
        # TODO call PrepareBlockVoteSet differently -> routinely / on parent reached qc event or sth
        if msg.block == GiskardGenesisBlock():
            parent_block = GiskardGenesisBlock()
        else:
            if self._validator_connect[-1] == "0" and msg.block.block_num == 3:
                """import pdb;
                pdb.set_trace()"""
            parent_block = self.node.block_cache.block_store.get_parent_block(msg.block)
        if parent_block is not None and Giskard.prepare_stage(self.nstate, parent_block, self.peers):
            #    """ first block to be proposed apart from the genesis block """
            LOGGER.info("parent in prepare stage")
            #if msg.block.block_num == 5 \
            #        and self._validator_connect[-1] == "0":
            #    import pdb; pdb.set_trace()
            self.nstate, lm = Giskard.process_PrepareBlock_vote_set(
                self.nstate, msg, self.node.block_cache, self.peers)
        else:
            LOGGER.info("parent not in prepare stage")
            self.nstate, lm = Giskard.process_PrepareBlock_pending_vote_set(self.nstate, msg)
        state_msg = pickle.dumps([self.nstate, lm], pickle.DEFAULT_PROTOCOL)
        tracker: zmq.MessageTracker = self.socket.send(state_msg, 0, False, True)
        while not tracker.done:
            continue
        self._send_out_msgs(lm)

    def _handle_prepare_vote(self, msg):
        LOGGER.info("Handle PrepareVote")
        self.node.block_cache.remove_pending_block(msg.block.block_id)
        self.nstate = Giskard.add(self.nstate, msg)
        state_msg = pickle.dumps([self.nstate, []], pickle.DEFAULT_PROTOCOL)
        tracker: zmq.MessageTracker = self.socket.send(state_msg, 0, False, True)
        while not tracker.done:
            continue
        # TODO replace with new get transition function
        if Giskard.prepare_stage(Giskard.process(self.nstate, msg), msg.block, self.peers):
            LOGGER.info("prepvote in prep stage")
            if msg.block not in self.node.block_cache.block_store.uncommitted_blocks:
                self.node.block_cache.block_store.uncommitted_blocks.append(msg.block)
            self.nstate, lm = Giskard.process_PrepareVote_vote_set(
                self.nstate, msg, self.node.block_cache)
        else:
            LOGGER.info("prepvote not in prep stage")
            if Giskard.vote_quorum_in_view(self.nstate, 0, msg.block, self.peers):
                LOGGER.info("but got vote quorum")
            else:
                LOGGER.info("no vote quorum")
            self.nstate, lm = Giskard.process_PrepareVote_wait_set(
                self.nstate, msg)
        for m in lm:
            if m.message_type == GiskardMessage.CONSENSUS_GISKARD_PREPARE_QC:
                if msg.block not in self.node.block_cache.blocks_reached_qc_current_view:
                    #self.node.block_cache.blocks_reached_qc_current_view.append(msg.block)
                    if msg.block.block_index == LAST_BLOCK_INDEX_IDENTIFIER:
                        self.prepareQC_last_view = msg  # TODO maybe change directly to view change here
        state_msg = pickle.dumps([self.nstate, lm], pickle.DEFAULT_PROTOCOL)
        tracker: zmq.MessageTracker = self.socket.send(state_msg, 0, False, True)
        while not tracker.done:
            continue
        self._send_out_msgs(lm)

    def _handle_view_change(self, msg):
        LOGGER.info("Handle ViewChange")
        self.nstate = Giskard.add(self.nstate, msg)
        state_msg = pickle.dumps([self.nstate, []], pickle.DEFAULT_PROTOCOL)
        tracker: zmq.MessageTracker = self.socket.send(state_msg, 0, False, True)
        while not tracker.done:
            continue

    def _handle_prepare_qc(self, msg):
        LOGGER.info("Handle PrepareQC")
        self.node.block_cache.remove_pending_block(msg.block.block_id)
        self.nstate = Giskard.add(self.nstate, msg)
        state_msg = pickle.dumps([self.nstate, []], pickle.DEFAULT_PROTOCOL)
        tracker: zmq.MessageTracker = self.socket.send(state_msg, 0, False, True)
        while not tracker.done:
            continue
        if msg.block not in self.node.block_cache.blocks_reached_qc_current_view:
            self.node.block_cache.blocks_reached_qc_current_view.append(msg.block)
        if msg.block.block_index == LAST_BLOCK_INDEX_IDENTIFIER:
            self.prepareQC_last_view = msg
        if len(self.node.block_cache.blocks_reached_qc_current_view) == LAST_BLOCK_INDEX_IDENTIFIER:
            if Giskard.is_block_proposer(self.node, self.nstate.node_view + 1, self.peers):
                LOGGER.info("last block new proposer node: " + self._validator_connect[-1])
                self.node.block_cache.blocks_proposed_num = 0  # resest proposed blocks count
                if len(self.node.block_cache.pending_blocks) >= 3:
                    self.nstate, lm = \
                        Giskard.process_PrepareQC_last_block_new_proposer_set(
                            self.nstate, msg, self.node.block_cache, self.peers)  # TODO prolong sending state update until all 3 blocks sent?
                    self.node.block_cache.blocks_proposed_num += len(lm)  # for the case less than 3 blocks were proposed
                else:
                    self.hanging_prepareQC_new_proposer = True
                    return
            else:
                LOGGER.info("last block no new proposer node: " + self._validator_connect[-1])
                self.nstate, lm = \
                    Giskard.process_PrepareQC_last_block_set(self.nstate, msg)
                LOGGER.info("not the new proposer: " + self._validator_connect[-1] + " got view: " + str(self.nstate.node_view))
            self.node.block_cache.blocks_reached_qc_current_view = []  # reset those as view change happened
        else:
            LOGGER.info("non-last block")
            self.nstate, lm = Giskard.process_PrepareQC_non_last_block_set(
                self.nstate, msg, self.node.block_cache)
        state_msg = pickle.dumps([self.nstate, lm], pickle.DEFAULT_PROTOCOL)
        tracker: zmq.MessageTracker = self.socket.send(state_msg, 0, False, True)
        while not tracker.done:
            continue
        self._send_out_msgs(lm)
        for msg in lm:
            if msg.message_type == GiskardMessage.CONSENSUS_GISKARD_PREPARE_BLOCK:
                self._handle_prepare_block(msg)

    def _handle_view_change_qc(self, msg):
        LOGGER.info("Handle ViewChangeQC")
        self.nstate = Giskard.add(self.nstate, msg)
        state_msg = pickle.dumps([self.nstate, []], pickle.DEFAULT_PROTOCOL)
        tracker: zmq.MessageTracker = self.socket.send(state_msg, 0, False, True)
        while not tracker.done:
            continue
        self.nstate, lm = Giskard.process_ViewChangeQC_single_set(self.nstate, msg)
        state_msg = pickle.dumps([self.nstate, lm], pickle.DEFAULT_PROTOCOL)
        tracker: zmq.MessageTracker = self.socket.send(state_msg, 0, False, True)
        while not tracker.done:
            continue

    def _send_out_msgs(self, lm):
        LOGGER.info("send message: " + str(len(lm)))
        for msg in lm:  # TODO messages can be received out of order
            LOGGER.info(
                "node: " + self._validator_connect[-1] + " broadcasting: " + str(msg.message_type) + " block: " + str(
                    msg.block.block_num))
            self._service.broadcast(bytes(str(msg.message_type), encoding='utf-8'),
                                    bytes(jsonpickle.encode(msg, unpicklable=True), encoding='utf-8'))
