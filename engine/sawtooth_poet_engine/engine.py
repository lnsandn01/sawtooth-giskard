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
from itertools import filterfalse

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
LOGGER.setLevel(logging.INFO)


class GiskardEngine(Engine):
    """The entrypoint for Giskard
        Keeps state
        Handles incoming messages, validates blocks
        Proposes new Blocks if it is Proposer in the current view"""

    def __init__(self, path_config, component_endpoint, validator_connect, dishonest=False, k_peers=2, timeout_test=0):
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
        self.started = False

        self._validating_blocks = set()
        self._pending_forks_to_resolve = PendingForks()

        """ Giskard stuff"""
        self.dishonest = dishonest  # dishonest nodes propose their own blocks of same height as another block
        if self.dishonest:
            LOGGER.info("I am dishonest, node: " + self._validator_connect[-1])
        self.k_peers = k_peers  # the number of participating nodes in this epoche (as of now only one epoche is tested)
        self.timeout_test = timeout_test  # choosing the kind of controlled timeout test

        self.majority_factor = 1
        self.peers = []  # the node_ids of the participating nodes
        self.all_initial_blocks_proposed = False  # used to switch from propose_block_init_set
        self.genesis_received = False  # so the first block to be proposed is the gensis block
        self.new_network = False  # also used for switching between initially proposing the genesis block or not
        self.prepareQC_last_view = None  # the highest prepareQC of the last view, used as the parent block for the view
        self.hanging_prepareQC_new_proposer = False  # used to postpone the generation of new blocks, as there are not always 3 blocks available
        self.hanging_ViewChange_new_proposer = False
        self.in_buffer = []  # used for the case that this node is not fully connected yet, but other nodes are and already sent messages
        self.node = None
        self.nstate = None
        self.sent_prep_votes = {}
        self.recv_malicious_block = False
        self.msg_from_buffer = False
        self.start_time_view = 0
        self.timeout_after = 20000
        self.accept_msgs_after_timeout = False
        self.test_special_timeout = False
        """ connection to GiskardTester, to send state updates """
        tester_endpoint = self.tester_endpoint(int(self._validator_connect[-1]))
        self.context = zmq.Context()  # zmq socket to send nstate updates after a transition via tcp
        self.socket = self.context.socket(zmq.PUSH)
        self.socket.connect(tester_endpoint)
        LOGGER.info("socket connected " + tester_endpoint)

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
        if self.started:
            return
        self.started = True
        self._service = service

        self._oracle = PoetOracle(
            service=service,
            component_endpoint=self._component_endpoint,
            config_dir=self._path_config.config_dir,
            data_dir=self._path_config.data_dir,
            key_dir=self._path_config.key_dir)

        signer = _load_identity_signer(self._path_config.key_dir, 'validator')
        validator_id = signer.get_public_key().as_hex()
        """ Giskard stuff """
        self.peers.append(validator_id)  # append myself to the peers list, which is used to check for quorums
        stream = Stream(self._component_endpoint)  # message stream for the block iterator in the block_cache
        block_cache = _BlockCacheProxy(self._service, stream)  # here are blocks stored
        _batch_publisher = _BatchPublisherProxy(stream, signer)
        self.node = GiskardNode(validator_id, self.dishonest, block_cache)
        self.nstate = NState(self.node)
        self.new_network = True  # self._get_chain_head() == GiskardGenesisBlock()
        if self._get_chain_head().block_num >= 2:
            self.all_initial_blocks_proposed = True
        # send empty state to the GiskardTester
        self._send_state_update([])
        self.start_time_view = time.time()

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
            self._timeout_tests(self.timeout_test)  # for controlled testing of timeout behaviour

            if time.time() > self.start_time_view + self.timeout_after and not self.nstate.timeout:
                self._trigger_view_change()
            """ Do the initial block proposal if necessary """
            if not self.all_initial_blocks_proposed and self.new_network \
                    and len(self.peers) / self.k_peers >= self.majority_factor \
                    and Giskard.is_block_proposer(self.node, self.nstate.node_view, self.peers):
                self._initial_block_proposal()

            """ Are you now connected to a majority of peers, handle the buffered msgs"""
            if len(self.peers) / self.k_peers >= self.majority_factor and len(self.in_buffer) > 0 \
                    and not self.hanging_prepareQC_new_proposer:
                self._handle_buffered_msg()

            """ handle incoming messages """
            try:
                try:
                    type_tag, data = updates.get(timeout=0.1)
                except queue.Empty:
                    pass
                else:
                    LOGGER.debug('Received message: %s',
                                 Message.MessageType.Name(type_tag))
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
                    LOGGER.info("\n\n\nCalled exit on engine\n\n\n")
                    self.socket.close()
                    self.context.destroy()
                    break

                self._try_to_publish()
            except Exception:  # pylint: disable=broad-except
                LOGGER.exception("Unhandled exception in message loop")

    def _initial_block_proposal(self):
        """ The chain head is the genesis block in a new network -> propose it as the initial block """
        if not self.genesis_received:
            self.node.block_cache.pending_blocks.append(self.node.block_cache.block_store.get_genesis())
            self.genesis_received = True
        """ Propose the initial 3 blocks """
        # TODO check what checks are necessary here by poet about the blocks
        if len(self.node.block_cache.pending_blocks) >= 3:
            self.nstate, lm = Giskard.propose_block_init_set(
                self.nstate, None, self.node.block_cache)
            LOGGER.info("propose init blocks; sending: " + str(len(lm)) + " msgs")
            if self.node.block_cache.blocks_proposed_num == LAST_BLOCK_INDEX_IDENTIFIER:
                self.all_initial_blocks_proposed = True
                self._send_state_update(lm)
            self._send_out_msgs(lm)
            for msg in lm:
                if msg.block not in self.node.block_cache.block_store.uncommitted_blocks:
                    self.node.block_cache.block_store.uncommitted_blocks.append(msg.block)
                self._handle_prepare_block(msg)  # send prepare blocks to yourself

    def _handle_buffered_msg(self):
        """ handle buffered msgs after all peers are connected"""
        LOGGER.info("Getting msg from buffer")
        msg = self.in_buffer.pop(0)
        self.msg_from_buffer = True
        self._handle_peer_msgs(msg)

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
        block = GiskardBlock(block)
        LOGGER.info('Received %s', block)
        self._check_block(block.block_id)
        self._validating_blocks.add(block.block_id)
        if block.block_num > 7:  # TODO remove after full tests are done
            return
        """ Giskard """
        """ append to pending blocks, the blocks that are not yet proposed """
        if not self.node.block_cache.pending_blocks_same_height_exists(block.block_num) \
                and block not in self.node.block_cache.block_store.uncommitted_blocks \
                and not Giskard.handled_block_same_height_in_msgs(block, self.nstate):  # \
            # and not self.node.block_cache.block_store.same_height_block_in_storage(block): TODO add back in, as soon as the net uses giskard
            self.node.block_cache.pending_blocks.append(block)
        else:
            LOGGER.info("block_num: " + str(block.block_num) + " has already been handled")
        """ If we are the next proposer and are waiting to receive 3 blocks """
        if self.hanging_prepareQC_new_proposer \
                and len(self.node.block_cache.pending_blocks) >= 3:
            self.hanging_prepareQC_new_proposer = False
            LOGGER.info("propose new blocks from handle_new_block prepare qc new proposer")
            self._prepareQC_last_block_new_proposer(self.nstate.in_messages[0])
        if self.hanging_ViewChange_new_proposer \
                and len(self.node.block_cache.pending_blocks) >= 3:
            self.hanging_ViewChange_new_proposer = False
            LOGGER.info("propose new blocks from handle_new_block view change qc new proposer")
            self._view_changeQC_new_proposer(self.nstate.in_messages[0])

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
        LOGGER.info("peer connected")
        self.peers.append(msg.peer_id.hex())
        self.peers.sort(key=lambda h: int(h, 16))

    def handle_peer_disconnected(self, msg):
        LOGGER.info("peer disconnected")
        self.peers.remove(msg.peer_id.hex())
        self.peers.sort(key=lambda h: int(h, 16))

    def _handle_peer_msgs(self, msg):
        LOGGER.info("Peer msg: " + str(msg[0].content))

        gmsg = jsonpickle.decode(msg[0].content, None, None, False, True, False, GiskardMessage)
        if self.hanging_ViewChange_new_proposer:
            LOGGER.info("Discarded msg: hanging viewchange")
            return
        if self.hanging_prepareQC_new_proposer:
            if gmsg.message_type != GiskardMessage.CONSENSUS_GISKARD_VIEW_CHANGE \
                    and gmsg.message_type != GiskardMessage.CONSENSUS_GISKARD_VIEW_CHANGE_QC:
                LOGGER.info("Discarded msg: hanging prepareqc")
                return
            else:
                if not self.msg_from_buffer:
                    self.in_buffer.append(msg)
            return  # view change all further messages of this view can be discarded
        if gmsg.view < self.nstate.node_view:
            LOGGER.info("Discarded msg, old view")
            return
        if not self.nstate.timeout and \
                (gmsg.message_type == GiskardMessage.CONSENSUS_GISKARD_VIEW_CHANGE \
                 or gmsg.message_type == GiskardMessage.CONSENSUS_GISKARD_VIEW_CHANGE_QC):
            self.in_buffer.append(msg)
            return
        """ after timeout, don't handle any other msg than viewchange msgs """
        if self.nstate.timeout and gmsg.message_type != GiskardMessage.CONSENSUS_GISKARD_VIEW_CHANGE \
                and gmsg.message_type != GiskardMessage.CONSENSUS_GISKARD_VIEW_CHANGE_QC\
                and gmsg.message_type != GiskardMessage.CONSENSUS_GISKARD_PREPARE_QC:
            LOGGER.info("Discarded msg, not a viewchange/QC nor prepareqc")
            return

        if len(self.peers) / self.k_peers < self.majority_factor \
                or (len(self.in_buffer) > 0
                    and not self.msg_from_buffer):  # when not all peers are connected yet, collect msgs in a buffer
            self.in_buffer.append(msg)
            LOGGER.info("Added msg to in_buffer")
            return
        self.msg_from_buffer = False
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
            """ Check if block legit """
            if gmsg.block.payload == "Beware, I am a malicious block" \
                    and self.dishonest \
                    and not self.recv_malicious_block \
                    and gmsg.message_type == GiskardMessage.CONSENSUS_GISKARD_PREPARE_BLOCK:
                LOGGER.info("other node recieved malicious block")
                self.recv_malicious_block = True
            signer = gmsg.block.signer_id
            if hasattr(gmsg.block.signer_id, 'hex'):
                signer = gmsg.block.signer_id.hex()
            if not self.dishonest \
                    and not Giskard.is_block_proposer(signer, self.nstate.node_view, self.peers) \
                    and not gmsg.message_type == GiskardMessage.CONSENSUS_GISKARD_VIEW_CHANGE \
                    and not gmsg.message_type == GiskardMessage.CONSENSUS_GISKARD_VIEW_CHANGE_QC:
                LOGGER.info("Discarded msg, block proposer not the proposer for this view")
                return
            if self.dishonest and gmsg.message_type == GiskardMessage.CONSENSUS_GISKARD_PREPARE_BLOCK:
                self._handle_prepare_block_malicious(gmsg)
                return
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
        # self.node.block_cache.remove_pending_block(block_id)
        # self.node.block_cache.block_store.remove_uncommitted_block(block_id) # TODO check if also need to do this at _handle_valid_block
        # Giskard End ---------

    def _handle_prepare_block(self, msg: GiskardMessage):
        LOGGER.info("Handle PrepareBlock")
        """  needed for when this node changes to block proposer, to not propose the init blocks again """
        if not self.all_initial_blocks_proposed and msg.block.block_num >= 2:
            self.all_initial_blocks_proposed = True

        self.nstate = Giskard.add(self.nstate, msg)
        self._send_state_update([])

        # TODO call PrepareBlockVoteSet differently -> routinely / on parent reached qc event or at the start of the loop
        """ blocks that are not committed in the network yet,
        we need this as we still need to iterate over them in the block cache to check for parent relations"""
        if msg.block not in self.node.block_cache.block_store.uncommitted_blocks:
            self.node.block_cache.block_store.uncommitted_blocks.append(msg.block)
        """ Pending blocks are to-be-proposed blocks, if it is already proposed, remove it here """
        # self.node.block_cache.remove_pending_block(msg.block.block_id)
        # if msg.block.block_num == 4 and msg.view == 2 and Giskard.is_block_proposer(self.nstate.node_id, 0, self.peers):
        #    import pdb;
        #    pdb.set_trace()
        if msg.block == GiskardGenesisBlock():
            parent_block = GiskardGenesisBlock()
        else:
            parent_block = self.node.block_cache.block_store.get_parent_block(msg.block)
        if (parent_block is not None and Giskard.prepare_stage(self.nstate, parent_block, self.peers)) \
                or (msg.block.block_num - 1 == msg.piggyback_block.block_num
                    and msg.block.previous_id == msg.piggyback_block.block_id):  # get prepareqc from msg
            LOGGER.info("parent in prepare stage")
            self.nstate, lm = Giskard.process_PrepareBlock_vote_set(
                self.nstate, msg, self.node.block_cache, self.peers)
        else:
            self.nstate, lm = Giskard.process_PrepareBlock_pending_vote_set(self.nstate, msg)
        self._send_state_update(lm)
        self._send_out_msgs(lm)

    def _handle_prepare_vote(self, msg):
        LOGGER.info("Handle PrepareVote")
        """ Pending blocks are to-be-proposed blocks, if it is already proposed, remove it here """
        # self.node.block_cache.remove_pending_block(msg.block.block_id)

        self.nstate = Giskard.add(self.nstate, msg)
        self._send_state_update([])

        if Giskard.prepare_stage_in_view(Giskard.process(self.nstate, msg), self.nstate.node_view, msg.block, self.peers):
            LOGGER.info("PrepareVote: block in prepare stage")
            """ block reached prepare stage, add it to the uncommitted blocks """
            if msg.block not in self.node.block_cache.block_store.uncommitted_blocks:
                self.node.block_cache.block_store.uncommitted_blocks.append(msg.block)
            self.node.block_cache.remove_pending_block(msg.block.block_id)
            self.nstate, lm = Giskard.process_PrepareVote_vote_set(
                self.nstate, msg, self.node.block_cache)
        else:
            LOGGER.info("PrepareVote: block not in prepare stage")
            self.nstate, lm = Giskard.process_PrepareVote_wait_set(
                self.nstate, msg)
        contains_qc_4 = False
        for m in lm:
            if m.message_type == GiskardMessage.CONSENSUS_GISKARD_PREPARE_QC \
                    and m.view == self.nstate.node_view:
                if msg.block not in self.node.block_cache.blocks_reached_qc_current_view:
                    # self.node.block_cache.blocks_reached_qc_current_view.append(msg.block)  # wait until another node sends prepareqc for this block, as transitions are triggered by receiving a msg
                    if msg.block.block_index == LAST_BLOCK_INDEX_IDENTIFIER \
                            and msg.block.payload != "Beware, I am a malicious block":  # malicious block only to test transitions and height injectivities
                        self.prepareQC_last_view = msg  # TODO maybe change directly to view change here
                if self.test_special_timeout and m.block.block_num == 4 and self.nstate.node_view == 1:
                    contains_qc_4 = True
        self._send_state_update(lm)
        """ Do not broadcast this prepqc for this special test
        block 4 should be in local prep stage for only this node"""
        if self.test_special_timeout and contains_qc_4:
            lm[:] = [m for m in lm if m.message_type != GiskardMessage.CONSENSUS_GISKARD_PREPARE_QC]
        self._send_out_msgs(lm)

    def _handle_prepare_qc(self, msg):
        LOGGER.info("Handle PrepareQC")
        """ Pending blocks are to-be-proposed blocks, if it is already proposed, remove it here """
        self.node.block_cache.remove_pending_block(msg.block.block_id)

        self.nstate = Giskard.add(self.nstate, msg)
        self._send_state_update([])

        """ The third block to reach qc in the view leads to a view change """
        if msg.block not in self.node.block_cache.blocks_reached_qc_current_view \
                and msg.view == self.nstate.node_view \
                and msg.block.payload != "Beware, I am a malicious block":  # malicious block only to test transitions and height injectivities:
            self.node.block_cache.blocks_reached_qc_current_view.append(msg.block)
        """ prepareQC_last_view will be used for block generation for the next view """
        if msg.block.block_index == LAST_BLOCK_INDEX_IDENTIFIER \
                and msg.block.payload != "Beware, I am a malicious block":  # malicious block only to test transitions and height injectivities
            self.prepareQC_last_view = msg

        """ 3 blocks reached qc -> view change; propose blocks if you are the next proposer """
        if len(self.node.block_cache.blocks_reached_qc_current_view) == LAST_BLOCK_INDEX_IDENTIFIER \
                and not self.nstate.timeout:
            self.start_time_view = time.time()  # start new view time directly here
            if Giskard.is_block_proposer(self.node, self.nstate.node_view + 1, self.peers):
                LOGGER.info("last block qc new proposer node: " + self._validator_connect[-1])
                """ Only propose blocks when you have 3 ready in the pending block cache """
                if len(self.node.block_cache.pending_blocks) >= 3:
                    LOGGER.info("propose new blocks from handle_prepare_qc")
                    self._prepareQC_last_block_new_proposer(msg)
                    return
                else:
                    """ postpone until 3 new blocks are received """
                    self.hanging_prepareQC_new_proposer = True
                    return
            else:
                """ You are not the next proposer, increment the view """
                self.nstate, lm = Giskard.process_PrepareQC_last_block_set(self.nstate, msg)
                LOGGER.info("last block qc not the new proposer: "
                            + self._validator_connect[-1]
                            + " got view: " + str(self.nstate.node_view))
                self.node.block_cache.blocks_reached_qc_current_view = []  # reset those as view change happened
        else:
            """ Less than 3 blocks have reached qc, or timeout period -> only process the msg """
            LOGGER.info("not all blocks qc yet")
            self.nstate, lm = Giskard.process_PrepareQC_non_last_block_set(
                self.nstate, msg, self.node.block_cache)
        self._send_state_update(lm)
        self._send_out_msgs(lm)
        for msg in lm:
            if msg.message_type == GiskardMessage.CONSENSUS_GISKARD_PREPARE_BLOCK:
                self._handle_prepare_block(msg)

    def _prepareQC_last_block_new_proposer(self, msg):
        """ Call this function when you are the proposer of the next view,
        and you have received 3 new blocks to be proposed """
        self.node.block_cache.blocks_proposed_num = 0  # reset proposed blocks count
        self.node.block_cache.pending_blocks[:] = [
            block for block in self.node.block_cache.pending_blocks
            if not Giskard.handled_block_same_height_in_msgs(block, self.nstate)
        ]
        self.nstate, lm = \
            Giskard.process_PrepareQC_last_block_new_proposer_set(
                self.nstate, msg, self.node.block_cache, self.peers)
        self.node.block_cache.blocks_proposed_num = 3
        self.node.block_cache.blocks_reached_qc_current_view = []  # reset those as view change happened
        self._send_state_update(lm)
        self._send_out_msgs(lm)
        for msg in lm:
            if msg.message_type == GiskardMessage.CONSENSUS_GISKARD_PREPARE_BLOCK:
                self._handle_prepare_block(msg)

    def _trigger_view_change(self):
        LOGGER.info("Trigger View Change")
        self.accept_msgs_after_timeout = False
        self.nstate = Giskard.flip_timeout(self.nstate)
        if len(self.nstate.in_messages) > 0:
            hanging_prepareqc = self.nstate.in_messages.pop(0)  # hanging PrepareQC could still be in the in_buffer
        self._send_state_update([])
        """ hanging proposers are still in the old view,
        as the state update is done in one step when there are 3 pending blocks
        update state here without proposing blocks """
        if self.hanging_prepareQC_new_proposer:
            self.nstate.node_view += 1  # TODO check if this makes transitions not work
            self.hanging_prepareQC_new_proposer = False
        view_change_msg = Giskard.make_ViewChange(self.nstate, self.peers)
        """ Special Test: Send ViewChange msg with parent block,
        as the timeout node with the locally higher block, 
        this is to guarantee a quorum of ViewChange messages,
        without introducing block 4 as the highest block """
        if self.test_special_timeout \
                and Giskard.is_block_proposer(self.node, 0, self.peers) \
                and self.nstate.node_view == 1:
            view_change_msg.block = Giskard.parent_of(view_change_msg.block, self.node.block_cache)
        self._send_out_msgs([view_change_msg])
        self._handle_view_change(view_change_msg)

    def _handle_view_change(self, msg):
        LOGGER.info("Handle ViewChange")
        if self.hanging_prepareQC_new_proposer and len(self.nstate.in_messages) > 0:
            hanging_prepareqc = self.nstate.in_messages.pop(0)  # hanging PrepareQC could still be in the in_buffer
        self.node.block_cache.remove_pending_block(msg.block.block_id)

        self.nstate = Giskard.add(self.nstate, msg)
        self._send_state_update([])
        if Giskard.view_change_quorum_in_view(Giskard.process(self.nstate, msg),
                                              # ViewChange messages can be processed during timeout.It is important that the parameter here is (process state msg) and not simply state
                                              self.nstate.node_view,
                                              self.peers):
            LOGGER.info("ViewChange quorum reached")
            self.accept_msgs_after_timeout = True
            # TODO add to transition test viewchange
            if Giskard.is_block_proposer(self.nstate.node_id, self.nstate.node_view + 1, self.peers):
                LOGGER.info("ViewChange new proposer")
                # TODO if own block was not chosen and was higher -> re-add to pending blocks
                if len(self.node.block_cache.pending_blocks) >= 3:
                    LOGGER.info("propose new blocks from view change quorum new proposer")
                    self._view_changeQC_new_proposer(msg)
                    return
                else:
                    """ postpone until 3 new blocks are received """
                    self.hanging_ViewChange_new_proposer = True
                    return
            else:
                LOGGER.info("ViewChange not the new proposer for view: " + str(self.nstate.node_view + 1) \
                            + " , wait for ViewChangeQC message")
                self.nstate, lm = Giskard.process_ViewChange_quorum_not_new_proposer_set(self.nstate, msg)
        else:
            LOGGER.info("ViewChange pre quorum")
            self.nstate, lm = Giskard.process_ViewChange_pre_quorum_set(self.nstate, msg)
        self._send_state_update(lm)
        self._send_out_msgs(lm)
        for msg in lm:
            if msg.message_type == GiskardMessage.CONSENSUS_GISKARD_PREPARE_BLOCK:
                self._handle_prepare_block(msg)

    def _handle_view_change_qc(self, msg):
        """ Timeout occured, which led to view change msgs being exchanged,
        which reached a quorum -> increment view, next proposer, propose blocks """
        if Giskard.is_block_proposer(self.nstate.node_id, self.nstate.node_view + 1, self.peers):
            LOGGER.info("Discarded ViewChangeQC message, which lets the next proposer increment its view 2 times")
            return
        LOGGER.info("Handle ViewChangeQC")
        self.nstate = Giskard.add(self.nstate, msg)
        self._send_state_update([])

        """ remove blocks as they gonna be re-proposed """
        self.node.block_cache.block_store.uncommitted_blocks[:] = [
            block for block in self.node.block_cache.block_store.uncommitted_blocks
            if block.block_num <= msg.block.block_num
        ]
        self.node.block_cache.blocks_reached_qc_current_view = []

        self.nstate, lm = Giskard.process_ViewChangeQC_single_set(self.nstate, msg)
        self._send_state_update(lm)

    def _view_changeQC_new_proposer(self, msg):
        """ Call this function when you are the proposer of the next view,
        and you have received 3 new blocks to be proposed """
        self.node.block_cache.blocks_proposed_num = 0  # reset proposed blocks count

        self.nstate, lm = \
            Giskard.process_ViewChange_quorum_new_proposer_set(
                self.nstate, msg, self.node.block_cache)
        self.node.block_cache.blocks_proposed_num = 3
        self.node.block_cache.blocks_reached_qc_current_view = []  # reset those as view change happened
        self._send_state_update(lm)
        self._send_out_msgs(lm)
        for msg in lm:
            if msg.message_type == GiskardMessage.CONSENSUS_GISKARD_PREPARE_BLOCK:
                self.node.block_cache.block_store.uncommitted_blocks[:] = [
                    block for block in self.node.block_cache.block_store.uncommitted_blocks
                    if block.block_num <= msg.block.block_num
                ]
                self._handle_prepare_block(msg)

    def _timeout_tests(self, test: int):
        """ As networks can be very unpredictable,
        is timeout behaviour tested in a controlled way,
        for only one timeout per run.
        TODO An actual timeout protocol / synchronization protocol is needed """
        if test == 0:
            return
        if test == 1:  # first block of view is carryover block
            if self.nstate.node_view == 1 \
                    and Giskard.highest_prepare_block_in_view(self.nstate,
                                                              self.nstate.node_view,
                                                              self.peers).block_num == 3:
                self.timeout_after = 0  # timeout immediately
            else:
                self.timeout_after = 20000
        elif test == 2:  # second block of view is carryover block
            if self.nstate.node_view == 1 \
                    and Giskard.highest_prepare_block_in_view(self.nstate,
                                                              self.nstate.node_view,
                                                              self.peers).block_num == 4:
                self.timeout_after = 0  # timeout immediately
            else:
                self.timeout_after = 20000
        elif test == 3:  # last block of view is carryover block; after viewchange
            if self.nstate.node_view == 1 \
                    or (self.hanging_prepareQC_new_proposer
                        and self.nstate.node_view == 0):  # hanging_prepareqc, as new proposer should also timeout the same as other nodes, even though it hasn't reached the next view yet
                self.timeout_after = 5  # recieving 3 new blocks takes longer than 5 seconds -> guaranteed timeout in view 1
            else:
                self.timeout_after = 20000  # to make sure no more timeouts that disrupt testing
        elif test == 4:
            """ only one node will receive all initial votes for block 4
            all other nodes will discard them, as they already timeout
            This test is intended to run with at least 3 nodes, 
            as quorum > 2/3, will also the viewchange msg with block 4 be received """
            self.test_special_timeout = True
            if self.nstate.node_view == 1 \
                    and ((Giskard.highest_prepare_block_in_view(self.nstate,
                                                                self.nstate.node_view,
                                                                self.peers).block_num == 3
                          and not Giskard.is_block_proposer(self.node, 0, self.peers))
                         or (Giskard.highest_prepare_block_in_view(self.nstate,
                                                                   self.nstate.node_view,
                                                                   self.peers).block_num == 4
                             and Giskard.is_block_proposer(self.node, 0, self.peers))):
                self.timeout_after = 0  # timeout immediately
            else:
                self.timeout_after = 20000

    def _handle_prepare_block_malicious(self, msg: GiskardMessage):
        """ A malicious node is allowed to double vote in the same view """
        LOGGER.info("Handle PrepareBlockMalicious")
        if msg.block.payload == "Beware, I am a malicious block":
            LOGGER.info("Handling a malicious prepareblock")
        """  needed for when this node changes to block proposer, to not propose the init blocks again """
        if not self.all_initial_blocks_proposed and msg.block.block_num >= 2:
            self.all_initial_blocks_proposed = True

        self.nstate = Giskard.add(self.nstate, msg)
        self._send_state_update([])

        # TODO call PrepareBlockVoteSet differently -> routinely / on parent reached qc event or at the start of the loop
        """ blocks that are not committed in the network yet,
        we need this as we still need to iterate over them in the block cache to check for parent relations"""
        if msg.block not in self.node.block_cache.block_store.uncommitted_blocks:
            """ remove competing block """
            for b in self.node.block_cache.block_store.uncommitted_blocks:
                if b.block_num == msg.block.block_num \
                        and msg.block.payload == "Beware, I am a malicious block":
                    self.node.block_cache.block_store.remove_uncommitted_block(b.block_id)
            self.node.block_cache.block_store.uncommitted_blocks.append(msg.block)
        """ Pending blocks are to-be-proposed blocks, if it is already proposed, remove it here """
        self.node.block_cache.remove_pending_block(msg.block.block_id)
        malicious_propose = None
        if msg.block == GiskardGenesisBlock():
            parent_block = GiskardGenesisBlock()
        else:
            parent_block = self.node.block_cache.block_store.get_parent_block(msg.block)
        if (parent_block is not None and Giskard.prepare_stage(self.nstate, parent_block, self.peers)) \
                or (msg.block.block_num - 1 == msg.piggyback_block.block_num
                    and msg.block.previous_id == msg.piggyback_block.block_id):  # get prepareqc from msg
            LOGGER.info("parent in prepare stage")
            """ Malicious behaviour """
            # Double voting possible
            self.nstate, lm = Giskard.process_PrepareBlock_malicious_vote_set(
                self.nstate, msg, self.node.block_cache, self.peers)
            if len(lm) > 0:
                if msg.block.signer_id == "NotTrustworthy":
                    LOGGER.info("voted for nottrustworthy")
            """ Generate one malicious block for testing """
            if msg.block.payload != "Beware, I am a malicious block" \
                    and not self.recv_malicious_block \
                    and (msg.block.block_num == 3):
                malicious_propose = self._malicious_propose(msg, parent_block)
        else:
            self.nstate, lm = Giskard.process_PrepareBlock_pending_vote_set(self.nstate, msg)
        self._send_state_update(lm)
        self._send_out_msgs(lm)
        if malicious_propose is not None:
            LOGGER.info(malicious_propose)
            self._send_out_msgs([malicious_propose])
            self._handle_prepare_block_malicious(malicious_propose)

    def _malicious_propose(self, msg, parent_block):
        LOGGER.info("trying to be malicious")
        self.recv_malicious_block = True
        malicious_block = copy.deepcopy(msg.block)
        malicious_block.payload = "Beware, I am a malicious block"
        malicious_block.signer_id = "NotTrustworthy"
        if parent_block is None:
            parent_block = msg.piggyback_block
            previous_adhoc_msg = Giskard.adhoc_ParentBlock_msg(self.nstate,
                                                               parent_block,
                                                               self.nstate.node_view - 1)
        else:
            previous_adhoc_msg = Giskard.get_quorum_msg_for_block(self.nstate, parent_block, self.peers)
        malicious_propose = Giskard.make_PrepareBlock(self.nstate, previous_adhoc_msg,
                                                      self.node.block_cache, msg.block.block_index,
                                                      malicious_block)
        LOGGER.info("node: " + self._validator_connect[-1] + " proposed a malicious block: " + str(
            malicious_block.block_num))
        return malicious_propose

    def _send_state_update(self, lm):
        """ Sends state updates to the GiskardTester
        this slows the network down by a lot,
        on a simple ubuntu vm will the GiskardTester be killed,
        when you have more than 4 nodes and or 11 blocks """
        state_msg = pickle.dumps([self.nstate, lm], pickle.DEFAULT_PROTOCOL)
        tracker: zmq.MessageTracker = self.socket.send(state_msg, 0, False, True)
        while not tracker.done:
            continue

    def _send_out_msgs(self, lm):
        """ Broadcasts messages to all connected peers """
        LOGGER.info("send message: " + str(len(lm)))
        for msg in lm:  # TODO messages can be received out of order
            LOGGER.info(
                "node: " + self._validator_connect[-1] + " broadcasting: " + str(msg.message_type) + " block: " + str(
                    msg.block.block_num))

            self._service.broadcast(bytes(str(msg.message_type), encoding='utf-8'),
                                    bytes(jsonpickle.encode(msg, unpicklable=True), encoding='utf-8'))
