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
        self.started = False

        self._validating_blocks = set()
        self._pending_forks_to_resolve = PendingForks()

        """ Giskard stuff"""
        self.dishonest = dishonest  # dishonest nodes propose their own blocks of same height as another block
        self.k_peers = k_peers  # the number of participating nodes in this epoche (as of now only one epoche is tested)

        self.majority_factor = 1
        self.peers = []  # the node_ids of the participating nodes
        self.all_initial_blocks_proposed = False  # used to switch from propose_block_init_set
        self.genesis_received = False  # so the first block to be proposed is the gensis block
        self.new_network = False  # also used for switching between initially proposing the genesis block or not
        self.prepareQC_last_view = None  # the highest prepareQC of the last view, used as the parent block for the view
        self.hanging_prepareQC_new_proposer = False  # used to postpone the generation of new blocks, as there are not always 3 blocks available
        self.in_buffer = []  # used for the case that this node is not fully connected yet, but other nodes are and already sent messages
        self.node = None
        self.nstate = None
        self.sent_prep_votes = {}
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
            """ Do the initial block proposal if necessary """
            if not self.all_initial_blocks_proposed and self.new_network \
                    and len(self.peers) / self.k_peers >= self.majority_factor \
                    and Giskard.is_block_proposer(self.node, self.nstate.node_view, self.peers):
                self._initial_block_proposal()

            """ Are you now connected to a majority of peers, handle the buffered msgs"""
            if len(self.peers) / self.k_peers >= self.majority_factor and len(self.in_buffer) > 0 \
                    and not self.hanging_prepareQC_new_proposer:
                self._handle_buffered_msg()
                continue

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
                    self._write_prepvotes()
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
        handlers_peer = {
            GiskardMessage.CONSENSUS_GISKARD_PREPARE_BLOCK: self._handle_prepare_block,
            GiskardMessage.CONSENSUS_GISKARD_PREPARE_VOTE: self._handle_prepare_vote,
            GiskardMessage.CONSENSUS_GISKARD_VIEW_CHANGE: self._handle_view_change,
            GiskardMessage.CONSENSUS_GISKARD_PREPARE_QC: self._handle_prepare_qc,
            GiskardMessage.CONSENSUS_GISKARD_VIEW_CHANGE_QC: self._handle_view_change_qc
        }
        try:
            gmsg = self.in_buffer.pop(0)
            if gmsg.view < self.nstate.node_view:
                return
            handle_peer_msg = handlers_peer[gmsg.message_type]
        except:
            LOGGER.error('Unknown Messagetype: %s', gmsg.message_type)
        else:
            handle_peer_msg(gmsg)

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
            LOGGER.info("propose new blocks from handle_new_block")
            self._prepareQC_last_block_new_proposer(self.nstate.in_messages[0])

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

        if self.hanging_prepareQC_new_proposer:
            return  # view change all further messages of this view can be discarded
            # TODO handle recovery viewchange msgs if timeout

        gmsg = jsonpickle.decode(msg[0].content, None, None, False, True, False, GiskardMessage)
        if gmsg.view < self.nstate.node_view:
            LOGGER.info("Discarded msg, old view")
            return
        if len(self.peers) / self.k_peers < self.majority_factor \
                or len(self.in_buffer) > 0:  # when not all peers are connected yet, collect msgs in a buffer
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
        """ If the sender of the propose block msg is not the proposer of the current view, discard msg
        except you are a malicious node """
        signer = msg.block.signer_id
        if hasattr(msg.block.signer_id, 'hex'):
            signer = msg.block.signer_id.hex()
        if not self.node.dishonest \
                and not Giskard.is_block_proposer(signer, self.nstate.node_view, self.peers):
            self.nstate, lm = Giskard.process_PrepareBlock_wrong_proposer_set(self.nstate, msg)
            LOGGER.info("node: " + self._validator_connect[-1] + " discarded ProposeBlock: sender is not the proposer")
            self._send_state_update(lm)
            self._send_out_msgs(lm)
            return

        """ blocks that are not committed in the network yet,
        we need this as we still need to iterate over them in the block cache to check for parent relations"""
        if msg.block not in self.node.block_cache.block_store.uncommitted_blocks:
            if self.node.dishonest:
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
            if not self.node.dishonest \
                    or Giskard.is_block_proposer(self.node, self.nstate.node_view, self.peers):  # only propose malicious blocks when you are not the proposer for now
                self.nstate, lm = Giskard.process_PrepareBlock_vote_set(
                    self.nstate, msg, self.node.block_cache, self.peers)
            else:  # dishonest nodes don't check if there exists a same height block
                """ Malicious behaviour """
                # Double voting possible
                self.nstate, lm = Giskard.process_PrepareBlock_malicious_vote_set(
                    self.nstate, msg, self.node.block_cache, self.peers)
                """ Proposing own version of received block,
                so there exist malicious blocks in the first place """
                if msg.block.payload != "Beware, I am a malicious block":  # Only propose one malicious block per received block
                    malicious_block = copy.deepcopy(msg.block)
                    malicious_block.payload = "Beware, I am a malicious block"
                    malicious_block.signer_id = self.node.node_id
                    if parent_block is None:
                        parent_block = msg.piggyback_block
                    previous_adhoc_msg = Giskard.adhoc_ParentBlock_msg(self.nstate,
                                                                       parent_block)
                    malicious_propose = Giskard.make_PrepareBlock(self.nstate, previous_adhoc_msg,
                                                                  self.node.block_cache, msg.block.block_index,
                                                                  malicious_block)
                    LOGGER.info("node: " + self._validator_connect[-1] + " proposed a malicious block: " + str(malicious_block.block_num))
        else:
            self.nstate, lm = Giskard.process_PrepareBlock_pending_vote_set(self.nstate, msg)
        self._send_state_update(lm)
        self._send_out_msgs(lm)
        if malicious_propose is not None:
            self._send_out_msgs([malicious_propose])
            self._handle_prepare_block(malicious_propose)

    def _handle_prepare_vote(self, msg):
        LOGGER.info("Handle PrepareVote")
        """ Pending blocks are to-be-proposed blocks, if it is already proposed, remove it here """
        """ Check if block legit """
        signer = msg.block.signer_id
        if hasattr(msg.block.signer_id, 'hex'):
            signer = msg.block.signer_id.hex()
        if not self.node.dishonest \
                and not Giskard.is_block_proposer(signer, self.nstate.node_view, self.peers):
            return

        self.node.block_cache.remove_pending_block(msg.block.block_id)

        self.nstate = Giskard.add(self.nstate, msg)
        self._send_state_update([])

        if Giskard.prepare_stage(Giskard.process(self.nstate, msg), msg.block, self.peers):
            LOGGER.info("PrepareVote: block in prepare stage")
            """ block reached prepare stage, add it to the uncommitted blocks """
            if msg.block not in self.node.block_cache.block_store.uncommitted_blocks:
                self.node.block_cache.block_store.uncommitted_blocks.append(msg.block)
            self.nstate, lm = Giskard.process_PrepareVote_vote_set(
                self.nstate, msg, self.node.block_cache)
        else:
            LOGGER.info("PrepareVote: block not in prepare stage")
            self.nstate, lm = Giskard.process_PrepareVote_wait_set(
                self.nstate, msg)
        for m in lm:
            if m.message_type == GiskardMessage.CONSENSUS_GISKARD_PREPARE_QC \
                    and m.view == self.nstate.node_view:
                if msg.block not in self.node.block_cache.blocks_reached_qc_current_view:
                    # self.node.block_cache.blocks_reached_qc_current_view.append(msg.block)  # wait until another node sends prepareqc for this block, as transitions are triggered by receiving a msg
                    if msg.block.block_index == LAST_BLOCK_INDEX_IDENTIFIER:
                        self.prepareQC_last_view = msg  # TODO maybe change directly to view change here
        self._send_state_update(lm)
        self._send_out_msgs(lm)

    def _handle_view_change(self, msg):
        LOGGER.info("Handle ViewChange")
        self.nstate = Giskard.add(self.nstate, msg)
        self._send_state_update([])

    def _handle_prepare_qc(self, msg):
        LOGGER.info("Handle PrepareQC")
        """ Check if block is legit """
        signer = msg.block.signer_id
        if hasattr(msg.block.signer_id, 'hex'):
            signer = msg.block.signer_id.hex()
        if not self.node.dishonest \
                and not Giskard.is_block_proposer(signer, self.nstate.node_view, self.peers):
            return
        """ Pending blocks are to-be-proposed blocks, if it is already proposed, remove it here """
        self.node.block_cache.remove_pending_block(msg.block.block_id)

        self.nstate = Giskard.add(self.nstate, msg)
        self._send_state_update([])

        """ The third block to reach qc in the view leads to a view change """
        if msg.block not in self.node.block_cache.blocks_reached_qc_current_view \
                and msg.view == self.nstate.node_view:
            self.node.block_cache.blocks_reached_qc_current_view.append(msg.block)
        """ prepareQC_last_view will be used for block generation for the next view """
        if msg.block.block_index == LAST_BLOCK_INDEX_IDENTIFIER:
            self.prepareQC_last_view = msg

        """ 3 blocks reached qc -> view change; propose blocks if you are the next proposer """
        if len(self.node.block_cache.blocks_reached_qc_current_view) == LAST_BLOCK_INDEX_IDENTIFIER:
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
            """ Less than 3 blocks have reached qc -> only process the msg """
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

    def _handle_view_change_qc(self, msg):
        """ Timeout occured, which led to view change msgs being exchanged,
        which reached a quorum -> increment view, next proposer, propose blocks """
        LOGGER.info("Handle ViewChangeQC")
        self.nstate = Giskard.add(self.nstate, msg)
        self._send_state_update([])
        self.nstate, lm = Giskard.process_ViewChangeQC_single_set(self.nstate, msg)
        self._send_state_update(lm)

    def _send_state_update(self, lm):
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

    def _write_prepvotes(self):
        """ for msg in lm:
            if msg.message_type == GiskardMessage.CONSENSUS_GISKARD_PREPARE_VOTE:
                if msg.block.block_id.hex() not in self.sent_prep_votes.keys():
                    self.sent_prep_votes.update({msg.block.block_id.hex(): 1})
                else:
                    self.sent_prep_votes[msg.block.block_id.hex()] += 1
        self._write_prepvotes() """
        file_name = "/mnt/c/repos/sawtooth-giskard/tests/sawtooth_poet_tests/prep_votes_node" \
            + self._validator_connect[-1] + ".txt"
        f = open(file_name, 'w')
        f.write(self.sent_prep_votes.__str__())
        f.close()

