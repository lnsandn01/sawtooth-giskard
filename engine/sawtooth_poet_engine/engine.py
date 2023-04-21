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

import os
import logging
import queue
import json
import time
import zmq
from collections import namedtuple

import sawtooth_signing as signing
from journal.block_wrapper import LAST_BLOCK_INDEX_IDENTIFIER
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

    def __init__(self, path_config, component_endpoint, validator_connect):
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
        self.dishonest = False
        # if dishonest == "dishonest":
        #    dishonest = True
        self.is_proposer = False
        self.peers = []
        self.k_peers = len([])
        self.node = None
        # original NState from the formal specification
        self.nstate = None  # node identifier TODO get that from the registry service / the epoch protocol
        self.block_cache = None
        self.latest_block_index = 0
        # connection to GiskardTester, to send state updates
        tester_endpoint = self.tester_endpoint(int(self._validator_connect[-1]))
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PUB)
        LOGGER.info("socket created " + tester_endpoint)
        self.socket.connect(tester_endpoint)

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
        LOGGER.info("\n\nChain head: " + str(self._get_chain_head().block_num))
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
        self.block_cache = _BlockCacheProxy(self._service, stream)
        _batch_publisher = _BatchPublisherProxy(stream, signer)
        self.node = GiskardNode(validator_id, 0, self.dishonest, self.block_cache)
        self.nstate = NState(self.node)
        self.socket.send_pyobj(self.nstate)
        """file_name = "/mnt/c/repos/sawtooth-giskard/tests/sawtooth_poet_tests/engine_" + validator_id + ".txt"
        f = open(file_name, "w")
        f.write("")
        f.close()"""
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
            1000: self._handle_prepare_block,
            1001: self._handle_prepare_vote,
            1002: self._handle_view_change,
            1003: self._handle_prepare_qc,
            1004: self._handle_view_change_qc
        }

        while True:
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
                        if type_tag != Message.CONSENSUS_NOTIFY_PEER_CONNECTED \
                                and type_tag != Message.CONSENSUS_NOTIFY_PEER_DISCONNECTED \
                                and type_tag != Message.CONSENSUS_NOTIFY_PEER_MESSAGE:
                            self.socket.send_pyobj(self.nstate)
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
        #
        self.block_cache.pending_blocks.append(block)
        if Giskard.is_block_proposer(self.node, self.nstate.node_view, self.peers) \
                and self.latest_block_index < LAST_BLOCK_INDEX_IDENTIFIER:
            self.nstate, lm = Giskard.propose_block_init_set(self.nstate, None)

        block = PoetBlock(block)
        LOGGER.info('Received %s', block)

        # self._check_block(block.block_id)
        # self._validating_blocks.add(block.block_id)

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
        self.peers.append(msg.peer_id)
        self.peers.sort(key=lambda h: int(h, 16))

    def handle_peer_disconnected(self, msg):
        self.peers.remove(msg.peer_id)
        self.peers.sort(key=lambda h: int(h, 16))

    def _handle_peer_msgs(self, msg):
        # PoET does not care about peer notifications
        pass

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

    def _handle_prepare_block(self, msg):
        pass

    def _handle_prepare_vote(self, msg):
        pass

    def _handle_view_change(self, msg):
        pass

    def _handle_prepare_qc(self, msg):
        pass

    def _handle_view_change_qc(self, msg):
        pass
