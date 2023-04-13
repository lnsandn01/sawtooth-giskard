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
from collections import namedtuple

import sawtooth_signing as signing
from sawtooth_signing import CryptoFactory
from sawtooth_signing.secp256k1 import Secp256k1PrivateKey

from sawtooth_sdk.messaging.stream import Stream
from sawtooth_sdk.consensus.engine import Engine
from sawtooth_sdk.consensus import exceptions
from sawtooth_sdk.protobuf.validator_pb2 import Message

from sawtooth_poet_engine.oracle import PoetOracle, PoetBlock, _BlockCacheProxy
from sawtooth_poet_engine.giskard_block import GiskardBlock, GiskardGenesisBlock
from sawtooth_poet_engine.giskard_message import GiskardMessage
from sawtooth_poet_engine.giskard_nstate import NState
from sawtooth_poet_engine.giskard import Giskard
from sawtooth_poet_engine.pending import PendingForks
from sawtooth_poet_engine.giskard_node import GiskardNode


LOGGER = logging.getLogger(__name__)


class GiskardEngine(Engine): # is a GiskardNode
    """The entrypoint for Giskard
        Keeps state
        Handles incoming messages, validates blocks
        Proposes new Blocks if it is Proposer in the current view"""
    def __init__(self, path_config, component_endpoint, dishonest, peers):
        # components
        self._path_config = path_config
        self._component_endpoint = component_endpoint
        self._service = None
        self._signer = self._load_identity_signer(self._path_config.key_dir, 'validator')
        self.validator_id = self._signer.get_public_key().as_hex()

        # mostly still poet state variables
        self._exit = False
        self._published = False
        self._building = False
        self._committing = False
        dishonest = False
        if dishonest == "dishonest":
            dishonest = True
        self._validating_blocks = set()
        self.peers = peers
        self.k_peers = len(peers)

        self.node = GiskardNode(self.validator_id, 0, dishonest)
        # original NState from the formal specification
        self.nstate = NState(self)# node identifier TODO get that from the registry service / the epoch protocol


    # Ignore invalid override pylint issues
    # pylint: disable=invalid-overridden-method
    def name(self):
        return 'Giskard'

    # Ignore invalid override pylint issues
    # pylint: disable=invalid-overridden-method
    def version(self):
        return '0.1'

    def additional_protocols(self):
        return [('giskard', '0.1')]

    def stop(self):
        self._exit = True

    # TODO write tests to compare all running nodes
    def __eq__(self, other):
        return self._path_config == other._path_config \
            and self._component_endpoint == other._component_endpoint \
            and self._service == other._service \
            and self._oracle == other._oracle \
            and self._exit == other._exit \
            and self._published == other._published \
            and self._building == other._building \
            and self._commiting == other._commiting \
            and self.dishonest == other.dishonest \
            and self._validating_blocks == other._validating_blocks \
            and self._peers == other._peers

    def start(self, updates, service, startup_state):
        """is called from main to start the Engine,
         so it starts receiving messages"""
        self._service = service
        stream = Stream(self._component_endpoint)
        self.node.block_cache = _BlockCacheProxy(self._service, stream)

        # 1. Wait for an incoming message.
        # 2. Check for exit.
        # 3. Handle the message.
        # 4. Check for publishing.

        handlers = {
            Message.CONSENSUS_NOTIFY_BLOCK_NEW: self._handle_new_block,
            Message.CONSENSUS_NOTIFY_BLOCK_VALID: self._handle_valid_block,
            Message.CONSENSUS_NOTIFY_BLOCK_INVALID: self._handle_invalid_block,
            Message.CONSENSUS_NOTIFY_BLOCK_COMMIT: self._handle_committed_block,
            Message.CONSENSUS_NOTIFY_PEER_CONNECTED: self._handle_peer_msgs,
            Message.CONSENSUS_NOTIFY_PEER_DISCONNECTED: self._handle_peer_msgs,
            Message.CONSENSUS_NOTIFY_PEER_MESSAGE: self._handle_peer_msgs,
            Message.CONSENSUS_PREPARE_BLOCK: self._handle_prepare_block,
            Message.CONSENSUS_PREPARE_VOTE: self._handle_prepare_vote,
            Message.CONSENSUS_VIEW_CHANGE: self._handle_view_change,
            Message.CONSENSUS_PREPARE_QC: self._handle_prepare_qc,
            Message.CONSENSUS_VIEW_CHANGE_QC: self._handle_view_change_qc
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

                    try:
                        handle_message = handlers[type_tag]
                    except KeyError:
                        LOGGER.error('Unknown type tag: %s',
                                     Message.MessageType.Name(type_tag))
                    else:
                        handle_message(data)

                if self._exit:
                    break

                self._try_to_publish()
            except Exception:  # pylint: disable=broad-except
                LOGGER.exception("Unhandled exception in message loop")

    @staticmethod
    def _load_identity_signer(key_dir, key_name):
        """Loads a private key from the key directory, based on a validator's
        identity.

        Args:
            key_dir (str): The path to the key directory.
            key_name (str): The name of the key to load.

        Returns:
            Signer: the cryptographic signer for the key
        """
        key_path = os.path.join(key_dir, '{}.priv'.format(key_name))

        if not os.path.exists(key_path):
            raise Exception(
                "No such signing key file: {}".format(key_path))
        if not os.access(key_path, os.R_OK):
            raise Exception(
                "Key file is not readable: {}".format(key_path))

        LOGGER.info('Loading signing key: %s', key_path)
        try:
            with open(key_path, 'r') as key_file:
                private_key_str = key_file.read().strip()
        except IOError as e:
            raise Exception(
                "Could not load key file: {}".format(str(e))) from e

        try:
            private_key = Secp256k1PrivateKey.from_hex(private_key_str)
        except signing.ParseError as e:
            raise Exception(
                "Invalid key in file {}: {}".format(key_path, str(e))) from e

        context = signing.create_context('secp256k1')
        crypto_factory = CryptoFactory(context)
        return crypto_factory.new_signer(private_key)

class PoetEngine(Engine):
    def __init__(self, path_config, component_endpoint):
        # components
        self._path_config = path_config
        self._component_endpoint = component_endpoint
        self._service = None
        self._oracle = None

        # state variables
        self._exit = False
        self._published = False
        self._building = False
        self._committing = False

        self._validating_blocks = set()
        self._pending_forks_to_resolve = PendingForks()

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
        self._service = service
        self._oracle = PoetOracle(
            service=service,
            component_endpoint=self._component_endpoint,
            config_dir=self._path_config.config_dir,
            data_dir=self._path_config.data_dir,
            key_dir=self._path_config.key_dir)

        # 1. Wait for an incoming message.
        # 2. Check for exit.
        # 3. Handle the message.
        # 4. Check for publishing.

        handlers = {
            Message.CONSENSUS_NOTIFY_BLOCK_NEW: self._handle_new_block,
            Message.CONSENSUS_NOTIFY_BLOCK_VALID: self._handle_valid_block,
            Message.CONSENSUS_NOTIFY_BLOCK_INVALID: self._handle_invalid_block,
            Message.CONSENSUS_NOTIFY_BLOCK_COMMIT: self._handle_committed_block,
            Message.CONSENSUS_NOTIFY_PEER_CONNECTED: self._handle_peer_msgs,
            Message.CONSENSUS_NOTIFY_PEER_DISCONNECTED: self._handle_peer_msgs,
            Message.CONSENSUS_NOTIFY_PEER_MESSAGE: self._handle_peer_msgs,
            Message.CONSENSUS_PREPARE_BLOCK: self._handle_prepare_block,
            Message.CONSENSUS_PREPARE_VOTE: self._handle_prepare_vote,
            Message.CONSENSUS_VIEW_CHANGE: self._handle_view_change,
            Message.CONSENSUS_PREPARE_QC: self._handle_prepare_qc,
            Message.CONSENSUS_VIEW_CHANGE_QC: self._handle_view_change_qc
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

                    try:
                        handle_message = handlers[type_tag]
                    except KeyError:
                        LOGGER.error('Unknown type tag: %s',
                                     Message.MessageType.Name(type_tag))
                    else:
                        handle_message(data)

                if self._exit:
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

