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

import logging
import os
from collections import namedtuple

import sawtooth_signing as signing
from sawtooth_signing import CryptoFactory
from sawtooth_signing.secp256k1 import Secp256k1PrivateKey
from sawtooth_poet_engine.giskard_block import GiskardBlock
from sawtooth_sdk.consensus.exceptions import UnknownBlock, SameAsPreviousBlockAndNotGenesis
from sawtooth_sdk.messaging.stream import Stream
from sawtooth_sdk.protobuf.batch_pb2 import Batch
from sawtooth_sdk.protobuf.batch_pb2 import BatchHeader
from sawtooth_sdk.protobuf.client_batch_submit_pb2 \
    import ClientBatchSubmitRequest
from sawtooth_sdk.protobuf.client_batch_submit_pb2 \
    import ClientBatchSubmitResponse
from sawtooth_sdk.protobuf.client_block_pb2 \
    import ClientBlockGetByTransactionIdRequest
from sawtooth_sdk.protobuf.client_block_pb2 \
    import ClientBlockGetResponse
from sawtooth_sdk.protobuf.block_pb2 import BlockHeader
from sawtooth_sdk.protobuf.consensus_pb2 import ConsensusBlock
from sawtooth_sdk.protobuf.validator_pb2 import Message

from sawtooth_poet.poet_consensus.poet_block_publisher \
    import PoetBlockPublisher
from sawtooth_poet.poet_consensus.poet_block_verifier import PoetBlockVerifier
from sawtooth_poet.poet_consensus.poet_fork_resolver import PoetForkResolver
from sawtooth_poet.journal.block_wrapper import NULL_BLOCK_IDENTIFIER, LAST_BLOCK_INDEX_IDENTIFIER
from sawtooth_poet.poet_consensus import utils


LOGGER = logging.getLogger(__name__)

class PoetOracle:
    '''This is a wrapper around the PoET structures (publisher,
    verifier, fork resolver) and their attendant proxies.
    '''
    def __init__(self, service, component_endpoint,
                 config_dir, data_dir, key_dir):
        self._config_dir = config_dir
        self._data_dir = data_dir
        self._signer = _load_identity_signer(key_dir, 'validator')
        self._validator_id = self._signer.get_public_key().as_hex()

        stream = Stream(component_endpoint)

        self._block_cache = _BlockCacheProxy(service, stream)
        self._state_view_factory = _StateViewFactoryProxy(service)

        self._batch_publisher = _BatchPublisherProxy(stream, self._signer)
        self._publisher = None

    def initialize_block(self, previous_block):
        block_header = NewBlockHeader(
            previous_block,
            self._signer.get_public_key().as_hex())

        self._publisher = PoetBlockPublisher(
            block_cache=self._block_cache,
            state_view_factory=self._state_view_factory,
            batch_publisher=self._batch_publisher,
            data_dir=self._data_dir,
            config_dir=self._config_dir,
            validator_id=self._validator_id)

        return self._publisher.initialize_block(block_header)

    def check_publish_block(self, block):
        return self._publisher.check_publish_block(block)

    def finalize_block(self, block):
        return self._publisher.finalize_block(block)

    def verify_block(self, block):
        verifier = PoetBlockVerifier(
            block_cache=self._block_cache,
            state_view_factory=self._state_view_factory,
            data_dir=self._data_dir,
            config_dir=self._config_dir,
            validator_id=self._validator_id)

        return verifier.verify_block(block)

    def switch_forks(self, cur_fork_head, new_fork_head):
        '''"compare_forks" is not an intuitive name.'''
        fork_resolver = PoetForkResolver(
            block_cache=self._block_cache,
            state_view_factory=self._state_view_factory,
            data_dir=self._data_dir,
            config_dir=self._config_dir,
            validator_id=self._validator_id)

        return fork_resolver.compare_forks(cur_fork_head, new_fork_head)


class PoetBlock:
    def __init__(self, block):
        # fields that come with consensus blocks
        self.block_id = block.block_id
        self.previous_id = block.previous_id
        self.signer_id = block.signer_id
        self.block_num = block.block_num
        self.payload = block.payload
        self.summary = block.summary

        # fields that poet requires
        identifier = block.block_id.hex()
        previous_block_id = block.previous_id.hex()
        signer_public_key = block.signer_id.hex()

        self.identifier = identifier
        self.header_signature = identifier
        self.previous_block_id = previous_block_id
        self.signer_public_key = signer_public_key

        self.header = _DummyHeader(
            consensus=block.payload,
            signer_public_key=signer_public_key,
            previous_block_id=previous_block_id)

        # this is a trick
        self.state_root_hash = block.block_id

    def __str__(self):
        return (
            "Block("
            + ", ".join([
                "block_num: {}".format(self.block_num),
                "block_id: {}".format(self.block_id.hex()),
                "previous_id: {}".format(self.previous_id.hex()),
                "signer_id: {}".format(self.signer_id.hex()),
                "payload: {}".format(self.payload),
                "summary: {}".format(self.summary.hex()),
            ])
            + ")"
        )

class NewBlockHeader:
    '''The header for the block that is to be initialized.'''
    def __init__(self, previous_block, signer_public_key):
        self.consensus = None
        self.signer_public_key = signer_public_key
        self.previous_block_id = previous_block.identifier
        self.block_num = previous_block.block_num + 1


class _DummyHeader:
    def __init__(self, consensus, signer_public_key, previous_block_id):
        self.consensus = consensus
        self.signer_public_key = signer_public_key
        self.previous_block_id = previous_block_id


class _BlockCacheProxy:
    def __init__(self, service, stream):
        self.block_store = _BlockStoreProxy(service, stream)  # public
        self._service = service
        self.pending_blocks = []
        self.blocks_proposed_num = 0
        self.last_proposed_block = None
        self.blocks_reached_qc_current_view = []

    def __eq__(self, other):
        if not isinstance(other, _BlockCacheProxy):
            return NotImplemented

        return self.block_store == other.block_store \
            and self._service == other._service

    def __getitem__(self, block_id):
        block_id = bytes.fromhex(block_id)

        try:
            return PoetBlock(self._service.get_blocks([block_id])[block_id])
        except UnknownBlock:
            return None

    def pending_blocks_same_height_exists(self, block_num):
        for b in self.pending_blocks:
            if b.block_num == block_num:
                return True
        return False

    def remove_pending_block(self, block_id) -> bool:
        for b in self.pending_blocks:
            if b.block_id == block_id:
                self.pending_blocks.remove(b)
                return True
        return False


class _BlockStoreProxy:
    def __init__(self, service, stream):
        self._service = service
        self._stream = stream
        self.uncommitted_blocks = []

    def __eq__(self, other):
        if not isinstance(other, _BlockStoreProxy):
            return NotImplemented

        return self._service == other._service \
            and self._stream == other._stream

    def remove_uncommitted_block(self, block_id):
        for block in self.uncommitted_blocks:
            if block.block_id == block_id:
                self.uncommitted_blocks.remove(block)
                return

    @property
    def chain_head(self):
        return PoetBlock(self._service.get_chain_head())

    def get_block_by_transaction_id(self, transaction_id):
        future = self._stream.send(
            message_type=Message.CLIENT_BLOCK_GET_BY_TRANSACTION_ID_REQUEST,
            content=ClientBlockGetByTransactionIdRequest(
                transaction_id=transaction_id).SerializeToString())

        content = future.result().content

        response = ClientBlockGetResponse()
        response.ParseFromString(content)

        if response.status == ClientBlockGetResponse.NO_RESOURCE:
            raise ValueError("The transaction supplied is not in a block")

        block = response.block

        header = BlockHeader()
        header.ParseFromString(block.header)

        consensus_block = ConsensusBlock(
            block_id=bytes.fromhex(block.header_signature),
            previous_id=bytes.fromhex(header.previous_block_id),
            signer_id=bytes.fromhex(header.signer_public_key),
            block_num=header.block_num,
            payload=header.consensus,
            summary=b'')

        poet_block = PoetBlock(consensus_block)

        return poet_block

    def get_block_iter(self, reverse=False):
        # Ignore the reverse flag, since we can only get blocks
        # starting from the head.
        self.uncommitted_blocks.sort(key=lambda b1: b1.block_num)
        for block in reversed(self.uncommitted_blocks):
            yield block

        chain_head = self.chain_head

        yield chain_head

        curr = chain_head

        while curr.previous_id:
            try:
                previous_block = PoetBlock(
                    self._service.get_blocks(
                        [curr.previous_id]
                    )[curr.previous_id])
            except UnknownBlock:
                return

            try:
                if curr.previous_id == curr.block_id and curr.block_id != NULL_BLOCK_IDENTIFIER:
                    raise
            except SameAsPreviousBlockAndNotGenesis:
                return

            yield previous_block

            curr = previous_block

    def get_genesis(self):
        for block in self.get_block_iter():
            if block.block_num == 0:
                return GiskardBlock(block)
        return None

    def get_parent_block(self, child_block, ignore_block=None):
        """
        returns the parent block, if there is one in storage
        :param ignore_block: block to ignore, potential same height block to iterate over
        :param child_block:
        :return: GiskardBlock(parent_block) or None
        """
        for block in self.get_block_iter():
            if ignore_block is not None and block == ignore_block:
                continue
            if child_block.previous_id == block.block_id \
                    and child_block.block_num - 1 == block.block_num:
                return GiskardBlock(block)
            if block.block_num < child_block.block_num:
                return None
        return None

    def get_child_block(self, parent_block, ignore_block=None):
        """
        returns the child block, if there is one in storage
        :param ignore_block: block to ignore, potential same height block to iterate over
        :param parent_block:
        :return: GiskardBlock(parent_block) or None
        """
        for block in self.get_block_iter():
            if ignore_block is not None and block == ignore_block:
                continue
            if parent_block.block_id == block.previous_id \
                    and parent_block.block_num + 1 == block.block_num:
                return GiskardBlock(block)
            if block.block_num < parent_block.block_num:
                return None
        return None

    def same_height_block_in_storage(self, block) -> bool:
        """ :return True if a block of the same height
        e.g. block_num exists in the block storage """
        for b in self.get_block_iter():
            if block.block_num == b.block_num:
                return True
            if b.block_num < block.block_num:
                return True  # went past the block height
        return False


class _StateViewFactoryProxy:
    def __init__(self, service):
        self._service = service

    def __eq__(self, other):
        if not isinstance(other, _StateViewFactoryProxy):
            return NotImplemented

        return self._service == other._service

    def create_view(self, state_root_hash=None):
        '''The "state_root_hash" is really the block_id.'''

        block_id = state_root_hash

        return _StateViewProxy(self._service, block_id)


class _StateViewProxy:
    def __init__(self, service, block_id):
        self._service = service
        self._block_id = block_id

    def __eq__(self, other):
        if not isinstance(other, _StateViewProxy):
            return NotImplemented

        return self._service == other._service \
            and self._block_id == other._block_id

    def get(self, address):
        result = self._service.get_state(
            block_id=self._block_id,
            addresses=[address])

        return result[address]

    def leaves(self, prefix):
        result = self._service.get_state(
            block_id=self._block_id,
            addresses=[prefix])

        return list(result.items())


class _BatchPublisherProxy:
    def __init__(self, stream, signer):
        self.identity_signer = signer  # public
        self._stream = stream

    def __eq__(self, other):
        if not isinstance(other, _BatchPublisherProxy):
            return NotImplemented

        return self.identity_signer == other.identity_signer \
            and self._stream == other._stream

    def send(self, transactions):
        txn_signatures = [txn.header_signature for txn in transactions]

        header = BatchHeader(
            signer_public_key=self.identity_signer.get_public_key().as_hex(),
            transaction_ids=txn_signatures
        ).SerializeToString()

        signature = self.identity_signer.sign(header)

        batch = Batch(
            header=header,
            transactions=transactions,
            header_signature=signature)

        future = self._stream.send(
            message_type=Message.CLIENT_BATCH_SUBMIT_REQUEST,
            content=ClientBatchSubmitRequest(
                batches=[batch]).SerializeToString())

        result = future.result()
        assert result.message_type == Message.CLIENT_BATCH_SUBMIT_RESPONSE
        response = ClientBatchSubmitResponse()
        response.ParseFromString(result.content)
        if response.status != ClientBatchSubmitResponse.OK:
            LOGGER.warning("Submitting batch failed with status %s", response)


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
