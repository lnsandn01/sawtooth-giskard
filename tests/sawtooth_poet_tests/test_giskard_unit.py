import unittest
import logging
from typing import List

from integration_tools import BlockCacheMock
from sawtooth_sdk.protobuf.validator_pb2 import Message
from sawtooth_poet.journal.block_wrapper import NULL_BLOCK_IDENTIFIER, LAST_BLOCK_INDEX_IDENTIFIER
from sawtooth_poet_engine.giskard_block import Block, GiskardBlock, GiskardGenesisBlock
from sawtooth_poet_engine.giskard_message import GiskardMessage
from sawtooth_poet_engine.giskard_nstate import NState
from sawtooth_poet_engine.giskard import Giskard

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)


class TestGiskardUnit(unittest.TestCase):
    def test_processed_ViewChange_in_view(self):
        """Test for processed_ViewChange_in_view"""
        state = NState(0, 0, [], [GiskardMessage(Message.CONSENSUS_GISKARD_VIEW_CHANGE, 3, [], [], [])],
                       [], False)
        msg = GiskardMessage(Message.CONSENSUS_GISKARD_VIEW_CHANGE, 3, [], [], [])
        view = 3

        # test with correct input
        assert msg in Giskard.processed_ViewChange_in_view(state, view)
        assert msg in state.counting_messages \
               and msg.message_type == Message.CONSENSUS_GISKARD_VIEW_CHANGE \
               and msg.view == view
        # test wrong input
        view = 4
        self.assertFalse(msg in Giskard.processed_ViewChange_in_view(state, view))

    """@staticmethod
    def processed_ViewChange_in_view_correct(state: NState, view, msg) -> bool:
        if msg in Giskard.processed_ViewChange_in_view(state, view):
            return msg in state.counting_messages and msg.message_type == Message.CONSENSUS_GISKARD_VIEW_CHANGE and msg.view == view"""

    def test_parent_ofb_for_all_blocks(self, depth=0, block_cache=None):
        """Test if parent relation correct for all blocks in storage
        :param depth: of the chain until testing is stopped
        :asserts True if all parents are correct, False if one is not:
        """
        if block_cache is None:
            block_cache = BlockCacheMock([
                Block(NULL_BLOCK_IDENTIFIER, 0, 12345, 0, "", ""),
                Block(1, NULL_BLOCK_IDENTIFIER, 54321, 1, "", ""),
                Block(2, 1, 54321, 2, "", "")])
            depth = 3
        i = 0
        child_block = None
        for block in reversed(block_cache.block_store.blocks):
            if i == depth:
                assert True
            if i == 0:
                child_block = block
                i += 1
                continue
            else:
                if Giskard.parent_of(child_block, block_cache) != block:
                    assert False
                else:
                    i += 1
                    child_block = block
        assert True

    def test_parent_block_height(self, depth=0, block_cache=None):
        """Test if all parent blocks in storage have correct heights
        :param block_cache:
        :param depth: of the chain until testing is stopped
        :asserts True if all parents' heights are correct, False if one is not:
        """
        if block_cache is None:
            block_cache = BlockCacheMock([
                Block(NULL_BLOCK_IDENTIFIER, 0, 12345, 0, "", ""),
                Block(1, NULL_BLOCK_IDENTIFIER, 54321, 1, "", ""),
                Block(2, 1, 54321, 2, "", "")])
            depth = 3
        i = 0
        child_block = None
        for block in reversed(block_cache.block_store.blocks):
            if i == depth:
                assert True
            if i == 0:
                child_block = block
                i += 1
                continue
            else:
                if not Giskard.parent_of(child_block, block_cache).block_num + 1 == child_block.block_num:
                    assert False
                else:
                    i += 1
                    child_block = block
        assert True

    def test_about_generate_new_block(self, block_parent: GiskardBlock = None, block_cache=None) -> bool:
        """ Lemma: proofs that all heights are correct;
        GiskardBlock.b_height(Giskard.generate_new_block(bock)) == GiskardBlock.b_height(block) + 1"""
        if block_cache is None:
            block_cache = BlockCacheMock([
                Block(NULL_BLOCK_IDENTIFIER, 0, 12345, 0, "", ""),
                Block(1, NULL_BLOCK_IDENTIFIER, 54321, 1, "", ""),
                Block(2, 1, 54321, 2, "", "")])
            block_parent = block_cache.block_store.blocks[-1]
        block = Giskard.generate_new_block(block_parent, block_cache, 3)
        block_cache.block_store.blocks.append(block)
        assert self.generate_new_block_parent(block, block_cache, block_parent)

    def generate_new_block_parent(self, block: GiskardBlock, block_cache, block_parent: GiskardBlock) -> bool:
        """Test if parent block realtion works with generation of new block"""
        return Giskard.parent_of(block, block_cache) == block_parent

    """Lemma make_PrepareBlocks_message_type :
          forall s msg0 msg, 
          In msg (make_PrepareBlocks s msg0) ->
          get_message_type msg = PrepareBlock"""

    """Lemma pending_PrepareVote_correct :
          forall s msg0 msg,
            In msg (pending_PrepareVote s msg0) ->
            get_message_type msg = PrepareVote /\
            get_sender msg = node_id s /\
            get_view msg = node_view s."""

    def test_about_highest_message_in_list(self, node=None, message_type=0, lm: List[GiskardMessage] = None, msg=None):
        """Test if a message actually has a lower height block as the highest in the list"""
        # TODO lemma where I don't know if it is actually that important to test
        if not node:
            node = "me"
            message_type = Message.CONSENSUS_GISKARD_PREPARE_BLOCK
            lm = [GiskardMessage(Message.CONSENSUS_GISKARD_PREPARE_BLOCK,
                                 0,
                                 "",
                                 GiskardBlock(Block(1, 0, "", 10, "", ""), 1),
                                 GiskardGenesisBlock()),
                  GiskardMessage(Message.CONSENSUS_GISKARD_PREPARE_BLOCK,
                                 0,
                                 "",
                                 GiskardBlock(Block(1, 0, "", 3, "", ""), 1),
                                 GiskardGenesisBlock())
                  ]
            msg = GiskardMessage(Message.CONSENSUS_GISKARD_PREPARE_BLOCK,
                                 0,
                                 "",
                                 GiskardBlock(Block(1, 0, "", 5, "", ""), 1),
                                 GiskardGenesisBlock())
            msg2 = GiskardMessage(Message.CONSENSUS_GISKARD_PREPARE_BLOCK,
                                  0,
                                  "",
                                  GiskardBlock(Block(1, 0, "", 20, "", ""), 1),
                                  GiskardGenesisBlock())
            self.assertFalse(
                msg2.block.block_num <= Giskard.highest_message_in_list(node, message_type, lm).block.block_num)

        assert msg.block.block_num <= Giskard.highest_message_in_list(node, message_type, lm).block.block_num

    def test_highest_ViewChange_message_type_eq_ViewChange(self, state=None):
        """Test if message with the highest block is type actually of type ViewChange in a list of ViewChange msgs"""
        # TODO lemma where I don't know if it is actually that important to test
        if not state:
            state = NState(0, 0, [], [GiskardMessage(Message.CONSENSUS_GISKARD_VIEW_CHANGE,
                                                     0,
                                                     "",
                                                     GiskardBlock(Block(1, 0, "", 10, "", ""), 1),
                                                     GiskardGenesisBlock()),
                                      GiskardMessage(Message.CONSENSUS_GISKARD_PREPARE_BLOCK,
                                                     7,
                                                     "",
                                                     GiskardBlock(Block(1, 0, "", 3, "", ""), 1),
                                                     GiskardGenesisBlock()),
                                      GiskardMessage(Message.CONSENSUS_GISKARD_VIEW_CHANGE,
                                                     3,
                                                     "",
                                                     GiskardBlock(Block(1, 0, "", 3, "", ""), 1),
                                                     GiskardGenesisBlock())
                                      ],
                           [], False)
        assert Giskard.highest_ViewChange_message(state).message_type == Message.CONSENSUS_GISKARD_VIEW_CHANGE
