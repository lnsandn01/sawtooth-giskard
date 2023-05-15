import unittest
import logging
from typing import List

import sawtooth_poet_engine.giskard_state_transition_type as giskard_state_transition_type
from sawtooth_poet_engine.giskard_global_trace import GTrace
from sawtooth_poet_engine.giskard_node import GiskardNode
from sawtooth_poet_tests.integration_tools import BlockCacheMock
from sawtooth_sdk.protobuf.validator_pb2 import Message
from sawtooth_poet.journal.block_wrapper import NULL_BLOCK_IDENTIFIER, LAST_BLOCK_INDEX_IDENTIFIER
from sawtooth_poet_engine.giskard_block import Block, GiskardBlock, GiskardGenesisBlock
from sawtooth_poet_engine.giskard_message import GiskardMessage
from sawtooth_poet_engine.giskard_nstate import NState
from sawtooth_poet_engine.giskard import Giskard

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)


class TestGiskardUnit(unittest.TestCase):
    # region basic inputs
    @staticmethod
    def get_basic_nodes():
        node = GiskardNode("me", False)
        proposer = GiskardNode("proposer", False)
        you1 = GiskardNode("you1", False)
        you2 = GiskardNode("you2", False)
        you3 = GiskardNode("you3", False)
        return [node, proposer, you1, you2, you3, [node, proposer, you1, you2, you3]]
    # endregion

    # region block tests
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
        block = Giskard.generate_new_block(block_parent, block_cache, 3, block_parent.signer_id)
        block_cache.block_store.blocks.append(block)
        assert self.generate_new_block_parent(block, block_cache, block_parent)

    def generate_new_block_parent(self, block: GiskardBlock, block_cache, block_parent: GiskardBlock) -> bool:
        """Test if parent block realtion works with generation of new block"""
        return Giskard.parent_of(block, block_cache) == block_parent
    # endregion

    # region message tests
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
    def test_processed_ViewChange_in_view(self):
        """Test for processed_ViewChange_in_view"""
        state = NState(None, 0, 0, [], [GiskardMessage(GiskardMessage.CONSENSUS_GISKARD_VIEW_CHANGE, 3, [], [], [])],
                       [], False)
        msg = GiskardMessage(GiskardMessage.CONSENSUS_GISKARD_VIEW_CHANGE, 3, [], [], [])
        view = 3

        # test with correct input
        assert msg in Giskard.processed_ViewChange_in_view(state, view)
        assert msg in state.counting_messages \
               and msg.message_type == GiskardMessage.CONSENSUS_GISKARD_VIEW_CHANGE \
               and msg.view == view
        # test wrong input
        view = 4
        self.assertFalse(msg in Giskard.processed_ViewChange_in_view(state, view))

    """@staticmethod
    def processed_ViewChange_in_view_correct(state: NState, view, msg) -> bool:
        if msg in Giskard.processed_ViewChange_in_view(state, view):
            return msg in state.counting_messages and msg.message_type == GiskardMessage.CONSENSUS_GISKARD_VIEW_CHANGE and msg.view == view"""


    def test_about_highest_message_in_list(self, node=None, message_type=0, lm: List[GiskardMessage] = None, msg=None):
        """Test if a message actually has a lower height block as the highest in the list"""
        # TODO lemma where I don't know if it is actually that important to test
        if not node:
            node = "me"
            message_type = GiskardMessage.CONSENSUS_GISKARD_PREPARE_BLOCK
            lm = [GiskardMessage(GiskardMessage.CONSENSUS_GISKARD_PREPARE_BLOCK,
                                 0,
                                 "",
                                 GiskardBlock(Block(1, 0, "", 10, "", ""), 1),
                                 GiskardGenesisBlock()),
                  GiskardMessage(GiskardMessage.CONSENSUS_GISKARD_PREPARE_BLOCK,
                                 0,
                                 "",
                                 GiskardBlock(Block(1, 0, "", 3, "", ""), 1),
                                 GiskardGenesisBlock())
                  ]
            msg = GiskardMessage(GiskardMessage.CONSENSUS_GISKARD_PREPARE_BLOCK,
                                 0,
                                 "",
                                 GiskardBlock(Block(1, 0, "", 5, "", ""), 1),
                                 GiskardGenesisBlock())
            msg2 = GiskardMessage(GiskardMessage.CONSENSUS_GISKARD_PREPARE_BLOCK,
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
            state = NState(None, 0, 0, [], [GiskardMessage(GiskardMessage.CONSENSUS_GISKARD_VIEW_CHANGE,
                                                     0,
                                                     "",
                                                     GiskardBlock(Block(1, 0, "", 10, "", ""), 1),
                                                     GiskardGenesisBlock()),
                                      GiskardMessage(GiskardMessage.CONSENSUS_GISKARD_PREPARE_BLOCK,
                                                     7,
                                                     "",
                                                     GiskardBlock(Block(1, 0, "", 3, "", ""), 1),
                                                     GiskardGenesisBlock()),
                                      GiskardMessage(GiskardMessage.CONSENSUS_GISKARD_VIEW_CHANGE,
                                                     3,
                                                     "",
                                                     GiskardBlock(Block(1, 0, "", 3, "", ""), 1),
                                                     GiskardGenesisBlock())
                                      ],
                           [], False)
        assert Giskard.highest_ViewChange_message(state).message_type == GiskardMessage.CONSENSUS_GISKARD_VIEW_CHANGE
    # endregion

    # region state transitions
    def test_out_messages_local_monotonic(self, state1: NState = None, state2: NState = None,
                                          msg: GiskardMessage = None, lm: List[GiskardMessage] = None, t=None,
                                          node=None, block_cache=None, peers=None):
        # TODO probably should write a test outside that calls this and test every transition
        if not state1:
            node, proposer, you1, you2, you3, peers = TestGiskardUnit.get_basic_nodes()
            msg = GiskardMessage(GiskardMessage.CONSENSUS_GISKARD_PREPARE_VOTE,
                                 1,
                                 proposer,
                                 GiskardBlock(Block(1, NULL_BLOCK_IDENTIFIER, "proposer", 1, "", ""), 1),
                                 GiskardGenesisBlock())
            state1 = NState(None, 1, node.node_id,
                            [msg],
                            [GiskardMessage(GiskardMessage.CONSENSUS_GISKARD_PREPARE_VOTE,
                                            1,
                                            you3,
                                            GiskardBlock(
                                                Block(1, NULL_BLOCK_IDENTIFIER, "proposer", 1, "", ""), 1),
                                            GiskardGenesisBlock()),
                             GiskardMessage(GiskardMessage.CONSENSUS_GISKARD_PREPARE_VOTE,
                                            1,
                                            you1,
                                            GiskardBlock(
                                                Block(1, NULL_BLOCK_IDENTIFIER, "proposer", 1, "", ""), 1),
                                            GiskardGenesisBlock()),
                             GiskardMessage(GiskardMessage.CONSENSUS_GISKARD_PREPARE_BLOCK,
                                            1,
                                            proposer,
                                            GiskardBlock(Block(1, NULL_BLOCK_IDENTIFIER, "proposer", 1, "", ""), 1),
                                            GiskardGenesisBlock()),
                             GiskardMessage(GiskardMessage.CONSENSUS_GISKARD_PREPARE_VOTE,
                                            1,
                                            you2,
                                            GiskardBlock(
                                                Block(1, NULL_BLOCK_IDENTIFIER, "proposer", 1, "", ""), 1),
                                            GiskardGenesisBlock())
                             ], [], False)
            block_cache = BlockCacheMock([GiskardGenesisBlock()])
            state2 = Giskard.process_PrepareVote_vote_set(state1, msg, block_cache)[0]
            lm = [Giskard.make_PrepareQC(state1, msg), Giskard.pending_PrepareVote(state1, msg, block_cache)]

        if Giskard.get_transition(giskard_state_transition_type.PROCESS_PREPAREVOTE_VOTE_TYPE,
                                  state1, msg, state2, lm, node, block_cache, peers):
            for msg0 in state1.out_messages:
                assert msg0 in state2.out_messages
        else:
            assert False, "state transition didn't work out"

    def test_counting_messages_same_view_monotonic(self, state1: NState = None, state2: NState = None,
                                                   msg: GiskardMessage = None, lm: List[GiskardMessage] = None, t=None,
                                                   node=None, block_cache=None, peers=None):
        # TODO probably should write a test outside that calls this and test every transition
        if not state1:
            node, proposer, you1, you2, you3, peers = TestGiskardUnit.get_basic_nodes()
            msg = GiskardMessage(GiskardMessage.CONSENSUS_GISKARD_PREPARE_VOTE,
                                 1,
                                 proposer,
                                 GiskardBlock(Block(1, NULL_BLOCK_IDENTIFIER, "proposer", 1, "", ""), 1),
                                 GiskardGenesisBlock())
            state1 = NState(None, 1, node.node_id,
                            [msg],
                            [GiskardMessage(GiskardMessage.CONSENSUS_GISKARD_PREPARE_VOTE,
                                            1,
                                            you3,
                                            GiskardBlock(
                                                Block(1, NULL_BLOCK_IDENTIFIER, "proposer", 1, "", ""), 1),
                                            GiskardGenesisBlock()),
                             GiskardMessage(GiskardMessage.CONSENSUS_GISKARD_PREPARE_VOTE,
                                            1,
                                            you1,
                                            GiskardBlock(
                                                Block(1, NULL_BLOCK_IDENTIFIER, "proposer", 1, "", ""), 1),
                                            GiskardGenesisBlock()),
                             GiskardMessage(GiskardMessage.CONSENSUS_GISKARD_PREPARE_BLOCK,
                                            1,
                                            proposer,
                                            GiskardBlock(Block(1, NULL_BLOCK_IDENTIFIER, "proposer", 1, "", ""), 1),
                                            GiskardGenesisBlock()),
                             GiskardMessage(GiskardMessage.CONSENSUS_GISKARD_PREPARE_VOTE,
                                            1,
                                            you2,
                                            GiskardBlock(
                                                Block(1, NULL_BLOCK_IDENTIFIER, "proposer", 1, "", ""), 1),
                                            GiskardGenesisBlock())
                             ], [], False)
            block_cache = BlockCacheMock([GiskardGenesisBlock()])
            state2 = Giskard.process_PrepareVote_vote_set(state1, msg, block_cache)[0]
            lm = [Giskard.make_PrepareQC(state1, msg), Giskard.pending_PrepareVote(state1, msg, block_cache)]
            peers = [node, proposer, you1, you2, you3]

        if Giskard.get_transition(giskard_state_transition_type.PROCESS_PREPAREVOTE_VOTE_TYPE,
                                  state1, msg, state2, lm, node, block_cache, peers) \
                and state1.node_view == state2.node_view:
            for msg0 in state1.counting_messages:
                assert msg0 in state2.counting_messages
        else:
            assert False, "state transition didn't work out"

    def test_counting_messages_local_monotonic(self, state1: NState = None, state2: NState = None,
                                               msg: GiskardMessage = None, lm: List[GiskardMessage] = None, t=None,
                                               node=None, block_cache=None, peers=None):
        # TODO probably should write a test outside that calls this and test every transition
        if not state1:
            node, proposer, you1, you2, you3, peers = TestGiskardUnit.get_basic_nodes()
            msg = GiskardMessage(GiskardMessage.CONSENSUS_GISKARD_PREPARE_VOTE,
                                 1,
                                 proposer,
                                 GiskardBlock(Block(1, NULL_BLOCK_IDENTIFIER, "proposer", 1, "", ""), 1),
                                 GiskardGenesisBlock())
            state1 = NState(None, 1, node.node_id,
                            [msg],
                            [GiskardMessage(GiskardMessage.CONSENSUS_GISKARD_PREPARE_VOTE,
                                            1,
                                            you3,
                                            GiskardBlock(
                                                Block(1, NULL_BLOCK_IDENTIFIER, "proposer", 1, "", ""), 1),
                                            GiskardGenesisBlock()),
                             GiskardMessage(GiskardMessage.CONSENSUS_GISKARD_PREPARE_VOTE,
                                            1,
                                            you1,
                                            GiskardBlock(
                                                Block(1, NULL_BLOCK_IDENTIFIER, "proposer", 1, "", ""), 1),
                                            GiskardGenesisBlock()),
                             GiskardMessage(GiskardMessage.CONSENSUS_GISKARD_PREPARE_BLOCK,
                                            1,
                                            proposer,
                                            GiskardBlock(Block(1, NULL_BLOCK_IDENTIFIER, "proposer", 1, "", ""), 1),
                                            GiskardGenesisBlock()),
                             GiskardMessage(GiskardMessage.CONSENSUS_GISKARD_PREPARE_VOTE,
                                            1,
                                            you2,
                                            GiskardBlock(
                                                Block(1, NULL_BLOCK_IDENTIFIER, "proposer", 1, "", ""), 1),
                                            GiskardGenesisBlock())
                             ], [], False)
            block_cache = BlockCacheMock([GiskardGenesisBlock()])
            state2 = Giskard.process_PrepareVote_vote_set(state1, msg, block_cache)[0]
            lm = [Giskard.make_PrepareQC(state1, msg), Giskard.pending_PrepareVote(state1, msg, block_cache)]
            peers = [node, proposer, you1, you2, you3]

        if Giskard.get_transition(giskard_state_transition_type.PROCESS_PREPAREVOTE_VOTE_TYPE,
                                  state1, msg, state2, lm, node, block_cache, peers):
            for msg0 in state1.counting_messages:
                assert msg0 in state2.counting_messages
        else:
            assert False, "state transition didn't work out"

    def test_about_local_out_messages(self, state1: NState = None, state2: NState = None,
                                      msg: GiskardMessage = None, lm: List[GiskardMessage] = None, t=None,
                                      node=None, block_cache=None, peers=None):
        # TODO probably should write a test outside that calls this and test every transition
        if not state1:
            node, proposer, you1, you2, you3, peers = TestGiskardUnit.get_basic_nodes()
            msg = GiskardMessage(GiskardMessage.CONSENSUS_GISKARD_PREPARE_VOTE,
                                 1,
                                 proposer,
                                 GiskardBlock(Block(1, NULL_BLOCK_IDENTIFIER, "proposer", 1, "", ""), 1),
                                 GiskardGenesisBlock())
            state1 = NState(None, 1, node.node_id,
                            [msg],
                            [GiskardMessage(Message.CONSENSUS_GISKARD_PREPARE_VOTE,
                                            1,
                                            you3,
                                            GiskardBlock(
                                                Block(1, NULL_BLOCK_IDENTIFIER, "proposer", 1, "", ""), 1),
                                            GiskardGenesisBlock()),
                             GiskardMessage(Message.CONSENSUS_GISKARD_PREPARE_VOTE,
                                            1,
                                            you1,
                                            GiskardBlock(
                                                Block(1, NULL_BLOCK_IDENTIFIER, "proposer", 1, "", ""), 1),
                                            GiskardGenesisBlock()),
                             GiskardMessage(Message.CONSENSUS_GISKARD_PREPARE_BLOCK,
                                            1,
                                            proposer,
                                            GiskardBlock(Block(1, NULL_BLOCK_IDENTIFIER, "proposer", 1, "", ""), 1),
                                            GiskardGenesisBlock()),
                             GiskardMessage(Message.CONSENSUS_GISKARD_PREPARE_VOTE,
                                            1,
                                            you2,
                                            GiskardBlock(
                                                Block(1, NULL_BLOCK_IDENTIFIER, "proposer", 1, "", ""), 1),
                                            GiskardGenesisBlock())
                             ], [], False)
            block_cache = BlockCacheMock([GiskardGenesisBlock()])
            state2 = Giskard.process_PrepareVote_vote_set(state1, msg, block_cache)[0]
            lm = [Giskard.make_PrepareQC(state1, msg), Giskard.pending_PrepareVote(state1, msg, block_cache)]

        if Giskard.get_transition(giskard_state_transition_type.PROCESS_PREPAREVOTE_VOTE_TYPE,
                                  state1, msg, state2, lm, node, block_cache, peers):
            for msg0 in lm:
                assert msg0 in state2.out_messages
        else:
            assert False, "state transition didn't work out"

    def test_not_prepare_stage(self, state: NState = None, b: GiskardBlock = None):
        if not state:
            node, proposer, you1, you2, you3, peers = TestGiskardUnit.get_basic_nodes()
            b = GiskardBlock(Block(1, NULL_BLOCK_IDENTIFIER, "proposer", 1, "", ""), 1)
            state = NState(None, 1, node.node_id, [],
                           [GiskardMessage(Message.CONSENSUS_GISKARD_PREPARE_BLOCK,
                                           1,
                                           proposer,
                                           GiskardBlock(Block(1, NULL_BLOCK_IDENTIFIER, "proposer", 1, "", ""), 1),
                                           GiskardGenesisBlock()),
                            GiskardMessage(Message.CONSENSUS_GISKARD_PREPARE_VOTE,
                                           1,
                                           proposer,
                                           GiskardBlock(
                                               Block(1, NULL_BLOCK_IDENTIFIER, "proposer", 1, "", ""), 1),
                                           GiskardGenesisBlock())
                            ], [], False)

        if not Giskard.prepare_stage(state, b, peers) \
                and not Giskard.vote_quorum_in_view(state, state.node_view, b, peers):
            for msg in state.counting_messages:
                self.assertFalse(msg.block == b
                                 and msg.view == state.node_view
                                 and msg.message_type == Message.CONSENSUS_GISKARD_PREPARE_QC)
        else:
            assert False, "was in prepare stage, or vote quorum"

    def test_prepare_stage_record_agnostic(self, state: NState = None, b: GiskardBlock = None,
                                           msg: GiskardMessage = None):
        if not state:
            node, proposer, you1, you2, you3, peers = TestGiskardUnit.get_basic_nodes()
            b = GiskardBlock(Block(1, NULL_BLOCK_IDENTIFIER, "proposer", 1, "", ""), 1)
            state = NState(None, 1, node.node_id, [],
                           [GiskardMessage(Message.CONSENSUS_GISKARD_PREPARE_BLOCK,
                                           1,
                                           proposer,
                                           GiskardBlock(Block(1, NULL_BLOCK_IDENTIFIER, "proposer", 1, "", ""), 1),
                                           GiskardGenesisBlock()),
                            GiskardMessage(Message.CONSENSUS_GISKARD_PREPARE_VOTE,
                                           1,
                                           proposer,
                                           GiskardBlock(
                                               Block(1, NULL_BLOCK_IDENTIFIER, "proposer", 1, "", ""), 1),
                                           GiskardGenesisBlock()),
                            GiskardMessage(Message.CONSENSUS_GISKARD_PREPARE_VOTE,
                                           1,
                                           node,
                                           GiskardBlock(
                                               Block(1, NULL_BLOCK_IDENTIFIER, "proposer", 1, "", ""), 1),
                                           GiskardGenesisBlock())
                            ], [], False)
            msg = GiskardMessage(Message.CONSENSUS_GISKARD_PREPARE_VOTE,
                                 1,
                                 you1,
                                 GiskardBlock(
                                     Block(1, NULL_BLOCK_IDENTIFIER, "proposer", 1, "", ""), 1),
                                 GiskardGenesisBlock())

        if Giskard.prepare_stage(Giskard.record(state, msg), b, peers):
            assert Giskard.prepare_stage(state, b, peers)
        else:
            assert False, "was not in prepare stage"

    def test_prepare_stage_record_plural_agnostic(self, state: NState = None, b: GiskardBlock = None,
                                                  lm: List[GiskardMessage] = None):
        if not state:
            node, proposer, you1, you2, you3, peers = TestGiskardUnit.get_basic_nodes()
            b = GiskardBlock(Block(1, NULL_BLOCK_IDENTIFIER, "proposer", 1, "", ""), 1)
            state = NState(None, 1, node.node_id, [],
                           [GiskardMessage(Message.CONSENSUS_GISKARD_PREPARE_BLOCK,
                                           1,
                                           proposer,
                                           GiskardBlock(Block(1, NULL_BLOCK_IDENTIFIER, "proposer", 1, "", ""), 1),
                                           GiskardGenesisBlock()),
                            GiskardMessage(Message.CONSENSUS_GISKARD_PREPARE_VOTE,
                                           1,
                                           proposer,
                                           GiskardBlock(
                                               Block(1, NULL_BLOCK_IDENTIFIER, "proposer", 1, "", ""), 1),
                                           GiskardGenesisBlock()),
                            GiskardMessage(Message.CONSENSUS_GISKARD_PREPARE_VOTE,
                                           1,
                                           node,
                                           GiskardBlock(
                                               Block(1, NULL_BLOCK_IDENTIFIER, "proposer", 1, "", ""), 1),
                                           GiskardGenesisBlock())
                            ], [], False)
            lm = [GiskardMessage(Message.CONSENSUS_GISKARD_PREPARE_VOTE,
                                 1,
                                 you1,
                                 GiskardBlock(
                                     Block(1, NULL_BLOCK_IDENTIFIER, "proposer", 1, "", ""), 1),
                                 GiskardGenesisBlock()),
                  GiskardMessage(Message.CONSENSUS_GISKARD_PREPARE_VOTE,
                                 1,
                                 proposer,
                                 GiskardBlock(
                                     Block(1, NULL_BLOCK_IDENTIFIER, "proposer", 1, "", ""), 1),
                                 GiskardGenesisBlock())]

        if Giskard.prepare_stage(Giskard.record_plural(state, lm), b, peers):
            assert Giskard.prepare_stage(state, b, peers)
        else:
            assert False, "was not in prepare stage"

    def test_prepare_stage_process_record_plural_agnostic(self, state: NState = None, b: GiskardBlock = None,
                                                          lm: List[GiskardMessage] = None, msg: GiskardMessage = None):
        if not state:
            node, proposer, you1, you2, you3, peers = TestGiskardUnit.get_basic_nodes()
            b = GiskardBlock(Block(1, NULL_BLOCK_IDENTIFIER, "proposer", 1, "", ""), 1)
            msg = GiskardMessage(Message.CONSENSUS_GISKARD_PREPARE_VOTE,
                                 1,
                                 you1,
                                 GiskardBlock(
                                     Block(1, NULL_BLOCK_IDENTIFIER, "proposer", 1, "", ""), 1),
                                 GiskardGenesisBlock())
            state = NState(None, 1, node.node_id, [msg],
                           [GiskardMessage(Message.CONSENSUS_GISKARD_PREPARE_BLOCK,
                                           1,
                                           proposer,
                                           GiskardBlock(Block(1, NULL_BLOCK_IDENTIFIER, "proposer", 1, "", ""), 1),
                                           GiskardGenesisBlock()),
                            GiskardMessage(Message.CONSENSUS_GISKARD_PREPARE_VOTE,
                                           1,
                                           proposer,
                                           GiskardBlock(
                                               Block(1, NULL_BLOCK_IDENTIFIER, "proposer", 1, "", ""), 1),
                                           GiskardGenesisBlock()),
                            GiskardMessage(Message.CONSENSUS_GISKARD_PREPARE_VOTE,
                                           1,
                                           node,
                                           GiskardBlock(
                                               Block(1, NULL_BLOCK_IDENTIFIER, "proposer", 1, "", ""), 1),
                                           GiskardGenesisBlock())
                            ], [], False)
            lm = [GiskardMessage(Message.CONSENSUS_GISKARD_PREPARE_VOTE,
                                 1,
                                 you1,
                                 GiskardBlock(
                                     Block(1, NULL_BLOCK_IDENTIFIER, "proposer", 1, "", ""), 1),
                                 GiskardGenesisBlock()),
                  GiskardMessage(Message.CONSENSUS_GISKARD_PREPARE_VOTE,
                                 1,
                                 proposer,
                                 GiskardBlock(
                                     Block(1, NULL_BLOCK_IDENTIFIER, "proposer", 1, "", ""), 1),
                                 GiskardGenesisBlock())]

        if Giskard.prepare_stage(Giskard.process(Giskard.record_plural(state, lm), msg), b, peers):
            assert Giskard.prepare_stage(Giskard.process(state, msg), b, peers)
        else:
            assert False, "was not in prepare stage"

    def test_global_state_transitions(self, peers=None, gtrace=None):
        if gtrace is None:
            gtrace = GTrace(peers)
        else:
            if peers is None:
                peers = [tuple(list(gstate.gstate.keys())) for gstate in gtrace.gtrace]
                peers = list(set([item for sublist in peers for item in sublist]))
                peers.sort(key=lambda h: int(h, 16))
        if peers is None:
            node, proposer, you1, you2, you3, peers = TestGiskardUnit.get_basic_nodes()
            peers.sort()
        assert Giskard.protocol_trace(gtrace, False), "A state transition was incorrect"
    # endregion

    # region safety property tests
    def test_all_stage_height_injectivity(self, peers=None, gtrace=None):
        """ Generates input for testing the safety properties """
        if gtrace is None:
            gtrace = GTrace(peers)
        else:
            if peers is None:
                peers = [tuple(list(gstate.gstate.keys())) for gstate in gtrace.gtrace]
                peers = list(set([item for sublist in peers for item in sublist]))
                peers.sort(key=lambda h: int(h, 16))
        if peers is None:
            node, proposer, you1, you2, you3, peers = TestGiskardUnit.get_basic_nodes()
            peers.sort()
        assert Giskard.prepare_stage_same_view_height_injective_statement([gtrace], peers), "prepare_stage_height_injectivity failed"
        assert Giskard.precommit_stage_height_injective_statement([gtrace], peers), "precommit_stage_height_injectivity failed"
        assert Giskard.commit_height_injective_statement([gtrace], peers), "commit_stage_height_injectivity failed"
        print("Tested stage_height_injectivity")
    # endregion
