from collections import namedtuple
from functools import reduce
from typing import List

from integration_tools import Block
from oracle import _BlockCacheProxy
from sawtooth_poet_engine.giskard_block import GiskardBlock, GiskardGenesisBlock
from sawtooth_poet_engine.giskard_message import GiskardMessage
from sawtooth_poet_engine.giskard_nstate import NState
from sawtooth_sdk.protobuf.validator_pb2 import Message
from sawtooth_poet.journal.block_wrapper import NULL_BLOCK_IDENTIFIER, LAST_BLOCK_INDEX_IDENTIFIER


class Giskard:
    """The main class for Giskard
        gets called by GiskardEngine for incoming messages"""

    # region node methods
    def honest_node(node):
        """returns True if the node is honest"""
        return not node.dishonest

    def is_block_proposer(
            node):  # TODO check how to do this with the peers, blocks proposed, view_number, node_id, timeout
        """returns True if the node is a block proposer for the current view"""
        return True

    # def is_new_proposer_unique TODO write test for that that checks if indeed all views had unique proposers
    # endregion

    # region local state operations
    @staticmethod
    def received(state: NState, msg: GiskardMessage) -> bool:
        """Returns True if the msg is in the in_messages buffer"""
        return state.in_messages.__contains__(msg)

    @staticmethod
    def sent(state: NState, msg: GiskardMessage) -> bool:
        """Returns True if the msg is in the out_messages buffer"""
        return state.out_messages.__contains__(msg)

    @staticmethod
    def record(state: NState, msg: GiskardMessage) -> NState:
        """Adds a message to the out_message buffer"""
        state.out_messages.append(msg)
        return state

    @staticmethod
    def record_plural(state: NState, lm: List[GiskardMessage]) -> NState:
        """Adds several messages to the out_message buffer"""
        state.out_messages.extend(lm)
        return state

    @staticmethod
    def add(state: NState, msg: GiskardMessage) -> NState:
        """Broadcast messages are stored in the in_messages buffer,
        awaiting processing"""
        state.in_messages.append(msg)
        return state

    @staticmethod
    def add_plural(state: NState, lm: List[GiskardMessage]) -> NState:
        """Adds several messages to the in_messages buffer"""
        state.in_messages.extend(lm)
        return state

    @staticmethod
    def discard(state: NState, msg: GiskardMessage) -> NState:
        """Removes an invalid message from the in_messages buffer"""
        state.in_messages.remove(msg)
        return state

    @staticmethod
    def process(state: NState, msg: GiskardMessage) -> NState:
        """Takes a message, removes it from the in_messages buffer
         and moves it to the counting_messages buffer, as it has been processed."""
        state = Giskard.discard(state, msg)
        state.counting_messages.append(msg)
        return state

    @staticmethod
    def increment_view(state: NState) -> NState:
        """View change happened,
        increments the view number, resets in_messages buffer and timeout variable."""
        state.node_view += 1
        state.in_messages = []
        state.timeout = False
        return state

    @staticmethod
    def flip_timeout(state: NState) -> NState:
        """Sets the timeout variable to True"""
        state.timeout = True
        return state

    # endregion

    # region local state properties
    @staticmethod
    def view_valid(state: NState, msg: GiskardMessage) -> bool:
        """A message has a valid view with respect to a local state when it is equal to the
        local state's current view. Nodes only process "non-expired" messages sent in its current view."""
        return state.node_view == msg.view

    # endregion

    # region byzantine behaviour
    """ The primary form of Byzantine behavior Giskard considers is double voting:
    sending two PrepareVote messages for two different blocks of the same height within the same view.
    We call two messages equivocating if they evidence double voting behavior of their sender."""

    @staticmethod
    def equivocating_messages(state: NState, msg1: GiskardMessage, msg2: GiskardMessage) -> bool:
        return msg1 in state.out_messages \
            and msg2 in state.out_messages \
            and msg1.view == msg2.view \
            and msg1.message_type == Message.CONSENSUS_GISKARD_PREPARE_VOTE \
            and msg2.message_type == Message.CONSENSUS_GISKARD_PREPARE_VOTE \
            and msg1.block != msg2.block \
            and msg1.block.block_num == msg2.block.block_num

    @staticmethod
    def exists_same_height_block_old(state: NState, b: GiskardBlock) -> bool:
        """Duplicate block checking for either:
        - a PrepareVote message in the processed message buffer for a different block of the same height, or
        - a PrepareVote or PrepareQC message in the sent message buffer for a different block of the same height."""
        msg: GiskardMessage
        for msg in state.counting_messages:
            if msg.message_type == Message.CONSENSUS_GISKARD_PREPARE_BLOCK \
                    and b.block_num == msg.block_num \
                    and b != msg.block:
                return True
        for msg in state.out_messages:
            if (msg.message_type == Message.CONSENSUS_GISKARD_PREPARE_VOTE \
                or msg.message_type == Message.CONSENSUS_GISKARD_PREPARE_QC) \
                    and b.block_num == msg.block_num \
                    and b != msg.block:
                return True
        return False

    @staticmethod
    def exists_same_height_block(state: NState, b: GiskardBlock) -> bool:
        """Returns True if there is a block in the out_messages buffer with the same height"""
        msg: GiskardBlock
        for msg in state.out_messages:
            if msg.message_type == Message.CONSENSUS_GISKARD_PREPARE_VOTE \
                    and b.block_num == msg.block_num \
                    and b != msg.block:
                return True
        return False

    @staticmethod
    def exists_same_height_PrepareBlock(state: NState, b: GiskardBlock) -> bool:
        msg: GiskardBlock
        for msg in state.counting_messages:
            if msg.message_type == Message.CONSENSUS_GISKARD_PREPARE_BLOCK \
                    and b.block_num == msg.block_num \
                    and b != msg.block:
                return True
        return False

    @staticmethod
    def same_height_block_msg(b: GiskardBlock, msg: GiskardMessage) -> bool:
        return msg.message_type == Message.CONSENSUS_GISKARD_PREPARE_VOTE \
            and b.block_num == msg.block_num \
            and b != msg.block

    # endregion

    # region prepare stage definitions
    """Blocks in Giskard go through three stages: Prepare, Precommit and Commit.
    The local definitions of these three stages are: 
    - a block is in prepare stage in some local state s iff it has received quorum PrepareVote messages
      or a PrepareQC message in the current view or some previous view, and
    - a block is in precommit stage in some local state s iff its child block is in prepare stage in s, and
    - a block is in commit stage in some local state s iff its child block is in precommit stage in s.
    
    We can parameterize the definition of a block being in prepare stage by some view."""

    @staticmethod
    def processed_PrepareVote_in_view_about_block(state: NState, view: int, b: GiskardBlock) -> List[GiskardMessage]:
        """Processed PrepareVote messages in some view about some block: """
        return list(filter(lambda msg: msg.message_type == Message.CONSENSUS_GISKARD_PREPARE_VOTE
                                       and msg.block == b
                                       and msg.view == view, state.counting_messages))

    @staticmethod
    def vote_quorum_in_view(state: NState, view: int, b: GiskardBlock, peers) -> bool:
        """Returns True if there is a vote quorum in the given view, for the given block"""
        return Giskard.quorum(Giskard.processed_PrepareVote_in_view_about_block(state, view, b), peers)

    @staticmethod
    def PrepareQC_in_view(state: NState, view: int, b: GiskardBlock) -> bool:
        """Returns True if there is a PrepareQC message in the counting_blocks buffer,
        for the given block and view"""
        for msg in state.counting_messages:
            if msg.view == view \
                    and msg.block == b \
                    and msg.message_type == Message.CONSENSUS_GISKARD_PREPARE_QC:
                return True
        return False

    @staticmethod
    def prepare_stage_in_view(state: NState, view: int, b: GiskardBlock) -> bool:
        """Returns True if there is a vote quorum or a PrepareQC message for the given block and view"""
        return Giskard.vote_quorum_in_view(state, view, b) or Giskard.PrepareQC_in_view(state, view, b)

    @staticmethod
    def prepare_stage(state: NState, b: GiskardBlock) -> bool:
        """We use this parameterized definition to define the general version of a block being
        in prepare stage: A block has reached prepare stage in some state iff it has reached prepare
        stage in some view that is less than or equal to the current view."""
        v_prime = state.node_view
        while v_prime >= 0:
            if Giskard.prepare_stage_in_view(state, v_prime, b):
                return True
            v_prime -= 1
        return False

    # endregion

    # region view change definitions
    """Participating nodes in Giskard vote in units of time called views. Each view has the same
    fixed set of participating nodes, which consists of one block proposer for the view, and
    validators. Participating nodes take turns to be the block proposer, and the identity of
    the block proposer for any given view is known to all participating nodes. A view generally
    proceeds as follows: at the beginning of the view, the block proposer proposes a fixed
    number of blocks, which validators vote on. If all blocks receive quorum votes, the nodes
    increment their local view and wait for new block proposals in the new view. Otherwise,
    a timeout occurs, and nodes exchange messages to communicate their acknowledgement of the
    timeout and determine the parent block for the first block of the new view. The end of a
    view for a participating node is marked by either: 
    - the last block proposed by the block proposer reaching prepare stage in its local state, or 
    - the receipt of a ViewChangeQC message following a timeout.
    
    We call the former a normal view change, and the latter an abnormal view change.
    Because we assume that all local clocks are synchronized, in the case of an abnormal
    view change, all nodes timeout at once. However, because nodes process messages at
    different speeds, blocks reach prepare stage at different speeds, and consequently,
    in the case of a normal view change, nodes are not guaranteed to increment their
    local view at the same time.
    
    In the case of an abnormal view change, the timeout flag is simultaneously flipped
    for all participating nodes, and each sends a ViewChange message containing their
    local highest block at prepare stage. Nodes then enter a liminal stage in which only
    three kinds of messages can be processed: PrepareQC, ViewChange and ViewChangeQC.
    
    Nodes will ignore all block proposals and block votes during this stage. Upon
    receiving quorum ViewChange messages, validator nodes increment their view and wait
    for new blocks to be proposed. The new block proposer aggregates the max height
    block from its received ViewChange messages, and uses it to produce new blocks
    for the new view.
    
    The following section contains definitions required for abnormal view changes."""

    @staticmethod
    def processed_ViewChange_in_view(state: NState, view: int) -> List[GiskardMessage]:
        """Quorum ViewChange messages in some view"""
        return list(filter(lambda msg: msg.message_type == Message.CONSENSUS_GISKARD_VIEW_CHANGE
                                       and msg.view == view, state.counting_messages))

    @staticmethod
    def view_change_quorum_in_view(state: NState, view: int, peers) -> bool:
        """Returns True if there is a quorum of ViewChange messages for the given view"""
        return Giskard.quorum(Giskard.processed_ViewChange_in_view(state, view), peers)

    @staticmethod
    def highest_ViewChange_block_in_view(state: NState, view: int) -> GiskardBlock:  # TODO test this one
        """Maximum height block from all processed ViewChange messages in view"""
        return reduce(lambda x, y: x if x.block_num > y.block_num else y,
                      [msg.block for msg in Giskard.processed_ViewChange_in_view(state, view)],
                      GiskardGenesisBlock)

    @staticmethod
    def highest_ViewChange_block_height_in_view(state: NState, view: int) -> int:
        return Giskard.highest_ViewChange_block_in_view(state, view).block_num

    @staticmethod
    def last_block(b: GiskardBlock) -> bool:
        """The last block of each view is identifiable to each participating node"""
        return Giskard.b_last(b)

    @staticmethod
    def highest_prepare_block_in_view(state: NState, view: int) -> GiskardBlock:  # TODO test this
        """Because not every view is guaranteed to have a block in prepare stage,
        the definition of <<highest_prepare_block_in_view>> must be able to recursively
        search for the highest prepare block in all past views"""
        if view == 0:
            return GiskardGenesisBlock
        else:
            return reduce(lambda x, y: x if x.height > y.height else y,
                          [msg.block for msg in state.counting_messages
                           if Giskard.prepare_stage_in_view(state, view, msg.block)],
                          Giskard.highest_prepare_block_in_view(state, view - 1))

    @staticmethod
    def highest_prepare_block_message(state: NState) -> GiskardMessage:
        """The following definition constructs the ViewChange message to be
        sent by each participating node upon a timeout."""
        return GiskardMessage(Message.CONSENSUS_GISKARD_PREPARE_QC,
                              state.node_view,
                              state.node_id,
                              Giskard.highest_prepare_block_in_view(state, state.node_view),
                              GiskardGenesisBlock)

    # endregion

    # region message construction
    """In Giskard, some message types "carry" other messages: 
    - PrepareBlock messages for the first block in a view carry the PrepareQC
      message of its parent block, PrepareBlock messages for non-first blocks
      carry the PrepareQC of the parent block of the first block, and
    - PrepareVote messages carry the PrepareQC message of its parent block, and
    - ViewChange messages carry the PrepareQC message of the highest local
      prepare stage block.
      
    We do not model this at the type level (i.e., by having inductive message
    type definitions), but rather simulate this behavior using pre- and post-conditions
    in our local transitions. For example, a node only processes a PrepareBlock message
    in the in message buffer if the PrepareQC message that is "piggybacked" onto has also
    been received, and the transition effectively processes both of these messages in one step.
    
    PrepareBlocks are computed from either: 
    - the final PrepareVote/PrepareQC message of the previous round, or 
    - the ViewChangeQC from the previous round. 
    
    The messages in both of these cases contain the parent block for the newly proposed blocks.
    Note that because blocks are proposed in sequence, only the first PrepareBlock message carries
    the parent block's PrepareQC message - the remaining blocks cannot do so because their parent
    blocks have not reached prepare stage yet, and therefore their PrepareQC messages cannot exist.
    Therefore, all PrepareBlock messages in a view carry the same PrepareQC message: that of
    the first block's parent."""

    @staticmethod
    def make_PrepareBlocks(state: NState, previous_msg: GiskardMessage, block_cache) -> List[GiskardMessage]:
        """Note that although all PrepareBlock messages are produced and sent together in one
        single transition, this does not mean that:
        - they are processed at the same time, and
        - we falsely enforce the discipline that the second proposed block contains the first
          block's PrepareQC when in fact it has not reached PrepareQC."""
        block1 = Giskard.generate_new_block(previous_msg.block, block_cache, 1)
        block2 = Giskard.generate_new_block(block1, block_cache, 2)
        block3 = Giskard.generate_last_block(block1, block_cache)  # labeled as last block in view
        return [GiskardMessage(Message.CONSENSUS_GISKARD_PREPARE_QC,
                               state.node_view,
                               state.node_id,
                               block1,
                               previous_msg.block),  # PrepareQC of the highest block from previous round
                GiskardMessage(Message.CONSENSUS_GISKARD_PREPARE_QC,
                               state.node_view,
                               state.node_id,
                               block2,
                               previous_msg.block),  # PrepareQC of the highest block from previous round
                GiskardMessage(Message.CONSENSUS_GISKARD_PREPARE_QC,
                               state.node_view,
                               state.node_id,
                               block3,
                               previous_msg.block)]  # PrepareQC of the highest block from previous round

    @staticmethod
    def make_PrepareVote(state: NState, quorum_msg: GiskardMessage,
                         prepareblock_msg: GiskardMessage) -> GiskardMessage:
        """A <<PrepareVote>> carries the <<PrepareQC>> of its parent, and can only be sent
        after parent block reaches prepare stage, which means one of its inputs must be
        either a <<PrepareVote>> or <<PrepareQC>>.

        <<PrepareVote>>s are also computed from <<PrepareBlock>> messages,
        which means another one of its inputs must be a <<PrepareBlock>> message."""
        return GiskardMessage(Message.CONSENSUS_GISKARD_PREPARE_VOTE,  # message type
                              state.node_view,  # view number
                              state.node_id,
                              prepareblock_msg.block,  # block to vote for
                              quorum_msg.block)

    @staticmethod
    def pending_PrepareVote(state: NState, quorum_msg: GiskardMessage, block_cache) -> List[GiskardMessage]:
        """Nodes create <<PrepareVote>> messages upon receiving <<PrepareBlock>> messages
        for each block, and "wait" to send it until the parent block reaches prepare stage.
        This is modeled by constructing <<PrepareVote>> messages on-demand given that:
        - the parent block has just reached prepare stage, and
        - a <<PrepareBlock>> message exists for the child block.

        Constructing pending PrepareVote messages for child messages with existing PrepareBlocks."""
        return list(map(lambda prepare_block_msg:
                        Giskard.make_PrepareVote(state, quorum_msg, prepare_block_msg),
                        filter(lambda msg: msg.view == quorum_msg.view
                                           and not Giskard.exists_same_height_block(state, msg.block)
                                           and Giskard.parent_ofb(msg.block, quorum_msg.block, block_cache)
                                           and msg.message_type == Message.CONSENSUS_GISKARD_PREPARE_BLOCK,
                               state.counting_messages)))

    @staticmethod
    def make_PrepareQC(state: NState, msg: GiskardMessage) -> GiskardMessage:
        """PrepareQC messages carry nothing, and can only be sent after a quorum number of PrepareVotes,
        which means its only input is a PrepareVote containing the relevant block."""
        return GiskardMessage(Message.CONSENSUS_GISKARD_PREPARE_QC,  # message type
                              state.node_view,  # view number
                              state.node_id,
                              msg.block,
                              GiskardGenesisBlock)

    @staticmethod
    def make_ViewChange(state: NState) -> GiskardMessage:
        """ViewChange messages carry the <<PrepareQC>> message of the highest block to
        reach prepare stage, and since they are triggered by timeouts, no input
        message is required."""
        return GiskardMessage(Message.CONSENSUS_GISKARD_VIEW_CHANGE,
                              state.node_view,
                              state.node_id,
                              Giskard.highest_prepare_block_in_view(state, state.node_view),
                              GiskardGenesisBlock)

    @staticmethod
    def make_ViewChangeQC(state: NState, highest_msg: GiskardMessage) -> GiskardMessage:
        """Upon receiving quorum <<ViewChange>> messages, the block proposer for the new
        view aggregates the max height block from all the <<ViewChange>> messages and
        sends a <<ViewChangeQC>> containing this block, alongside a <<PrepareQC>> message
        evidencing its prepare stage."""
        return GiskardMessage(Message.CONSENSUS_GISKARD_VIEW_CHANGE_QC,
                              state.node_view,
                              state.node_id,
                              highest_msg.block,
                              GiskardGenesisBlock)

    # endregion

    # region block methods
    @staticmethod
    def generate_new_block(block: GiskardBlock, block_cache,
                           block_index) -> GiskardBlock:  # TODO determine the block index via parent relation i guess via hash from parent
        """Generates a new giskard block
        TODO block as input is the parent block -> have to get the new child block from the handler in engine"""
        new_block = Block(block.block_id + 1,
                          block.block_id,
                          block.signer_id,
                          block.block_num + 1,
                          block.payload,
                          block.summary)
        return GiskardBlock(new_block, block_index)

    @staticmethod
    def b_last(b: GiskardBlock):
        """Returns True if the block index is the last block in the current view 3"""
        return b.block_index == LAST_BLOCK_INDEX_IDENTIFIER

    @staticmethod
    def generate_last_block(block: GiskardBlock, block_cache) -> GiskardBlock:
        """TODO still have to figure out if this should be a function or a test"""
        return Giskard.generate_new_block(block, block_cache, 3)

    @staticmethod
    def about_generate_last_block(block: GiskardBlock, block_cache, block_index) -> bool:
        """Test if the next block to generate would be the last block"""
        return Giskard.generate_last_block(block,
                                           block_cache).block_height == block.block_height + 1 and Giskard.b_last(
            Giskard.generate_last_block(block, block_cache))

    @staticmethod
    def about_non_last_block(block: GiskardBlock, block_cache, block_index) -> bool:
        """Test if the next to be generated block will be the last"""
        return not Giskard.b_last(Giskard.generate_new_block(block, block_cache, block_index))

    @staticmethod
    def parent_of(block: GiskardBlock, block_cache) -> GiskardBlock:
        """tries to get the parent block from the store
        :param block_cache:
        :param block:
        :return: GiskardBlock, or None
        """
        return block_cache.block_store.get_parent_block(
            block)  # check in Store if there is a block with height -1 and the previous id

    @staticmethod
    def parent_ofb(block: GiskardBlock, parent: GiskardBlock, block_cache) -> bool:
        """Test if parent block relation works with blocks in storage"""
        return Giskard.parent_of(block, block_cache) == parent

    @staticmethod
    def higher_block(b1: GiskardBlock, b2: GiskardBlock) -> GiskardBlock:
        return b1 if (b1.block_num > b2.block_num) else b2

    # endregion

    # region messages and quorum methods
    @staticmethod
    def message_with_higher_block(msg1: GiskardMessage, msg2: GiskardMessage) -> GiskardMessage:
        """returns the given message with the higher block"""
        Giskard.higher_block(msg1.block, msg2.block)

    @staticmethod
    def has_at_least_two_thirds(nodes, peers) -> bool:
        """Check if the given nodes are a two third majority in the current view"""
        matches = [node for node in nodes if node in peers]
        return len(matches) / len(peers) >= 2 / 3

    @staticmethod
    def has_at_least_one_third(nodes, peers) -> bool:
        """Check if the given nodes are at least a third of the peers in the current view"""
        matches = [node for node in nodes if node in peers]
        return len(matches) / len(peers) >= 1 / 3

    @staticmethod
    def majority_growth(nodes, node, peers) -> bool:
        """is a two third majority reached when this node is added?"""
        return Giskard.has_at_least_two_thirds(nodes.append(node), peers)

    @staticmethod
    def majority_shrink(nodes, node, peers) -> bool:
        """if the given node is removed from nodes, is the two third majority lost?"""
        return not Giskard.has_at_least_two_thirds(nodes.remove(node), peers)

    @staticmethod
    def intersection_property(nodes1, nodes2, peers) -> bool:
        """Don't know if actually needed;
        Checks if the intersection between two lists of nodes, is at least one third of the peers"""
        if Giskard.has_at_least_two_thirds(nodes1, peers) \
                and Giskard.has_at_least_two_thirds(nodes2, peers):
            matches = [value for value in nodes1 if value in nodes2]
            return Giskard.has_at_least_one_third(matches, peers)
        return False

    @staticmethod
    def is_member(node, nodes) -> bool:
        """checks if a node is actually in a list of nodes"""
        return node in nodes

    @staticmethod
    def evil_participants_no_majority(peers) -> bool:
        """Returns True if there is no majority of dishonest nodes
        TODO make peers of type GiskardNode"""
        return not Giskard.has_at_least_one_third(list(filter(lambda peer: peer.dishonest, peers)))

    @staticmethod
    def quorum(lm: List[GiskardMessage], peers) -> bool:
        """returns True if a sender from a list of messages has a quorum"""
        return Giskard.has_at_least_two_thirds([msg.sender for msg in lm], peers)

    # Don't know if i even need this
    """@staticmethod
        def quorum_subset(self, lm1, lm2):
        if self.quorum(lm1) \
                and self.quorum(lm2):
            matches = [value for value in  if value in nodes2]
            return self.has_at_least_one_third(matches)
        return False"""

    @staticmethod
    def quorum_growth(lm: List[GiskardMessage], msg: GiskardMessage, peers) -> bool:
        """returns True if a quorum can be reached when a msg is added to a list of messages"""
        return Giskard.has_at_least_two_thirds([msg1.sender for msg1 in lm.append(msg)], peers)
    # endregion
