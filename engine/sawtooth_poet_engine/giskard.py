from collections import namedtuple

from engine import GiskardEngine
from giskard_block import GiskardBlock
from poet_consensus import utils
from sawtooth_sdk.protobuf import Message


class NState:
    """State the Giskard node is in"""

    def __init__(self, node: GiskardEngine):
        self.node_view = 0
        self.node_id = node.validator_id
        self.in_messages = []
        self.counting_messages = []
        self.out_messages = []
        self.timeout = False

    def __eq__(self, other):
        if not isinstance(other, NState):
            return NotImplemented
        return self.node_view == other.node_view \
            and self.node_id == other.node_id \
            and self.in_messages == other.in_messages \
            and self.counting_messages == other.counting_messages \
            and self.out_messages == other.out_messages \
            and self.timeout == other.timeout


class GiskardMessage:
    """All Giskard messages have at least those fields"""

    def __init__(self, message_type, view, sender, block, piggyback_block):
        self.message_type = message_type
        self.view = view
        self.sender = sender
        self.block = block
        self.piggyback_block = piggyback_block

    def __eq__(self, other):
        # TODO check if sender should be node instead of just address
        if not isinstance(other, GiskardMessage):
            return NotImplemented
        return self.message_type == other.message_type \
            and self.view == other.view \
            and self.sender == other.sender \
            and self.block == other.block \
            and self.piggyback_block == other.piggyback_block


class Giskard:
    """The main class for Giskard
        gets called by GiskardEngine for incoming messages"""

    # region local state operations
    @staticmethod
    def received(state: NState, msg: GiskardMessage):
        """Returns True if the msg is in the in_messages buffer"""
        return state.in_messages.__contains__(msg)

    @staticmethod
    def sent(state: NState, msg: GiskardMessage):
        """Returns True if the msg is in the out_messages buffer"""
        return state.out_messages.__contains__(msg)

    @staticmethod
    def record(state: NState, msg: GiskardMessage):
        """Adds a message to the out_message buffer"""
        state.out_messages.append(msg)
        return state

    @staticmethod
    def record_plural(state: NState, lm: list(GiskardMessage)):
        """Adds several messages to the out_message buffer"""
        state.out_messages.extend(lm)
        return state

    @staticmethod
    def add(state: NState, msg: GiskardMessage):
        """Broadcast messages are stored in the in_messages buffer,
        awaiting processing"""
        state.in_messages.append(msg)
        return state

    @staticmethod
    def add_plural(state: NState, lm: list(GiskardMessage)):
        """Adds several messages to the in_messages buffer"""
        state.in_messages.extend(lm)
        return state

    @staticmethod
    def discard(state: NState, msg: GiskardMessage):
        """Removes an invalid message from the in_messages buffer"""
        state.in_messages.remove(msg)
        return state

    @staticmethod
    def process(state: NState, msg: GiskardMessage):
        """Takes a message, removes it from the in_messages buffer
         and moves it to the counting_messages buffer, as it has been processed."""
        state = Giskard.discard(state, msg)
        state.counting_messages.append(msg)
        return state

    @staticmethod
    def increment_view(state: NState):
        """View change happened,
        increments the view number, resets in_messages buffer and timeout variable."""
        ++state.node_view
        state.in_messages = []
        state.timeout = False

    @staticmethod
    def flip_timeout(state: NState):
        """Sets the timeout variable to True"""
        state.timeout = True
        return state

    # endregion

    # region local state properties
    @staticmethod
    def view_valid(state: NState, msg: GiskardMessage):
        """A message has a valid view with respect to a local state when it is equal to the
        local state's current view. Nodes only process "non-expired" messages sent in its current view."""
        return state.node_view == msg.view

    # endregion

    # region byzantine behaviour
    """ The primary form of Byzantine behavior Giskard considers is double voting:
    sending two PrepareVote messages for two different blocks of the same height within the same view.
    We call two messages equivocating if they evidence double voting behavior of their sender."""
    @staticmethod
    def equivocating_messages(state: NState, msg1: GiskardMessage, msg2: GiskardMessage):
        return msg1 in state.out_messages \
            and msg2 in state.out_messages \
            and msg1.view == msg2.view \
            and msg1.message_type == Message.CONSENSUS_GISKARD_PREPARE_VOTE \
            and msg2.message_type == Message.CONSENSUS_GISKARD_PREPARE_VOTE \
            and msg1.block != msg2.block \
            and msg1.block.block_num == msg2.block.block_num

    @staticmethod
    def exists_same_height_block_old(state: NState, b: GiskardBlock):
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
    def exists_same_height_block(state: NState, b: GiskardBlock):
        """Returns True if there is a block in the out_messages buffer with the same height"""
        msg: GiskardBlock
        for msg in state.out_messages:
            if msg.message_type == Message.CONSENSUS_GISKARD_PREPARE_VOTE \
                    and b.block_num == msg.block_num \
                    and b != msg.block:
                return True
        return False

    @staticmethod
    def exists_same_height_PrepareBlock(state: NState, b: GiskardBlock):
        msg: GiskardBlock
        for msg in state.counting_messages:
            if msg.message_type == Message.CONSENSUS_GISKARD_PREPARE_BLOCK \
                    and b.block_num == msg.block_num \
                    and b != msg.block:
                return True
        return False

    @staticmethod
    def same_height_block_msg(b: GiskardBlock, msg: GiskardMessage):
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
    def processed_PrepareVote_in_view_about_block(state: NState, view: int, b: GiskardBlock):
        """Processed PrepareVote messages in some view about some block: """
        return filter(lambda msg: msg.message_type == Message.CONSENSUS_GISKARD_PREPARE_VOTE
                                  and msg.block == b
                                  and msg.view == view, state.counting_messages)

    @staticmethod
    def vote_quorum_in_view(state: NState, view: int, b: GiskardBlock):
        return Giskard.quorum(Giskard.processed_PrepareVote_in_view_about_block(state, view, b))
    # endregion

    # region block methods
    @staticmethod
    def generate_new_block(state: NState, block: GiskardBlock,
                           block_index):  # TODO determine the block index via parent relation i guess via hash from parent
        """Generates a new giskard block
        TODO block as input is the parent block -> have to get the new child block from the handler in engine"""
        return GiskardBlock(block, 0)

    @staticmethod
    def generate_last_block(state: NState, block: GiskardBlock):
        """TODO still have to figure out if this should be a function or a test"""
        return Giskard.generate_new_block(state, block, 3)

    @staticmethod
    def about_generate_last_block(state: NState, block: GiskardBlock):
        """Test if the next block to generate would be the last block"""
        return Giskard.generate_last_block(state, block).block_height == block.block_height + 1 and utils.b_last(
            Giskard.generate_last_block(state, block))

    @staticmethod
    def about_non_last_block(state: NState, block: GiskardBlock):
        """Test if the next to be generated block will be the last"""
        return not utils.b_last(Giskard.generate_new_block(state, block))

    @staticmethod
    def about_generate_new_block(state: NState, block: GiskardBlock):
        """ Lemma: proofs that all heights are correct;
        GiskardBlock.b_height(Giskard.generate_new_block(bock)) == GiskardBlock.b_height(block) + 1"""
        return Giskard.parent_block_height(state, block.block_num) and Giskard.generate_new_block_parent(state, block)

    @staticmethod
    def parent_of(state: NState, block: GiskardBlock):
        """tries to get the parent block from the store
        :param block:
        :return: GiskardBlock, or None
        """
        return state._block_cache.block_store.get_child_block(
            block)  # check in Store if there is a block with height -1 and the previous id

    @staticmethod
    def parent_ofb(state: NState, block: GiskardBlock, parent: GiskardBlock):
        """Test if parent block relation works with blocks in storage"""
        return Giskard.parent_of(state, block) == parent

    @staticmethod
    def parent_ofb_correct(state: NState, depth):
        """
        Test if parent relation correct for all blocks in storage
        :param depth: of the chain until testing is stopped
        :return True if all parents are correct, False if one is not:
        """
        (i, child_block) = (0, None)
        for block in state._block_cache.block_store.get_block_iter(reverse=True):
            if i == depth:
                return True
            if i == 0:
                child_block = block
                continue
            else:
                if Giskard.parent_of(state, child_block) != block:
                    return False
                else:
                    i += 1
                    child_block = block
        return True

    @staticmethod
    def parent_block_height(state: NState, depth):
        """Test if all parent blocks in storage have correct heights
        :param depth: of the chain until testing is stopped
        :return True if all parents' heights are correct, False if one is not:
        """
        (i, child_block) = (0, None)
        for block in state._block_cache.block_store.get_block_iter(reverse=True):
            if i == depth:
                return True
            if i == 0:
                child_block = block
                continue
            else:
                if not Giskard.parent_of(state, child_block).block_num + 1 == child_block.block_num:
                    return False
                else:
                    i += 1
                    child_block = block
        return True

    @staticmethod
    def generate_new_block_parent(state: NState, block: GiskardBlock):
        """Test if parent block realtion works with generation of new block"""
        return Giskard.parent_of(Giskard.generate_new_block(state, block)) == block

    @staticmethod
    def higher_block(b1: GiskardBlock, b2: GiskardBlock):
        return b1 if (b1.block_num > b2.block_num) else b2

    # endregion

    # region messages and quorum methods
    @staticmethod
    def message_with_higher_block(msg1: GiskardMessage, msg2: GiskardMessage):
        """returns the given message with the higher block"""
        Giskard.higher_block(msg1.block, msg2.block)

    @staticmethod
    def has_at_least_two_thirds(nodes: list(GiskardEngine), peers: list(GiskardEngine)):
        """Check if the given nodes are a two third majority in the current view"""
        matches = [node for node in nodes if node in peers]
        return len(matches) / len(peers) >= 2 / 3

    @staticmethod
    def has_at_least_one_third(nodes: list(GiskardEngine), peers: list(GiskardEngine)):
        """Check if the given nodes are at least a third of the peers in the current view"""
        matches = [node for node in nodes if node in peers]
        return len(matches) / len(peers) >= 1 / 3

    @staticmethod
    def majority_growth(nodes: list(GiskardEngine), node: GiskardEngine, peers: list(GiskardEngine)):
        """is a two third majority reached when this node is added?"""
        return Giskard.has_at_least_two_thirds(nodes.append(node), peers)

    @staticmethod
    def majority_shrink(nodes: list(GiskardEngine), node: GiskardEngine, peers: list(GiskardEngine)):
        """if the given node is removed from nodes, is the two third majority lost?"""
        return not Giskard.has_at_least_two_thirds(nodes.remove(node), peers)

    @staticmethod
    def intersection_property(state: NState, nodes1: list(GiskardEngine), nodes2: list(GiskardEngine)):
        """Don't know if actually needed;
        Checks if the intersection between two lists of nodes, is at least one third of the peers"""
        if Giskard.has_at_least_two_thirds(state, nodes1) \
                and Giskard.has_at_least_two_thirds(state, nodes2):
            matches = [value for value in nodes1 if value in nodes2]
            return Giskard.has_at_least_one_third(state, matches)
        return False

    @staticmethod
    def is_member(node: GiskardEngine, nodes: list(GiskardEngine)):
        """checks if a node is actually in a list of nodes"""
        return node in nodes

    @staticmethod
    def evil_participants_no_majority(peers: list(GiskardEngine)):
        """Returns True if there is no majority of dishonest nodes
        TODO make peers of type GiskardNode"""
        return not Giskard.has_at_least_one_third(filter(lambda peer: not GiskardEngine.honest_node(peer), peers))

    @staticmethod
    def quorum(lm: list(GiskardMessage), peers: list(GiskardEngine)):
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
    def quorum_growth(lm: list(GiskardMessage), msg: GiskardMessage, peers: list(GiskardEngine)):
        """returns True if a quorum can be reached when a msg is added to a list of messages"""
        return Giskard.has_at_least_two_thirds([msg1.sender for msg1 in lm.append(msg)], peers)
    # endregion
