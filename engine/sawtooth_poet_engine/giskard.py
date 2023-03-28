from collections import namedtuple

from engine import GiskardEngine
from giskard_block import GiskardBlock
from poet_consensus import utils


class NState:
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
        gets called by GiskardEngine for incoming messages
        has all functionalities in a procedural style"""

    # region state methods

    @staticmethod
    def record_set(state: NState, msg: GiskardMessage):
        """adds a message to the out_message buffer
        input NState, GiskardMessage
        returns NState"""
        state.out_messages.append(msg)
        return state

    @staticmethod
    def record_plural_set(state: NState, lm: list(GiskardMessage)):
        state.out_messages.extend(lm)
        return state

    @staticmethod
    def add_set(state: NState, msg: GiskardMessage):
        state.in_messages.append(msg)
        return state

    @staticmethod
    def add_plural_set(state: NState, lm: list(GiskardMessage)):
        state.in_messages.extend(lm)
        return state

    @staticmethod
    def discard_set(state: NState, msg: GiskardMessage):
        state.in_messages.remove(msg)
        return state

    @staticmethod
    def process_set(state: NState, msg: GiskardMessage):
        state = Giskard.discard_set(state, msg)
        state.counting_messages.append(msg)
        return state
    # endregion

    # region block methods
    @staticmethod
    def generate_new_block(state: NState, block: GiskardBlock, block_index):  # TODO determine the block index via parent relation i guess via hash from parent
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
        Giskard.has_at_least_two_thirds([msg.sender for msg in lm], peers)

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
        return Giskard.has_at_least_two_thirds([msg1.sender for msg1 in lm.append(msg)], peers)
    # endregion
