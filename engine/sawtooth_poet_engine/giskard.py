from collections import namedtuple

from giskard_block import GiskardBlock
from poet_consensus import utils


class Giskard:
    """The main class for Giskard
        gets called by GiskardEngine for incoming messages
        has all functionalities in a procedural style"""

    # region state methods
    # TODO maybe extend to more required fields, or keep as a comparison to the formal specification
    NState = namedtuple("_node_view, node_id, _in_messages, _counting_messages, _out_messages, _timeout")

    @staticmethod
    def NState_get(state):
        return state.NState(state._node_view,
                           state.node_id,
                           state._in_messages,
                           state._counting_messages,
                           state._out_messages,
                           state._timeout)

    @staticmethod
    def NState_eqb(s1: NState, s2: NState):
        return s1 == s2

    @staticmethod
    def honest_nodeb(state):
        return not state._dishonest

    @staticmethod
    def is_block_proposer(state):  # TODO check how to do this with the peers, blocks proposed, view_number, node_id, timeout
        return True

    # def is_new_proposer_unique TODO write test for that that checks if indeed all views had unique proposers

    @staticmethod
    def record_set(state, m: GiskardMessage):
        """adds a message to the out_message buffer
        input NState, GiskardMessage
        returns NState"""
        return state.out_messages.Append(m)
    # endregion

    # region block methods
    @staticmethod
    def generate_new_block(state, block, block_index):  # TODO determine the block index via parent relation i guess via hash from parent
        """Generates a new giskard block
        TODO block as input is the parent block -> have to get the new child block from the handler in engine"""
        return GiskardBlock(block, 0)

    @staticmethod
    def generate_last_block(state, block):
        """TODO still have to figure out if this should be a function or a test"""
        return Giskard.generate_new_block(state, block, 3)

    @staticmethod
    def about_generate_last_block(state, block):
        """Test if the next block to generate would be the last block"""
        return Giskard.generate_last_block(state, block).block_height == block.block_height + 1 and utils.b_last(
            Giskard.generate_last_block(state, block))

    @staticmethod
    def about_non_last_block(state, block):
        """Test if the next to be generated block will be the last"""
        return not utils.b_last(Giskard.generate_new_block(state, block))

    @staticmethod
    def about_generate_new_block(state, block):
        """ Lemma: proofs that all heights are correct;
        GiskardBlock.b_height(Giskard.generate_new_block(bock)) == GiskardBlock.b_height(block) + 1"""
        return Giskard.parent_block_height(state, block.block_num) and Giskard.generate_new_block_parent(state, block)

    @staticmethod
    def parent_of(state, block):
        """tries to get the parent block from the store
        :param block:
        :return: GiskardBlock, or None
        """
        return state._block_cache.block_store.get_child_block(block)  # check in Store if there is a block with height -1 and the previous id

    @staticmethod
    def parent_ofb(state, block, parent):
        """Test if parent block relation works with blocks in storage"""
        return Giskard.parent_of(state, block) == parent

    @staticmethod
    def parent_ofb_correct(state, depth):
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
    def parent_block_height(state, depth):
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
    def generate_new_block_parent(state, block):
        """Test if parent block realtion works with generation of new block"""
        return Giskard.parent_of(Giskard.generate_new_block(state, block)) == block

    @staticmethod
    def higher_block(b1: GiskardBlock, b2: GiskardBlock):
        return b1 if (b1.block_num > b2.block_num) else b2
    # endregion

    # region messages and quorum methods
    @staticmethod
    def message_with_higher_block(msg1, msg2):
        Giskard.higher_block(msg1.block, msg2.block)

    @staticmethod
    def has_at_least_two_thirds(state, nodes):
        """Check if the given nodes are a two third majority in the current view"""
        matches = [value for value in nodes if value in state._peers]
        return len(matches) / state._k_peers >= 2 / 3

    @staticmethod
    def has_at_least_one_third(state, nodes):
        """Check if the given nodes are at least a third of the peers in the current view"""
        matches = [value for value in nodes if value in state._peers]
        return len(matches) / state._k_peers >= 1 / 3

    @staticmethod
    def majority_growth(state, nodes, node):
        """is a two third majority reached when this node is added?"""
        return Giskard.has_at_least_two_thirds(state, nodes.append(node))

    @staticmethod
    def majority_shrink(state, nodes, node):
        """if the given node is removed from nodes, is the two third majority lost?"""
        return not Giskard.has_at_least_two_thirds(state, nodes.remove(node))

    @staticmethod
    def intersection_property(state, nodes1, nodes2):
        """Don't know if actually needed;
        Checks if the intersection between two lists of nodes, is at least one third of the peers"""
        if Giskard.has_at_least_two_thirds(state, nodes1) \
                and Giskard.has_at_least_two_thirds(state, nodes2):
            matches = [value for value in nodes1 if value in nodes2]
            return Giskard.has_at_least_one_third(state, matches)
        return False

    @staticmethod
    def is_member(node, nodes):
        """checks if a node is actually in a list of nodes"""
        return node in nodes

    @staticmethod
    def evil_participants_no_majority(state):
        """Returns True if there is no majority of dishonest nodes
        TODO make peers of type GiskardNode"""
        return not Giskard.has_at_least_one_third(state, filter(lambda peer: not peer.honest_nodeb(), state._peers))

    # messages
    @staticmethod
    def message_eqb(message1, message2):
        return message1.message_type == message2.message_type \
            and message1.view == message2.view \
            and message1.sender == message2.sender \
            and message1.block == message2.block \
            and message1.piggy_back_block == message2.piggy_back_block

    @staticmethod
    def quorum(state, lm):
        Giskard.has_at_least_two_thirds(state, map(GiskardMessage.sender(), lm))  # TODO get the message class

    # Dont know if i even need this
    """@staticmethod
        def quorum_subset(self, lm1, lm2):
        if self.quorum(lm1) \
                and self.quorum(lm2):
            matches = [value for value in  if value in nodes2]
            return self.has_at_least_one_third(matches)
        return False"""

    @staticmethod
    def quorum_growth(state, lm, msg):
        return Giskard.has_at_least_two_thirds(state,
            map(GiskardMessage.sender(), lm.Append(msg)))  # TODO get the message class
    # endregion