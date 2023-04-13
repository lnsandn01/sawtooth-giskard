from typing import List

from giskard_node import GiskardNode
from giskard_nstate import NState
from giskard_message import GiskardMessage


class GState:
    """ A global state is defined as a pair containing:
    - a mapping from node identifier to its local state, and
    - a list of messages containing all broadcasted messages thus far. """

    def __init__(self, nodes: List[GiskardNode] = [], gstate=None, broadcast_msgs: List[GiskardMessage] = []):
        """ The initial global state is defined as the collection of initial local
        states, paired with an empty message buffer. """
        self.gstate = gstate
        if not gstate:
            self.gstate = {node.node_id: NState(node) for node in nodes}
        self.broadcast_msgs = broadcast_msgs

    def __eq__(self, other):
        if not isinstance(other, GState):
            return NotImplemented
        return self.gstate == other.gstate \
            and self.broadcast_msgs == other.broadcast_msgs
