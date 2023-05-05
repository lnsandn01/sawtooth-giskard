from typing import List

from sawtooth_poet_engine.giskard_node import GiskardNode
from sawtooth_poet_engine.giskard_nstate import NState
from sawtooth_poet_engine.giskard_message import GiskardMessage


class GState(object):
    """ A global state is defined as a pair containing:
    - a mapping from node identifier to its local state, and
    - a list of messages containing all broadcasted messages thus far. """

    def __init__(self, nodes = [], gstate={}, broadcast_msgs: List[GiskardMessage] = []):
        """ The initial global state is defined as the collection of initial local
        states, paired with an empty message buffer. """
        self.gstate = gstate
        if not gstate:
            if not nodes:
                self.gstate = {}
            else:
                if len(nodes) > 0:
                    if isinstance(nodes[0], str):
                        self.gstate = {node: [NState(None, 0, node)] for node in nodes}
                    else:
                        self.gstate = {node.node_id: [NState(node)] for node in nodes}
        self.broadcast_msgs = broadcast_msgs

    def __eq__(self, other):
        if not isinstance(other, GState):
            return NotImplemented
        return self.gstate == other.gstate \
            and self.broadcast_msgs == other.broadcast_msgs

    def __str__(self):
        return (self.gstate.__str__()
                + self.broadcast_msgs.__str__())
