from typing import List

from sawtooth_poet_engine.giskard_message import GiskardMessage
from sawtooth_poet_engine.giskard_node import GiskardNode


class NState:
    """State the Giskard node is in"""

    def __init__(self, node: GiskardNode = None,  node_view: int = 0, node_id="",
                 in_messages: List[GiskardMessage] = [], counting_messages: List[GiskardMessage] = [],
                 out_messages: List[GiskardMessage] = [], timeout: bool = False):
        self.node_view = node_view
        self.node_id = node_id
        if node is not None:
            self.node_id = node.node_id
        self.in_messages = in_messages
        self.counting_messages = counting_messages
        self.out_messages = out_messages
        self.timeout = timeout

    def __eq__(self, other):
        if not isinstance(other, NState):
            return NotImplemented
        return self.node_view == other.node_view \
            and self.node_id == other.node_id \
            and self.in_messages == other.in_messages \
            and self.counting_messages == other.counting_messages \
            and self.out_messages == other.out_messages \
            and self.timeout == other.timeout
