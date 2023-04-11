class NState:
    """State the Giskard node is in"""

    def __init__(self, node):
        self.node_view = 0
        self.node_id = node.node_id
        self.in_messages = []
        self.counting_messages = []
        self.out_messages = []
        self.timeout = False

    def __init__(self, node_view, node_id, in_messages, counting_messages, out_messages, timeout):
        self.node_view = node_view
        self.node_id = node_id
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
