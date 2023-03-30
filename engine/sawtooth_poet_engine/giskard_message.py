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
