
class GiskardMessage(object):
    """All Giskard messages have at least those fields"""

    def __init__(self, message_type, view, sender, block, piggyback_block):
        self.message_type = message_type
        self.view = view
        self.sender = sender
        self.block = block
        self.piggyback_block = piggyback_block

    def __eq__(self, other):
        if not isinstance(other, GiskardMessage):
            return NotImplemented
        return self.message_type == other.message_type \
            and self.view == other.view \
            and self.sender == other.sender \
            and self.block == other.block \
            and self.piggyback_block == other.piggyback_block

    def __str__(self):
        return ("message_type: " + str(self.message_type) +
                "view: " + str(self.view) +
                "sender: " + self.sender +
                "block" + self.block.__str__() +
                "piggyback_block" + self.piggyback_block.__str__())
