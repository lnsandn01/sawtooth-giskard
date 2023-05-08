from collections import namedtuple

from sawtooth_poet.journal.block_wrapper import NULL_BLOCK_IDENTIFIER


class Block(object):
    def __init__(self, block_id, previous_id, signer_id, block_num, payload, summary):
        self.block_id = block_id
        self.previous_id = previous_id
        self.signer_id = signer_id
        self.block_num = block_num
        self.payload = payload
        self.summary = summary


class GiskardBlock(object):
    def __init__(self, block, block_index=0):
        # fields that come with consensus blocks
        self.block_id = block.block_id  # hash of the block -> corresponds to giskard b_h
        self.previous_id = block.previous_id  # hash of the previous block
        self.signer_id = block.signer_id
        self.block_num = block.block_num  # block height
        self.payload = block.payload
        self.summary = block.summary

        # fields that giskard requires
        self.block_index = block_index  # the block index in the current view

    def __eq__(self, other):
        return self.block_id == other.block_id \
            and self.previous_id == other.previous_id \
            and self.signer_id == other.signer_id \
            and self.block_num == other.block_num \
            and self.payload == other.payload \
            and self.summary == other.summary

    def __str__(self):
        block_id = self.block_id
        previous_id = self.previous_id
        signer_id = self.signer_id
        summary = self.summary
        if hasattr(block_id, 'hex'):
            block_id = block_id.hex()
        if hasattr(previous_id, 'hex'):
            previous_id = previous_id.hex()
        if hasattr(signer_id, 'hex'):
            signer_id = signer_id.hex()
        if hasattr(summary, 'hex'):
            summary = summary.hex()

        return (
            "Block("
            + ", ".join([
                "block_num: {}".format(self.block_num),
                "block_id: {}".format(block_id),
                "previous_id: {}".format(previous_id),
                "signer_id: {}".format(signer_id),
                "payload: {}".format(self.payload),
                "summary: {}".format(summary),
            ])
            + ")"
        )

    def b_height(block):
        return block.block_num

    def b_index(block):
        return block.block_index


class GiskardGenesisBlock(GiskardBlock):
    def __init__(self):
        super().__init__(Block(NULL_BLOCK_IDENTIFIER, NULL_BLOCK_IDENTIFIER, 0, 0, "", ""), 0)

    def __eq__(self, other):
        if not hasattr(other, 'block_index'):  # for comparison with non-GiskardBlocks
            return self.block_num == other.block_num

        return self.block_num == other.block_num \
            and self.previous_id == other.previous_id
