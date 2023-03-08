class GiskardBlock:
    def __init__(self, block, block_index):
        # fields that come with consensus blocks
        self.block_id = block.block_id  # hash of the block -> corresponds to giskard b_h
        self.previous_id = block.previous_id  # hash of the previous block
        self.signer_id = block.signer_id
        self.block_num = block.block_num  # block height
        self.payload = block.payload
        self.summary = block.summary

        # fields that giskard requires
        self.block_index = 0  # the block index in the current view


class GiskardOracle:
    '''This is a wrapper around the Giskard structures (publisher,
    verifier, fork resolver) and their attendant proxies.
    '''

    def generate_new_block(block, block_index):  # TODO determine the block index via parent relation i guess via hash from parent
        return GiskardBlock(block, 0)

    def generate_last_block(block):
        return GiskardOracle.generate_new_block(block, 3)



