from sawtooth_poet_tests.integration_tools import BlockCacheMock


class GiskardNode(object):
    def __init__(self, node_id="", dishonest=False, block_cache=None):
        self.node_id = node_id
        self.dishonest = dishonest
        self.block_cache = block_cache
        if not block_cache:
            self.block_cache = BlockCacheMock([])

    def __eq__(self, other):
        if not isinstance(other, GiskardNode):
            return NotImplemented
        return self.node_id == other.node_id \
            and self.dishonest == other.dishonest
