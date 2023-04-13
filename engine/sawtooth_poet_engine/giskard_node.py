from sawtooth_poet_tests.integration_tools import BlockCacheMock


class GiskardNode:
    def __init__(self, node_id="", node_view=0, dishonest=False, block_cache=None):
        self.node_id = node_id
        self.node_view = node_view
        self.dishonest = dishonest
        self.block_cache = block_cache
        if not block_cache:
            self.block_cache = BlockCacheMock([])

    def __eq__(self, other):
        if not isinstance(other, GiskardNode):
            return NotImplemented
        return self.node_id == other.node_id \
            and self.node_view == other.node_view \
            and self.dishonest == other.dishonest
