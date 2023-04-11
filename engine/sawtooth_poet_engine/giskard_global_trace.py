class GTrace:
    """ A trace is a mapping from the natural numbers to global states. """
    def __init__(self):
        self.gtrace = []

    def __eq__(self, other):
        if not isinstance(other, GTrace):
            return NotImplemented
        return self.gtrace == other.gtrace

    def __repr__(self):
        return self.gtrace
