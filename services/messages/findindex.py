__author__ = 'Sagar'

class FindIndex(object):
    def __init__(self, index_name, key):
        super(FindIndex, self).__init__()
        self._index_name = index_name
        self._key = key

    def get_index_name(self):
        return self._index_name

    def get_key(self):
        return self._key
