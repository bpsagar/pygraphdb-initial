__author__ = 'Sagar'

class DeleteIndex(object):
    def __init__(self, index_name, index_node):
        super(DeleteIndex, self).__init__()
        self._index_name = index_name
        self._index_node = index_node

    def get_index_name(self):
        return self._index_name

    def get_index_node(self):
        return self._index_node
