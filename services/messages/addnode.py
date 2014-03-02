__author__ = 'Sagar'
from pygraphdb.services.messages.message import Message

class AddNode(Message):
    def __init__(self, node):
        super(AddNode, self).__init__()
        self._node = node

    def get_node(self):
        return self._node