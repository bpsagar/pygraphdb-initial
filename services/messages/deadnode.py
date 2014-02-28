__author__ = 'Sagar'

class DeadNode(object):
    def __init__(self, client):
        super(DeadNode, self).__init__()
        self._client = client

    def get_client(self):
        return self._client