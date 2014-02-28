__author__ = 'Sagar'

class HeartBeat(object):
    def __init__(self, sender, timestamp, counter):
        super(HeartBeat, self).__init__()
        self._sender = sender
        self._timestamp = timestamp
        self._counter = counter

    def get_sender(self):
        return self._sender

    def get_counter(self):
        return self._counter
