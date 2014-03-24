__author__ = 'Sagar'
import os
from pygraphdb.services.worker.indexnode import IndexNode
from struct import pack, unpack

class IndexBlock(object):
    def __init__(self, location, index_name, index_directory):
        super(IndexBlock, self).__init__()
        self._location = location
        self._index_name = index_name
        self._index_directory = index_directory
        self._index_nodes = []
        self._parent_block = -1
        self._max_size = 1024
        self._size = 0

    def create_dummy_node(self):
        dummy_node = IndexNode('~', -1, -1)
        self.add_index_node(dummy_node)

    def update_child_blocks(self, fd):
        for node in self._index_nodes:
            location = node.get_index_block_pointer()
            if location != -1:
                fd.seek(location+1, 0)
                self.write_integer_field(fd, self._location)
        fd.flush()

    def is_leaf(self):
        for node in self._index_nodes:
            if node.get_index_block_pointer() != -1:
                return False
        return True

    def delete_index_node(self, index_node):
        i = 0
        while i < len(self._index_nodes):
            if self._index_nodes[i].get_key() == index_node.get_key():
                self._index_nodes.pop(i)
                break
            i += 1

    def add_index_node(self, index_node):
        self._index_nodes.append(index_node)
        self._index_nodes = sorted(self._index_nodes, key = lambda index_node: index_node.get_key())

    def get_location(self):
        return self._location

    def set_location(self, location):
        self._location = location

    def get_index_nodes(self):
        return self._index_nodes

    def get_parent_block(self):
        return self._parent_block

    def set_parent_block(self, parent):
        self._parent_block = parent

    def read_block(self, fd):
        fd.seek(self._location, 0)
        size = self._max_size
        full_data = b''
        while size > 0:
            data = fd.read(size)
            size -= len(data)
            full_data += data
        self.parse_block(full_data)

    def parse_block(self, data):
        if len(data) < self._max_size:
            x = 1
        delete_marker = data[:1]
        data = data[1:]
        if delete_marker == b'*':
            print("Accessing deleted block: ", self._location)
            return
        self._parent_block = self.read_integer_field(data[:4])
        data = data[4:]
        node_count = (self.read_integer_field(data[:4]))
        data = data[4:]
        i = 0
        while i < node_count:
            key_length = self.read_integer_field(data[:4])
            data = data[4:]
            key = data[:key_length].decode("UTF-8")
            data = data[key_length:]
            value = self.read_integer_field(data[:4])
            data = data[4:]
            if len(data[:4]) < 4 :
                x = 1
            index_block_pointer = self.read_integer_field(data[:4])
            data = data[4:]
            new_node = IndexNode(key, value, index_block_pointer)
            self._index_nodes.append(new_node)
            i += 1

    def read_field(self, fd):
        size = self.read_integer_field(fd)
        full_data = ''
        while size > 0:
            data = fd.read(size).decode("UTF-8")
            size -= len(data)
            full_data += data
        return full_data

    def read_integer_field(self, data):
        if len(data) < 4 :
            x = 1
        value = unpack('l', data)
        return value[0]

    def write_block(self, fd):
        data = b''
        data += b' '
        data += (pack('l', self._parent_block))
        node_count = len(self._index_nodes)
        data += (pack('l', node_count))
        for node in self._index_nodes:
            data += (pack('l', len(node.get_key())))
            data += bytes(node.get_key(), 'UTF-8')
            data += (pack('l', node.get_value()))
            data += (pack('l', node.get_index_block_pointer()))

        size = len(data)
        while len(data) < self._max_size:
            data += b' '

        fd.seek(self._location, 0)
        fd.write(data)
        fd.flush()

    def write_field(self, fd, data):
        data = str(data)
        self.write_integer_field(fd, len(data))
        fd.write(bytes(data, 'UTF-8'))

    def write_integer_field(self, fd, data):
        packed_data = pack('l', data)
        fd.write(packed_data)

    def get_size(self):
        self._size = 1 + 4 + 4
        for node in self._index_nodes:
            self._size += 4 + len(node.get_key()) + 4 + 4
        return self._size

    def delete_block(self, fd, free_location):
        fd.seek(self._location, 0)
        fd.write(bytes('*', 'UTF-8'))
        self.write_integer_field(fd, free_location)
        remaining = self._max_size - 5
        for i in range(remaining):
            fd.write(bytes(' ', 'UTF-8'))
        fd.flush()