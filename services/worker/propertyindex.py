__author__ = 'Sagar'
import os
from struct import pack, unpack
from pygraphdb.services.worker.indexblock import IndexBlock


class PropertyIndex(object):
    def __init__(self, index_name, index_directory, block_size):
        super(PropertyIndex, self).__init__()
        self._index_name = index_name
        self._index_directory = index_directory
        self._block_size = block_size
        file = os.path.join(self._index_directory, self._index_name)
        self._fd = open(file, 'rb+')

    def stop(self):
        self._fd.close()

    # Basic functions.

    def read_integer_field(self):
        packed_data = self._fd.read(4)
        value = unpack('l', packed_data)
        return value[0]

    def write_integer_field(self, data):
        packed_data = pack('l', data)
        self._fd.write(packed_data)
        self._fd.flush()

    def get_free_block_pointer(self):
        self._fd.seek(4, 0)
        free_block = self.read_integer_field()
        if free_block == -1:
            self._fd.seek(0, 2)
            free_block = self._fd.tell()
            for i in range(self._block_size):
                self._fd.write(bytes(' ', 'UTF-8'))
                self._fd.flush()
        else:
            self._fd.seek(free_block+1, 0)
            next_free_block = self.read_integer_field()
            self.set_free_block_pointer(next_free_block)
        return free_block

    def set_free_block_pointer(self, free_block):
        self._fd.seek(4, 0)
        self.write_integer_field(free_block)

    def get_root_block(self):
        self._fd.seek(0, 0)
        packed_data = self._fd.read(4)
        if packed_data == b'':
            self.set_root_block(-1)
            self.set_free_block_pointer(-1)
            return -1
        else:
            root_block = unpack('l', packed_data)
        return root_block[0]

    def set_root_block(self, root):
        self._fd.seek(0, 0)
        self.write_integer_field(root)

    # Functions to find an index in the index tree.

    def find_index_node(self, key):
        block_location = self.get_root_block()
        while block_location != -1:
            block = IndexBlock(block_location, self._index_name, self._index_directory)
            block.read_block(self._fd)
            flag = 0
            for node in block.get_index_nodes():
                if key < node.get_key():
                    flag = 1
                    block_location = node.get_index_block_pointer()
                    break
                elif key == node.get_key():
                    return node.get_key(), node.get_value()

            if flag == 0:
                return key, None
        return key,

    # Functions to add a new index key to the index tree.

    def find_leaf_block(self, key, root):
        if root == -1:
            block = IndexBlock(self.get_free_block_pointer(), self._index_name, self._index_directory)
            block.create_dummy_node()
            return block
        block = IndexBlock(root, self._index_name, self._index_directory)
        parent = -1
        block.read_block(self._fd)

        while not block.is_leaf():
            parent = block.get_location()
            index_nodes = block.get_index_nodes()
            for node in index_nodes:
                if key < node.get_key():
                    block = IndexBlock(node.get_index_block_pointer(), self._index_name, self._index_directory)
                    break
            if block.get_location() == -1:
                return parent
            block.read_block(self._fd)
        return block

    def split_block(self, block):
        index_nodes = block.get_index_nodes()
        median = int(len(index_nodes)/2 - 1)
        median_node = index_nodes[median]

        new_block = IndexBlock(self.get_free_block_pointer(), self._index_name, self._index_directory)
        new_block.create_dummy_node()
        i = 0
        while i < median:
            new_block.add_index_node(index_nodes[i])
            i += 1

        new_block.get_index_nodes()[-1].set_index_block_pointer(median_node.get_index_block_pointer())
        i = 0
        while i <= median:
            block.delete_index_node(index_nodes[0])
            i += 1

        new_block.update_child_blocks(self._fd)
        median_node.set_index_block_pointer(new_block.get_location())
        new_block.set_parent_block(block.get_parent_block())
        return new_block, median_node

    def add_index(self, index_node, block=None):
        key = index_node.get_key()
        root_block_location = self.get_root_block()

        if block is None:
            block = self.find_leaf_block(key, root_block_location)

        parent = block.get_parent_block()
        block.add_index_node(index_node)

        if block.get_size() < self._block_size:
            block.write_block(self._fd)
            if root_block_location == -1:
                self.set_root_block(block.get_location())
            return

        new_block, median_node = self.split_block(block)
        if parent != -1:
            parent = IndexBlock(parent, self._index_name, self._index_directory)
            parent.read_block(self._fd)
            block.write_block(self._fd)
            new_block.write_block(self._fd)
            self.add_index(median_node, parent)
        else:
            root_block = IndexBlock(self.get_free_block_pointer(), self._index_name, self._index_directory)
            root_block.create_dummy_node()
            root_block.get_index_nodes()[-1].set_index_block_pointer(block.get_location())
            root_block.add_index_node(median_node)
            block.set_parent_block(root_block.get_location())
            new_block.set_parent_block(root_block.get_location())
            self.set_root_block(root_block.get_location())
            root_block.write_block(self._fd)
            new_block.write_block(self._fd)
            block.write_block(self._fd)

    # Functions to delete an index from the index tree.

    def find_index_block(self, index_node):
        block_location = self.get_root_block()
        while block_location != -1:
            block = IndexBlock(block_location, self._index_name, self._index_directory)
            block.read_block(self._fd)

            for node in block.get_index_nodes():
                if index_node.get_key() < node.get_key():
                    block_location = node.get_index_block_pointer()
                    break
                elif index_node.get_key() == node.get_key():
                    return block, node
        return None, None

    def find_successor(self, block, index_node):
        index_nodes = block.get_index_nodes()

        for i in range(len(index_nodes)):
            if index_nodes[i].get_key() == index_node.get_key():
                break
        i += 1

        if block.get_index_nodes()[i].get_index_block_pointer() == -1:
            return block, index_nodes[i]

        block_location = block.get_index_nodes()[i].get_index_block_pointer()
        block = IndexBlock(block_location, self._index_name, self._index_directory)
        block.read_block(self._fd)
        while block.get_index_nodes()[0].get_index_block_pointer() != -1:
            block_location = block.get_index_nodes()[0].get_index_block_pointer()
            block = IndexBlock(block_location, self._index_name, self._index_directory)
            block.read_block(self._fd)

        return block, block.get_index_nodes()[0]

    def delete_index_node(self, index_node):
        block, index_node = self.find_index_block(index_node)
        if block is None:
            print("Fail")
            return

        successor_block, successor_node = self.find_successor(block, index_node)

        if block == successor_block:
            successor_node.set_index_block_pointer(index_node.get_index_block_pointer())
            block.delete_index_node(index_node)
            if len(block.get_index_nodes()) == 1:
                if block.get_location() != self.get_root_block():
                    parent_location = block.get_parent_block()
                    if parent_location != -1:
                        parent = IndexBlock(parent_location, self._index_name, self._index_directory)
                        parent.read_block(self._fd)
                        for node in parent.get_index_nodes():
                            if node.get_index_block_pointer() == block.get_location():
                                node.set_index_block_pointer(block.get_index_nodes()[0].get_index_block_pointer())

                        parent.update_child_blocks(self._fd)
                        parent.write_block(self._fd)
                        block.delete_block(self._fd, self.get_free_block_pointer())
                else:
                    block.write_block(self._fd)
            else:
                block.write_block(self._fd)
        else :
            successor_block.delete_index_node(successor_node)
            block.delete_index_node(index_node)
            successor_node.set_index_block_pointer(index_node.get_index_block_pointer())
            block.add_index_node(successor_node)
            block.write_block(self._fd)
            if len(successor_block.get_index_nodes()) == 1:
                if successor_block.get_location() != self.get_root_block():
                    parent_location = successor_block.get_parent_block()
                    if parent_location != -1:
                        parent = IndexBlock(parent_location, self._index_name, self._index_directory)
                        parent.read_block(self._fd)
                        for node in parent.get_index_nodes():
                            if node.get_index_block_pointer() == successor_block.get_location():
                                node.set_index_block_pointer(successor_block.get_index_nodes()[0].get_index_block_pointer())
                        parent.update_child_blocks(self._fd)
                        parent.write_block(self._fd)
                    successor_block.delete_block(self._fd, self.get_free_block_pointer())
                else:
                    successor_block.write_block(self._fd)
            else:
                successor_block.write_block(self._fd)