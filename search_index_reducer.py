#!/usr/bin/python
"""
Reduces (word,node id) (key,value) pairs to (word,sorted node id list)
"""
import sys

def initialize_nodelist_str(node_list):
    """Initializes a sorted csv node list string

    Args:
        node_list: List that contains node id associated with
                   a specific word

    Returns:
        node_list_str: Sorted csv node list string"""
    sorted_node_list = sorted(list(set(node_list)))

    node_list_str = str(sorted_node_list[0])
    for idx in range(len(sorted_node_list)-1):
        node_list_str += "," + str(sorted_node_list[idx+1])

    return node_list_str

def reducer(stdin):
    """ Reduces (word,node id) (key,value) pairs to (word,
    sorted node id list)

    Args:
        stdin: Standard input file handle

    Returns:
        None"""
    old_key = None
    node_list = []

    for line in stdin:
        data_mapped = line.strip().split("\t")

        if len(data_mapped) == 2:
            this_key = data_mapped[0]
            this_node = int(data_mapped[1])

            if old_key and old_key != this_key:
                node_list_str =\
                    initialize_nodelist_str(node_list)

                print "%s\t%s" % (old_key,
                                  node_list_str)

                old_key = this_key
                node_list = []

            old_key = this_key
            node_list.append(this_node)

    if old_key != None:
        node_list_str = initialize_nodelist_str(node_list)
        print "%s\t%s" % (old_key, node_list_str)

if __name__ == "__main__":
    reducer(sys.stdin)
