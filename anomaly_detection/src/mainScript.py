import sys
import networkx as nx
import os
import json
from collections import deque
import numpy
import datetime

# assign these to their default values and change later
T = 2
D = 1

# build the address for input files and output file
batch_file_path = os.path.join(os.getcwd(), '..', sys.argv[1])
stream_file_path = os.path.join(os.getcwd(), '..', sys.argv[2])
output_file_path = os.path.join(os.getcwd(), '..', sys.argv[3])

# parse the files and extract the data from them
batch_data = []
stream_data = []
flagged_data = []  # this is going to contain all the flagged purchases later

with open(batch_file_path, 'r') as f:
    for i, line in enumerate(f):
        if i == 0:
            T = int(json.loads(line)['T'])
            D = int(json.loads(line)['D'])
        else:
            batch_data.append(json.loads(line))

with open(stream_file_path, 'r') as f:
    for line in f:
        try:
            stream_data.append(json.loads(line))
        except:
            continue

# some handy map for the params name.
self_purch = 'self_purch'
net_purch = 'net_purch'
net_ave = 'net_ave'
net_sd = 'net_sd'


def build_my_node(this_id):
    G.add_node(this_id, attr_dict={self_purch: deque(maxlen=T),
                                   net_purch: deque(maxlen=T),
                                   net_ave: None, net_sd: None})


def find_my_net(some_id, degree=D):
    my_net = set()
    found = {some_id}

    for i in range(degree):
        ith_layer = set()
        for elem in found:
            ith_layer.update(G.neighbors(elem))
        found = ith_layer
        my_net.update(ith_layer)

    my_net.discard(some_id)
    return my_net


def find_my_close_net(some_id):
    return find_my_net(some_id, degree=D-1)


def update_my_ave(some_id):
    '''calculates the mean of the network for a node'''
    try:
        G.node[some_id][net_ave] = numpy.mean([elem[0] for elem in G.node[some_id][net_purch]])
    except:
        G.node[some_id][net_ave] = 0

def update_my_std(some_id):
    '''calculates the std of the network for a node'''
    try:
        G.node[some_id][net_sd] = numpy.std([elem[0] for elem in G.node[some_id][net_purch]])
    except:
        G.node[some_id][net_sd] = 0

def update_my_params(some_id):
    '''updates all the attributes of the node by analyzing its network'''
    my_net = find_my_net(some_id)

    G.node[some_id][net_purch] = deque(maxlen=T)
    net_purched = set()
    for _node in my_net:
        net_purched.update(G.node[_node][self_purch])
    net_purched = list(net_purched)
    net_purched = sorted(net_purched, key=lambda x: x[1])  # this will sort from small to large
    for elem in net_purched:
        G.node[some_id][net_purch].append(elem)  # only last T of the purchases will remain
    update_my_ave(some_id)
    update_my_std(some_id)

    return G


def update_net_between_nodes(id1, id2):
    '''finds close friends (D-1)th network to id1 and id2. These are the
    ones that need to be updated in addition to id1 and id2.'''

    _updated_nodes = set()
    _updated_nodes.add(id1)
    _updated_nodes.update(find_my_close_net(id1))
    _updated_nodes.add(id2)
    _updated_nodes.update(find_my_close_net(id2))

    for _node in _updated_nodes:
        update_my_params(_node)


def process_data(G, my_data, data_type='batch'):
    ''' processed the data from json file

    my_data: data you want to analyze
    datat_type: If data is used to build the network use "batch".
                If it is streaming data use "stream" '''
    for elem in my_data:
        this_event = elem['event_type']
        this_time = datetime.datetime.strptime(elem['timestamp'], '%Y-%m-%d %H:%M:%S')

        if this_event == 'purchase':
            this_id = int(elem['id'])
            this_amount = float(elem['amount'])

            # if node with id equal to "this_id" exists add purchase info to the node and its network
            try:
                G.node[this_id][self_purch].append((this_amount, this_time))
                my_net = find_my_net(this_id)
                for neighb in my_net:
                    G.node[neighb][net_purch].append((this_amount, this_time))

                    if data_type == 'stream':
                        # calculate/update the net_ave and net_sd params for this_id
                        update_my_ave(this_id)
                        update_my_std(this_id)
                        threshhold = G.node[this_id][net_ave] + 3 * G.node[this_id][net_sd]

                        if this_amount >= threshhold:
                            elem['mean'] = "%.2f" % G.node[this_id][net_ave]
                            elem['sd'] = "%.2f" % G.node[this_id][net_sd]
                            flagged_data.append(elem)
                            print('Just found an Anomaly!!! - user with id'
                                  ' {} has made a big purchase. \n'.format(this_id))

                    elif data_type != 'batch':
                        raise Exception('wrong data_type param')

            except:
                # if node with id equal to "this_id" does not exist, then add it to the graph and initialize its params
                build_my_node(this_id)
                G.node[this_id][self_purch].append((this_amount, this_time))

        elif this_event == 'befriend':
            # get ids
            id1 = int(elem['id1'])
            id2 = int(elem['id2'])

            # add them if they don't exist
            try:
                G.node[id1][self_purch]
            except:
                G.add_node(id1, attr_dict={self_purch: deque(maxlen=T),
                                           net_purch: deque(maxlen=T),
                                           net_ave: None, net_sd: None})

            try:
                G.node[id2][self_purch]
            except:
                G.add_node(id2, attr_dict={self_purch: deque(maxlen=T),
                                           net_purch: deque(maxlen=T),
                                           net_ave: None, net_sd: None})
            # add the edge
            G.add_edge(id1, id2)

            # update the sub_network between id1 and id2
            update_net_between_nodes(id1, id2)

        elif this_event == 'unfriend':
            # get ids
            id1 = int(elem['id1'])
            id2 = int(elem['id2'])

            # delete the edge between id1 and id2
            G.remove_edge(id1, id2)

            # update the sub_network between id1 and id2
            update_net_between_nodes(id1, id2)

        else:
            raise Exception('Unknown event_type')

    return G

def write_output():
    with open(output_file_path, 'w') as outFile:
        for line in flagged_data:
            json.dump(line, outFile)

if __name__ == '__main__':
    G = nx.Graph()
    G = process_data(G, batch_data, data_type='batch')
    G = process_data(G, stream_data, data_type='stream')
    write_output()
    print(flagged_data)
