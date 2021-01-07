import requests
import time
import sys
import json

# Standard headers when dealing with posting data
HEADERS = {'content-type': 'application/json'}

# Dictionary storing the keys and values
kvs = dict()
# List storing all views that we know about (just the IP and port)
known_views = list()
# Time to wait before checking all views again (seconds)
sleep_time = 45
# The current view ip and port
curr_view = ''
# List of alive views that are reachable
alive_views = []
# list of shards
shard_count = {}
# The shard group that we are a part of
curr_shard = 0



"""
Verify that all views in the list are active

Sends a GET request to each view every 'sleep_time' seconds to check if the view
is alive.
Also calls broadcast_dead_views() with the remaining views and the dead ones to broadcast
the views that are no longer accessible
This should be run in a separate thread so it doesn't interfere with anything else

Runs an infinite loop, loops through each view in each iteration of the inf loop,
then runs the get. If it doesn't respond, then add that to a list of dead views,
then broadcast the list of dead views to all remaining peers
"""
def verify_views():
    print("Thread started")
    while True:
        time.sleep(sleep_time)
        dead_view_list = []
        for view in known_views[:]:
            try:
                response = requests.get('http://{}/key-value-store-view'.format(view))
                if response.status_code != 200:
                    print("The view '{}' is no longer reachable!".format(view), file=sys.stderr)
                    dead_view_list.append(view)
                    if view in alive_views:
                        alive_views.remove(view)
                else:
                    if view not in alive_views:
                        alive_views.append(view)
            except Exception as e:
                # This means we were unable to connect, such as a timeout
                print("The view '{}' is no longer reachable!".format(view), file=sys.stderr)
                dead_view_list.append(view)
                if view in alive_views:
                    alive_views.remove(view)

"""
Propagate a new view to all other views

This function should be called every time a new view is **SUCCESSFULLY** added.
That way, a new view will be sent to all other views, and if the other views already
have the new view, they will return a 404, otherwise will return 201 and send to all others.
This will terminate since it should only be invoked upon successfully adding a view.
"""
def send_new(new_view):
    for old_view in known_views:
        if new_view == old_view or old_view == curr_view:
            continue
        payload = {'socket-address': new_view}
        try:
            response = requests.put( 'http://' + old_view + '/key-value-store-view', headers=HEADERS, data=json.dumps(payload))
        except:
            pass



#Checks whether the current view has the necessary amount of shards, if it doesn't if makes them
#I don't this is a feasible way to communicate to all the nodes but works for individual ones
def verify_shards(shards):
    global shard_count
    if(shards > len(shard_count)):
        for i in range(1,shards+1):
            #testing
            #if i == 1:
                #shard_count[i] = [i]
                #shard_count[i].append(100)
            #else:
            #shard_count[i] = [i]
            shard_count[i] = []
        ret_mess = "Shard count now = ", len(shard_count), ", array is: ", shard_count
        return ret_mess
    else:
        ret_mess = "Same Shard count already. ", shard_count
        return ret_mess

#shard_count {1: [node1,node2,nod3], 2:[node4,node5]}
#when trying to add a node to a shard we want to find
#the shard with the smallest count and add the node to that one
def add_to_Shard(node):
    shard_len_array = [0]*len(shard_count)
    count = 0
    #finds the shard with the minimum length
    for key in shard_count.keys():
        nodes = shard_count[key]
        shard_len_array[count] = 0
        for i in nodes:
            shard_len_array[count] += 1
        count += 1

    #found the shard to add the node to
    min_index = shard_len_array.index(min(shard_len_array))+1
    print("length of the shards",shard_len_array,"min_index = ", min_index)

    #add the node to the shard
    shard_count[min_index].append(node)
    print("added the node to the smallest shard", shard_count)
    if node == curr_view:
        global curr_shard
        print("Curr shard has been set")
        curr_shard = min_index

#returns array of all the members in a shard
def shard_members(shard):
    nodes = []
    for i in shard_count[int(shard)]:
        print(i)
        nodes.append(i)  
    print("returning:",nodes)  
    return nodes

def im_new():
    for old_view in known_views:
        if old_view == curr_view:
            continue
        payload = {'socket-address': curr_view}
        try:
            response = requests.put( 'http://' + old_view + '/key-value-store-view-new', headers=HEADERS, data=json.dumps(payload), timeout=3)
        except:
            pass

def update_shard(new_shard):
    global curr_shard
    curr_shard = int(new_shard)