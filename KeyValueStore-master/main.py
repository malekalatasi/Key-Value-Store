from flask import Flask, request, make_response, jsonify
import views
import threading
import os
import sys
import json, requests

app = Flask(__name__)
class vars:
	kvs_dict = {}
	history_dict = {}

###################
# VIEW OPERATIONS #
###################

"""
Handle the receive of a GET request

Bind to /key-value-store-view, and listen for a GET
Will simply return the list of views in JSON form
"""
@app.route('/key-value-store-view', methods=['GET'])
def get_view():
	json_response = jsonify(message="View retrieved successfully", view=views.alive_views)
	response = make_response(json_response, 200)
	return response

"""
Handle the receive of a DELETE request

Bind to /key-value-store-view, and listen for a delete
Will verify that the received data is correct and contains the necessary data.
Will then check if the requested view exists or not. If it does not, it will return
404, otherwise it will delete the view and return 200.
"""
@app.route('/key-value-store-view', methods=['DELETE'])
def delete_view():
	json_value = request.get_json()
	if json_value is not None and 'socket-address' in json_value:
		view_to_delete = json_value['socket-address']
		if view_to_delete in views.known_views: # Handle the case where the view is in the list
			views.known_views.remove(view_to_delete)
			json_response = jsonify(message="Replica deleted successfully from the view")
			response = make_response(json_response, 200)
		else: # Handle the case where the view does not exist
			json_response = jsonify(error="Socket address does not exist in the view", message="Error in DELETE")
			response = make_response(json_response, 404)
	else:
		json_response = jsonify(error="Invalid usage of DELETE", message="Error in DELETE")
		response = make_response(json_response, 500)
	return response

@app.route('/key-value-store-view-new', methods=['PUT'])
def new_view():
	json_value = request.get_json()
	if json_value is not None and 'socket-address' in json_value:
		new_view = json_value['socket-address']
		if new_view not in views.known_views: # Handle the case where the view is in the list
			views.known_views.append(new_view)
			views.alive_views.append(new_view)
			json_response = jsonify(message="Replica added successfully from the view")
			response = make_response(json_response, 200)
		else: # Handle the case where the view does not exist
			json_response = jsonify(error="View already exists", message="Error in DELETE")
			response = make_response(json_response, 201)
	else:
		json_response = jsonify(error="Invalid usage of kvs-view-new", message="Error in PUT")
		response = make_response(json_response, 500)
	return response

"""
Handle the receive of a PUT request

Bind to /key-value-store-view, and listen to a put
Will verify that the received data is correct and contains the necessary
data.
Will then check if the requested view already exists or not. If it does, then it
will return the 404, otherwise it will add it, start a thread to propagate, then
return 201 along with the desired information.
"""
@app.route('/key-value-store-view', methods=['PUT'])
def put_view():
	json_value = request.get_json()
	if json_value is not None and 'socket-address' in json_value:
		view_to_add = json_value['socket-address']
		if view_to_add not in views.known_views: # Handle the case where we added the view
			views.known_views.append(view_to_add)
			views.alive_views.append(view_to_add)
			# start thread to propagate changes across nodes without impacting service
			new_thread = threading.Thread(target=views.send_new, args=(view_to_add,))
			new_thread.start()
			json_response = jsonify( message="Replica added successfully to the view")
			response = make_response(json_response, 201)
		else: # Handle the case where the view already exists
			json_response = jsonify(error="Socket address already exists in the view", message="Error in PUT")
			response = make_response(json_response, 404)
	else: # Handle the case where the request is broken
		json_response = jsonify(error="Invalid usage of PUT", message="Error in PUT")
		response = make_response(json_response, 500)
	return response

####################################
# Synchronization Helper Functions #
####################################

@app.route('/new-replica-kvs', methods=['GET'])
def new_replica_kvs():
	payload = jsonify(vars.kvs_dict)
	response = make_response(payload, 200)
	return response

@app.route('/new-replica-history', methods=['GET'])
def new_replica_history():
	payload = jsonify(vars.history_dict)
	response = make_response(payload, 200)
	return response

def update_dicts():
	#for other_view in views.known_views:
	for other_view in views.shard_count[views.curr_shard]:
		if other_view == views.curr_view:
			continue
		try:
			resp1 = requests.get('http://' + other_view + '/new-replica-kvs', timeout=3)
			resp3 = requests.get('http://' + other_view + '/new-replica-history', timeout=3)
			
			new_kvs = resp1.json()
			new_hist = resp3.json()
			if (len(new_kvs) > len(vars.kvs_dict)) or (len(new_hist) > len(vars.history_dict)):
				vars.kvs_dict = new_kvs
				vars.history_dict = new_hist
				break
		except Exception as e:
			pass


##################
# KVS Operations #
##################

def find_shard(key):
	k = str(key)
	c = 0
	for char in k:
		c += ord(char)
	return (c % len(views.shard_count)) + 1

def common_put(meta, value, key):
	shard_to_put = find_shard(key) # Find the shard

	if meta == '':
		# If meta wasn't provided, find the old one and generate new
		if len(vars.history_dict) == 0:
			meta = 'V0' 
			new_meta = 'V1'
		else:
			meta = 'V'  + str(len(vars.history_dict) +1)
			new_meta = 'V' + str(len(vars.history_dict) + 2)
	else:
		# Find the latest metadata and generate the new one
		meta = meta.strip('<')
		meta = meta.strip('>')

		while meta in vars.history_dict:
			meta = vars.history_dict[meta]
		# Verify that the found metadata is ok:
		if meta in vars.history_dict.values():
			new_meta = "V" + str(len(vars.history_dict)+1)
		else:
			if shard_to_put == views.curr_shard:
				json_response = jsonify( message="Metadata is not in local dictionary", views=views.known_views)
				response = make_response(json_response, 400)
				return response
			return None

	# Update history
	vars.history_dict[meta] = new_meta

	# Check if the shard is correct
	if shard_to_put == views.curr_shard:
		payload = {}
		if key in vars.kvs_dict:
			payload['message'] = "Updated successfully"
			payload['causal-metadata'] = '<' + new_meta + '>'
			payload['shard-id'] = shard_to_put
			json_response = jsonify(payload)
			response = make_response(json_response, 200)
		else:
			payload['message'] = "Added successfully"
			payload['causal-metadata'] = '<' + new_meta + '>'
			payload['shard-id'] = shard_to_put
			json_response = jsonify(payload)
			response = make_response(json_response, 201)
		vars.kvs_dict[key] = [value, new_meta]
		return response
	else:
		return None


@app.route('/key-value-store/<string:key>', methods=['PUT'])
def put_kv(key):
	#update_dicts()
	json_value = request.get_json()
	meta = json_value['causal-metadata']
	value = str(json_value['value'])

	return_val = common_put(meta, value, key)
	if return_val is None:
		# This is not the correct shard, it should be forwarded
		best_json = None
		best_status = 404
		for view in views.known_views[:]:
			if view != views.curr_view:
				payload = {'value': value, 'causal-metadata': meta}
				response = requests.put( 'http://' + view + '/selfish-key-value-store/' + key, headers=views.HEADERS, data=json.dumps(payload))
				if response.status_code < best_status and response.status_code != 202:
					best_status = response.status_code
					best_json = response.json()
		json_response = jsonify(best_json)
		response = make_response(json_response, best_status)
		return response
	else:
		for view in views.known_views[:]:
			if view != views.curr_view:
				payload = {'value': value, 'causal-metadata': meta}
				response = requests.put( 'http://' + view + '/selfish-key-value-store/' + key, headers=views.HEADERS, data=json.dumps(payload))
		return return_val

# Same as put, but don't propagate to other nodes to prevent cycles
@app.route('/selfish-key-value-store/<string:key>', methods=['PUT'])
def selfish_put_kv(key):
	json_value = request.get_json()
	meta = json_value['causal-metadata']
	value = str(json_value['value'])
	return_val = common_put(meta, value, key)
	if return_val is None:
		payload = {'Message': 'History updated'}
		json_response = jsonify(payload)
		response = make_response(json_response, 202)
		return response
	else:
		return return_val


@app.route('/key-value-store/<string:key>', methods=['GET'])
def get_kv(key):
	update_dicts()
	correct_shard = find_shard(key)
	if correct_shard != views.curr_shard:
		# Handle the case where we should forward
		response = requests.get('http://' + views.shard_count[correct_shard][0] + '/key-value-store/' + key, timeout=5)
		json_response = jsonify(response.json())
		response = make_response(json_response, response.status_code)
		return response
		
	value = ''
	if key in vars.kvs_dict and vars.kvs_dict[key][0] != 'NULL':
		value = vars.kvs_dict[key][0]
		metadata = "<" + str(vars.kvs_dict[key][1]) + ">"
		
		payload = {}
		payload['message'] = "Retrieved successfully"
		payload['causal-metadata'] = metadata
		payload['value'] = value
		json_response = jsonify(payload)
		
		response = make_response(json_response, 200)
		return response
	else:
		json_response = jsonify( message="Key does not exist",error="Error in GET")
		response = make_response(json_response, 404)
		return response

def common_delete(meta, key):
	shard_to_put = find_shard(key) # Find the shard
	if meta == "": # Metadata not given
		json_response = jsonify( message="Metadata not provided!")
		response = make_response(json_response, 400)
		return response
	else:
		meta = meta.strip('<')
		meta = meta.strip('>')
		#checks to see if metadata is in casual history of this thread
		
		while meta in vars.history_dict:
			meta = vars.history_dict[meta]
		# Verify that the found metadata is ok:
		if meta in vars.history_dict.values():
			new_meta = "V" + str(len(vars.history_dict)+1)
		else:
			if shard_to_put == views.curr_shard:
				json_response = jsonify( message="Metadata is not in local dictionary", views=views.known_views)
				response = make_response(json_response, 400)
				return response
			return None
	
	# Update history
	vars.history_dict[meta] = new_meta

	# Check if the shard is correct
	if shard_to_put == views.curr_shard:
		payload = {}
		if key in vars.kvs_dict:
			payload['message'] = "Updated successfully"
			payload['causal-metadata'] = '<' + new_meta + '>'
			payload['shard-id'] = shard_to_put
			json_response = jsonify(payload)
			response = make_response(json_response, 200)
		else:
			payload['message'] = "Added successfully"
			payload['causal-metadata'] = '<' + new_meta + '>'
			payload['shard-id'] = shard_to_put
			json_response = jsonify(payload)
			response = make_response(json_response, 201)
		vars.kvs_dict[key] = ['NULL', None]
		return response
	else:
		return None

@app.route('/key-value-store/<string:key>', methods=['DELETE'])
def delete_kv(key):
	json_value = request.get_json()
	meta = json_value['causal-metadata']

	return_val = common_put(meta, key)
	if return_val is None:
		# This is not the correct shard, it should be forwarded
		best_json = None
		best_status = 404
		for view in views.known_views[:]:
			if view != views.curr_view:
				payload = {'causal-metadata': meta}
				response = requests.delete( 'http://' + view + '/selfish-key-value-store/' + key, headers=views.HEADERS, data=json.dumps(payload))
				if response.status_code < best_status and response.status_code != 202:
					best_status = response.status_code
					best_json = response.json()
		json_response = jsonify(best_json)
		response = make_response(json_response, best_status)
		return response
	else:
		for view in views.known_views[:]:
			if view != views.curr_view:
				payload = {'causal-metadata': meta}
				response = requests.delete( 'http://' + view + '/selfish-key-value-store/' + key, headers=views.HEADERS, data=json.dumps(payload))
		return return_val


@app.route('/selfish-key-value-store/<string:key>', methods=['DELETE'])
def seflish_delete_kv(key):
	json_value = request.get_json()
	meta = json_value['causal-metadata']
	return_val = common_put(meta, key)
	if return_val is None:
		payload = {'Message': 'Entry deleted updated'}
		json_response = jsonify(payload)
		response = make_response(json_response, 202)
		return response
	else:
		return return_val


#################
# SHARD methods #
#################

@app.route('/key-value-store-shard/shard-ids', methods=['GET'])
def get_allShards():
	shard_dict = views.shard_count
	shard_array = []
	for key in shard_dict.keys():
		shard_array.append(key)
	print(shard_array)
	payload = {}
	payload['message'] = "Shard IDs retrieved successfully"
	payload['shard-ids'] = shard_array
	json_response = jsonify(payload)
	response = make_response(json_response, 200)
	return response

@app.route('/key-value-store-shard/node-shard-id', methods=['GET'])
def get_nodeShardID():
	print("Trying to get node:",views.curr_view)
	shard_id = 0
	curr_shards = views.shard_count
	for key in curr_shards.keys():
		nodes = curr_shards[key]
		for i in nodes:
			if i == views.curr_view:
				shard_id = key
				break
	if(shard_id == 0):
		print("Couldn't find node")
	else:
		payload = {}
		payload['message'] = "Shard ID of the node retrieved successfully"
		payload['shard-id'] = shard_id
		json_response = jsonify(payload)
		response = make_response(json_response, 200)
		return response

@app.route('/key-value-store-shard/shard-id-members/<string:ID>', methods=['GET'])
def get_nodeShardMembers(ID):
	members = views.shard_members(ID)
	payload = {}
	payload['message'] = "Members of shard ID retrieved successfully"
	payload['shard-id-members'] = members
	json_response = jsonify(payload)
	response = make_response(json_response, 200)
	return response

@app.route('/key-value-store-shard/shard-id-key-count/<string:ID>', methods=['GET'])
def get_numKeys(ID):
	curr_shards = views.shard_count
	shard_to_get = int(ID)
	if(len(curr_shards) < shard_to_get):
		json_response = jsonify( message="Shard does not exist")
		response = make_response(json_response, 404)
		return response
	else:
		if shard_to_get == views.curr_shard:
			payload = {}
			payload['message'] = "Key count of shard ID retrieved successfully"
			payload['shard-id-key-count'] = len(vars.kvs_dict)
			json_response = jsonify(payload)
			response = make_response(json_response, 200)
			return response
		else:
			response = requests.get('http://' + views.shard_count[shard_to_get][0] + '/key-value-store-shard/shard-id-key-count/' + ID, timeout=5)
			json_response = jsonify(response.json())
			response = make_response(json_response, 200)
			return response

@app.route('/key-value-store-shard/reshard', methods=['PUT'])
def reshard():
	json_value = request.get_json()
	new_num = int(json_value['shard-count'])
	if (len(views.known_views) // new_num) < 2:
		payload = {'message': "Not enough nodes to provide fault-tolerance with the given shard count!"}
		json_response = jsonify(payload)
		response = make_response(json_response, 400)
		return response
	else:
		# Get all key/vals
		full_kvs = dict()
		for k,v in views.shard_count.items():
			if k == views.curr_shard:
				for kv, vv in vars.kvs_dict.items():
					full_kvs[kv] = vv
			else:
				response = requests.get('http://' + v[0] + '/new-replica-kvs', timeout=5)
				js_response = response.json()
				for kv, vv in js_response.items():
					full_kvs[kv] = vv

		# Reassign all replicas to new shards
		shard_replica_list = [[] for x in range(0, new_num)]
		curr = 0
		for replica in views.known_views:
			shard_replica_list[curr].append(replica)

			# Update our shard val
			if replica == views.curr_view:
				views.curr_shard = curr

			curr+=1
			if curr == new_num:
				curr = 0

		# Update the shard_count dict
		views.shard_count.clear()
		for x in range(0, new_num):
			views.shard_count[(x+1)] = shard_replica_list[x]

		# Divide the KVS
		new_kvs_list = [{} for x in range(0, new_num+1)]
		for k,v in full_kvs.items():
			shard_to_be = find_shard(k)
			print("Curr k: {} v: {} is going to shard {}".format(k,v, shard_to_be), file=sys.stderr)
			new_kvs_list[shard_to_be][k] = v

		print(new_kvs_list, file=sys.stderr)
		#del new_kvs_list[0]
		#print(new_kvs_list, file=sys.stderr)
		# Update our KVS
		#vars.kvs_dict = new_kvs_list[views.curr_shard-1]
		vars.kvs_dict = new_kvs_list[views.curr_shard]

		# Send new shard-ID, shard_count, and KVS to every replica
		#for x in range(0, new_num):
		for x in range(1, new_num+1):
			payload = {'new-shard': x, 'shard_count': views.shard_count, 'kvs': new_kvs_list[x]}
			print("Shard is {}and these keys: {}".format(x, new_kvs_list[x]), file=sys.stderr)
			for replica_in_shard in shard_replica_list[x-1]:
				if replica_in_shard == views.curr_view:
					views.update_shard(x)
					vars.kvs_dict.clear()
					for k,v in new_kvs_list[x].items():
						vars.kvs_dict[k] = v
					continue
				requests.put('http://' + replica_in_shard + '/key-value-store-shard/reshard-helper', headers=views.HEADERS, data=json.dumps(payload), timeout=8)
		resp_payload = {'message': 'Resharding done successfully'}
		js_response = jsonify(resp_payload)
		response=make_response(js_response, 200)
		return response

@app.route('/key-value-store-shard/reshard-helper', methods=['PUT'])
def reshard_helper():
	json_value = request.get_json()
	views.curr_shard = int(json_value['new-shard'])
	views.shard_count.clear()
	for k,v in json_value['shard_count'].items():
		if int(k) in views.shard_count:
			views.shard_count[int(k)].extend(v)
		else:
			views.shard_count[int(k)] = list()
			views.shard_count[int(k)].extend(v)
	vars.kvs_dict.clear()
	for k,v in json_value['kvs'].items():
		print("We are shard: {} and we have key: {} and val {}".format(views.curr_shard, k, v), file=sys.stderr)
		vars.kvs_dict[k] = v
	payload = {'message': 'updated'}
	js_response = jsonify(payload)
	response=make_response(js_response, 200)
	return response
			

@app.route('/key-value-store-shard/add-member/<string:ID>', methods=['PUT'])
def add_member(ID):
	json_value = request.get_json()
	new_node = json_value['socket-address']
	if new_node in views.known_views:
		if int(ID) not in views.shard_count:
			payload = {'message': "Unknown shard!", 'error': 'Error in add-member'}
			json_response = jsonify(payload)
			response = make_response(json_response, 400)
			return response
		else:
			views.shard_count[int(ID)].append(new_node)
			for other in views.known_views:
				if other==views.curr_view:
					continue
				if other==new_node:
					requests.put('http://' + other + '/key-value-store-shard/added-to-shard/' + ID, headers=views.HEADERS, data=json.dumps(views.shard_count), timeout=10)
					pass
				data = {'socket-address': new_node}
				requests.put('http://' + other + '/key-value-store-shard/add-member-selfish/' + ID, headers=views.HEADERS, data=json.dumps(data), timeout=5)
			payload = {'message': "Success!"}
			json_response = jsonify(payload)
			response = make_response(json_response, 200)
			return response
	else:
		payload = {'message': "Unknown node!", 'error': 'Error in add-member'}
		json_response = jsonify(payload)
		response = make_response(json_response, 400)
		return response

@app.route('/key-value-store-shard/add-member-selfish/<string:ID>', methods=['PUT'])
def add_member_selfish(ID):
	json_value = request.get_json()
	new_node = json_value['socket-address']
	if new_node in views.known_views:
		if int(ID) not in views.shard_count:
			payload = {'message': "Unknown shard!", 'error': 'Error in add-member'}
			json_response = jsonify(payload)
			response = make_response(json_response, 400)
			return response
		else:
			views.shard_count[int(ID)].append(new_node)
			payload = {'message': "Success!"}
			json_response = jsonify(payload)
			response = make_response(json_response, 200)
			return response
	else:
		payload = {'message': "Unknown node!", 'error': 'Error in add-member'}
		json_response = jsonify(payload)
		response = make_response(json_response, 400)
		return response

@app.route('/key-value-store-shard/added-to-shard/<string:ID>', methods=['PUT'])
def added_to_shard(ID):
	json_value = request.get_json()
	nd = dict()
	for k,v in json_value.items():
		nd[int(k)] = v
	views.shard_count = nd
	views.curr_shard = int(ID)
	update_dicts()
	payload = {'message': "Success!"}
	json_response = jsonify(payload)
	response = make_response(json_response, 200)
	return response
		

if __name__ ==  '__main__':
	# Get the socket address from the env variable and split it to IP and port
	#sock_addr = "127.0.0.1:8085"
	sock_addr = os.environ.get('SOCKET_ADDRESS')
	if sock_addr is None:
		print("No socket address specified! Exit", file=sys.stderr)
		sys.exit(1)
	views.curr_view = sock_addr

	#creating shards
	num_shards = os.environ.get('SHARD_COUNT')
	if num_shards is not None:
		#print("Please include SHARD_COUNT! Exit", file=sys.stderr)
		#sys.exit(1)
		num_shards = num_shards.replace('"','')
		ret_mess = views.verify_shards(int(num_shards))
		print(ret_mess)
	

	#add node to shard
	#views.add_to_Shard(sock_addr)
	
	# Get the initial views from the env variable and add it to the view list
	initial_views = os.environ.get('VIEW')
	if initial_views is not None:
		for initial_view in initial_views.split(','):
			if initial_view == '':
				continue
			views.known_views.append(initial_view)
			views.alive_views.append(initial_view)
			# Add node to shard
			if num_shards is not None:
				views.add_to_Shard(initial_view)

	print(str(views.curr_shard))
	views.send_new(views.curr_view)

	# Update the dictionaries to the newest possible
	dict_thread = threading.Thread(target=update_dicts)
	dict_thread.start()

	# Start thread to tell others i exist
	newbie_thread = threading.Thread(target=views.im_new)
	newbie_thread.start()

	# Start the thread to verify periodically
	new_thread = threading.Thread(target=views.verify_views)
	new_thread.start()

	# Start the flask app
	addr_parts = sock_addr.split(':')
	app.run(host=addr_parts[0], debug=True, port=int(addr_parts[1]))