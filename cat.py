import json

dfs_setup_config = "/users/vinaynaidu/DFS/setup.json"
setupfiledir = "/users/vinaynaidu/DFS/"

f = open(dfs_setup_config)
config = json.load(f)
block_size = config['block_size']
path_to_datanodes = config['path_to_datanodes']
path_to_namenodes = config['path_to_namenodes']
replication_factor = config['replication_factor']
num_datanodes = config['num_datanodes']
datanode_size = config['datanode_size']
sync_period = config['sync_period']
datanode_log_path = config['datanode_log_path']
namenode_log_path = config['namenode_log_path']
namenode_checkpoints = config['namenode_checkpoints']
fs_path = config['fs_path']
dfs_setup_config = config['dfs_setup_config']
setupfiledir = config['dfs_setup_config'][:-10]

def cat(fileName):
	metaDataOfDatanodespath = path_to_namenodes + 'metaDataofDatanodes.json'
	metaDataOfInputFilespath = path_to_namenodes + 'metaDataofInputFiles.json'
	f = open(metaDataOfInputFilespath)
	data=json.load(f)
	fileSplits=[]
	try:
		for i in range(1, data[fileName][0] + 1):
			fileSplits.append(path_to_datanodes + data[fileName][i][2] + '/' + data[fileName][i][1] + '.' + fileName.split('.')[1]) #relativepaths of splits
	except:
		print("File not found in the DFS")

	for splits in fileSplits:
		file1 = open(splits,'r')

		lines = file1.readlines()
		for line in lines:
			print(line.strip())

		file1.close()