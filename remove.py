import json
import os

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

def remove(fileName):
	metaDataOfDatanodespath = path_to_namenodes + 'metaDataofDatanodes.json'
	metaDataOfInputFilespath = path_to_namenodes + 'metaDataofInputFiles.json'
	f=open(metaDataOfInputFilespath)
	f3=open(metaDataOfDatanodespath)
	data=json.load(f)  #metadata of files
	metadataOfDatanodes=json.load(f3)  #metadata of datanodes

	f.close()
	f3.close()

	fileSplits=[]
	try:
		for i in range(1,data[fileName][0]+1):
			fileSplits.append(path_to_datanodes + data[fileName][i][2]+'/'+data[fileName][i][1]+'.'+fileName.split('.')[1]) #relativepaths of splits

	except:
		print("File not found in the DFS") 


	i=1 #to keep track of splits in data{}
	for splits in fileSplits:
		freeDatanode= data[fileName][i][2]     #block from this datanode this removed
		freeBlockNumber=data[fileName][i][3]    #freed block
		#print(freeDatanode,freeBlockNumber)
		
		os.remove(splits)
		i=i + 1
		#update datanodemetafile since splits got deleted the blocks of datnode has become free
		metadataOfDatanodes[freeDatanode]["occupiedBlocks"].pop(str(freeBlockNumber))
		metadataOfDatanodes[freeDatanode]["freeBlocks"].insert(0, int(freeBlockNumber))
		print("Remove file successful")
	try:
		data.pop(fileName) #file doesnt exits anymore so remove it off from the fileMetadata
		f2 = open(metaDataOfInputFilespath,'w')
		f2.write(str(json.dumps(data, indent=4)))
		f2.close()
	except:
		pass
		
	#updating datanode metadata
	f4 = open(metaDataOfDatanodespath,'w')
	f4.write(str(json.dumps(metadataOfDatanodes, indent = 4)))
	