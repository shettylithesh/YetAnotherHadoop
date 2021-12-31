import shutil #to move splits to datanodes
import json
import os
import glob

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

def fileUpload(input):
	metaDataOfDatanodespath = path_to_namenodes + 'metaDataofDatanodes.json'
	metaDataOfInputFilespath = path_to_namenodes + 'metaDataofInputFiles.json'
	metaDataOfReplicaspath = path_to_namenodes + 'metaDataofReplicas.json'
	f1 = open(metaDataOfDatanodespath)
	f2 = open(metaDataOfInputFilespath)
	f3 = open(metaDataOfReplicaspath)
	metaDataOfDatanodes = json.load(f1)
	metaDataOfInputFiles = json.load(f2)
	metaDataOfReplicas = json.load(f3)
	f1.close()
	f2.close()
	f3.close()

	metaDataOfInputFiles[input[0]]=list()
	metaDataOfInputFiles[input[0]].append(input[1])
	metaDataOfReplicas[input[0]] = {}
	splitNumberCount=1
	#check which datanode has free blocks then assign those blocks for storing abc.txt's splits 
	for i in range(1, num_datanodes + 1):
		if(len(metaDataOfDatanodes['datanode{}'.format(i)]['freeBlocks']) >= 1): #checking for atleast one freeblock
			freeBlockNumber = metaDataOfDatanodes['datanode{}'.format(i)]['freeBlocks'].pop(0) #pop based on hashing with split and block number
			metaDataOfDatanodes['datanode{}'.format(i)]['occupiedBlocks'][freeBlockNumber]=[input[0],splitNumberCount]
			
			#now move the corresponfing splits to datanode{i}'s  ie to hdfs previously it was stored in temporary folder
			
			source = input[splitNumberCount+1]
			destination = path_to_datanodes + "datanode{}".format(i)
			
			#print(source, destination)
			shutil.move(source, destination)
			# os.remove(destination + '/block{}.txt'.format(freeBlockNumber)
			#replication of newly added split the splits are replicated to the free blocks of the next  datanodes

			j = i + 1
			replicaCount = 0
			if (j == num_datanodes + 1):
				j =  1
			replicaCounter = 0 #number of replicas  produced in hdfs
			replicaInfo=[]  #will store locations of replicasplits later added to metadata of replicas
			
			while(j<= num_datanodes and replicaCount < replication_factor ): #this part does replication
				if(j==i): #full traversal done still no free blocks ie no space left for freeblocks no storing replica's in same datanode
					break 
				
				if(len(metaDataOfDatanodes['datanode{}'.format(j)]['freeBlocks']) >= 1):
					
					replifreeBlockNumber=metaDataOfDatanodes['datanode{}'.format(j)]['freeBlocks'].pop(0)
					metaDataOfDatanodes['datanode{}'.format(j)]['occupiedBlocks'][replifreeBlockNumber]=[input[0],splitNumberCount]
					replicaCount += 1
					replicaDestiny = path_to_datanodes + "/datanode{}".format(j)
					splitFileName = source.split('/')[-1] 
					replicaSource = destination+'/'+ splitFileName  #this is the path where the split to be replicated resides
					try:
						shutil.copy(replicaSource,replicaDestiny)
						replicaCounter+=1
						replicaPath=replicaDestiny+'/'+splitFileName
						replicaInfo.append(replicaPath)
					except:
						pass #no copying splits to same datanode so moving same splits to same datanode is checked here
				j+=1
				if (j == num_datanodes + 1):
					j = 1
						
			replicaInfo.insert(0,replicaCounter)

			metaDataOfReplicas[input[0]][input[splitNumberCount+1].split('/')[-1]]=replicaInfo

			splitName=input[splitNumberCount+1].split('/')[-1].split('.')[0]
			splitInfo=list()
			splitInfo.append(splitNumberCount)
			splitInfo.append(splitName)   
			splitInfo.append('datanode{}'.format(i))  #datanode number where the split is stored      
			splitInfo.append(freeBlockNumber)		
			metaDataOfInputFiles[input[0]].append(splitInfo)

			
		if (splitNumberCount == input[1]): # all splits successfully placed in the free datanodes
			message = "Write file into HDFS successful"
			break
		splitNumberCount += 1

	f1 = open(metaDataOfDatanodespath, 'w')
	f2 = open(metaDataOfInputFilespath, 'w')
	f3 = open(metaDataOfReplicaspath, 'w')
	f3.write(str(json.dumps(metaDataOfReplicas, indent=4)))
	f1.write(str(json.dumps(metaDataOfDatanodes, indent=4)))
	f2.write(str(json.dumps(metaDataOfInputFiles, indent=4)))
	f1.close()
	f2.close()
	f3.close()
	return message


def split(filePath):
    input=[]  #this is sent as input for nn.py
    try:
        shutil.rmtree('temp')
    except:
        pass
    os.mkdir('temp')

    fileName=filePath.split('/')[-1]

    #Code to split depending on the size of data
    os.system('split -b {}m '.format(block_size) + filePath + ' ./temp/block_')

    files = glob.glob('./temp/block*')
    files.reverse()  #since files list had splits in a reverse order

    input.append(fileName)
    input.append(len(files))
    i = 1
    for file in files:
        newFilePath='./temp/'+fileName.split('.')[0]+'*{}'.format(i)+'.'+fileName.split('.')[1]
        os.rename(file, newFilePath)  #extension of file needed so last split() 
        splitPath=newFilePath
        i += 1
        input.append(splitPath)
    message = fileUpload(input)
    return message

