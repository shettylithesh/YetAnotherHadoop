import json
import os
from shutil import rmtree


#default configuration
config = {}
config['block_size'] = 64
config['path_to_datanodes'] = "/users/vinaynaidu/DATANODE/"
config['path_to_namenodes'] = "/users/vinaynaidu/NAMENODE/"
config['replication_factor'] = 3
config['num_datanodes'] = 5
config['datanode_size'] = 10
config['sync_period'] = 60
config['datanode_log_path'] = "/users/vinaynaidu/DATANODE/DATANODE_LOGS.txt"
config['namenode_log_path'] = "/users/vinaynaidu/NAMENODE/NAMENODE_LOGS.txt"
config['namenode_checkpoints'] = "/users/vinaynaidu/NAMENODE/CHECKPOINTS/"
config['fs_path'] = "/users/vinaynaidu/DFS/FILE_SYSTEM/"
config['dfs_setup_config'] = "/users/vinaynaidu/DFS/setup.json"
setupfiledir = "/users/vinaynaidu/DFS/"

try:
    f = open("config_sample.json")
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
except:
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


try:
    os.mkdir(setupfiledir)
except:
    pass
setupfile = open(dfs_setup_config, 'w')
setupfile.write(str(json.dumps(config)))

try:
    rmtree(path_to_datanodes)
except:
    pass
try:
    rmtree(path_to_namenodes)
except:
    pass

os.mkdir(path_to_datanodes)
os.mkdir(path_to_namenodes)

datanodestring = '''import socket
import time

def datanode{}HB():
	msgFromClient = "{}"
	bytesToSend = str.encode(msgFromClient)
	serverAddressPort = ("127.0.0.1", 2000)
	bufferSize = 1024
	UDPClientSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
	while True:
		UDPClientSocket.sendto(bytesToSend, serverAddressPort)
		time.sleep({})'''


namenodestring = '''import socket
import time
import datetime
import json
import os

f = open('{}')
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

namenodelogs = open(namenode_log_path, 'a')
datanodelogs = open(datanode_log_path, 'a')

masterset = set(range(1, num_datanodes + 1))

def namenodereceiveheartbeat1():
	namenodelogs = open(namenode_log_path, 'a')
	datanodelogs = open(datanode_log_path, 'a')
	localIP = "127.0.0.1"
	localPort = 2000
	bufferSize = 1024

	secserverAddressPort = ("127.0.0.1", 3000)
	UDPClientSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
	msgFromClient = "1"
	bytesToSend = str.encode(msgFromClient)

	# msgFromServer = "Hello UDP Client"
	# bytesToSend = str.encode(msgFromServer)
	UDPServerSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
	UDPServerSocket.bind((localIP, localPort))
	# print("NAMENODE server up and listening")

	set1 = set()

	while(True):
		start = time.time()
		while(time.time() < start + sync_period):
			bytesAddressPair = UDPServerSocket.recvfrom(bufferSize)
			message = int(bytesAddressPair[0].decode())
			set1.add(int(message))
		if len(set1) == num_datanodes:
			# print("200, All datanodes functioning", datetime.datetime.fromtimestamp(time.time()))
			UDPClientSocket.sendto(bytesToSend, secserverAddressPort)
			datanodelogs.write("200, All datanodes functioning at - ")
			datanodelogs.write(str(datetime.datetime.fromtimestamp(time.time())) + '\\n')
			datanodelogs.flush()
			# os.fsync(datanodelogs.fileno())
			set1 = set()
		else:
			faultydatanodes = masterset - set1
			# print("404, Some datanodes didn't send heartbeat", faultydatanodes)
			datanodelogs.write("404, Some datanodes didn't send heartbeat - ")
			datanodelogs.write(str(faultydatanodes) + '\\n')
			datanodelogs.flush()
			set1 = set()
			#should write code to remove the datanode from the metadata file
			#todo'''.format(dfs_setup_config)

secondarynamenodestring = '''import socket
import time
import datetime
import json
import os
import threading
from namenode import namenodereceiveheartbeat1

f = open('{}')
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

metaDataOfDatanodespath = path_to_namenodes + 'metaDataofDatanodes.json'
metaDataOfInputFilespath = path_to_namenodes + 'metaDataofInputFiles.json'

namenodelogs = open(namenode_log_path, 'a')
datanodelogs = open(datanode_log_path, 'a')

masterset = set(range(1, num_datanodes + 1))

def secnamenodereceiveheartbeat():
    prevstart = 0
    count = 0
    namenodelogs = open(namenode_log_path, 'a')
    datanodelogs = open(datanode_log_path, 'a')
    localIP = "127.0.0.1"
    localPort = 3000
    bufferSize = 1024

    msgFromServer = "Hello UDP Client"
    bytesToSend = str.encode(msgFromServer)
    UDPServerSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    UDPServerSocket.bind((localIP, localPort))
    # print("NAMENODE server up and listening")

    while(True):
        bytesAddressPair = UDPServerSocket.recvfrom(bufferSize)
        message = int(bytesAddressPair[0].decode())
        
        if message == 1:
            start = time.time()
            #print("message coming", message)
            if prevstart:
                start = time.time()

                if int(time.time()) - prevstart <= sync_period + 1 :
                    namenodelogs.write("200, Primary Namenode functioning at - ")
                    namenodelogs.write(str(datetime.datetime.fromtimestamp(time.time())) + '\\n')
                    namenodelogs.flush()
                    message = ""
                    prevstart = start
                    f1 = open(metaDataOfDatanodespath, 'r')
                    f2 = open(metaDataOfInputFilespath, 'r')
                    metaDataOfDatanodes = json.load(f1)
                    metaDataOfInputFiles = json.load(f2)
                    metaDataOfDatanodescheckpoints = namenode_checkpoints + 'metaDataofDatanodescheckpoints.json'
                    metaDataOfInputFilescheckpoints = namenode_checkpoints + 'metaDataofInputFilescheckpoints.json'
                    f3 = open(metaDataOfDatanodescheckpoints, 'w')
                    f4 = open(metaDataOfInputFilescheckpoints, 'w')
                    f3.write(str(json.dumps(metaDataOfDatanodes, indent=4)))
                    f4.write(str(json.dumps(metaDataOfInputFiles, indent=4)))
                    namenodelogs.write("Secondary Namenode performed backup at - ")
                    namenodelogs.write(str(datetime.datetime.fromtimestamp(time.time())) + '\\n')
                    namenodelogs.flush()
                    f1.close()
                    f2.close()
                    f3.close()
                    f4.close()

                else:
                    count = count + 1
                    namenodelogs.write("404, Primary Namenode didn't send heartbeat - " + str(datetime.datetime.fromtimestamp(time.time())))
                    namenodelogs.flush()
                    if count > 5:
                        #primary namenode is not functioning
                        #start new primary namenode
                        namenodeHBthread = threading.Thread(target=namenodereceiveheartbeat1, name='newnamenodeHBthread')
                        namenodeHBthread.start()
                        pass
            else:
                prevstart = start'''.format(dfs_setup_config)


try:
    os.mkdir(path_to_namenodes)
except:
    pass
filename = path_to_namenodes + 'namenode.py'
handle = open(filename, 'w')
handle.write(namenodestring)
filename = path_to_namenodes + 'secondarynamenode.py'
handle = open(filename, 'w')
handle.write(secondarynamenodestring)

for i in range(1, num_datanodes + 1):
    dirname = path_to_datanodes + 'datanode{}/'.format(i)
    os.mkdir(dirname)
    filename = path_to_datanodes + 'datanode{}'.format(i) + '/datanode{}.py'.format(i)
    open(filename, 'w').close()
    filehandle = open(filename,"w")
    filehandle.write(datanodestring.format(i, i, sync_period/3))
    filehandle.close()


metaDataOfDatanodes = {}  #keeps of track of datanodes and availability basically needed for writing file to hdfs
metaDataOfInputFiles = {}  #keeps track of files uploaded needed for reading from hdfs for cat command
metaDataOfReplicas = {}
for i in range(1, num_datanodes+1):
	metaDataOfDatanodes["datanode{}".format(i)] = {"freeBlocks":list(range(1, datanode_size + 1)),"occupiedBlocks":{}}

metaDataOfDatanodespath = path_to_namenodes + 'metaDataofDatanodes.json'
metaDataOfInputFilespath = path_to_namenodes + 'metaDataofInputFiles.json'
metaDataOfReplicaspath = path_to_namenodes + 'metaDataofReplicas.json'
handle1 = open(metaDataOfDatanodespath, 'w')
handle1.write(str(json.dumps(metaDataOfDatanodes, indent=4)))
handle2 = open(metaDataOfInputFilespath, 'w')
handle2.write(str(json.dumps(metaDataOfInputFiles, indent=4)))
handle3 = open(metaDataOfReplicaspath, 'w')
handle3.write(str(json.dumps(metaDataOfReplicas, indent=4)))
handle1.close()
handle2.close()
handle3.close()

try:
    os.mkdir(namenode_checkpoints)
except:
    pass

metaDataOfDatanodescheckpoints = namenode_checkpoints + 'metaDataofDatanodescheckpoints.json'
metaDataOfInputFilescheckpoints = namenode_checkpoints + 'metaDataofInputFilescheckpoints.json'
metaDataOfReplicascheckpoints = namenode_checkpoints + 'metaDataOfReplicascheckpoints.json'
open(metaDataOfDatanodescheckpoints, 'w').close()
open(metaDataOfInputFilescheckpoints, 'w').close()
open(metaDataOfReplicascheckpoints, 'w').close()