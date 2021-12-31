from os import error
import threading
from threading import Thread
from multiprocessing import Process
import json
import sys
from put import split
from cat import cat
from remove import remove
from ls import listallfiles
from mapreduce import mapreduce


#change path to this file accordingly
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


sys.path.append(path_to_datanodes)
sys.path.append(path_to_namenodes)
from namenode import namenodereceiveheartbeat1
from secondarynamenode import secnamenodereceiveheartbeat

dsthreads = {}

for i in range(1, num_datanodes + 1):
    sys.path.append(path_to_datanodes + 'datanode{}/'.format(i))
    exec("from datanode{} import datanode{}HB".format(i, i))
    exec("dsthreads['datanodehbthread{}'] = threading.Thread(target = datanode{}HB, name = 'DatanodeHBThread{}')".format(i, i, i))


namenodeHBthread = threading.Thread(target=namenodereceiveheartbeat1, name='namenodeHBthread')
secnamenodeHBthread = threading.Thread(target=secnamenodereceiveheartbeat, name='secnamenodeHBthread')
namenodeHBthread.start()
secnamenodeHBthread.start()

for i in range(1, num_datanodes + 1):
    dsthreads['datanodehbthread{}'.format(i)].start()

functionality = '''put, syntax - put <absolute path of the file>
cat, syntax - cat <filename>
ls, syntax - ls
rm, syntax - rm <filename>
runmapreducejob -i <absolute path of input file> -o <absolute path of output> -c <dfs setup file> -m <mapper absolute path> -r <reducer absolute path>'''

print("The default HDFS or the previous session is loaded...")
print("Provide configuration file and run createhdfs.py if you wish to create a new DFS.")

while True:
    print()
    print("Enter the DFS command...")
    print(functionality)
    print()
    command  = input().split()
    if command[0] == "put":
        if len(command) == 2:
            try:
                message = split(command[1])
                print(message)
                print()
            except error as e:
                print(e)
        else:
            print("Invalid syntax for put command")
    if command[0] == "cat":
        if len(command) == 2:
            try:
                cat(command[1])
            except error as e:
                print(e)
        else:
            print("Invalid syntax for cat command")
    if command[0] == "rm":
        if len(command) == 2:
            try:
                remove(command[1])
            except error as e:
                print(e)
        else:
            print("Invalid syntax for rm command")
    if command[0] == "runmapreducejob":
        if len(command) == 11:
            inputfilepath = command[2]
            outputfilepath = command[4]
            setupfilepath = command[6]
            mapperpath = command[8]
            reducerpath = command[10]
            mapreduce(inputfilepath, outputfilepath, setupfilepath, mapperpath, reducerpath)
        else:
            print("Invalid syntax for running Map Reduce job")
    if command[0] == "ls":
        print()
        print("Files present in the DFS -")
        listallfiles()
    

