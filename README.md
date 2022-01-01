# YetAnotherHadoop

Yet Another Hadoop or YAH is a mini-HDFS setup on your system, complete with the architectural structure consisting of Data Nodes and Name Nodes and replication of data across nodes, as a part of the UE19CS322 Big Data course Project at PES University.

With YAH, you will possess finer control over your data blocks and their replication, and allow you to create a distributed file system as per your needs. YAH also allows running Hadoop like jobs to break down computationally heavy tasks into smaller distributed tasks.

Project specifications can be found in the specifications.pdf file

## Software/Languages used:

Python 3.8.x

## Team members:

1. [Lithesh Shetty](https://github.com/shettylithesh)

2. [Vinay P](https://github.com/Vinaypnaidu)

3. [K S Abhisheka](https://github.com/Abhi-k-s)

### Creating the DFS :

This process initialises the distributed file system. Datanode folders, datanode code files, namenode and secondary namenode files, metadata files get created. It takes the config_sample.json file as the configuration file. If the file is not provided, a default configuration will be loaded.

$ python3 createhdfs.py


### Starting the Command Line Interface (CLI) :

This script starts the CLI. Commands such as put, cat, ls, rm can be executed and map reduce jobs can be executed.

$ python3 cli.py

Writing a file to the DFS -

put <absolute path of the file>

Reading a file from the DFS -

cat <filename>
  
Listing all files in the DFS -

ls

Deleting a file from the DFS -

rm <filename>

Running a Map-Reduce job -

runmapreducejob -i <absolute path of input file> -o <absolute path of output file> -c <absolute path of dfs setup 
file> -m <absolute path of mapper file> -r <absolute path of reducer file>
  
The example mapper.py and reducer.py files can be used to test the working. The US Accident datset and the map reduce specifications are also included in the MapReduceExample folder.
  
### Implementation details :
  
The Heartbeat functionality is implemented using UDP client-server model. After every sync_period seconds, the datanodes send a message to the primary namenode. The primary namenode also sends the heartbeat to secondary namenode every sync_period seconds. The secondary namenode periodically performs backup of the primary namenode metadata files every sync_period seconds. Data will be persistent. When a file is written to DFS, replication will be performed according to the size of the file, block_size and replication_factor.

The deletion of replicas when rm <filename> command is executed and the implementation of datanodes as servers is yet to be done.
