# YetAnotherHadoop
Yet Another Hadoop
Yet Another Hadoop or YAH is a mini-HDFS setup on your system, complete with the architectural structure consisting of Data Nodes and Name Nodes and replication of data across nodes, as a part of the UE19CS322 Big Data course Project at PES University.

With YAH, you will possess finer control over your data blocks and their replication, and allow you to create a distributed file system as per your needs. YAH also allows running Hadoop like jobs to break down computationally heavy tasks into smaller distributed tasks.

Project specifications can be found in the specifications.pdf file

Software/Languages used:

Python 3.8.x

Team members:

1.Vinay P

2.Lithesh Shetty

3.K S Abhisheka

Creating the DFS
This process initialises the distributed file system. Datanode folders, datanode code files, namenode and secondary namenode files, metadata files get created. It takes the config_sample.json file as the configuration file. If the file is not provided, a default configuration will be loaded.
