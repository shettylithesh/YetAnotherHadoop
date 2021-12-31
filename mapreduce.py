import os
import json
import shutil
import time
from put import split

def mapreduce(inputfilepath, outputfilepath, setupfilepath, mapperpath, reducerpath):
    try:
        split(inputfilepath)
        print("Writing file into HDFS...")
        print("Starting MR job...")
    except:
        print("Starting MR job...")
    f1 = open(setupfilepath)

    config = json.load(f1)
    dfspath = config['dfs_setup_config'][:-10]
    pathtodatanode = config['path_to_datanodes']
    pathtonamenode = config['path_to_namenodes']
    f = open(pathtonamenode + 'metaDataofInputFiles.json')
    text = json.load(f)
    inputfilename = inputfilepath.split('/')[-1]
    a = text[inputfilename]
    S = ""	

    for each in range(1, len(a)):

        destpath = pathtodatanode + "{}".format(a[each][2])
        shutil.copy(mapperpath, destpath)
        try:
            shutil.rmtree(dfspath + '/outputofmapper.txt')
        except:
            pass
        S = S + "cd {}{}\npython3 mapper.py < {}.json >> {}/outputofmapper.txt &\n".format(pathtodatanode, a[each][2], a[each][1], dfspath)
        if each == len(a) - 1:
            shutil.copy(reducerpath, destpath)
            

    os.system(S)
    time.sleep(2)
    try:
        shutil.rmtree(outputfilepath)
    except:
        pass
    S = "cd {}\n".format(destpath) + "python3 reducer.py < {}/outputofmapper.txt >> {}".format(dfspath, outputfilepath)
    os.system(S)
    print("Finished MR job...")
