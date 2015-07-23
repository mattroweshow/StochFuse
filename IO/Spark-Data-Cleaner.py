from __future__ import print_function

import sys
from random import random
from pyspark import SparkContext, SparkConf
import json
from LineParser import LineParser

if __name__ == "__main__":

    ##### Utility functions
    def getHDFSFileLocation(dataset_name):
        if dataset_name is "facebook":
            return "hdfs://scc-culture-mind.lancs.ac.uk/user/rowem/data/facebook/facebook-posts.tsv"
        elif dataset_name is "boards":
            return "hdfs://scc-culture-mind.lancs.ac.uk/user/rowem/data/boards/boards-posts.tsv"
        elif dataset_name is "reddit":
            return "hdfs://scc-culture-mind.lancs.ac.uk/user/kershad1/data/reddit/reddit-all.json"
        elif dataset_name is "twitter":
            return "hdfs://scc-culture-mind.lancs.ac.uk/user/kershad1/data/twitter/tweets2.json"


    ##### Map-Reduce Functions
    ###### For processing export file
    def lineMapper(line):
        # get the topics from the broadcast
        dataset_name = datasetName.value

        # process each line using the designated line processor for the dataset - given the different
        # formats that the data comes in
        if dataset_name is "facebook":
            datasetObj = LineParser.parseFacebookLine(line, dataset_name)
            return (dataset_name, datasetObj)
        elif dataset_name is "boards":
            datasetObj = LineParser.parseFacebookLine(line, dataset_name)
            return (dataset_name, datasetObj)
        elif dataset_name is "reddit":
            datasetObj = LineParser.parseFacebookLine(line, dataset_name)
            return (dataset_name, datasetObj)
        elif dataset_name is "twitter":
            datasetObj = LineParser.parseFacebookLine(line, dataset_name)
            return (dataset_name, datasetObj)

    def reduceDatasets(dataset1, dataset2):
        dataset1.merge_posts(dataset2)
        return dataset1

    ##### Main Execution Code
    conf = SparkConf().setAppName("StochFuse - Dataset Cleaning")
    conf.set("spark.python.worker.memory","10g")
    conf.set("spark.driver.memory","15g")
    conf.set("spark.executor.memory","10g")
    conf.set("spark.default.parallelism", "12")
    conf.set("spark.mesos.coarse", "true")
    conf.set("spark.driver.maxResultSize", "10g")
    # Added the core limit to avoid resource allocation overruns
    conf.set("spark.cores.max", "20")
    conf.setMaster("mesos://zk://scc-culture-mind.lancs.ac.uk:2181/mesos")
    conf.set("spark.executor.uri", "hdfs://scc-culture-mind.lancs.ac.uk/lib/spark-1.3.0-bin-hadoop2.4.tgz")
    conf.set("spark.broadcast.factory", "org.apache.spark.broadcast.TorrentBroadcastFactory")

    sc = SparkContext(conf=conf)
    sc.setCheckpointDir("hdfs://scc-culture-mind.lancs.ac.uk/data/checkpointing")

    # set the datasets to be processed
    datasets = ["facebook"]

    # clean each dataset
    for dataset in datasets:
        # get the HDFS url of the dataset
        hdfsUrl = getHDFSFileLocation(dataset)

        # broadcast the name of the dataset to the cluster
        print("Broadcasting the name of the dataset being processed")
        datasetName = sc.broadcast(dataset)

        # run a map-reduce job to first compile the RDD for the dataset loaded from the file
        rawPostsFile = sc.textFile(hdfsUrl)
        dataset_map = datasetName.map(lambda line: lineMapper(line)).reduceByKey(reduceDatasets)

        output = dataset_map.collect()
        print("Filter Accuracy...")
        for (dataset_name, datasetObj) in output:
            size = str(len(datasetObj.posts))
            print("%s: %s" % (datasetName, size))
        sc.stop()


