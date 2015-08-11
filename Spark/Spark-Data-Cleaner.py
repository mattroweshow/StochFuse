from __future__ import print_function

from pyspark import SparkContext, SparkConf
from LineParser import LineParser
from Dataset import Dataset

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


    def lineMapperLists(lines):
        posts_global = []
        dataset_name = datasetName.value
        for line in lines:
            if "facebook" in dataset_name:
                posts = LineParser.parseFacebookLine(line)
                posts_global += posts
            elif "boards" in dataset_name:
                posts = LineParser.parseBoardsLine(line)
                posts_global += posts
            elif "reddit" in dataset_name:
                posts = LineParser.parseRedditLine(line, dataset_name)
                posts_global += posts
            elif "twitter" in dataset_name:
                posts = LineParser.parseTwitterLine(line, dataset_name)
                posts_global += posts
        return posts_global

    def combineListsLengths(posts1, posts2):
        print("Output is %s" % str(posts1))
        postsLength = len(posts1) + len(posts2)
        return postsLength

    def lineMapper(line):
        # test that MR is actually working!
#        vals = line.split("\t")
#        vals_length = len(vals)

        dataset_name = datasetName.value

        # process each line using the designated line processor for the dataset - given the different
        # formats that the data comes in
        if "facebook" in dataset_name:
            posts = LineParser.parseFacebookLine(line)
            return(dataset_name, posts)
        elif "boards" in dataset_name:
            posts = LineParser.parseBoardsLine(line)
            return (dataset_name, posts)
        elif "reddit" in dataset_name:
            posts = LineParser.parseRedditLine(line, dataset_name)
            return (dataset_name, posts)
        elif "twitter" in dataset_name:
            posts = LineParser.parseTwitterLine(line, dataset_name)
            return (dataset_name, posts)

    #### Combines the posts together
    def reduceDatasets(posts1, posts2):
        posts = posts1 + posts2
        return posts





    ##### Main Execution Code
    conf = SparkConf().setAppName("StochFuse - Dataset Cleaning")
    conf.set("spark.python.worker.memory","10g")
    conf.set("spark.driver.memory","15g")
    conf.set("spark.executor.memory","10g")
    conf.set("spark.default.parallelism", "12")
    conf.set("spark.mesos.coarse", "true")
    conf.set("spark.driver.maxResultSize", "10g")
    # Added the core limit to avoid resource allocation overruns
    conf.set("spark.cores.max", "15")
#    conf.setMaster("mesos://zk://scc-culture-mind.lancs.ac.uk:2181/mesos")
    conf.setMaster("mesos://zk://scc-culture-slave4.lancs.ac.uk:2181/mesos")
    conf.set("spark.executor.uri", "hdfs://scc-culture-mind.lancs.ac.uk/lib/spark-1.3.0-bin-hadoop2.4.tgz")
    conf.set("spark.broadcast.factory", "org.apache.spark.broadcast.TorrentBroadcastFactory")

    sc = SparkContext(conf=conf)
    sc.setCheckpointDir("hdfs://scc-culture-mind.lancs.ac.uk/data/checkpointing")

    # set the datasets to be processed
    # datasets = ["facebook"]
    datasets = ["boards"]


    # clean each dataset
    for dataset in datasets:
        # get the HDFS url of the dataset
        hdfsUrl = getHDFSFileLocation(dataset)

        # broadcast the name of the dataset to the cluster
        print("----Broadcasting the name of the dataset being processed")
        datasetName = sc.broadcast(dataset)

        # run a map-reduce job to first compile the RDD for the dataset loaded from the file
        print("-----Dataset file: " + hdfsUrl)
        rawPostsFile = sc.textFile(hdfsUrl, minPartitions=12)

        # Go through each partition and then count how many posts are stored within each
        print("-----Computing partition-level MR job..")

        # Effort 2: running mapPartitions
        # y = rawPostsFile.mapPartitions(lineMapperLists).collect()

        y = rawPostsFile.mapPartitions(lineMapperLists).reduce(combineListsLengths)

        # output = sum(y)
        print("-----Result Array: %s" % str(y))
        # print("-----Result: %s" % str(output))
        
        

#        #### Effort 1:  define the partition filter function
#        def make_part_filter(index):
#            def part_filter(split_index, iterator):
#                if split_index == index:
#                    for el in iterator:
#                        yield el
#            return part_filter
#
#        # iterate through each partition of the file
#        posts_count = 0
#
#        for part_id in range(rawPostsFile.getNumPartitions()):
#            print("----Patition id: " + str(part_id))
#            part_rdd = rawPostsFile.mapPartitionsWithIndex(make_part_filter(part_id), True)\
#                .map(lineMapper)\
#                .reduceByKey(reduceDatasets)
#            print("----Collecting parition result")
#            data_from_part_rdd = part_rdd.collect()
#
#            # data_str = str(data_from_part_rdd)
#            # print("%s elements: %s" % (part_id, data_str[0:50]))
#            # ('facebook', [<Post.Post instance at 0x7f4adf28305
#            # data_str = str(data_from_part_rdd[0])
#            posts_count += len(data_from_part_rdd[0][1])
#            print("%s" % str(len(data_from_part_rdd[0][1])))
#
#        print("Total posts count: %s" % str(posts_count))
        sc.stop()


