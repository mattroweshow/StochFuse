from __future__ import print_function

from pyspark import SparkContext, SparkConf
from LineParser import LineParser
from nltk.tokenize import RegexpTokenizer
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

    def cleanLines(lines):
        posts_global = []
        dataset_name = datasetName.value
        for line in lines:
            if "facebook" in dataset_name:
                posts = LineParser.parseFacebookLine(line)
                posts_global += posts
            elif "boards" in dataset_name:
                posts = LineParser.parseBoardsLine(line)
                posts_global += posts
                # count += len(posts)
            elif "reddit" in dataset_name:
                posts = LineParser.parseRedditLine(line, dataset_name)
                posts_global += posts
            elif "twitter" in dataset_name:
                posts = LineParser.parseTwitterLine(line, dataset_name)
                posts_global += posts

        # Clean each line that has been loaded
        return [posts_global]
        # return [count]

    def combineListsLengths(count1, count2):
        return count1 + count2


    ##### Data Cleaning Spark Functions

    def lineTokenizer(line):
        dataset_name = datasetName.value
        posts = []

        if "facebook" in dataset_name:
            posts = LineParser.parseFacebookLine(line)
        elif "boards" in dataset_name:
            posts = LineParser.parseBoardsLine(line)
        elif "reddit" in dataset_name:
            posts = LineParser.parseRedditLine(line, dataset_name)
        elif "twitter" in dataset_name:
            posts = LineParser.parseTwitterLine(line, dataset_name)

        terms = []
        if len(posts) == 1:
            tokenizer = RegexpTokenizer(r'\w+')
            terms = tokenizer.tokenize(posts[0].content.lower())
        return terms

    # Compiles a dictionary of terms using a basic term count distribution and MR design pattern
    def tokenFrequencyMapper(token):
        return (token, 1)

    def tokenFrequencyReducer(count1, count2):
        count = count1 + count2
        return count


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

        # Working examples:
        # y = rawPostsFile.mapPartitions(lineMapperLists).reduce(combineListsLengths)
        # y = rawPostsFile.mapPartitions(cleanLines, preservesPartitioning=True).collect()

        y = rawPostsFile\
            .flatMap(lineTokenizer)\
            .map(tokenFrequencyMapper)\
            .reduceByKey(tokenFrequencyReducer)

        print("Tokens dictionary : %s" % str(y))

        # # 0. Calculate the number of posts within the dataset
        # print "Original Dataset Number of Posts = " + str(len(dataset.posts))
        # print "# terms = " + str(calculateTermDimensionality(dataset))
        #
        # # Apply the pre-processing
        # # 1. Remove Sparse Terms
        # minFreq = 5
        # newDataset = removeSparseTerms(dataset, minFreq)
        # print "# Posts after removing sparse terms = " + str(len(newDataset.posts))
        # print "# terms = " + str(calculateTermDimensionality(newDataset))
        #
        # # 2. Remove Stopwords
        # newDataset2 = removeStopWords(newDataset)
        # print "# Posts after removing stop words = " + str(len(newDataset2.posts))
        # print "# terms = " + str(calculateTermDimensionality(newDataset2))
        #
        # # 3. Perform stemming
        # newDataset3 = stemWords(newDataset2)
        # print "# Posts after stemming = " + str(len(newDataset3.posts))
        # print "# terms = " + str(calculateTermDimensionality(newDataset3))
        #
        # # Write to the file
        # newVersion = "cleaned"
        # fileWriter = FileWriter(datasetName)
        # fileWriter.writeDatasetToFile(newDataset3, newVersion)

        # output = sum(y)
        print("-----Result Array: %s" % str())
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


