__author__ = 'rowem'

from pyspark import SparkContext, SparkConf
from datetime import datetime, timedelta
from Post import Post

if __name__ == "__main__":

    #### Test Functions
    def testMap(line):
        # convert the line to an asci representation from unicode so that it can be worked with
        line = line.encode('ascii', 'ignore')

        # # This gets the length of the line
        line = line.replace("\'", "")
        # ## assumes that line is a tab delimited string
        lineTokens = line.split("\\t")

        return (line, 1)
        # return (len(lineTokens), 1)

    def testReduce(count1, count2):
        count = count1 + count2
        return count


    #### Functions
    def getHDFSCleanedFileLocation(dataset_name):
        if dataset_name is "facebook":
            return "hdfs://scc-culture-mind.lancs.ac.uk/user/rowem/data/facebook/facebook-cleaned-posts"
        elif dataset_name is "boards":
            return "hdfs://scc-culture-mind.lancs.ac.uk/user/rowem/data/boards/boards-cleaned-posts"
        elif dataset_name is "reddit":
            return "hdfs://scc-culture-mind.lancs.ac.uk/user/rowem/data/reddit/reddit-cleaned-posts"
        elif dataset_name is "twitter":
            return "hdfs://scc-culture-mind.lancs.ac.uk/user/kershad1/data/twitter/twitter-cleaned-posts"

    def lineLoader(line):
        lineOrig = line
        # convert the line to an asci representation from unicode so that it can be worked with
        line = line.encode('ascii', 'ignore')

        # # This gets the length of the line
        line = line.replace("\'", "")
        # ## assumes that line is a tab delimited string
        lineTokens = line.split("\\t")

        posts = []

        if len(lineTokens) is 5:
            postid = lineTokens[0]
            forumid = lineTokens[1]
            userid = lineTokens[2]
            content = lineTokens[3]
            # 2011-06-13 09:27:30
            # date = datetime.strptime(lineTokens[4], '%Y-%m-%d %H:%M:%S')
            date = lineTokens[4]

            post = Post(userid, postid, forumid, date)
            post.addContent("Orginal line: " + lineOrig + " WITH mod line: " + line)
            posts.append(post)
        return posts

    def stripDates(post):
        date = post.date
        return date

    def weekPostsMapper(post):

        startDate = minDate.value
        endDate = maxDate.value
        total_weeks = totalWeeks.value

        # log week key
        week_key = 1
        for i in range(1, total_weeks):
            # print "Processing week: " + str(i)
            # get the start of the window
            startWindow = startDate
            weeksDelta = timedelta(days = 7)
            endWindow = startWindow + weeksDelta

            # get all posts within the interval
            if post.date >= startWindow and post.date < endWindow :
                break

            # knock on the start date by one week
            startDate = endWindow

        posts = []
        posts.append(post)
        return (week_key, posts)

    def postsReducer(posts1, posts2):
        posts = posts1 + posts2
        return posts


    ##### Main Execution Code
    conf = SparkConf().setAppName("StochFuse - Shift Detector")
    conf.set("spark.python.worker.memory", "10g")
    conf.set("spark.driver.memory", "15g")
    conf.set("spark.executor.memory", "10g")
    conf.set("spark.default.parallelism", "12")
    conf.set("spark.mesos.coarse", "true")
    conf.set("spark.driver.maxResultSize", "10g")
    # Added the core limit to avoid resource allocation overruns
    conf.set("spark.cores.max", "5")
#    conf.setMaster("mesos://zk://scc-culture-mind.lancs.ac.uk:2181/mesos")
    conf.setMaster("mesos://zk://scc-culture-slave4.lancs.ac.uk:2181/mesos")
    conf.set("spark.executor.uri", "hdfs://scc-culture-mind.lancs.ac.uk/lib/spark-1.3.0-bin-hadoop2.4.tgz")
    conf.set("spark.broadcast.factory", "org.apache.spark.broadcast.TorrentBroadcastFactory")

    sc = SparkContext(conf=conf)
    sc.setCheckpointDir("hdfs://scc-culture-mind.lancs.ac.uk/data/checkpointing")

    # datasets = ["facebook", "boards", "twitter"]
    datasets = ["facebook"]

    for dataset in datasets:

        print("----Broadcasting the name of the dataset being processed")
        datasetName = sc.broadcast(dataset)

        # Load the data into an RDD (shared across the cluster)
        print("----Loading the cleaned posts RDD")
        cleanFileLocation = getHDFSCleanedFileLocation(dataset)
        cleanedFile = sc.textFile(cleanFileLocation)

        # postsRDD = cleanedFile.flatMap(lineLoader).collect()
        testPostsRDD = cleanedFile.flatMap(lambda x: x.split(",")).map(testMap).reduceByKey(testReduce).collect()
        postsRDD = cleanedFile.flatMap(lambda x: x.split(",")).flatMap(lineLoader).collect()

        # print("----Cleaned posts RDD length : %s" % str(len(postsRDD)))

        print("\n----Test RDD")
        for i in range(0, 10, 1):
            post1 = testPostsRDD[i][0]
            print(post1)

        # sample the first element of the rdd
        print("\n----Posts RDD")
        for i in range(0, 10, 1):
            post1 = postsRDD[i]
            print(post1)

        # get the minimum and maximum dates from the RDD's posts
        # print("----Getting dates RDD and computing min and max dates for window")
        # datesRDD = postsRDD.flatMap(stripDates).collect()
        # minDate = datesRDD.min()
        # maxDate = datesRDD.max()
        # # work out the total number of weeks that the entire dataset covers
        # timeDelta = maxDate - minDate
        # daysDelta = timeDelta.days
        # totalWeeks = daysDelta / 7
        # # broadcast variables to the cluster to use within the map operations
        # minDate = sc.broadcast(minDate)
        # maxDate = sc.broadcast(maxDate)
        # totalWeeks = sc.broadcast(totalWeeks)
        # print("--------Start date: %s" % str(minDate))
        # print("--------End date: %s" % str(maxDate))
        # print("--------Total weeks: %s" % str(totalWeeks))
        #
        # # Derive the burn-in window over the first 25% of data
        # print("----Computing posts to week number")
        # weekPostsRDD = postsRDD.map(weekPostsMapper).foldByKey((0, None), weekPostsReducer).collect()
        # print("--------Week Posts RDD length: %s" % str(len(weekPostsRDD)))
        # # Filter to the 25% week number
        # weekCutoff = int(0.25 * totalWeeks)
        # cutOffRDD = weekPostsRDD.filter(lambda x: x[0] <= weekCutoff).map(lambda x: (x[0], x[1])).collect()
        # print("--------Cutoff Posts RDD length: %s" % str(len(cutOffRDD)))


        #### check point - ensure that the code works up to this point
        # Derive burn-in distribution for each term signal - over the first 25% of data


        # Run per-week analysis of each token's signal: update the dirichlet each week

        # Derive delta distributions per week, plot shape

        # Run shapiro wilk test on each token's distribution and plot the shape of the


