__author__ = 'rowem'

from pyspark import SparkContext, SparkConf
from datetime import datetime, timedelta
from Post import Post

if __name__ == "__main__":

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
        posts = []
        lineTokens = line.split("\t")

        if len(lineTokens) is 5:
            postid = lineTokens[0]
            forumid = lineTokens[1]
            userid = lineTokens[2]
            content = lineTokens[3]
            # 2011-06-13 09:27:30
            date = datetime.strptime(lineTokens[4], '%Y-%m-%d %H:%M:%S')

            post = Post(userid, postid, forumid, date)
            post.addContent(content)
            posts.append(post)
        return posts

    def stripDates(post):
        date = post.date
        return date

    def weekPostsMapper(post):

        min_date = minDate.value
        max_date = maxDate.value
        total_weeks = totalWeeks.value

        # log week key
        week_key = 1

        for i in range(1, totalWeeks):
            # print "Processing week: " + str(i)
            # get the start of the window
            startWindow = min_date
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

    def weekPostsReducer(posts1, posts2):
        posts = posts1
        posts = posts.union(posts2)











    ##### Main Execution Code
    conf = SparkConf().setAppName("StochFuse - Shift Detector")
    conf.set("spark.python.worker.memory", "10g")
    conf.set("spark.driver.memory", "15g")
    conf.set("spark.executor.memory", "10g")
    conf.set("spark.default.parallelism", "12")
    conf.set("spark.mesos.coarse", "true")
    conf.set("spark.driver.maxResultSize", "10g")
    # Added the core limit to avoid resource allocation overruns
    conf.set("spark.cores.max", "10")
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
        postsRDD = cleanFileLocation.flatMap(lineLoader).collect()

        # get the minimum and maximum dates from the RDD's posts
        print("----Getting dates RDD and computing min and max dates for window")
        datesRDD = postsRDD.flatMap(stripDates).collect()
        minDate = datesRDD.min()
        maxDate = datesRDD.max()
        # work out the total number of weeks that the entire dataset covers
        timeDelta = maxDate - minDate
        daysDelta = timeDelta.days
        totalWeeks = daysDelta / 7
        # broadcast variables to the cluster to use within the map operations
        minDate = sc.broadcast(minDate)
        maxDate = sc.broadcast(maxDate)
        totalWeeks = sc.broadcast(totalWeeks)

        # Derive the burn-in window over the first 25% of data



        # Derive burn-in distribution for each term signal - over the first 25% of data

        # Run per-week analysis of each token's signal: update the dirichlet each week

        # Derive delta distributions per week, plot shape

        # Run shapiro wilk test on each token's distribution and plot the shape of the


