__author__ = 'rowem'

from pyspark import SparkContext, SparkConf
from datetime import datetime, timedelta
from Post import Post
import numpy as np
import scipy.stats as sp

if __name__ == "__main__":

    #### Test Functions
    def testMap(line):
        # convert the line to an asci representation from unicode so that it can be worked with
        line = line.encode('ascii', 'ignore')

        # # This gets the length of the line
        line = line.replace("u\'", "")
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
        line = line.replace("u\'", "")
        line = line.replace("\'", "")
        line = line.replace("[", "").replace("]", "")

        # ## assumes that line is a tab delimited string
        lineTokens = line.split("\\t")

        posts = []

        if len(lineTokens) is 5:
            postid = lineTokens[0]
            forumid = lineTokens[1]
            userid = lineTokens[2]
            content = lineTokens[3]
            # 2011-06-13 09:27:30

            dateString = lineTokens[4]
            date = datetime.strptime(dateString, '%Y-%m-%d %H:%M:%S')

            post = Post(userid, postid, forumid, date)
            post.addContent(content)
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
        startWindow = startDate
        for i in range(1, total_weeks):
            # print "Processing week: " + str(i)
            # get the start of the window
            weeksDelta = timedelta(days = 7)
            endWindow = startWindow + weeksDelta

            # get all posts within the interval
            if post.date >= startWindow and post.date < endWindow:
                week_key = i
                break

            # knock on the start date by one week
            startWindow = endWindow

        posts = []
        posts.append(post)
        return (week_key, posts)

    def postsReducer(posts1, posts2):
        posts = posts1 + posts2
        return posts

    #input: tuple of (week_key, [post])
    def computeWeeklyTerms(tuple):
        week_key = int(tuple[0])
        posts = tuple[1]
        term_frequency = {}
        for post in posts:
            terms = post.content.lower().split()
            for term in terms:
                if term in term_frequency:
                    term_frequency[term] = 1
                else:
                    term_frequency[term] += 1
        return (week_key, term_frequency)

    # Sequential functions
    def runSequentialanalysis(weekly_term_distributions):
        print("Deriving total term distribution")
        terms = set()
        # Derive the term set
        for week_key in sorted(weekly_term_distributions):
            terms.add(set(weekly_term_distributions[week_key].keys()))
        print("Dimensionality: " + str(len(terms)))

        # prime the beta using a uniform prior
        alpha = 1
        beta = 1
        prior_0_betas = {}
        for term in terms:
            prior_0_betas[term] = {'alpha': alpha, 'beta': beta}
        weekly_betas = {0: prior_0_betas}

        print("Running weekly sequential updating using Beta-Bin conjugate")
        for week_key in sorted(weekly_term_distributions):
            weekly_term_distribution = weekly_term_distributions[week_key]

            # get the likelihood params: X ~ B(n, p) => (x,n) - where x is number of t occurrences, n is total occurrences
            n = sum(weekly_term_distribution.values())
            posterior_betas = {}
            for term in terms:
                x = 0
                if term in weekly_term_distribution:
                    x = weekly_term_distribution[term]

                # derive the beta for the week by updating the prior to get the posterior
                prior_beta = weekly_betas[week_key-1][term]
                posterior_beta = {'alpha': (prior_beta['alpha'] + x),
                                  'beta': (prior_beta['beta'] + n - x)}
                posterior_betas[term] = posterior_beta

            # Log tha weekly betas following conjugate analysis
            weekly_betas[week_key] = posterior_betas

        print("Deriving weekly means of terms")
        weekly_term_means = {}
        weekly_term_means_delta = {}
        for week_key in weekly_betas:
            # Get the weekly term means
            week_term_means = {}
            weekly_term_means_delta = {}
            for term in weekly_betas[week_key]:
                term_alpha = weekly_betas[week_key][term]['alpha']
                term_beta = weekly_betas[week_key][term]['beta']
                mean = float(term_alpha) / (float(term_alpha) + float(term_beta))
                week_term_means[term] = mean

                # get the delta between the current mean and the previous week's mean
                if week_key > 1:
                    previous_mean = weekly_term_means[week_key-1][term]
                    delta = mean - previous_mean
                    weekly_term_means_delta[term] = delta

            # Derive the delta distribution between the beta distributions means
            weekly_term_means[week_key] = week_term_means
            weekly_term_means_delta[week_key] = weekly_term_means_delta

        print("Weekly Term Means")
        for week_key in weekly_term_means:
            print("Week: " + str(week_key))
            print(str(len(weekly_term_means[week_key])))
            print(str(np.array(weekly_term_means[week_key])))
            print(str(np.mean(np.array(weekly_term_means[week_key]))))
            print(str(np.std(np.array(weekly_term_means[week_key]))))
            print(str(sp.shapiro(np.array(weekly_term_means[week_key]))))

        print("Weekly Term Deltas")
        for week_key in weekly_term_means_delta:
            print("Week: " + str(week_key))
            print(str(len(weekly_term_means_delta[week_key])))
            print(str(np.array(weekly_term_means_delta[week_key])))
            print(str(np.mean(np.array(weekly_term_means_delta[week_key]))))
            print(str(np.std(np.array(weekly_term_means_delta[week_key]))))
            print(str(sp.shapiro(np.array(weekly_term_means[week_key]))))


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
        # testPostsRDD = cleanedFile.flatMap(lambda x: x.split(",")).map(testMap).reduceByKey(testReduce).collect()
        # print("\n----Test RDD")
        # for i in range(0, 10, 1):
        #     post1 = testPostsRDD[i][0]
        #     print(post1)

        postsRDD = cleanedFile.flatMap(lambda x: x.split(",")).flatMap(lineLoader)
        # postsRDD = cleanedFile.flatMap(lambda x: x.split(",")).flatMap(lineLoader).collect()
        # print("----Cleaned posts RDD length : %s" % str(len(postsRDD)))

        # sample the first element of the rdd
        # print("\n----Posts RDD")
        # for i in range(0, len(postsRDD), 1):
        #     post1 = postsRDD[i]
        #     print(post1)

        # get the minimum and maximum dates from the RDD's posts
        print("----Getting dates RDD and computing min and max dates for window")
        datesRDD = postsRDD.map(stripDates)
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
        print("--------Start date: %s" % str(minDate.value))
        print("--------End date: %s" % str(maxDate.value))
        print("--------Total weeks: %s" % str(totalWeeks.value))

        # # Derive the burn-in window over the first 25% of data
        print("----Computing posts to week number")
        # weekPostsRDD = postsRDD.map(weekPostsMapper).foldByKey((0, None), postsReducer).collect()
        weekPostsRDD = postsRDD.map(weekPostsMapper).reduceByKey(postsReducer)
        weekPostsRDD.cache()
        # Filter to the 25% week number
        weekCutoff = int(0.25 * int(totalWeeks.value))
        burninRDD = weekPostsRDD.filter(lambda x: x[0] <= weekCutoff).map(lambda x: (x[0], x[1]))
        # print("--------Cutoff Posts RDD length: %s" % str(len(burninRDD)))
        # Filter post 25% week number
        postRDD = weekPostsRDD.filter(lambda x: x[0] > weekCutoff).map(lambda x: (x[0], x[1]))
        # print("--------Cutoff Posts RDD length: %s" % str(len(postRDD)))

        #### check point - ensure that the code works up to this point
        # Step 1: EDA
        # Derive burn-in distribution for each term signal - over the first 25% of data - induce term specific beta priors
        # Compute weekly term (category) distributions
        print("------Comoputing the weekly term distribution over the burn in period")
        weeklyBurnInTermDistributions = burninRDD\
            .map(computeWeeklyTerms)\
            .sortByKey()\
            .collect()


        print("-----Running conjugate analysis on a single node")
        runSequentialanalysis(weeklyBurnInTermDistributions)





        # Derive delta distributions per week, plot shape




