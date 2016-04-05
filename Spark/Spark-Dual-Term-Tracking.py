__author__ = 'rowem'
from pyspark import SparkContext, SparkConf
from LineParser import LineParser
from Post import Post
import re
import string
from datetime import datetime, timedelta

if __name__ == "__main__":


    ##### Utility functions
    def getHDFSFileLocation(dataset_name):
        if dataset_name is "facebook":
            return "hdfs://scc-culture-mind.lancs.ac.uk/user/rowem/data/facebook/facebook-posts.tsv"
        elif dataset_name is "boards":
            return "hdfs://scc-culture-mind.lancs.ac.uk/user/rowem/data/boards/boards-posts.tsv"
        elif dataset_name is "reddit":
            return "hdfs://scc-culture-mind.lancs.ac.uk/reddit/uncompressed"
        elif dataset_name is "twitter":
            return "hdfs://scc-culture-mind.lancs.ac.uk/user/kershad1/data/twitter/tweets2.json"

    def getHDFSCleanedFileLocation(dataset_name):
        if dataset_name is "facebook":
            return "hdfs://scc-culture-mind.lancs.ac.uk/user/rowem/data/facebook/facebook-cleaned-posts"
        elif dataset_name is "boards":
            return "hdfs://scc-culture-mind.lancs.ac.uk/user/rowem/data/boards/boards-cleaned-posts"
        elif dataset_name is "reddit":
            return "hdfs://scc-culture-mind.lancs.ac.uk/user/rowem/data/reddit/reddit-cleaned-posts"
        elif dataset_name is "twitter":
            return "hdfs://scc-culture-mind.lancs.ac.uk/user/rowem/data/twitter/twitter-cleaned-posts"

    def cleanLines(lines):
        posts_global = []

        # receive the broadcast variables
        dataset_name = datasetName.value
        tokensDictBroadcastV = tokensDictBroadcast.value
        stopwords = stopwordsSet.value

        for line in lines:
            # clean the line
            line = filter(lambda x: x in string.printable, line)

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
        minFreq = 5
        newPosts = []
        for post in posts_global:
            newMessage = ""
            postString = post.content.lower()
            postString = filter(lambda x: x in string.printable, postString)
            postContent = postString.encode("utf-8")

            # prep the content by removing punctuation
            postContent = postContent.replace("'", "").replace(".", "").replace(",", "")
            # remove the square brackets content
            postContent = postContent.replace("[b]", "").replace("[/b]", "")
            postContent = postContent.replace("[i]", "").replace("[/i]", "")
            postContent = postContent.replace("[quote]", "").replace("[/quote]", "")
            postContent = postContent.replace("[url]", "").replace("[/url]", "")
            pat = re.compile(r"\t")
            postContent = pat.sub(" ", postContent)

            terms = postContent.split()
            for term in terms:
                if tokensDictBroadcastV[term] >= minFreq and term not in stopwords:
                    # Create the new post
                    newMessage += term + " "

            # Check that the new message is not just blank
            if len(newMessage) > 0:
                newPost = Post(post.author, post.postid, post.forumid, post.date)
                newPost.addContent(newMessage)
                newPosts.append(newPost.toTSVString())

        # Write the new posts to HDFS

        return [newPosts]
        # return [count]

    def combineListsLengths(count1, count2):
        return count1 + count2

    ### Tokenizer functions to handle term distributions
    # single term line tokenizer - for single term density functions
    def dualTermlineTokenizer(line):
        dataset_name = datasetName.value
        posts = []
        # get the stopwords from the broadcast variable
        stopwords = stopwordsSet.value
        # clean the line
        line = filter(lambda x: x in string.printable, line)
        if "facebook" in dataset_name:
            posts = LineParser.parseFacebookLine(line)
        elif "boards" in dataset_name:
            posts = LineParser.parseBoardsLine(line)
        elif "reddit" in dataset_name:
            posts = LineParser.parseRedditLine(line, dataset_name)
        elif "twitter" in dataset_name:
            posts = LineParser.parseTwitterLine(line, dataset_name)
        terms_dates = []
        if len(posts) == 1:
            # tokenizer = RegexpTokenizer(r'\w+')
            # terms = posts[0].content.lower().split()
            postString = posts[0].content.lower()
            postString = filter(lambda x: x in string.printable, postString)
            postContent = postString.encode("utf-8")

            # prep the content by removing punctuation
            postContent = postContent.replace("'", "").replace(",", "")
            # remove the square brackets content
            postContent = postContent.replace("[b]", "").replace("[/b]", "")
            postContent = postContent.replace("[i]", "").replace("[/i]", "")
            postContent = postContent.replace("[quote]", "").replace("[/quote]", "")
            postContent = postContent.replace("[url]", "").replace("[/url]", "")
            pat = re.compile(r"\t")
            postContent = pat.sub(" ", postContent)

            terms = postContent.split()

            # get the date when the post was made
            post_date = posts[0].date
            for term in terms:
                for term2 in terms:
                    if term not in stopwords and term2 not in stopwords and term is not term2:
                        term = term.replace(".", "")
                        term2 = term2.replace(".", "")
                        # mint the key from term and term2
                        terms_key = term + "_" + term2
                        terms_dates.append((terms_key, {post_date: 1}))
        return terms_dates

    def terms_reducer(dates_dict_1, dates_dict_2):
        dates = dates_dict_1
        for date in dates_dict_2:
            if date in dates:
                dates[date] += dates_dict_2[date]
            else:
                dates[date] = dates_dict_2[date]
        return dates

    def terms_weekly_counts_mapper(x):
        startDate = min_date_b.value
        total_weeks = total_weeks_b.value
        day1 = (startDate - timedelta(days=startDate.weekday()))
        term_key = x[0]
        dates_counts = x[1]
        weekly_count = {}
        for date in dates_counts:
            count = dates_counts[date]
            # get the week key based on the difference in weeks between the date and the start date
            day2 = (date - timedelta(days=date.weekday()))
            week_key = (day2 - day1).days / 7
            if week_key in weekly_count:
                weekly_count[week_key] += count
            else:
                weekly_count[week_key] = count
        # prime the remaining weeks to be 0
        for i in range(0, total_weeks+1):
            if i not in weekly_count:
                weekly_count[i] = 0
        return (term_key, weekly_count)

    def terms_weekly_relfreqs_mapper(x):
        weekly_counts = weekly_counts_b.value
        term_key = x[0]
        weekly_term_counts = x[1]
        weekly_term_relfreqs = {}
        for week_key in weekly_term_counts:
            count = float(weekly_term_counts[week_key])
            denom = float(weekly_counts[week_key])
            if count > 0 and denom > 0:
                count /= denom
            # weekly_term_relfreqs[week_key] = str(count) + " - " + str(denom)
            weekly_term_relfreqs[week_key] = count
        return (term_key, weekly_term_relfreqs)


    # Compiles a dictionary of terms using a basic term count distribution and MR design pattern
    def tokenFrequencyMapper(token):
        return (token, 1)

    def tokenFrequencyReducer(count1, count2):
        count = count1 + count2
        return count

    ##### Main Execution Code
    conf = SparkConf().setAppName("StochFuse - Dual-Term Joint Density Calculation")
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
    datasets = ["facebook"]

    # clean each dataset
    for dataset in datasets:
        # get the HDFS url of the dataset
        hdfsUrl = getHDFSFileLocation(dataset)

        # broadcast the name of the dataset to the cluster
        print("----Broadcasting the name of the dataset being processed")
        datasetName = sc.broadcast(dataset)

        # Load the stopwords file from hdfs
        print("----Loading stopwords file and broadcasting to the cluster")
        stopwordsFile = sc.textFile("hdfs://scc-culture-mind.lancs.ac.uk/user/rowem/data/stopwords.csv")
        stopwords = stopwordsFile.map(lambda x: [str(x)]).reduce(lambda x, y: x + y)
        print("---Stopwords: %s" % str(stopwords))
        print("---Stopwords Length: %s" % str(len(stopwords)))
        stopwordsSet = sc.broadcast(stopwords)

        # run a map-reduce job to first compile the RDD for the dataset loaded from the file
        print("-----Dataset file: " + hdfsUrl)
        rawPostsFile = sc.textFile(hdfsUrl, minPartitions=12)

        print("-----Computing partition-level MR job..")

        # Go through and derive the per term date counts - only use terms with > 5 occurrences
        daily_term_counts_rdd = rawPostsFile.flatMap(dualTermlineTokenizer)\
            .reduceByKey(terms_reducer)\
            .filter(lambda x: sum([count for count in x[1].values()]) > 5)
        daily_term_counts = daily_term_counts_rdd.collectAsMap()

        # test that this worked
        # print(str(weekly_term_counts))
        print("Term daily counts length = " + str(len(daily_term_counts)))

        # get the minimum date from the map
        dates_rdd = daily_term_counts_rdd.flatMap(lambda x: [date for date in x[1]])
        min_date = dates_rdd.min()
        max_date = dates_rdd.max()
        print("Min date = " + str(min_date))
        print("Max date = " + str(max_date))

        # work out the total number of weeks that the entire dataset covers
        time_delta = max_date - min_date
        days_delta = time_delta.days
        total_weeks = days_delta / 7
        # broadcast variables to the cluster to use within the map operations
        min_date_b = sc.broadcast(min_date)
        max_date_b = sc.broadcast(max_date)
        total_weeks_b = sc.broadcast(total_weeks)
        print("--------Start date: %s" % str(min_date_b.value))
        print("--------End date: %s" % str(max_date_b.value))
        print("--------Total weeks: %s" % str(total_weeks_b.value))

        # Go back through the per-term date counts, and derive weekly counts
        weekly_term_counts_rdd = daily_term_counts_rdd.map(terms_weekly_counts_mapper)
        weekly_term_counts = weekly_term_counts_rdd.collectAsMap()
        print ("Term weekly counts length = " + str(len(weekly_term_counts)))
        for term in weekly_term_counts:
            print(term)
            print(weekly_term_counts[term])
            break

        # Derive the weekly counts of term occurences
        weekly_counts_rdd = weekly_term_counts_rdd.flatMap(lambda x: [(week_key, x[1][week_key]) for week_key in x[1]])\
            .reduceByKey(lambda count1, count2: count1 + count2)
        weekly_counts = weekly_counts_rdd.collectAsMap()
        print("Global weekly term counts")
        print(str(weekly_counts))
        print(str(len(weekly_counts)))
        weekly_counts_b = sc.broadcast(weekly_counts)

        # Derive the relative frequencies of terms
        weekly_term_relfreqs_rdd = weekly_term_counts_rdd.map(terms_weekly_relfreqs_mapper)
        weekly_term_relfreqs = weekly_term_relfreqs_rdd.collectAsMap()
        print("Term Relative Frequencies")
        print(str(len(weekly_term_relfreqs)))
        for term in weekly_term_relfreqs:
            term_counts = weekly_term_relfreqs[term]
            print("Term = "  + term)
            for week in term_counts:
                print(str(week) + " -> " + str(term_counts[week]))
            break

        # Print results to a local file for analysis
        output_string = ""
        for term in weekly_term_relfreqs:
            term_counts = weekly_term_relfreqs[term]
            output_string += term
            for week_key in sorted(term_counts):
                output_string += "\t" + str(term_counts[week_key])
            output_string += "\n"
        f = open("../data/" + str(dataset) + "_dual_term_probs.tsv", "w")
        f.write(output_string)
        f.close()

        sc.stop()



