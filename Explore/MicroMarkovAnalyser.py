__author__ = 'mrowe'

from nltk.corpus import stopwords
import math
import re
from Output import FileWriter
from Input import FileInput
from Data import Dataset

class UserAnalyser:

    def __init__(self, minPosts):
        self.minPosts = minPosts


    def analyseWeeklyTerms(self, dataset):
        # get the first date of the first post
        start = Dataset.getFirstPost(dataset)
        end = Dataset.getLastPost(dataset, start)

        # go through week by week and segment the posts into weekly bins
        postsSegment = Dataset.segmentPosts(dataset, start, end)

        # get the users
        users = self.getUsers(dataset)

        # get the entropy for each user
        print "Computing per user weekly entropies"
        userWeeklyEntropies = {}
        for user in users:
            # print user
            weeklyEntropies = {}
            for weekKey in postsSegment:
                # compute entropy values
                weeklyEntropies[weekKey] = self.computeUserEntropy(postsSegment[weekKey], user)
            userWeeklyEntropies[user] = weeklyEntropies
            print weeklyEntropies

        # write the entropy values to the file
        writer = FileWriter(dataset.datasetname)
        writer.writeUserEntropyValuesToFile(userWeeklyEntropies, "user-entropy")

        # get the conditional entropy values of
        print "Computing per user weekly conditional entropies"
        userWeeklyCondEntropies = {}
        for user in users:
            weeklyCrossEntropy = {}
            afterFirst = False
            pastWeekPosts = []
            for weekKey in postsSegment :
                # get the current week's posts
                intervalPosts = postsSegment[weekKey]

                if afterFirst :
                    weeklyCrossEntropy[weekKey] = self.computeUserConditionalEntropy(pastWeekPosts, intervalPosts, user)
                else :
                    afterFirst = True

                pastWeekPosts = intervalPosts
            userWeeklyCondEntropies[user] = weeklyCrossEntropy
        writer.writeUserEntropyValuesToFile(userWeeklyEntropies, "user-cond-entropy")


    def computeUserEntropy(self, posts, user):
        # Get the relative frequency distribution of the posts
        relFreqDist = self.deriveRelativeFrequencyDist(posts, user)

        # compute the entropy of this distribution
        entropy = 0
        for term in relFreqDist:
            prob = relFreqDist[term]
            entropy += prob * math.log10(prob)
        if entropy < 0 :
            entropy *= -1
        return entropy


    def computeUserConditionalEntropy(self, postsA, postsB, user):
        # Get the relative frequency distribution of the posts
        relFreqDistA = self.deriveRelativeFrequencyDist(postsA, user)
        relFreqDistB = self.deriveRelativeFrequencyDist(postsB, user)

        # First derive the joint probability distribution
        jointProbDist = {}
        for term in relFreqDistA:
            probA = relFreqDistA[term]
            probB = 0
            if term in relFreqDistB :
                probB = relFreqDistB[term]
            condProb = (probA + probB) * 0.5
            jointProbDist[term] = condProb
        for term in relFreqDistB:
            if term not in jointProbDist:
                jointProbDist[term] = relFreqDistB[term] * 0.5

        # Now derive the conditional entropy values
        condEntropy = 0
        for term in jointProbDist:
            jointProb = jointProbDist[term]
            probA = 0
            if term in relFreqDistA:
                probA = relFreqDistA[term]

            # work out the quotient, its log, etc.
            quotient = probA / jointProb
            logQuotient = 0
            if probA != 0:
                logQuotient = math.log10(quotient)
            condEntropy += jointProb * logQuotient
        return condEntropy


    def getUsers(self, dataset):
        print "Getting users from the dataset"
        users = []
        userPostCounts = {}
        for post in dataset.posts:
            if post.author in userPostCounts :
                tally = userPostCounts[post.author]
                userPostCounts[post.author] = tally + 1
            else :
                userPostCounts[post.author] = 1

        # Only include users who posted more times that the threshold (provided a priori
        for user in userPostCounts:
            # print str(userPostCounts[user])
            if userPostCounts[user] >= self.minPosts :
                users.append(user)

        return users


    # Assumes a first order markov model
    def deriveRelativeFrequencyDist(self, posts, user):
        # remove stop words from all posts
        cachedStopWords = stopwords.words("english")
        content_trans = ''.join(chr(c) if chr(c).isupper() or chr(c).islower() else '' for c in range(256))

        termCounts = {}

        # go through and process each post
        for post in posts :
            if post.author == user :
                text = ' '.join([word for word in post.content.lower().split() if word not in cachedStopWords])
                # print text

                # tokenize text by white space
                tokens = text.split()
                for token in tokens:
                    # remove non-characters from the string
                    token = re.sub("[\W\d]", "", token.strip())
                    stem = token
                    # compile the frequency dictionary for the terms
                    if stem in termCounts :
                        tally = termCounts[stem]
                        termCounts[stem] = tally+1
                    else :
                        termCounts[stem] = 1

        ## compute the entropy of the distribution
        # determine the relative frequency distribution of the terms
        totalCount = 0
        for term in termCounts :
            totalCount += termCounts[term]

        relFreqDist = {}
        for term in termCounts :
            relFreq = float(termCounts[term]) / float(totalCount)
            # print relFreq
            relFreqDist[term] = relFreq

        return relFreqDist


# Excution code
datasetName = "facebook"
minPosts = 10

# Get the dataset
fileInput = FileInput(datasetName)
dataset = fileInput.readfromfile()

# Analyse the dataset
analyser = UserAnalyser(minPosts)
analyser.analyseWeeklyTerms(dataset)
