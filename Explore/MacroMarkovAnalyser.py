__author__ = 'mrowe'

from nltk.corpus import stopwords
from IO.Input import FileInput
from IO.Output import FileWriter
from Data import Dataset
import math
import re

class MacroAnalyser:

    def analyseWeeklyTerms(self, dataset):
        # get the first date of the first post
        start = Dataset.getFirstPost(dataset)
        end = Dataset.getLastPost(dataset,start)

        # go through week by week and segment the posts into weekly bins
        postsSegment = Dataset.segmentPosts(dataset,start, end)

        # # go through and output the term variance in each week across the posts
        print "Computing Entropy values"
        weeklyEntropy = {}
        for weekKey in postsSegment :
            # get posts for that week
            intervalPosts = postsSegment[weekKey]
            weeklyEntropy[weekKey] = self.computeSegmentEntropy(intervalPosts)

        # write the entropy values to the file
        writer = FileWriter(dataset.datasetname)
        writer.writeEntropyValueToFile(weeklyEntropy, "entropy")


        # go through and compute the consecutive week conditional entropy
        print "Computing Conditonal Entropy values"
        weeklyCrossEntropy = {}
        afterFirst = False
        pastWeekPosts = []
        for weekKey in postsSegment :
            # get the current week's posts
            intervalPosts = postsSegment[weekKey]

            if afterFirst :
                weeklyCrossEntropy[weekKey] = self.computeSegmentConditionalEntropy(pastWeekPosts,intervalPosts)
            else :
                afterFirst = True

            pastWeekPosts = intervalPosts

        # write the conditional entropy values to the file
        writer.writeEntropyValueToFile(weeklyCrossEntropy, "cond-entropy")

    def computeSegmentEntropy(self, posts):

        # Get the relative frequency distribution of the posts
        relFreqDist = self.deriveRelativeFrequencyDist(posts)

        # compute the entropy of this distribution
        entropy = 0
        for term in relFreqDist:
            prob = relFreqDist[term]
            entropy += prob * math.log10(prob)
        if entropy < 0 :
            entropy *= -1
        return entropy

    def computeSegmentConditionalEntropy(self, postsA, postsB):
        # Get the relative frequency distribution of the posts
        relFreqDistA = self.deriveRelativeFrequencyDist(postsA)
        relFreqDistB = self.deriveRelativeFrequencyDist(postsB)

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


    # Assumes a first order markov model
    def deriveRelativeFrequencyDist(self, posts):
        # remove stop words from all posts
        cachedStopWords = stopwords.words("english")
        content_trans = ''.join(chr(c) if chr(c).isupper() or chr(c).islower() else '' for c in range(256))

        termCounts = {}

        # go through and process each post
        for post in posts :
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
datasetName = "boards"

# Get the dataset
fileInput = FileInput(datasetName)
dataset = fileInput.readfromfile()

# Analyse the dataset
analyser = MacroAnalyser()
analyser.analyseWeeklyTerms(dataset)





