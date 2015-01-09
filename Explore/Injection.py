__author__ = 'mrowe'

from IO.Input import FileInput
from Data.Dataset import *
import numpy as np

class InjectionDetector:

    def __init__(self, dataset):
        self.dataset = dataset


    def detectMidPoint(self):
        minDate = getFirstPost(self.dataset)
        maxDate = getLastPost(self.dataset, minDate)

        # Work out the difference between the dates and thus the midpoint
        timeDelta = (maxDate - minDate) / 2


        midPoint = minDate + timeDelta
        return midPoint


    # Derives the global term distribution up until and including the time point
    def deriveGlobalDistribution(self, point):
        # Derive the frequency distribution of prior posts
        termCount = {}
        termIndices = {}
        termIndex = 0
        for post in self.dataset.posts:
            if post.date <= point:
                for term in post.content.lower().split():
                    # map the term to its count
                    if term in termCount:
                        tally = termCount[term]
                        termCount[term] = tally + 1
                    else:
                        termCount[term] = 1

                    # map the term to its index
                    if term not in termIndices:
                        termIndices[term] = termIndex
                        termIndex += 1

        # Initialise the datastructure for the array object
        names = ["term", "count"]
        formats = ["S20", "f8"]
        dtype = dict(names = names, formats = formats)
        dat = np.fromiter(termCount.iteritems(), dtype=dtype, count=len(termCount))
        # print(repr(dat))

        # Normalise the vector by the sum: to give the relative frequency distribution
        datNorm = dat
        datNorm['count'] = dat['count'] / sum(dat['count'])
        # print repr(datNorm)

        # Return the global relative frequency distribution
        return datNorm



    def checkDivergence(self, dataset, globDist, point):
        # Get the 1 week cutoff from the point
        intervalEnd = point + 7

        # Get the posts that are in this window
        intervalPosts = []
        for post in dataset.posts:
            if post.date > point and post.date <= intervalEnd:
                intervalPosts.append(post)

        # compare each post's information distribution to the global distribution
        for post in intervalPosts:
            # form the numpy version of the post
            postVec = self.vectoriseMessage(post)

            # work out the deviation between the vectors
            deviation = self.deviationCalculation(globDist, postVec)
            print "Deviation = " + deviation


    def vectoriseMessage(self, post):
        termCount = {}
        # count the term distribution
        for term in post.content.lower().split():
            if term in termCount:
                tally = termCount[term]
                termCount[term] = tally + 1
            else:
                termCount[term] = 1

        # form the vector of term frequencies
        names = ["term", "count"]
        formats = ["S20", "f8"]
        dtype = dict(names = names, formats = formats)
        dat = np.fromiter(termCount.iteritems(), dtype=dtype, count=len(termCount))

        # Normalise the vector by the sum: to give the relative frequency distribution
        datNorm = dat
        datNorm['count'] = dat['count'] / sum(dat['count'])
        # print repr(datNorm)

        # Return the global relative frequency distribution
        return datNorm


    def deviationCalculation(self, datA, datB):
        deviation = 0
        return deviation


#### Execution commands
datasetName = "facebook"
# datasetName = "boards"

# Get the dataset
version = "cleaned"
fileInput = FileInput(datasetName)
dataset = fileInput.readfromfile(version)

# get the midpoint
detector = InjectionDetector(dataset)
midPoint = detector.detectMidPoint()
detector.deriveGlobalDistribution(midPoint)