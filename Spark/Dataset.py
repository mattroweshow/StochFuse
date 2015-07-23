__author__ = 'mrowe'

from datetime import datetime, timedelta

class Dataset:

    def __init__(self, datasetname):
        self.datasetname = datasetname
        self.posts = []

    def addpost(self,post):
        self.posts.append(post)

    def __str__(self):
        return str(self.posts.__len__())

    def merge_posts(self, dataset2):
        self.posts = self.posts + dataset2.posts


def getFirstPost(dataset):
    minDate = datetime.now()
    for post in dataset.posts :
        if post.date < minDate :
            minDate = post.date
    return minDate

def getLastPost(dataset, minDate):
    maxDate = minDate
    for post in dataset.posts :
        if post.date > maxDate :
            maxDate = post.date
    return maxDate

def segmentPosts(dataset, startDate, endDate):
    print "Determning weekly segments of posts"
    timeDelta = endDate - startDate
    daysDelta = timeDelta.days
    totalWeeks = daysDelta / 7

    postsSegment = {}

    for i in range(1, totalWeeks) :
        # print "Processing week: " + str(i)
        # get the start of the window
        startWindow = startDate
        weeksDelta = timedelta(days = 7)
        endWindow = startWindow + weeksDelta

        # print str(startWindow) + " -> " + str(endWindow)

        # get all posts within the interval
        intervalPosts = []
        for post in dataset.posts :
            if post.date >= startWindow and post.date < endWindow :
                intervalPosts.append(post)

        # map the interval posts to the week they appear
        postsSegment[i] = intervalPosts

        # knock on the start date by one week
        startDate = endWindow

    return postsSegment