__author__ = 'mrowe'

from Data.Dataset import Dataset
from Data.Post import Post
from IO.Input import FileInput
from IO.Output import FileWriter
from nltk.corpus import stopwords
from nltk.stem import *
from nltk.tokenize import RegexpTokenizer

def removeStopWords(dataset):
    newDataset = Dataset(dataset.datasetname)
    cachedStopWords = stopwords.words("english")

    # Add to the new dataset if the terms are not in the stopwords list
    for post in dataset.posts:
        newMessage = ""
        for term in post.content.lower().split():
            if term not in cachedStopWords:
                newMessage += term + " "

        # Check that the new message is not just blank
        if len(newMessage) > 0:
            # print newMessage
            newPost = Post(post.author,post.postid, post.forumid, post.date)
            newPost.addContent(newMessage)
            newDataset.addpost(newPost)

    return newDataset


def removeSparseTerms(dataset, minFreq):

    newDataset = Dataset(dataset.datasetname)
    tokenizer = RegexpTokenizer(r'\w+')

    # Generate a count of the number of terms in the dataset
    termCount = {}
    for post in dataset.posts:
        terms = tokenizer.tokenize(post.content.lower())
        for term in terms:
            # map the term to its frequency
            if term in termCount :
                tally = termCount[term]
                termCount[term] = tally + 1
            else:
                termCount[term] = 1

    # Add to the new dataset the messages for which we have more that the minFreq of terms
    for post in dataset.posts:
        newMessage = ""
        terms = tokenizer.tokenize(post.content.lower())
        for term in terms:
            if termCount[term] >= minFreq:
                # Create the new post
                newMessage += term + " "

        # Check that the new message is not just blank
        if len(newMessage) > 0:
            newPost = Post(post.author,post.postid, post.forumid, post.date)
            newPost.addContent(newMessage)
            newDataset.addpost(newPost)

    return newDataset


def stemWords(dataset):
    newDataset = Dataset(dataset.datasetname)
    stemmer = PorterStemmer()

     # Add to the new dataset if the terms are not in the stopwords list
    for post in dataset.posts:
        newMessage = ""
        for term in post.content.lower().split():
            try:
                stem = stemmer.stem(term)
                # print "Term = " + term + " | Stem = " + stem
                newMessage += stem + " "
            except:
                pass

        # Check that the new message is not just blank
        if len(newMessage) > 0:
            newPost = Post(post.author,post.postid, post.forumid, post.date)
            newPost.addContent(newMessage)
            newDataset.addpost(newPost)
    return newDataset


def calculateTermDimensionality(dataset):
    # print str(len(dataset.posts))
    uniqueTerms = set()

    for post in dataset.posts:
        for term in post.content.lower().split():
            uniqueTerms.add(term)

    return len(uniqueTerms)


#### Execution commands
# datasetName = "facebook"
datasetName = "boards"

# Get the dataset
version = ""
fileInput = FileInput(datasetName)
dataset = fileInput.readfromfile(version)
print "Original Dataset Number of Posts = " + str(len(dataset.posts))
print "# terms = " + str(calculateTermDimensionality(dataset))

# Apply the pre-processing
# 1. Remove Sparse Terms
minFreq = 5
newDataset = removeSparseTerms(dataset, minFreq)
print "# Posts after removing sparse terms = " + str(len(newDataset.posts))
print "# terms = " + str(calculateTermDimensionality(newDataset))

# 2. Remove Stopwords
newDataset2 = removeStopWords(newDataset)
print "# Posts after removing stop words = " + str(len(newDataset2.posts))
print "# terms = " + str(calculateTermDimensionality(newDataset2))

# 3. Perform stemming
newDataset3 = stemWords(newDataset2)
print "# Posts after stemming = " + str(len(newDataset3.posts))
print "# terms = " + str(calculateTermDimensionality(newDataset3))

# Write to the file
newVersion = "cleaned"
fileWriter = FileWriter(datasetName)
fileWriter.writeDatasetToFile(newDataset3, newVersion)