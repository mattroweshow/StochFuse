import csv
from Data.Dataset import Dataset

__author__ = 'mrowe'

class FileWriter:

    def __init__(self, datasetname):
        self.datasetname = datasetname

    def writeEntropyValueToFile(self, weeklyEntropies, entropyType):
        print "Writing " + entropyType + " values to a local file"
        file = open("../logs/" + self.datasetname + "-" + entropyType +".tsv", "w")
        file.write("Week\t" + entropyType + "\n")
        for week in weeklyEntropies:
            try:
                file.write(str(week) + "\t" + str(weeklyEntropies[week]) + "\n")
            except UnicodeEncodeError:
                print "UnicodeEncodeError"
        file.close()

    def writeUserEntropyValuesToFile(self, userWeeklyEntropies, entropyType):
        print "Writing " + entropyType + " values to a local file"
        file = open("../logs/" + self.datasetname + "-" + entropyType +".tsv", "w")
        # Write each user's weekly entropy values
        for user in userWeeklyEntropies:
            line = user
            for week in userWeeklyEntropies[user]:
                line += "\t" + str(userWeeklyEntropies[user][week])
            try:
                file.write(line + "\n")
            except UnicodeEncodeError:
                print "UnicodeEncodeError"
        file.close()


    def writeDatasetToFile(self, dataset, version):
        pathToDataDir = "../../data/diffusion/"

        print "Writing the updated dataset for : " + dataset.datasetname + " | Based on version: " + version
        file = open(pathToDataDir + self.datasetname + "-posts-" + version +".tsv", "w")
        # Write each post to the file
        for post in dataset.posts:
            try:
                file.write(post.toTSVString() + "\n")
            except:
                print "Error Writing"
        file.close()
