import csv
from Data.Dataset import Dataset
from Data.Post import Post
from datetime import datetime

__author__ = 'mrowe'

# For reading from a file
class FileInput:
    def __init__(self, datasetname):
        self.datasetname = datasetname
        self.dataset = Dataset(datasetname)

    def readfromfile(self, version):
        print "Reading in dataset from local file: " + self.datasetname
        # populate the dataset from the local tsv file
        if self.datasetname == "facebook":
            self.readFacebookPosts(version)
        elif  self.datasetname == "boards":
            self.readBoardsPosts(version)

        return self.dataset


    def readFacebookPosts(self, version):
         filepath = "../../data/diffusion/facebook-posts"
         filepath += ".tsv"

         with open(filepath, 'rb') as tsvin:
             tsvReader = csv.reader(tsvin, delimiter="\t")
             # skip the first row
             next(tsvReader, None)
             # count = 1

             for row in tsvReader:
                 # print row
                 # print count
                 # count += 1

                 userid = row[2]
                 postid = row[0]
                 forumid = row[1]
                 content = row[3]
                 try:
                    # Date format: 2012-09-12 22:59:25 = '%Y-%m-%d %H:%M:%S'
                    # 'Jun 1 2005  1:33PM', '%b %d %Y %I:%M%p'
                    # print row[4]
                    date = datetime.strptime(row[4], '%Y-%m-%d %H:%M:%S')
                    # build a post
                    post = Post(userid, postid, forumid, date)
                    post.addContent(content)
                    self.dataset.addpost(post)
                 except:
                     pass


    def readBoardsPosts(self, version):
        filepath = "../../data/diffusion/boards-posts"
        filepath += ".tsv"

        with open(filepath, 'rbU') as tsvin:
             tsvReader = csv.reader(tsvin, delimiter='\t')
             # skip the first row
             next(tsvReader, None)
             for row in tsvReader:

                # print row
                try:
                    userid = row[2]
                    postid = row[0]
                    # This is actually threadid (but we assume that the users will see the same thing)
                    forumid = row[1]
                    content = row[5]
                    # Date format: 2001-04-24 16:49:00 = '%Y-%m-%d %H:%M:%S'
                    # 'Jun 1 2005  1:33PM', '%b %d %Y %I:%M%p'
                    date = datetime.strptime(row[4], '%Y-%m-%d %H:%M:%S')
                    # build a post
                    post = Post(userid, postid, forumid, date)
                    post.addContent(content)
                    self.dataset.addpost(post)
                except:
                     pass


# Test code
# dataset_name = "facebook"
# fileinput = FileInput(dataset_name)
# fileinput.readfromfile(dataset_name)
# print fileinput.dataset


