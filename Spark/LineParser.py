__author__ = 'rowem'

from datetime import datetime
import json
from Dataset import Dataset
from Post import Post

class LineParser(object):

    @staticmethod
    def parseFacebookLine(line):
        # initialise the dataset
#        dataset = Dataset(datasetName)

        # Try with post sets for now
        posts = []
        try:

            toks = line.split("\t")
            userid = toks[2]
            postid = toks[0]
            forumid = toks[1]
            content = toks[3]

            # Date format: 2012-09-12 22:59:25 = '%Y-%m-%d %H:%M:%S'
            # 'Jun 1 2005  1:33PM', '%b %d %Y %I:%M%p'
            # print row[4]
            date = datetime.strptime(toks[4], '%Y-%m-%d %H:%M:%S')
            # build a post
            post = Post(userid, postid, forumid, date)
            post.addContent(content)
            posts.add(post)
#            dataset.addpost(post)
        except:
            pass
        return posts

    @staticmethod
    def parseBoardsLine(line, datasetName):
        # initialise the dataset
        dataset = Dataset(datasetName)
        try:
            toks = line.split("\t")
            userid = toks[2]
            postid = toks[0]
            forumid = toks[1]
            content = toks[5]
            # Date format: 2001-04-24 16:49:00 = '%Y-%m-%d %H:%M:%S'
            # 'Jun 1 2005  1:33PM', '%b %d %Y %I:%M%p'
            date = datetime.strptime(toks[4], '%Y-%m-%d %H:%M:%S')
            # build a post
            post = Post(userid, postid, forumid, date)
            post.addContent(content)
            dataset.addpost(post)
        except:
             pass
        return dataset

    @staticmethod
    def parseRedditLine(line, datasetName):
        # initialise the dataset
        dataset = Dataset(datasetName)

        # load the json object from the line
        j = json.loads(line)

        userid = j['author']
        postid = j['id']
        forumid = j['subreddit_id']

        # get the content and clean it
        content = j['body']
        content = content.replace("\t", "")
        content = content.replace("\n", "")

        date = datetime.strptime(j['created_utc'], '%Y-%m-%d %H:%M:%S')

        post = Post(userid, postid, forumid, date)
        post.addContent(content)
        dataset.addpost(post)

        return dataset

    @staticmethod
    def parseTwitterLine(line, datasetName):
        # initialise the dataset
        dataset = Dataset(datasetName)

        # load the json object from the line
        j = json.loads(line)

        userid = j['user']['id']
        postid = j['id']
        forumid = str(j['coordinates'][0]) + "-" + str(j['coordinates'][1])

        # get the content and clean it
        content = j['text']
        content = content.replace("\t", "")
        content = content.replace("\n", "")

        ### Wed Aug 20 21:51:49 +0000 2014
        date = datetime.strptime(j['created_at'], '%a %b %d %H:%M:%S %z %Y')

        post = Post(userid, postid, forumid, date)
        post.addContent(content)
        dataset.addpost(post)

        return dataset

