__author__ = 'rowem'

from datetime import datetime
import json
from Dataset import Dataset
from Post import Post
import dateutil.parser

class LineParser(object):

    @staticmethod
    def parseFacebookLine(line):
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
            posts.append(post)
#            dataset.addpost(post)
        except:
            pass
        return posts

    @staticmethod
    def parseBoardsLine(line):
        # initialise the dataset
        posts = []
        try:
            toks = line.split("\t")
            userid = toks[2]
            postid = toks[0]
            forumid = toks[1]
            content = toks[5]
            # Date format: 2001-04-24 16:49:00 = '%Y-%m-%d %H:%M:%S'
            # 'Jun 1 2005  1:33PM', '%b %d %Y %I:%M%p'
            date = datetime.strptime(toks[4], '\"%Y-%m-%d %H:%M:%S\"')
            # build a post
            post = Post(userid, postid, forumid, date)
            post.addContent(content)
            posts.append(post)
        except:
             pass
        return posts

    @staticmethod
    def parseRedditLine(line, datasetName):
        posts = []

        # load the json object from the line
        j = json.loads(line)

        userid = j['author']
        postid = j['id']
        forumid = j['subreddit_id']

        # get the content and clean it
        content = j['body']
        content = content.replace("\t", "")
        content = content.replace("\n", "")

        # handle unix timestamp long format
        date = datetime.fromtimestamp(int(j['created_utc']))

        post = Post(userid, postid, forumid, date)
        post.addContent(content)
        posts.append(post)

        return posts

    @staticmethod
    def parseTwitterLine(line, datasetName):
        posts = []

        # load the json object from the line
        j = json.loads(line)

        userid = j['user']['id']
        postid = j['id']

        # check that geo is not null
        forumid = "none"
        if "null" not in str(j['coordinates']):
            forumid = str(j['coordinates'])

        # get the content and clean it
        content = j['text']
        content = content.replace("\t", "")
        content = content.replace("\n", "")

        ### Wed Aug 20 21:51:49 +0000 2014
        ### "Sun Apr 03 20:24:49 +0000 2011"
        # date = datetime.strptime(j['created_at'], '%a %b %d %H:%M:%S %z %Y')
        date = dateutil.parser.parser(j['created_at'])

        post = Post(userid, postid, forumid, date)
        post.addContent(content)
        posts.append(post)

        return posts

