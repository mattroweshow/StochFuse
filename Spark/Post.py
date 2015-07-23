__author__ = 'mrowe'

class Post:

    # def __init__(self, author, postid, forumid, date, content):
    #     self.author = author
    #     self.postid = postid
    #     self.forumid = forumid
    #     self.date = date
    #     self.content = content

    def __init__(self, author, postid, forumid, date):
        self.author = author
        self.postid = postid
        self.forumid = forumid
        self.date = date

    def __str__(self):
        return self.author + " | " + self.postid + " | " + self.forumid + " | " + self.date + " | " + self.content


    def addContent(self, content):
        self.content = content


    # post_id group_id        user_id message created_time
    def toTSVString(self):
        return self.postid + "\t" + self.forumid + "\t" + self.author + "\t" + self.content + "\t" + str(self.date)

