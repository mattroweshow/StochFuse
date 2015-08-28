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
        return self.author + " | " + str(self.postid) + " | " + str(self.forumid) + " | " + str(self.date) + " | " + self.content


    def addContent(self, content):
        self.content = content


    # post_id group_id        user_id message created_time
    def toTSVString(self):
        return str(self.postid) + "\t" \
               + str(self.forumid) + "\t" \
               + str(self.author) + "\t" \
               + str(self.content.encode("utf-8")) + "\t" \
               + str(self.date)

