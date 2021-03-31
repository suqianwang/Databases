class Article():

    def __init__(self):
        self.key = ""
        self.title = ""
        self.journal = ""
        self.year = 0
        self.authors = []
    
    def setKey(self, key):
        self.key = key
    
    def setTitle(self, title):
        self.title = title
        
    def setJournal(self, journal):
        self.journal = journal
        
    def setYear(self, year):
        self.year = year

    def addAuthors(self, author):
        self.authors.append(author)

    def reset(self):
        self.key = ""
        self.title = ""
        self.journal = ""
        self.year = 0 
        self.authors=[]

    # for debugging purpose
    def getAuthor(self):
        return self.authors

    def printArticle(self):
        print("Key: " + self.key + "\nTitle: " + self.title + "\nJournal: " + self.journal + "\nYear: " + self.year)
        
