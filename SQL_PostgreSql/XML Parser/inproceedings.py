class Inproceedings():

    def __init__(self):
        self.key = ""
        self.title = ""
        self.booktitle = ""
        self.year = 0 
        self.authors = []
    
    def setKey(self, key):
        self.key = key
    
    def setTitle(self, title):
        self.title = title
        
    def setBooktitle(self, booktitle):
        self.booktitle = booktitle
        
    def setYear(self, year):
        self.year = year

    def addAuthors(self, author):
        self.authors.append(author)

    def reset(self):
        self.key = ""
        self.title = ""
        self.booktitle = ""
        self.year = 0
        self.authors=[]

    # for debugging purpose
    def getAuthors(self):
        return self.authors
        
    def printInproceeding(self):
        print("Key: " + self.key + "\nTitle: " + self.title + "\nBooktitle: " + self.booktitle + "\nYear: " + self.year)     
