from dbConnector import DBConnector


class Author():
	
	def __init__(self):
		self.key = ""

	def setKey(self, key):
		self.key = key
	
	def setAuthor(self, key, list, dbConnection):
		for author in list:
			dbConnection.insertAuthorship(key, author)
                
