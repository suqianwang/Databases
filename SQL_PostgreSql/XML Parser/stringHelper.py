class StringHelper():

	def __init__(self):
		self.str = ""

	def concatString(self, list):
		self.str=""
		for i in list:
			self.str = self.str + i
		return self.str
