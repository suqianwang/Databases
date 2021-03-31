#!/usr/bin/python

import time
import xml.sax
import sys
from article import Article
from author import Author
from dbConnector import DBConnector
from inproceedings import Inproceedings
from stringHelper import StringHelper

class DBLPHandler(xml.sax.ContentHandler):
	
	def __init__(self):
		# class attributes
		self.CurrentData = ""
		self.key = ""
		self.author = ""
		self.title = ""
		self.journal = ""
		self.booktitle = ""
		self.year = 0 
		self.type = ""
		
		# class objects
		self.article = Article()
		self.inproceedings = Inproceedings()
		self.authorObject = Author()
		
		# help with concat strings
		self.strhelper = StringHelper()
		self.authorStringList = []
		self.titleStringList = []
		self.journalStringList = []
		self.booktitleStringList = []
		
		# 
		self.dbConnector = DBConnector
	
	def resetList(self):
	    self.authorStringList = []
	    self.titleStringList = []
	    self.journalStringList = []
	    self.booktitleStringList = []
	
	# Call when an element starts
	def startElement(self, tag, attributes):
            # errorKey = "dblpnote"
	    self.CurrentData = tag
	    if tag == "article":
	        key = attributes["key"]
            # if key.find(errorKey) == -1:
                self.type = tag
                self.key = key
                self.article.setKey(key)
                self.authorObject.setKey(key)
            # else:
                # self.type = ""

	    if tag == "inproceedings":
	        key = attributes["key"]
                # if key.find(errorKey) == -1: 
                self.type = tag
                self.key = key
                self.inproceedings.setKey(key)
                self.authorObject.setKey(key)
                # else:
                #     self.type = ""
	
	# Call when an elements ends
	def endElement(self, tag):
		if self.type == "article":
			if self.CurrentData == "author":
			    self.article.addAuthors(self.author)
                            self.author=""
			elif self.CurrentData == "title":
			    self.article.setTitle(self.title)
                            self.title=""
			elif self.CurrentData == "year":
			    self.article.setYear(self.year)
                            self.year = 0
			elif self.CurrentData == "journal":
			    self.article.setJournal(self.journal)
                            self.journal=""
			self.CurrentData = ""
	
		if self.type == "inproceedings":
			if self.CurrentData == "author":
			    self.inproceedings.addAuthors(self.author)
                            self.author=""
			elif self.CurrentData == "title":
			    self.inproceedings.setTitle(self.title)
                            self.title=""
			elif self.CurrentData == "year":
			    self.inproceedings.setYear(self.year)
                            self.year =0
			elif self.CurrentData == "booktitle":
			    self.inproceedings.setBooktitle(self.booktitle)
                            self.booktitle=""
			self.CurrentData = ""
	        
                if tag == "article":
             # and self.key.find("dblpnote") == -1:
	                DBConnector.insertArticle(self.article)
                        self.authorObject.setAuthor(self.article.key, self.article.authors, self.dbConnector)
	                self.article.reset()
                        self.type=""
                elif tag == "inproceedings":
             # and self.key.find("dblpnote") == -1: 
	                DBConnector.insertInproceedings(self.inproceedings)
	                self.authorObject.setAuthor(self.inproceedings.key, self.inproceedings.authors, self.dbConnector)
	                self.inproceedings.reset()
                        self.type=""
	        self.resetList()
	
	# Call when a character is read
	def characters(self, content):
	    if (self.type == "article" or self.type == "inproceedings"):
	        if self.CurrentData == "author":
	            self.authorStringList.append(content.strip())
	            self.author = self.strhelper.concatString(self.authorStringList)
	        elif self.CurrentData == "title":
	            self.titleStringList.append(content.strip())
	            self.title = self.strhelper.concatString(self.titleStringList)
	        elif self.CurrentData == "year":
	            self.year = content.strip()
	        elif self.CurrentData == "journal":
	            self.journalStringList.append(content.strip())
	            self.journal = self.strhelper.concatString(self.journalStringList)
	        elif self.CurrentData == "booktitle":
	            self.booktitleStringList.append(content.strip())
	            self.booktitle = self.strhelper.concatString(self.booktitleStringList)
    

if (__name__ == "__main__"):
   
    start = time.time()
    # create an XMLReader
    parser = xml.sax.make_parser()
    # turn off namepsaces
    parser.setFeature(xml.sax.handler.feature_namespaces, 0)

    # setup connection to database and tables
    DBConnector = DBConnector()
    DBConnector.setup()

    # override the default ContextHandler
    Handler = DBLPHandler()
    parser.setContentHandler(Handler)

    # parsing xml file
    parser.parse("dblp-2019-09-05.xml")
    #parser.parse("dblpTest.xml")


    # close connection to database
    DBConnector.disconnect()
    print(time.time() - start)
    
# https://www.tutorialspoint.com/python/python_xml_processing.htm
    
