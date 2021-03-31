import sys
import psycopg2
from psycopg2.sql import NULL

from article import Article
from inproceedings import Inproceedings


class DBConnector(object):

    def __init__(self):
        self.conn = None
        self.user = "dblpuser"
        self.database = "dblp"
        self.password = "password"
        self.articleCount = 0
        self.inproceedingsCount = 0
        self.authorshipCount = 0
        
    def connect(self):
        try:
            connectString = "dbname = "+ self.database+ " user = "+ self.user+ " password="+ self.password
            self.conn = psycopg2.connect(connectString)

        except(Exception, psycopg2.DatabaseError) as error:
            print("Error: ", error)

    def reset(self):
        try:
            cur = self.conn.cursor()
            cur.execute("DROP TABLE IF EXISTS Article")
            cur.execute("DROP TABLE IF EXISTS Inproceedings")
            cur.execute("DROP TABLE IF EXISTS Authorship")
        
        except(Exception, psycopg2.DatabaseError) as error:
            print("Error: ", error)

    def createTables(self):
        commands = [
            """
            CREATE TABLE IF NOT EXISTS Article (
            pubkey TEXT NOT NULL PRIMARY KEY,
            title TEXT,
            journal TEXT,
            year INT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS Inproceedings (
            pubkey TEXT NOT NULL PRIMARY KEY,
            title TEXT,
            booktitle TEXT,
            year INT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS Authorship (
            pubkey TEXT NOT NULL,
            author TEXT NOT NULL,
            PRIMARY KEY(pubkey, author)
            )
            """
            ]

        try:
            cur = self.conn.cursor()
            for command in commands:
                cur.execute(command)

        except(Exception, psycopg2.DatabaseError) as error:
            print("Error: ", error)

    def setup(self):
        self.connect()
        self.reset()
        self.createTables()
        self.conn.commit()
                
    def insertArticle(self, article):
        if self.articleCount == 100000:
            self.conn.commit() 
            self.articleCount = 0  
        query = "INSERT INTO Article(pubkey, title, journal , year) values(%s, %s, %s, %s) ON CONFLICT (pubkey) DO NOTHING;"
        try:         
            cur = self.conn.cursor()
            cur.execute(query,(article.key, article.title, article.journal, article.year))
            self.articleCount += 1
        except(Exception, psycopg2.DatabaseError) as error:
            print("Error: insertArticle - ", error)
            sys.exit(0)

    def insertInproceedings(self, inproceedings):
        if self.inproceedingsCount == 100000:
            self.conn.commit() 
            self.inproceedingsCount = 0
        query = "INSERT INTO Inproceedings (pubkey, title, booktitle , year) values(%s, %s, %s, %s) ON CONFLICT (pubkey) DO NOTHING;"  
        try:         
            cur = self.conn.cursor()
            cur.execute(query, (inproceedings.key, inproceedings.title, inproceedings.booktitle, inproceedings.year))
            self.inproceedingsCount += 1
        except(Exception, psycopg2.DatabaseError) as error:
            print("Error: insertInproceeding - ", error)  
            print(inproceedings.printInproceeding())
            sys.exit(0)


    def insertAuthorship(self, key, author):
#         print(key + " " + author)
        if self.authorshipCount == 100000:
            self.conn.commit() 
            self.authorshipCount = 0
        query = "INSERT INTO Authorship(pubkey, author) values(%s, %s) ON CONFLICT (pubkey, author) DO NOTHING;"  
        try:         
            cur = self.conn.cursor()
            cur.execute(query, (key, author))
            self.authorshipCount += 1
        except(Exception, psycopg2.DatabaseError) as error:
            print("Error: insertAuthorship - ", error," ", query)
            print("key = ", key, "\nauthor= ", author)
            sys.exit()

    def disconnect(self):
        if self.conn is not NULL:
            self.conn.commit()
            self.conn.close()

