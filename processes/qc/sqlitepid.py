"""
Created on 14.11.2013

:author: tobiaskipp
"""
import sqlite3

class SqlitePid():
    """ The class is specifically designed for the use with the QC tool.
    
    Digital Object is abbreviated as DO
    The database is expected to contain a table with (location,identifier,url) entries.
    """

    def __init__(self,database_location):
        self.drop_whitelist=[]
        self.conn = sqlite3.connect(database_location)
        self.cursor=self.conn.cursor()
        self.tablename = "pidinfo"
        self.keywords = ["location","url","identifier"]
    
    def first_run(self):
        """Makes sure that if the database is empty the required table is created"""
        stmt="CREATE TABLE IF NOT EXISTS "+self.tablename+" (location text,identifier text,url text);"
        self.cursor.executescript(stmt)
        self.conn.commit()

    def add_do(self,location,identifier,url):
        """Adds a DO and its reference to the database.

        :param location: The referenced file or dataset.
        :param identifer: The string identifier for the handle system.
        :param url: The url to the DO.
        """
        self.cursor.execute("INSERT INTO "+self.tablename+" VALUES(?,?,?)",(location,identifier,url))
        self.conn.commit()

    def get_by_key_value(self,key,value):
        """ Search for database entries with the given key and value.

        :param key: A valid variable name. ["identifier","url","location"]
        :param value: The value the variable must have to be selected.
        :returns: A list of found tuples (location,identifier,url)
        """
        if(key in self.keywords):
            stmt="SELECT location,identifier,url FROM "+self.tablename+" WHERE "+key+"=?"
            self.cursor.execute(stmt,(value,))
            return self.cursor.fetchall()
        else:
            return []


    def get_identifiers(self):
        """ Search for all identifiers in the database 
        
        :returns: A list of DO identifiers.
        """
        stmt = "SELECT identifier FROM "+self.tablename+""
        self.cursor.execute(stmt)
        return self.cursor.fetchall()

    def remove_by_key_and_value(self,key,value):
        """Remove digital objects form the database with the given variable and value.

        To prevent SQLInjections a whitelist is used. If the key is not in the whitelist
        nothing is executed.

        :param key: The name of the variable. Must be one of [identifier, url, location]
        :param value: The value to match the variable with.
        """
        if(key in self.keywords):
            stmt = "DELETE FROM "+self.tablename+" WHERE "+key+"=?"
            self.cursor.execute(stmt,(value,))
            self.conn.commit()

