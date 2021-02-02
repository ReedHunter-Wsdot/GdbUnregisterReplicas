

import arcpy
import configparser
import os
import pandas
import sqlalchemy
import pyodbc
import urllib



class GdbReplicasUnregistrator:

    def create_pyodbc_connection_string(self, server: str, database: str, user: str, pwid: str) -> str:
        """Creates pyodbc connection string
        """
        if not server or not database:
            raise ValueError("server and database parameters must be provided")

        conn_string_parts = {
            "DRIVER": r"{ODBC Driver 17 for SQL Server}",
            "Server": server,
            "Database": database,
            "UID": user,
            "PWD" : pwid,
            "Trusted_Connection": "no",
            "applicationintent": "readonly"
        }

        # Construct the conneciton string from the dict.
        conn_string = ";".join([
            "{}={}".format(key, value)
            for key, value in conn_string_parts.items()
        ])
        return conn_string



    def __init__(self):
        self.thisCwd = os.getcwd()
        self.config = configparser.ConfigParser()
        self.config.read('UnregisterGdbReplicas.ini')
        qaOrProd = self.config.get('WhichGeodatabase','qaOrProd',fallback='QA')
        self.geodatabase = self.thisCwd+ "\\resources\\" + qaOrProd + "\\gdbWithReplicas_" + qaOrProd + ".sde"
        self.sqlConnServer = self.config.get(qaOrProd, 'sqlConnServer')
        self.sqlConnDatabase = self.config.get(qaOrProd, 'sqlConnDatabase')
        self.sqlConnUser = self.config.get(qaOrProd, 'sqlConnUser')
        self.sqlConnPwid = self.config.get(qaOrProd, 'sqlConnPwid')
        sqlConnString = self.create_pyodbc_connection_string(self.sqlConnServer, self.sqlConnDatabase, self.sqlConnUser, self.sqlConnPwid)
        sqlConnString = urllib.parse.quote_plus(sqlConnString)
        self.sqlConnString = "mssql+pyodbc:///?odbc_connect={}".format(sqlConnString)

        self.serviceUrl = self.config.get(qaOrProd,'serviceUrl')
        self.replicaCount = self.config.get('Default', 'replicaCount')

        self.destinationSde = self.thisCwd + '\\' + self.config.get(qaOrProd,'destinationSde')


    def unregisterReplicas(self):
        for replicaUuidValue in self.replicaItemsDf.to_numpy():

            arcpy.UnregisterReplica_management(self.destinationSde, replicaUuidValue[0])



    def getReplicasForProcessing(self, replicaCount: int):
        """Extract subset of GDB_ITEMS replicas that will be unregistered.
        """        
        #rather than unregister all the replicas for a given map services by passing
        # '*' to the replicaId parameter of the unregister function, this method
        # will set aside some X number of records for unregistration by first
        # copying them from the GDB_ITEMS table to a temp table.  

        query1  = 'IF OBJECT_ID(\'' + self.sqlConnDatabase + '.#TMP_GDB_ITEMS\') IS NOT NULL '
        query1 += 'DROP TABLE [' + self.sqlConnDatabase + '].#TMP_GDB_ITEMS;'

        query2  =  'SELECT TOP(' + replicaCount + ') * INTO [' + self.sqlConnDatabase + '].#TMP_GDB_ITEMS '
        query2 += 'FROM [' + self.sqlConnDatabase + '].[dbo].[GDB_ITEMS] '
        query2 += 'WHERE [Type] = \'5B966567-FB87-4DDE-938B-B4B37423539D\''
        query2 += 'AND [DatasetInfo1] like \'%%' + self.serviceUrl + '%%\''

        engine = sqlalchemy.create_engine(self.sqlConnString)

        self.dbConnection = engine.connect()
        with self.dbConnection.begin():
            self.dbConnection.execute(query1)
            self.dbConnection.execute(query2)


    def acquireReplicaIdsToRetire(self):
        """ Creates a pandas df member listing the gdb_items to be retired
        """

        query = 'SELECT [UUID],[DatasetInfo1],[Definition],[Type] from [DBO].[' + self.sqlConnDatabase + '].#TMP_GDB_ITEMS'
        self.replicaItemsDf = pandas.read_sql_query(query, self.dbConnection)
        replicaIdsList = ""
        isFirstElement = True
        for replicaUuidValue in self.replicaItemsDf.to_numpy():
            if (isFirstElement):
                replicaIdsList = '\'' + replicaUuidValue[0] + '\''
                isFirstElement = False
            else:
                replicaIdsList = replicaIdsList + ',\'' + replicaUuidValue[0] + '\''
        self.replicaIdsList = replicaIdsList




    def main(self):
        #update some X number of outdated replica records
        self.getReplicasForProcessing(self.replicaCount)
        self.acquireReplicaIdsToRetire()
        self.unregisterReplicas()
        #output the replica ids that were retired, 
        print(self.replicaIdsList) 

if __name__ == "__main__":
    objUnregReplicas = GdbReplicasUnregistrator()
    objUnregReplicas.main()



