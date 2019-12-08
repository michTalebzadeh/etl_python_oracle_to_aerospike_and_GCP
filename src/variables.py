#! /usr/bin/env python
# oracle variables
driverName = "oracle.jdbc.OracleDriver"
_username = "scratchpad"
_password = "oracle"
_dbschema = "SCRATCHPAD"
_dbtable = "DUMMY"
dump_dir = "/backup/hduser/"
filename = 'DUMMY.csv'
oracleHost = 'rhes564'
oraclePort = '1521'
oracleDB = 'mydb12'
url= "jdbc:oracle:thin:@"+oracleHost+":"+oraclePort+":"+oracleDB
# aerospike variables
dbHost = "rhes75"
dbPort = 3000
dbConnection = "mich"
namespace = "test"
dbPassword = "aerospike"
dbSet = "oracletoaerospike2"
dbKey = "ID"

# GCP variables
projectname = 'GCP First Project'
bucketname = 'etcbucket'
dataset = 'test'
bqTable = 'DUMMY' 
bqFields = 99
bqRows = 1000 

# GCP table schema
col_names = ['ID', 'CLUSTERED', 'SCATTERED','RANDOMISED', 'RANDOM_STRING', 'SMALL_VC', 'PADDING']
col_types = ['FLOAT', 'FLOAT', 'FLOAT', 'FLOAT', 'STRING', 'STRING', 'STRING']
col_modes = ['REQUIRED', 'NULLABLE', 'NULLABLE', 'NULLABLE', 'NULLABLE', 'NULLABLE', 'NULLABLE']
