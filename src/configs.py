#! /usr/bin/env python
import variables as v
import jaydebeapi
import aerospike
from aerospike import exception as ex
import json
import google
from google.cloud import storage
from google.oauth2 import service_account

# aerospike
write_policy = {'key': aerospike.POLICY_KEY_SEND}
policies = {'write': write_policy, 'total_timeout': 1000}
config = {
  'hosts': [(v.dbHost, v.dbPort)],
  'policies': policies
}
client = aerospike.client(config).connect(v.dbConnection, v.dbPassword)
# oracle stuff
connection = jaydebeapi.connect(v.driverName, v.url, [v._username, v._password])
metadata = connection.jconn.getMetaData()
rs = metadata.getTables(None, v._dbschema, v._dbtable, None)
cursor = connection.cursor()
sql="SELECT ID, CLUSTERED, SCATTERED, RANDOMISED, RANDOM_STRING, SMALL_VC, PADDING FROM " + v._dbschema + "." + v._dbtable + " WHERE ROWNUM <= 10000"

