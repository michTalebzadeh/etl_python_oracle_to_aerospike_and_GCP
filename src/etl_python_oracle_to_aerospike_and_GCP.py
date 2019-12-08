#! /usr/bin/env python
from __future__ import print_function
# jdbc stuff
import jaydebeapi
import variables as v
import configs as c
import sys
import pprint
import csv

# aerospike stuff
import aerospike
from aerospike import exception as ex

# google stuff
import google
from google.cloud import storage
from google.cloud import bigquery
import google.auth
from google import resumable_media
from google.resumable_media.requests import ChunkedDownload
from google.resumable_media.requests import Download
from google.resumable_media.requests import RawDownload
from google.resumable_media.requests import RawChunkedDownload
from google.resumable_media.requests import MultipartUpload
from google.resumable_media.requests import ResumableUpload

from operator import itemgetter

class main:

  rec = {}

  def read_oracle_table(self):
    # Check Oracle is accessible
    try:
      c.connection
    except jaydebeapi.Error as e:
      print("Error: {0} [{1}]".format(e.msg, e.code))
      sys.exit(1)
    else:
      # Check if table exists
      if (c.rs.next()):
        print("\nTable " + v._dbschema+"."+ v._dbtable + " exists\n")
        c.cursor.execute(c.sql)
        # get column descriptions
        columns = [i[0] for i in c.cursor.description]
        rows = c.cursor.fetchall()
        # write oracle data to the csv file
        csv_file = open(v.dump_dir+v.filename, mode='w')
        writer = csv.writer(csv_file, delimiter=',', lineterminator="\n", quoting=csv.QUOTE_NONNUMERIC)
        # write column headers to csv file
        writer.writerow(columns)
        for row in rows:
          writer.writerow(row)   ## write rows to csv file

        print("writing to csv file " + v.dump_dir+v.filename + " complete")
        c.cursor.close()
        c.connection.close()
        csv_file.close()
        sys.exit(0)
      else:
        print("Table " + v._dbschema+"."+ v._dbtable + " does not exist, quitting!")
        c.connection.close()
        sys.exit(1)

  def read_aerospike_set(self):
    # Check aerospike is accessible
    try:
      c.client
    except ex.ClientError as e:
      print("Error: {0} [{1}]".format(e.msg, e.code))
      sys.exit(1)
    else:
      print("Connection successful")
      keys = []
      for k in range(1,10000):
         key = (v.namespace, v.dbSet, str(k))
         keys.append(key)

      records = c.client.get_many(keys)
      pprint.PrettyPrinter(depth=4).pprint (records)
      print("\nget everyting for one record with pk = '9'")
      (key, meta, bins)= c.client.get((v.namespace, v.dbSet, '9'))
      print (key)
      print (meta)
      print (bins)
      c.client.close()
      sys.exit(0)

  def write_aerospike_set(self):
    # Check aerospike is accessible
    try:
      c.client
    except ex.ClientError as e:
      print("Error: {0} [{1}]".format(e.msg, e.code))
      sys.exit(1)
    else:
      print("Connection to aerospike successful")
      rec = {}
      # read from csv file
      csv_file = open(v.dump_dir+v.filename, mode='r')
      reader = csv.reader(csv_file, delimiter=',')
      rownum = 0
      for row in reader:
        if rownum == 0:
          header = row
        else:
          column = 0
          for col in row:
            # print (rownum,header[colnum],col)
            rec[header[column]] = col
            column += 1
        rownum += 1
        #print(rownum, rec)
        if rec:
          c.client.put((v.namespace, v.dbSet, str(rownum)), rec)
        rec = {}
      print("writing to aerospike set complete")
      csv_file.close()
      c.client.close()
      sys.exit(0)

  def drop_if_bqTable_exists(self):
    from google.cloud.exceptions import NotFound
    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset(v.dataset)
    table_ref = dataset_ref.table(v.bqTable)
    try:
      bigquery_client.get_table(table_ref)
    except NotFound:
      print('table ' + v.bqTable + ' does not exist')
      return False
    try:
      print('table ' + v.bqTable + ' exists, dropping it')
      bigquery_client.delete_table(table_ref) 
      return True
    except:
      print('Error deleting table ' + v.bqTable)
      sys.exit(1)

  def bq_create_table(self):
    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset(v.dataset)
    table_ref = dataset_ref.table(v.bqTable)
    schema = [
        bigquery.SchemaField(v.col_names[0], v.col_types[0], v.col_modes[0])
      , bigquery.SchemaField(v.col_names[1], v.col_types[1], v.col_modes[1])
      , bigquery.SchemaField(v.col_names[2], v.col_types[2], v.col_modes[2])
      , bigquery.SchemaField(v.col_names[3], v.col_types[3], v.col_modes[3])
      , bigquery.SchemaField(v.col_names[4], v.col_types[4], v.col_modes[4])
      , bigquery.SchemaField(v.col_names[5], v.col_types[5], v.col_modes[5])
      , bigquery.SchemaField(v.col_names[6], v.col_types[6], v.col_modes[6])
    ]
    table = bigquery.Table(table_ref, schema=schema)
    table = bigquery_client.create_table(table)
    print('table {} created.'.format(table.table_id))

  def delete_blob_if_exists_and_upload_to_GCP(self):
    credentials, _ = google.auth.default()
    storage_client = storage.Client(v.projectname, credentials=credentials)
    bucket = storage_client.get_bucket(v.bucketname)
    Exists = bucket.blob(v.filename).exists()
    if(Exists):
      try:
        print('file gs://' + v.bucketname + '/' + v.filename + ' exists, deleting it before uploading again')
        ## gsutil rm -r gs://etcbucket/DUMMY.csv
        bucket.blob(v.filename).delete()
        print('The file gs://' + v.bucketname + '/' + v.filename + ' deleted')
      except Exception as e:
        print("Error: {0} [{1}]".format(e.msg, e.code))
        sys.exit(1)
    else:
        print('The file gs://' + v.bucketname + '/' + v.filename + ' does not exist')

    # upload blob again
    print('uploading file ' + v.filename  + ' to gs://' + v.bucketname + '/' +v.filename)
    blob = bucket.blob(v.filename)
    try:
      blob.upload_from_filename(v.dump_dir+v.filename)
      print('The file gs://' + v.bucketname + '/' + v.filename + ' was uploaded ok')
    except Exception as e:
      print("Error: {0} [{1}]".format(e.msg, e.code))
      sys.exit(1)

    sys.exit(0)

  def bq_load_csv_in_gcs(self):
    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset(v.dataset)
    table_ref = dataset_ref.table(v.bqTable)
    job_config = bigquery.LoadJobConfig()
    schema = [
        bigquery.SchemaField(v.col_names[0], v.col_types[0], v.col_modes[0])
      , bigquery.SchemaField(v.col_names[1], v.col_types[1], v.col_modes[1])
      , bigquery.SchemaField(v.col_names[2], v.col_types[2], v.col_modes[2])
      , bigquery.SchemaField(v.col_names[3], v.col_types[3], v.col_modes[3])
      , bigquery.SchemaField(v.col_names[4], v.col_types[4], v.col_modes[4])
      , bigquery.SchemaField(v.col_names[5], v.col_types[5], v.col_modes[5])
      , bigquery.SchemaField(v.col_names[6], v.col_types[6], v.col_modes[6])
    ]
    job_config.schema = schema
    job_config.skip_leading_rows = 1
    job_config.source_format = bigquery.SourceFormat.CSV
    uri = 'gs://'+v.bucketname+'/'+v.filename
    load_job = bigquery_client.load_table_from_uri(
        uri,
        table_ref,
        job_config=job_config
    )
    try:
      print("Starting job {}".format(load_job.job_id))
      load_job.result()  # Waits for table load to complete.
      print("Job finished.")
      destination_table = bigquery_client.get_table(table_ref)
      print("Loaded {} rows.".format(destination_table.num_rows))
    except:
      print('Error loading table ' + v.bqTable)
      sys.exit(1)

  def bq_read_from_table(self):
    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset(v.dataset)
    table_ref = dataset_ref.table(v.bqTable)
    table = bigquery_client.get_table(table_ref)
    # Specify selected fields to limit the results to certain columns
    fields = table.schema[:v.bqFields]  # first two columns
    rows = bigquery_client.list_rows(table, selected_fields=fields, max_results=v.bqRows)

    # Print row data in tabular format.
    format_string = "{!s:<16} " * len(rows.schema)
    field_names = [field.name for field in rows.schema]
    try:
      print(format_string.format(*field_names)) 
      for row in rows:
        print(format_string.format(*row)) 
    except:
      print('Error querying ' + v.dataset+"."+v.bqTable)
      sys.exit(1)

a = main()
option = sys.argv[1]
if option == "1":
  a.read_oracle_table()
elif option == "2":
  a.write_aerospike_set()
elif option == "3":
  a.read_aerospike_set()
elif option == "4":
  a.delete_blob_if_exists_and_upload_to_GCP()
elif option == "5":
  a.drop_if_bqTable_exists()
  a.bq_create_table()
elif option == "6":
  a.bq_load_csv_in_gcs()
elif option == "7":
  a.bq_read_from_table()
else:
  print("incorrect option, valid options are: 1 - 7")
  sys.exit(1)
sys.exit(0)
