#!/usr/bin/env python
# -*- coding: utf-8 -*-

# snowflake api connection
connection = {
  "sfUrl": "<HOST URL>",
  "sfUser": "<USER>",
  "sfPassword": dbutils.secrets.get(scope="<SCOPE>", key="<KEY>"),
  "sfDatabase": "<DATABASE>",
  "sfSchema": "<SCHEMA>",
}


# Run SQL query
sql= """ <SQL QUERY>  """

df = spark.read.format("snowflake").options(**connection).option("query", sql).load()
display(df)


# Temporarily save file in Databricks HDFS
import datetime
def _getToday():
        return datetime.date.today().strftime("%Y%m%d")
outpath = r'/dbfs/FileStore/shared_uploads/<FOLDER>/'
filename = "%s_%s%s" % ("<FILE NAME>", _getToday() ,".csv")

df.toPandas().to_csv(filename, header=True, index=False)


# Export file to SFTP
import pysftp

cnopts = pysftp.CnOpts()
cnopts.hostkeys = None

with pysftp.Connection('<HOST>', \
                        username='<USER>', \
                        password='<PASS>', \
                        cnopts=cnopts) as sftp:
    with sftp.cd('/Import'):
         sftp.put(filename)
         

# Delete temp file
dbutils.fs.rm(filename, True) 
