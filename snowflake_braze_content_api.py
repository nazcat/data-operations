# connect to Snowflake API
connection = {
  "sfUrl": "<URL>",
  "sfUser": "<USER>",
  "sfPassword": dbutils.secrets.get(scope="<SCOPE>", key="<KEY>"),
  "sfDatabase": "<DATABASE>",
  "sfSchema": "<SCHEMA>",
}


# Run Snowflake SQL query
sql = """ <SNOWFLAKE_QUERY> """

df = spark.read.format("snowflake").options(**connection).option("query", sql).option("escape", "\"").load()
display(df)


# Rename columns for camelCase or other cases, Snowflake dataframe defaults to  uppercase headers
df2 = df.withColumnRenamed("FIRSTCOLUMN","firstColumn") \
        .withColumnRenamed("SECONDCOLUMN","secondColumn") \
        .withColumnRenamed("THIRDCOLUMN","thirdColumn") \

df2.printSchema()


# Convert dataframe into JSON
example_json = df2.toJSON(use_unicode=True).collect()
example_json


# Convert JSON from pyspark to python for reformatting before API import
import json
df3 = json.dumps(example_json).replace('\\"','\"').replace('["', '[').replace('"]', ']')
df3


# Braze content block API
import requests 
  
url = 'https://rest.iad-03.braze.com/content_blocks/update?content_block_id=<BRAZE_CONTENT_BLOCK_ID>'
auth_token='<BRAZE_DEV_API_TOKEN>'
headers = {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + auth_token}
data = {\
'content_block_id': '<CONTENT_BLOCK_ID>',\
'name': '<CONTENT_BLOCK_NAME>',\
'description': 'Content Block Test',\
'content': df3 \
}

r = requests.post(url=url, headers=headers, json=data)
r.status_code
