from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import json

# Replace with your secure connect bundle path and credentials
SECURE_CONNECT_BUNDLE = 'C:\csClasses\cs122d\HW2\python_proj\secure-connect-cs122dspring2024.zip'
ASTRA_CLIENT_ID = 'BkzEPjuHWGmKSmHRgqvChkiu'
ASTRA_CLIENT_SECRET = 'qmvOO_Bw,rReGz9XlenxL,yO-pc8xMc+0CQ312MmA0hBk6a04PTSeNG90MdqUwgmFN1eHo,lvCe_5zW5PojqpTuwo+yP0va0GmrIwgiUP0g1WrabWb+hP5.lN0R-1JOL'

def create_astra_connection():
  # use cassandra cluster
  cluster = Cluster(SECURE_CONNECT_BUNDLE, auth_provider=PlainTextAuthProvider(ASTRA_CLIENT_ID, ASTRA_CLIENT_SECRET))
  session = cluster.connect()
  return session

def insert_ad(session, ad_data):
  # assuming we insert to the original table and the new table
  # hint to self: use json library
  ad_data = json.loads(ad_data)

  # a lot like JDBC from 122B: write the query as a string and run an execute func on it 
  # the parameter filling works the same way
  adid = ad_data['adid']
  content = ad_data['content']
  itemid = ad_data['itemid']
  level = ad_data['level']
  merchantid = ad_data['merchantid']

  insert_query_1 = """
  INSERT INTO punkcity.ad (adid, content, itemid, level, merchantid)
  VALUES (? ? ? ? ?);
  """
  session.execute(insert_query_1, [adid, content, itemid, level, merchantid])

  insert_query_2 = """
  INSERT INTO punkcity.adq7c (level, itemid, adid)
  VALUES (? ? ?);
  """
  session.execute(insert_query_2, [level, itemid, adid])

# Use the function to insert the new ad
def main():
# Connect to Astra DB
# New ad data
# Insert new ad
# Clean up and close the connection
  session = create_astra_connection()
  ad_data = {
    "adid": "LFIR9",
    "content": "GenAI market",
    "itemid": "N108C",
    "level": "silver",
    "merchantid": "AP5ZX"
  }
  insert_ad(session, ad_data)
  session.shutdown()

if __name__ == "__main__":
  main()