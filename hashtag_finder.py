from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q, A
import csv
import urllib3

# Disable SSL warnings: 
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Create a client for connection
ELASTIC_SERVER_1= "YOUR SERVER LINK"
client = Elasticsearch(
    [ELASTIC_SERVER_1],
    timeout=240,
    maxsize=50,
    verify_certs=False)

# Get the inputs and store them on an array
print("Input the hashtags you want to be searched (comma seperate them)")
hashtag= input().replace(", ", ",")
hashtag_list= hashtag.split(',')

# Write inputs to csv file 
with open('hashtag.csv','w+') as file:   
  writer = csv.writer(file)
  writer.writerow(hashtag_list)
  reader = csv.reader(file, delimiter=',')

# Create the search object
s = Search(using=client, index="contents")

# Result set to store results
result_set=[]

# Looping through each hashtag
for word in hashtag_list:
    # operations on aggregations and match query 
    a = A("terms", field="hashtags", size=10)
    s = s.query(Q("match", hashtags=word))
    s.aggs.bucket("count",a)
    response= s.execute()
    response_dict = response.to_dict()
    list =response_dict["aggregations"]["count"]["buckets"]
    bucket_dict = {d['key']: d['doc_count'] for d in list}
    # Total number of documents 
    output_dict={}
    total_docs = s.count()


    for k, v in bucket_dict.items():
        percentage= v/total_docs
        output_dict[k]=percentage, v, total_docs

# Get time  
currentTime=datetime.now().strftime("%Y-%m-%d,%H:%M:%S")

# Write results to the output file
with open(f'Output[{currentTime}].csv', 'w', newline='') as csvfile:
    fields= ["Hashtag","Percentage", "Count", "TotalHashtagCount"]
    writer = csv.DictWriter(csvfile, fieldnames=fields)
    writer.writeheader()
    for key in output_dict:
        writer.writerow({'Hashtag': key, 'Percentage': round(output_dict[key][0],1), 'Count': output_dict[key][1], 'TotalHashtagCount': output_dict[key][2]})

