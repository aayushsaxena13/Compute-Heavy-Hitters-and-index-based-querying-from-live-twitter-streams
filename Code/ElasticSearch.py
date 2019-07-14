#Make Sure you have Elastic Search installed, up and running on some port usually: 9200 )
# If not, just run (sudo pip install elasticsearch)

#Do Necessary Imports: Elastic Search 

from elasticsearch import Elasticsearch
import logging
import json
from pprint import pprint

#This function exectures our search query. 

def search(es_object, index_name, search):
    res = es_object.search(index=index_name, body=search)
    pprint(res)

#Instantiate Elastic Search Object
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
# print es to see if the above command is running.

if not es.ping(): 
    raise ValueError("Connection failed")

#es.ping returns true if the elastic search is working fine and our object is able to access 
# through python script.

#Take Input Query Type from User
print("Enter the integer for type of query you want \n 0. Match_All Query \n 1. Term Query \n 2. Match Query \n 3. Prefix Query \n 4. Fuzzy Query")
query_inp = int(input())

#For all the queries, we create a search object to make a JSON object and then call the search # function that we defined above.

#If the user chose Match_All Query
if(query_inp == 0):
    if es is not None:
        print("Match all queries")
        search_object = {'_source' : ['Text'],'query': {'match_all': {}}}
        search(es, 'twitterdatajson', json.dumps(search_object))


#If the user chose Term Query
elif(query_inp == 1):
    query_type = "term"
    print("Enter the exact term you want to match. The exact term will be searched in Lang")
    term = str(input())

    if es is not None:
         search_object = {'_source' : ['Text','Lang'], 'query': {'term': {'Lang' : term}}}
         search(es, 'twitterdatajson', json.dumps(search_object))
#Our query will return ‘Text’ from ‘ _source’ and we check for term in ‘Lang' field.


#If the user chose Match Query

elif(query_inp == 2):
    query_type = "match"
    print("Enter input for matching query")
    match = str(input())
    if es is not None:
        search_object = {'_source' : ['Text'],'query': {'match_phrase': {'Text': match}}}
        search(es, 'twitterdatajson', json.dumps(search_object))
# Our query will return ‘Text’ from ‘ _source’ and we check for any phrase user enters in  the 
# ‘Text' field.



#If the user chose Prefix Query

elif(query_inp == 3):
    if es is not None:
        print("Enter term for Prefix Queries")
        prefix = str(input())
        search_object = {'_source' : ['Text'],'query': {'prefix': {'Text': prefix }}}
        search(es, 'twitterdatajson', json.dumps(search_object))
# Our query will return ‘Text’ from ‘ _source’ and we check for any phrase with prefix that user 
# entered in  ‘Text' field.


#If the user chose Fuzzy Query

elif(query_inp == 4):
    if es is not None:
        print("Enter term for Fuzzy Queries")
        fuzzy = str(input())
        search_object = {'_source' : ['Text'],'query': {'fuzzy': {'Text': fuzzy }}}
        search(es, 'twitterdatajson', json.dumps(search_object))

#Our query will return ‘Text’ from ‘ _source’ and we check for any fuzzy phrase in ‘Text' field that # user entered. You can set fuzziness using other parameters like the ‘fuzziness’.
