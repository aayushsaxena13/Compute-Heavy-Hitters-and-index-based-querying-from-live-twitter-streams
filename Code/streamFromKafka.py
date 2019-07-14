import json
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import numpy as np

import nltk
import mmh3
import math
import bitarray
import string

nltk.download('stopwords')
nltk.download('punkt')

from nltk.corpus import stopwords

#Depth of the count min sketch data structure(Number of hash functions used)
sketch_depth = 10
#Depth of the count min sketch data structure(Maximum values of the hash functions)
sketch_width = 2000

#Top K heavy hitters
K = 20

#Wrapper on the heavy hitters data structure broadcast variable
#We are making this as a broadcast variable as it is shared among all the tasks
#Wrapper made to make the Broadcast Variable mutable
#Singleton class for the broadcast variable
class BroadcastWrapper:
    
    def __init__(self, broadcastVar=None,sc = None):
        self.broadcastVar = broadcastVar
        self.sc = sc
        
    obj = None 

    #To avoid picking error caused due to passing of spark context
    def __getstate__(self):
            state = dict(self.__dict__)
            del state['sc']
            return state

    #Returns the instance of the BroadcastWrapper
    @staticmethod
    def getInstance(sc=None):
        if BroadcastWrapper.obj == None:
            if(sc == None):
                BroadcastWrapper.obj = BroadcastWrapper()
            else:
                BroadcastWrapper.obj = BroadcastWrapper(sc=sc)
            
        return BroadcastWrapper.obj
        

    #Updates the broadcast variable
    #by unpersisting the variable with blocking = True
    def update(self,newData):
        if(self.broadcastVar != None):
            self.broadcastVar.unpersist(blocking = True)
        self.broadcastVar = self.sc.broadcast(newData)

    #Returns the wrapped broadcast variable
    def getVar(self):
        return self.broadcastVar

#Wrapper on the count min sketch data structure broadcast variable
#We are making this as a broadcast variable as it is shared among all the tasks
#Wrapper made to make the Broadcast Variable mutable
#Singleton class for the broadcast variable
class BroadcastWrapperSketch:
    
    def __init__(self, broadcastVar=None,sc = None):
        self.broadcastVar = broadcastVar
        self.sc = sc
        
    obj = None 

    #To avoid picking error caused due to passing of spark context
    def __getstate__(self):
            state = dict(self.__dict__)
            del state['sc']
            return state

    #Returns the instance of the BroadcastWrapper
    @staticmethod
    def getInstance(sc=None):
        if BroadcastWrapperSketch.obj == None:
            if(sc == None):
                BroadcastWrapperSketch.obj = BroadcastWrapperSketch()
            else:
                BroadcastWrapperSketch.obj = BroadcastWrapperSketch(sc=sc)
            
        return BroadcastWrapperSketch.obj
        

    #Updates the broadcast variable
    #by unpersisting the variable with blocking = True
    def update(self,newData):
        if(self.broadcastVar != None):
            self.broadcastVar.unpersist(blocking = True)
        self.broadcastVar = self.sc.broadcast(newData)

    #Returns the wrapped broadcast variable
    def getVar(self):
        return self.broadcastVar


#Method to check if a word is present in the bloom filter for stop words
def check_candidacy(word, bloom_filter, filter_size, no_hash_func):
    #loop for the number of hash functions 
    for hash_seed in range(no_hash_func):
        #Compute the hashcode
        hash_code = mmh3.hash(word, hash_seed) % filter_size
        #If the entry for the hashcode is not present in the bloom filter rerturn false
        if not bloom_filter[hash_code]:
            return False
    return True

#Create bloom filter
#Set of stop words and punctuation are added to the bloom filter
def bloomFilter():
    stop_words = set(stopwords.words())
    urls = set(['https','http'])
    punct = set(string.punctuation+'’')
    #set union
    stop_words = stop_words | punct | urls
    
    #define filter size, no of hash functions
    filter_size = int(-(len(stop_words)*math.log(0.00001))/(math.log(2)**2))
    no_hash_func = int((filter_size/len(stop_words))*math.log(2))
    #define the bloom filter as a bitarray 
    bloom_filter = bitarray.bitarray(filter_size)
    bloom_filter.setall(0)
    
    #for all the words in the set
    for word in stop_words:
        #loop for the number of hash functions
        for hash_seed in range(no_hash_func):
            #Calculate the hash value and increment the count in the cont min sketch
            hash_code = mmh3.hash(word, hash_seed) % filter_size
            bloom_filter[hash_code] = 1

    return bloom_filter, filter_size, no_hash_func

#Function to process the spark stream
def stream(ssc, duration, bloom_filter, filter_size, no_hash_func):
    #Create the stream from kafka for the given topic
    jsonStream = KafkaUtils.createDirectStream(ssc, topics = ['twitterDataJson'], kafkaParams = {"metadata.broker.list": 'localhost:9092'})
    #Load the data as a JSON form kafka and extract the Text field from it
    dstream = jsonStream.map(lambda x: json.loads(x[1]))
    textStream = dstream.map(lambda x: x['Text'])
    #Convert the stream of tweet into stream of words by flatMap and using nltk library to tokenize the stream
    #Filter the stream to remove stop words and punctuations by checking candidacy
    tokens = textStream.flatMap(lambda line: nltk.word_tokenize(line.lower())).filter(lambda x: not check_candidacy(x,bloom_filter, filter_size, no_hash_func))
    tokens = tokens.filter(lambda x: not x.startswith(('/','\'')))
    #For stream collect the rdd and update the heavy hitters and count min sketch data structure based on the words
    tokens.foreachRDD(lambda rdd: collectAndUpdateHH(rdd))

    #Start the streaming context and awaitTermination
    ssc.start()
    ssc.awaitTermination() 

#Collect the rdd and update the heavy hitters and count min sketch data structure based on the words
def collectAndUpdateHH(rdd):
    words = rdd.collect()

    for word in words:
        updateCountMin_HeavyHitters(word)
    #Get the heavy hitters data structure
    hh = BroadcastWrapper.getInstance().getVar().value
    #Print the heavy hitters data structure
    for k,v in sorted(hh.items(), key=lambda p:p[1],reverse=True):
        print(k,int(v))
    print("---------------------------------------------------------------------")
    
#Update the heavy hitters and count min sketch data structure based on the word
def updateCountMin_HeavyHitters(word):
    #Extract the heavy hitters and count min sketch data structure from their respective broadcast variables
    heavy_hitters = BroadcastWrapper.getInstance().getVar().value
    sketch = BroadcastWrapperSketch.getInstance().getVar().value
    #Updates the count min sketch
    count_min_sketch(word,sketch)
    #Gets frequencyof the word from the updated count min sketch
    freq = get_count_min(word,sketch)

    #If word is already in the heavy hitters, we increase its frequency 
    if word in heavy_hitters:
        heavy_hitters[word]+=1
        BroadcastWrapper.getInstance().update(heavy_hitters)
        return
        
    #Check if we need to update the heavy hitters
    size = len(heavy_hitters)
    
    #If we have less than k elements add the word to the heavy hitters
    if(size<K):
        heavy_hitters[word] = freq
        BroadcastWrapper.getInstance().update(heavy_hitters)
    else:
        #If more than k elements
        #Find the frequency of the minimum element in the data structure 
        current_min = min(heavy_hitters, key=heavy_hitters.get)
        current_freq = heavy_hitters[current_min]
        #Compare the frequency of the minimum element with the frequency of the current word  
        #Remove this heavy hitter and add a new one if required
        if(freq>current_freq):
            del heavy_hitters[current_min]
            heavy_hitters[word] = freq
            BroadcastWrapper.getInstance().update(heavy_hitters)





# skeletal code for count-min sketch
def count_min_sketch(word,sketch):
    #iterate through all the hash functions
    for d in range(sketch_depth):
        #hash the word and increase the frequency in the sketch
        hash_code = mmh3.hash(word, d) % sketch_width
        sketch[d][hash_code] += 1
    BroadcastWrapperSketch.getInstance().update(sketch)

# get the frequency of a word from the count min sketch
def get_count_min(word,sketch):
    counts = []
    #iterate through all the hash functions
    for d in range(sketch_depth):
        #hash the word
        hash_code = mmh3.hash(word, d) % sketch_width
        #get the count and append to the array
        counts.append(sketch[d][hash_code])
    #get the minimum the counts
    return min(counts)



def main():
    #set the spark configurations
    conf = SparkConf().setMaster("local[5]").setAppName("TwitterDataStreamer")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, 10)
    ssc.checkpoint("checkpoint")
    #create the bloom filter
    bloom_filter, filter_size, no_hash_func = bloomFilter()
    #Create the broadcast variable for the heavy hitters data structure
    BroadcastWrapper.getInstance(sc).update({})
    #Create the broadcast variable for the count min sketch data structure
    BroadcastWrapperSketch.getInstance(sc).update(np.zeros((sketch_depth, sketch_width)))
    
    stream(ssc,10,bloom_filter, filter_size, no_hash_func)

if __name__=="__main__":
    main()

