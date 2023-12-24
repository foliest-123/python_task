import pandas as pd
import numpy as np
from pyspark.sql import Row
from datetime import datetime, date
from pyspark.sql import SparkSession


# 1
# sub_list = ["h", "i", "j"]
# ['a', 'b', ['c', ['d', 'e', ['f', 'g', 'h', 'i', 'j'], 'k'], 'l'], 'm', 'n']

def extend_and_addSubList():

    extend_list =  ["a", "b", ["c", ["d", "e", ["f", "g"], "k"], "l"], "m", "n"]
    sub_list = ["h", "i", "j"]
    
    for i in range(len(sub_list)):
     extend_list[2][1][2].extend([sub_list[i]])  # Adding sub_list at a specific nested level

    print(extend_list)


# 2. Convert two lists into a dictionary
# Input:

# Expected Output:
# {'Ten': 10, 'Twenty': 20, 'Thirty': 30}

def list_to_dict():
    keys = ['Ten', 'Twenty', 'Thirty']
    values = [10, 20, 30]

    res = dict(map(lambda i,j : (i,j) , keys,values))
    print(res)

# 3. Delete a list of keys from a dictionary
# Expected Output:
# {'city': 'New york', 'age': 25}


def delete_keys():
    sample_dict = {
        "name": "Kelly",
        "age": 25,
        "salary": 8000,
        "city": "New york"
    }
    my_dict_copy = sample_dict.copy()
    keys = ["name", "salary"]
    
    for key,value in sample_dict.items():
      if key in keys:
          del my_dict_copy[key]
    print(my_dict_copy)


# 4. Rename key of a dictionary
# Expected Output:
# {'name': 'Kelly', 'age': 25, 'salary': 8000, 'location': 'New york'}

def rename_key():
    sample_dict = {
    "name": "Kelly",
    "age":25,
    "salary": 8000,
    "city": "New york"
    }
    sample_dict['location'] = sample_dict.pop('city')
    print(sample_dict)


# 5. Get the key of a minimum value from the following dictionary
# Expected output:
# Math

def min_from_dict():
    sample_dict = {
    'Physics': 82,
    'Math': 65,
    'history': 75
    }
    print(min(sample_dict))


# 6. Read a file and pass it to a function which checks for the given input string 
# ( case insensitive check). 
# If the given string is found, the function should return True. 
# Create another function which will read the input file and scan it to get the count of each word in it. 
# After the process is completed print each word  along with the number of times it occurred in the file.

class reading_files():
    def checkWord(word):
        file = open("file.txt", "r")
        for line in file:
            if word in line:
                return True
        return False
    
    def count_word(filename):
        count_words = {}
        with open(filename,'r') as file:   
         for line in file:        
            for word in line.split(): 
                if word in count_words: 
                    count_words[word] += 1
                else:      
                     count_words[word] = 1
        print(count_words)
            
# 7. Explore the difference between iterrows(), itertuples(), apply(), map()
# Analyze the time taken for above operations using one wider dataset(large no of columns) and one taller dataset(Large no. of rows)
# Read dataset as spark dataframe

def Spark_dataframe():
    
    spark = SparkSession.builder.appName("ExampleApp").getOrCreate()

    dfs = spark.read.json("employee.json")

    data_collect = dfs.collect()
    print(data_collect)
















#Calling all functions

# extend_and_addSubList()
# list_to_dict()
# delete_keys()
# rename_key()
# min_from_dict()
# reading_files.count_word("file.txt")
# Spark_dataframe()