import pandas as pd
import numpy as np
from pyspark.sql import Row
from datetime import datetime, date
from pyspark.sql import SparkSession
from functools import reduce
import math
import random as rd



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
    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame(pd.read_csv('industry_data.csv'))
    df.iterrows()


# 8
def lambda_funtion():
    names = ["vijay" , "ajai" , "rahul" , "bob" , "kumar" , "ramesh"]
    numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    
    # for loop
    for i in names:
        print(i)
    
    # Using map with lambda to square each number
    map_names = list(map(lambda x:x,names))
    print(map_names.index("vijay"))

    # # Using filter with lambda to filter even numbers
    namewith_A = list(filter(lambda x: 'a' in x, names))
    print("Contains A: ", namewith_A)

    # # Using reduce with lambda to find the sum of all numbers
    sum = reduce(lambda x, y: x + y, names)
    print("sum of all numbers:", sum) 

# 9.

def sort_sorted():
    
    # sort : only for list
    # sorted : for any sequence
    #but both working is same
    
    names = ["ajai" , "mohan" , "babu" , "cummins"]
    print(sorted(names))
    words = ["Geeks", "For", "Geeks"]
 
# Sorting list of strings
    words.sort()
    
    print(words)


#10. OOPS concepts. 

class greeting:
    def __init__(self ,name , age):
        print("Hello,",name,"your age is,",age)
    
# greeting1 = greeting("vijay",20)

class circle():
    def __init__(self, radius):
        self.radius = radius
    
    def area(self):
        print(math.pi * self.radius ** 2)
    
class rectangle():
    def __init__(self , length ,width):
        self.length = length
        self.width = width
        
    def area(self):
        print(self.length * self.width)
        

# 11. Write a python program that randomly generates a 5 digit number. 
# The user has a maximum of 5 tries to guess the randomly generated number.

def find_number():
    guess_number = str(rd.randint(10000 , 100000))
    wrong = "A"
    wrong_position = "B"
    correct = "C"
    print(guess_number)
    
    for i in range(0,5):
        indicate = ""
        print(" Attempt = " ,i ,"\n" ,"Remaining = " ,5-i)
        Guessed_number = input("Enter 5 digit: ")
        if int(Guessed_number) == guess_number:
            print(correct)
            break
        else:
            for i in range(len(Guessed_number)):
                if Guessed_number[i] in guess_number:
                    indicate+=wrong_position
                else:
                    indicate+=wrong
            print(indicate)

# 11.
        
def working_data():
    data= [
    {"roll_no": 1,"name": "John", "games": ["cricket", "football"],
    "marks": {"maths": 90, "science": 93, "history": 81}, "rank": 1},
    {"roll_no": 2,"name": "Mick", "games": ["football", "hockey"],
    "marks": {"maths": 95, "science": 86, "cs": 70}, "rank": 2},
    {"roll_no": 3,"name": "June", "games": ["badminton", None],
    "marks": {"maths": 92, "science": 92, "geography": 78}, "rank": 3},
    {"roll_no": 4,"name": "Adam", "games": ["soccer", "badminton"],
    "marks": { "science": 91, "cs": 82},"rank": 4},
    {"roll_no": 5,"name": "Robb", "games": ["cricket", None],
    "marks": {"maths": 88, "science": 90, "economics": 84}, "rank": 5},
    {"roll_no": 6,"name": "Arya", "games": ["football", "hockey"],
    "marks": {"maths": 89, "science": 88, "history": 97}, "rank": 6}
    ]
    # 1.
    cs_data = list(filter(lambda x: 'cs' in x['marks'] , data))
    # print(cs_data)
    # 2.
    for item in data:
         if "maths" in item["marks"]:
            item["marks"]["maths"] *= 3
         if "science" in item["marks"]:
             item["marks"]["science"] *= 2   
         for subject, mark in item["marks"].items():
            item["percentage"] = (mark / 600) * 100
    for student in data:
        student['games'] = [game for game in student["games"] if game is not None]
    print(data)
    
    # 3.
    data_df = pd.DataFrame(data)
    data_df["pre_rank"] = data_df["rank"]
    data_df['new_rank'] = data_df.groupby('percentage', sort=True).ngroup()+1
    data_df = data_df.sort_values(by="new_rank")
    data_df['change_in_ranks'] = data_df.apply([lambda row:1 if row['new_rank'] < row["pre_rank"] 
                                               else -1 if row['new_rank'] > row["pre_rank"]
                                              else '-'] , axis=1)
    data_df_rank = data_df[['name', 'percentage' , 'pre_rank' , 'new_rank' , 'change_in_ranks']].copy()

    print(data_df_rank)
    
























#Calling all functions

# extend_and_addSubList()
# list_to_dict()
# delete_keys()
# rename_key()
# min_from_dict()
# reading_files.count_word("file.txt")
# Spark_dataframe()
# lambda_funtion()
# sort_sorted()
# circle = circle(5)
# rectangle  = rectangle(4,3)
# find_number()
working_data()