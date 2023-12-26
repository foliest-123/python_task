import pandas as pd, numpy as np
from pyspark.sql import Row, SparkSession
from datetime import datetime, date
from functools import reduce
import math, random as rd
from flask import Flask, request, jsonify
from sqlalchemy import create_engine, Column, Integer, String, JSON, TIMESTAMP
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.sql import func

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

# 12.
        
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
    cs_data = list(filter(lambda row: 'cs' in row['marks'] , data))
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
    
    
    


# 13. Get the following 3 parameters from a simple flask API. 
# Each time the API is called, it is considered as a new refresh.

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = f'postgresql+psycopg2://postgres:1234@localhost/sample_test'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False  # Optional but recommended for performance

db = SQLAlchemy(app)
# class Metrics(db.Model):
#     __tablename__ = 'dataset_metrics'
#     refresh_count = db.Column(db.Integer, primary_key=True)
#     new_row_count = db.Column(db.Integer)
#     metric_threshold = db.Column(db.Integer)
#     Window_size = db.Column(db.Integer)
#     def __init__(self, refresh_count,new_row_count, metric_threshold ,Window_size):
#         self.refresh_count = refresh_count
#         self.new_row_count = new_row_count
#         self.metric_threshold = metric_threshold
#         self.Window_size = Window_size
# with app.app_context():
#     db.create_all()
    
# refresh_id = 1
# @app.route("/")
# def get_param(methods=["GET"]):
#     global refresh_id
#     base_rowcount = 1000
#     last_row = Metrics.query.order_by(Metrics.refresh_count.desc()).first()
#     if last_row is not None:
#         refresh_id = last_row.refresh_count + 1
#         new_row_count = int(request.args.get('new_row_count'))
#         metric_threshold = int(request.args.get('metric_threshold'))
#         Window_size = int(request.args.get('Window_size'))
#         new_metric = Metrics(refresh_id , new_row_count, metric_threshold, Window_size)
#         db.session.add(new_metric)
#         db.session.commit()
#         average_rowCount = Metrics.query.with_entities(func.avg(Metrics.new_row_count).label("avg_rowCount")).first()
#         row_deviation = ((average_rowCount[0] - base_rowcount) / base_rowcount) * 100
#         print(row_deviation ," " , metric_threshold)
#         if row_deviation >= metric_threshold:
#             return "Threshold Alert...!"
#     else:
#         new_row_count = int(request.args.get('new_row_count'))
#         metric_threshold = int(request.args.get('metric_threshold'))
#         Window_size = int(request.args.get('Window_size'))
#         if new_row_count is not None and metric_threshold is not None and Window_size is not None:
#             new_metric = Metrics(refresh_id , new_row_count, metric_threshold, Window_size)
#             db.session.add(new_metric)
#             db.session.commit()
#     return jsonify({
#         "refresh_id": refresh_id,
#         "row": new_row_count,
#         "threshold": metric_threshold,
#         "window_size": Window_size
#     })
# if __name__ == "__main__":
#     app.run()


# 14 . Write a python program to store total issue count and aggregated issue details of attribute_issue_count and  
# dataset_issue_count tables in datasource_issue_count table  appropriate columns [issue_count_dataset_level,
# issue_details_dataset_level, issue_count_attribute_level, issue_details_attribute_level]
class AttributeIssueCount(db.Model):
    __tablename__ = 'attribute_issue_count'

    issue_count_id = db.Column(Integer, primary_key=True, autoincrement=True)
    tenant_id = db.Column(Integer, nullable=False)
    integration_id = db.Column(Integer, nullable=False)
    meta_data_id = db.Column(Integer, nullable=False)
    created_month = db.Column(TIMESTAMP, nullable=False)
    issue_count = db.Column(Integer, nullable=False)
    issue_details = db.Column(JSON, nullable=False)
    data_set_id = db.Column(Integer, nullable=False)
    env_id = db.Column(Integer, nullable=False)

    def __init__(self, tenant_id, integration_id, meta_data_id, created_month, issue_count, issue_details, data_set_id, env_id):
        self.tenant_id = tenant_id
        self.integration_id = integration_id
        self.meta_data_id = meta_data_id
        self.created_month = created_month
        self.issue_count = issue_count
        self.issue_details = issue_details
        self.data_set_id = data_set_id
        self.env_id = env_id

class DatasetIssueCount(db.Model):
    __tablename__ = 'dataset_issue_count'

    issue_count_id =db.Column(Integer, primary_key=True ,autoincrement=True)
    tenant_id = db.Column(Integer, nullable=False)
    integration_id = db.Column(Integer, nullable=False)
    data_set_id = db.Column(Integer, nullable=False)
    created_month = db.Column(TIMESTAMP, nullable=False)
    issue_count = db.Column(Integer, nullable=False)
    issue_details = db.Column(JSON, nullable=False)
    env_id = db.Column(Integer, nullable=False)

    def __init__(self, tenant_id, integration_id, data_set_id, created_month, issue_count, issue_details, env_id):
        self.tenant_id = tenant_id
        self.integration_id = integration_id
        self.data_set_id = data_set_id
        self.created_month = created_month
        self.issue_count = issue_count
        self.issue_details = issue_details
        self.env_id = env_id

def store_details():
    with app.app_context(): 
        attr_issue_value = AttributeIssueCount.query.all()
        dataset_issue_value = DatasetIssueCount.query.all()
        issue_count_dataset_level  =AttributeIssueCount.query.with_entities(func.sum(AttributeIssueCount.issue_count)).scalar()
        issue_count_attribute_level = DatasetIssueCount.query.with_entities(func.sum(DatasetIssueCount.issue_count)).scalar()
        count_per_issue = db.session.query(
            DatasetIssueCount.issue_count,
            func.count(DatasetIssueCount.issue_count).label('count_per_issue')
        ).group_by(DatasetIssueCount.issue_count).all()

            
        print(count_per_issue)
        













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
# working_data()
store_details()