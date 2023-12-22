import pandas as pd
import numpy as np
import time

# 1
# sub_list = ["h", "i", "j"]
# ['a', 'b', ['c', ['d', 'e', ['f', 'g', 'h', 'i', 'j'], 'k'], 'l'], 'm', 'n']

def extend_and_addSubList():

    extend_list =  ["a", "b", ["c", ["d", "e", ["f", "g"], "k"], "l"], "m", "n"]
    sub_list = ["h", "i", "j"]

    print(extend_list[3][0])


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

def iteration_performance():
    
    #small   Execution_time  = --- 0.027309894561767578 seconds ---
    data = {
    "calories": [420, 380, 390],
    "duration": [50, 40, 45]
    }
    #load data into a DataFrame object:
    small_df = pd.DataFrame(data)  
    # for index , row in small_df.iterrows():
    #     print(small_df)
    # print("--- %s seconds ---" % (time.time() -  start_time))
    
    #high execution_time = --- 9.177774429321289 --- to --- 10.407198667526245 seconds ---
    num_rows = 10000000
    num_cols = 10
    data = {}
    data = {}
    start_time = time.time()
    for i in range(num_cols):
         col_name = f'col{i+1}'
         data[col_name] = np.random.randint(low=0, high=100, size=num_rows)
    large_df = pd.DataFrame(data)
    print(large_df)
    print("--- %s seconds ---" % (time.time() -  start_time))
    

















#Calling all functions

# extend_and_addSubList()
# list_to_dict()
# delete_keys()
# rename_key()
# min_from_dict()
# reading_files.count_word("file.txt")
iteration_performance()