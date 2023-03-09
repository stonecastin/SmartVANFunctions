"""
SmartVAN Functions

A module to assist interactions with the SmartVAN API

note: csv files must be encoded to ANSI (or CP1215) for text
"""

#required imports
import requests
import pandas as pd
from time import sleep
import json
import os

#constants
##API constants
api_key = os.getenv(api_key) #string, note: |0 == smartvan, |1 == NGP
application = os.getenv(application)
baseUrl = "https://api.securevan.com/v4"
##DB unique constants 
#add useful constants for your DB, avoid making calls to get them every time
#for example, activist codes, methods you use often, district ids etc.
ward_nest = 0 #fill in the index position ward occupiesfor your DB
city_clause = "&city=" + os.getenv(city) #narrow given search by city


#functions

def call_get(method):
    """
a function to send a basic .get request to the API

args: method: str of method to be called, for example /people, /activistCodes, /user etc.

returns: response, a requests object that can be parsed into text (response.text) or JSON (response.json)

    """
    #api call
    url = baseUrl + method
    headers = {
    "accept": "application/json"
    }
    response = requests.get(url, headers=headers, auth=(application,api_key))
    return(response)

#

def call_get_url(url):
    """
    sends a basic .get request to the url using a full url

    args: url: str, a full url

    returns: response: a response object
    """
    headers = {
    "accept": "application/json"
    }
    response = requests.get(url, headers=headers, auth=(application,api_key))
    return(response)


def return_from_variable(response,variable):
    """
parses a response object (NOT response.text or response.json) and returns the requested variable

args: response: a response object from requests
variable: str, a key, the variable to be requested, for example "VanId", "firstName", "party" etc. 
    note that only first level variables (unnested) can be accessed this way

returns: response[variable]: str or int, the value associated with given key (variable)
    """
    response = response.json #parse response object to json
    return(response[variable]) #return value at variable key, assumes response is a dict

def return_from_variable_list(obj, key):
    """
    Pulls a key from each dict in a list of dicts
    returns the item type if obj is not a list

    args: obj: the list of dictionaries, where each dict is formatted identically
    key: str, the key for the desired variable

    returns: Var: a list of values of the given key
    str(type(obj)): str, the object type if not a list
    """
    if str(type(obj)) =="<class 'list'>": #checks if the object passed into the function is a list
        Var = [] #establish empty list
        for item in obj: #iterate over each entry (dict) in the list of dicts
            intermediate = item[key] #pull value associated with desired key
            Var.append(intermediate) #add desired value to list
        return(Var)
    else: #if object is not a list
        return(str(type(obj))) #return obj type for troubleshooting

def remove_dupes(list_w_dupes):
    """
    removes duplicates from a list

    args: list_w_dupes: list of strings

    returns: list_without_dupes, list of strings, index will not match first list
    """
    list_without_dupes = [*set(list_w_dupes)] #uses set notation to remove duplicates
    return(list_without_dupes)





def match_by_index(key_list, value_list):
    """
    combines two lists into a dictionary by index,
    meaning that the two lists must be in the same order

    args: key_list: the list of keys
    value_list: the list of values to be assigned to the keys
        note that the keys and values must be in the same order

    returns: created_dict: dict, result of matching each key to its value index wise
    """
    created_dict = {key_list[i]: value_list[i] for i in range(len(key_list))} #iterates over key_list, matching each key to the value at the same list position
    return(created_dict)



def get_ward(VanId):
    """
    a .get request to find the ward associated with a given VanId

    args: VanId: int or str, the VanId

    returns: ward: the city ward associated with the given VanId
    """
    response = call_get("/people/" + str(VanId) + "?$expand=districts") #calls the API for the specified voter, with district information
    response = response.json() #parse response to JSON
    districts = response["districts"] #isolate district information from voter file
    wardshell = districts[ward_nest] #isolate ward information from district information, in other DBs this value may be different
    wardinfo = wardshell["districtFieldValues"] #unnest ward information
    ward = return_from_variable_list(wardinfo, "name") #isolate ward number from wardinformation
    return(ward)


def find_ward(voterFile):
    """
    returns the ward number for a given voter file
    DOES NOT issue an API request

    args: voterFile: a list of dictionaries (JSON) output as a response.json()

    returns: ward: list of one str, the ward assigned to the voter file #i didn't catch this while working with zip_unpack etc, will need to fix in those functions when i fix it here
    """
    districts = voterFile["districts"] #isolate district information
    wardshell = districts[ward_nest] #isolate ward information from district information, in other DBs this value may be different
    wardinfo = wardshell["districtFieldValues"] #unnest ward information
    ward = return_from_variable_list(wardinfo, "name") #isolate ward number
    return(ward)


def ward_by_zip_unpack(response):
    """
unpacks a response object to a dict matching vanId with associated ward number

args: response: response.json object, MUST be from a "/people?zipOrPostalCode="+ zipCode +"&$expand=districts" call

returns: IDWardMatch: dict, keys are vanIds (str), values are ward numbers (str)
    """
    items = response["items"] #unnest items in response
    vanIds = return_from_variable_list(items, "vanId") #get list of vanIds from items
    wards = [] #establish empty list
    for voterFile in items: #iterate over the list items, where each elements are dictionaries (voterFile)
        ward = find_ward(voterFile) #isolate ward (list of one str) from dictionary voterFile
        wards.append(ward[0]) #unnest ward (list) to str
    IDWardMatch = match_by_index(vanIds, wards) #create dictionary IDWardMatch with keys vanIds, values wards
    return(IDWardMatch)
        
def ward_by_zip(zipCode):
    """
WARNING: VERY LONG COMMAND, ITERATES API REQUESTS
a function to pull all voters' vanIds and ward numbers within a specified zipcode

    args: zipCode: str, the zipcode to pull voters from

    returns: Ward_by_zip_dict: dict, dictionary where keys = vanIds and values = ward numbers
    """
    response = call_get("/people?zipOrPostalCode="+ zipCode +"&$expand=districts") #API call of all voters in specified zipCode, with district information
    response = response.json() #parse response object as JSON
    Ward_by_zip_dict = ward_by_zip_unpack(response) #create dictionary Ward_by_zip_dict from response, where keys = vanIds and values = ward numbers
    n=response["count"] #assign n value "count", the number of total voters in request
    iterations =  (n//50 + (n%50 > 0)) #50 responses per page, number of expected pages / calls / iterations as count of voters / 50 rounded up
    terminateLoop = False #unimplimented loop stop
    print(str(iterations) + " expected iterations") #return expected pages / calls / iterations for a call
    for loop in range(iterations): #loop with a limit of expected iterations / calls / pages
        sleep(0.1) #reccommended call rate: no more than 5 calls per second, calibrated to my computer (where a request takes at least 0.15 seconds to complete, NOT multithreaded or running multiple instances)
        url = response["nextPageLink"] #return url given for multi page API calls
        print(iterations) #displays status of loop at each iteration
        if url == None: #JSON returns None object if asked to pull from a field that does not exist
            terminateLoop = True #unimplimented loop stop
            print("loop complete") #announces end of loop
            break
        response = call_get_url(url) #API call to next page in the multi page response
        response = response.json() #parse response as JSON
        intermediate_dict = ward_by_zip_unpack(response) #intermediate_dict is a dict where keys = vanIds (str), values = ward number (str), of voters on current page
        Ward_by_zip_dict = Ward_by_zip_dict | intermediate_dict #merge root dict (established befoore loop) and the dict created with info from current page
        iterations = iterations - 1 #incriment iterations
    with open("wardbyzip" + str(zipCode) + ".json", "w") as file: #write JSON file to wd, default = directory of current file
        json.dump(Ward_by_zip_dict, file)
    return(Ward_by_zip_dict) #return dictionary containing all vanIds and ward numbers for entire zip code


def get_path():
    """
    prints the path of the current working directory, for debugging purposes

    args: none

    returns: none
    """
    path = os.getcwd()
    print(path)

def match_ward(voterFile, ward):
    """
    args: voterfile: a JSON object or nested dict with information on a single voter, gotten from a /person/VANID request or unnested /person search request
    ward: int or str, the ward number to match voter information against

    returns: boolean, true if the ward returned in response matches the given ward, false if not a match
    """
    returned_ward = find_ward(voterFile)
    returned_ward = str(returned_ward[0])
    if returned_ward == str(ward):
        return(True)
    else:
        return(False)

def csv_to_strings(csv_file):
    """
    converts a csv file into a list of lists, in order of original index, i.e., first column is the first item in list of lists

    args: csv_file: str, file to be converted, must be in wd or csv_file = path to file

    returns: final_list: list of lists, where final_list[0] = all values in first column, index wise
    """
    with open(csv_file,'r') as f: #code modified from jshrimp29 on stackoverflow 
        lines = f.readlines()

    headers=lines[0].rstrip().split(',')    # rstrip removes end-of-line chars
    numLines = len(lines)

    linelist = [x.rstrip().split(',') for x in lines[1:numLines+1]]       # create lineList to include only numLines elements
    outputDict = {keyVal:[x[idx] for x in linelist if len(x)==len(headers)] for idx,keyVal in enumerate(headers)}    # list comprehension within dictionary comprehension to split each element by its header and create dictionary of lists

    final_list = []

    for key in outputDict:
        data = outputDict[key]
        final_list.append(data)

    return(final_list)


def read_json(json_file): 
    """
    reads a given .json file into a python dict

    args: json_file: str, the name of a .json file to be read

    returns: python_dict: python dictionary consisting of .json file contents
    """
    file = open(json_file) 
    python_dict = json.load(file)
    return(python_dict)

def write_json(desired_name, python_dict):
    """
    writes a given python dict into a .json file stored in current directory

    args: desired_name: str, the name of the file to be created (must end in .json)

        python_dict: python dict, the dict to be written to json

    returns: none
    """
    with open(desired_name, "w") as file: #write JSON file to wd, default = directory of current file
        json.dump(python_dict, file)

def lists_to_csv(list_of_lists, desired_name):
    """
    writes a list of lists into a csv file, where each list is a column and each index number is a row

    args: list_of_lists: list of lists, where each list is to be a column in the csv
        desired_name: str, the name of the csv file to be written, must end in .csv

    returns: none
    """
    expected_rows = len(list_of_lists[0])
    str_init = ""
    for index in range(expected_rows):
        for column in list_of_lists:
            str_init = str_init + str(column[index]) + ","
        str_init = str_init + "\n"
    with open(desired_name, "a") as file:
        file.write(str_init)



def first_last(first_last):
    """
    series of /people search requests attempting to match first and last names to existing voterfiles

    args: first_last: list of two lists, where first_last[0] = a list of first names and first_last[1] = a list of last names, matched indexwise

   returns: list: list[0] = vanIds: listthe VANIDS successfully matched with search terms
                  list[1] = missing_persons: list, index numbers of unsuccessful matches, in terms of the index of the nested lists in first_last
  """

    n = 0 #integer to grab results from each list indexwise
    vanIds = [] #initiate list of vanIds
    missing_persons = [] #a list of indexes of unmatched persons
    max_index = len(first_last[0])
    for voter in range(max_index): 
        search = "/people?firstName=" + str(first_last[0][n]) + "&lastName=" + str(first_last[1][n]) #preparing /get request
        response = call_get(search) #api call
        response = response.json() #parse response to JSON
        voterfiles = response["items"] #list of dicts
        print("search " + str(n) + " out of expected " + str(max_index-1))

        if response["count"] == 1:
                voterfile = voterfiles[0] #unnest dict from list of dict
                vanIds.append(voterfile["vanId"])

        elif response["count"] == 0 or response["count"] > 1:
            missing_persons.append(n)

        else:
            print("INCORRECT INPUT")
        n = n + 1 #move to next position in index

    return([vanIds,missing_persons])

def first_last_ward(first_last_ward):
    """
    a series of /people get requests iterating over each entry in a list of search parameters (a list of lists)

    args: search_for: a list of lists, the first list consisting of first names, the second list consisting of last names, and the third list consisting of ward, each list must be matched indexwise, i.e., each index represents one search / one person to search for
            ALL STRING DATA INSIDE LISTS

    returns: list: list[0] = vanIds: listthe VANIDS successfully matched with search terms
                   list[1] = missing_persons: list, index numbers of unsuccessful matches, in terms of the index of the nested lists in search_for
    """
    n = 0
    vanIds = []
    missing_persons = [] #a list of index numbers of unmatched persons
    max_index = len(first_last_ward[0])
    for voter in range(max_index): #if missing first record, the issue is with loading the csv in csv_to_strings(), which assumes headers
        sleep(0.2) #5 requests per second is the reccommended api throttling
        search = "/people?firstName=" + first_last_ward[0][n] + "&lastName=" + first_last_ward[1][n] + city_clause + "&$expand=districts" #preparing method string, including district information
        ward = str(first_last_ward[2][n]) #save ward as a local variable to avoid calls to nested lists in iteration
        response = call_get(search) #api call
        response = response.json()
        voterfiles = response["items"]
        print("search " + str(n+1) + " out of expected " + str(max_index)) #debugging / progress message
        count = response["count"]
        if count >= 1: #a /people search will always return a "count" variable, /people/vanId search does not
            for voterfile in voterfiles: #iterate over the search results, works even if len(voterfiles) = 1
                add_check = len(vanIds)

                if match_ward(voterfile, ward): #if the voterFile contains the correct ward 
                    vanIds.append(voterfile["vanId"])

            if add_check == len(vanIds): #if no items were added to vanIds, add index position to missing_persons
                missing_persons.append(n)
                        


        elif count == 0: #if no results,
            missing_persons.append(n) #add index position to missing_persons
        else:
            print("INCORRECT INPUT") 
        n = n + 1 #incriment index position
    return([vanIds,missing_persons])


