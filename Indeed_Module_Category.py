# -*- coding: utf-8 -*-
"""
@About - Module for indeed multiprocessing - Job Category
--This modules leverages multiprocessing, so it processes 5 job categories urls at the same time
@author: Sanjay Tanneeru
"""

#Import packages
import urllib.request,re,json
from bs4 import BeautifulSoup
from multiprocessing import Process, Queue, freeze_support


def processurls(url,country):
    
    try:        
        BaseReq = urllib.request.Request(country + url, headers = {'User-Agent': 'Mozilla/5.0'})
        GetSubCategory = urllib.request.urlopen(BaseReq)
    except:
        return
        
    HTMLSubCategory = BeautifulSoup(GetSubCategory,'html.parser')
    
    MainCat = re.sub("Jobs", "", HTMLSubCategory.find('table',id='browsejobs_main_content').find('h1').text).strip()
    
    FinalSubCategory = HTMLSubCategory.find('table',id='titles')
    FinalSubCategory2 = FinalSubCategory.findAll('a', class_="jobTitle")
    SubCatList = ''
    
    for k in range(len(FinalSubCategory2)):
        Categoryurl = country.replace('/browsejobs','') + FinalSubCategory2[k].get('href') + "&fromage=7&start="
        SubCatList +=  '{"SubCategory": "' +FinalSubCategory2[k].text.replace('jobs','').strip()+ '",' \
                       '"CategoryURL": "' +Categoryurl + '"},' 

    SubCatList = '{"' + MainCat + '":[' + SubCatList[:-1] +']}'
    
    return json.loads(SubCatList)

# Function run by worker processes
def worker(input, output):
    #lock.acquire()
    for func, args in iter(input.get, 'STOP'):
        result = calculate(func, args)
        output.put(result)
    #lock.release()
    
# Function used to calculate result
def calculate(func, args):  
    result = func(*args)
    return (result)

def Mulprocessing(Plist=None,CountryCode=None):
    freeze_support()
    MainSubCategoryAll= []

    NUMBER_OF_PROCESSES = 4
    All_TASKS = [(processurls, (i,CountryCode)) for i in Plist]
    
    # Create queues
    task_queue = Queue()
    done_queue = Queue()
    
    # Submit tasks
    for task in All_TASKS:
        task_queue.put(task)

    # Start worker processes
    for i in range(NUMBER_OF_PROCESSES):
        Process(target=worker, args=(task_queue, done_queue)).start()

    # Get results
    for i in range(len(All_TASKS)):
        MainSubCategoryAll.append(done_queue.get())
        
    # Tell child processes to stop
    for i in range(NUMBER_OF_PROCESSES):
        task_queue.put('STOP')
        
    return MainSubCategoryAll

if __name__ == '__main__':
    Mulprocessing()
    

    
        
    