# -*- coding: utf-8 -*-
"""
@About - Module for indeed multiprocessing - Main jobs
--This modules leverages multiprocessing, so it processes 5 job categories urls at the same time
@author: Sanjay Tanneeru
"""
import multiprocessing,re,urllib.request,traceback,random
from bs4 import BeautifulSoup
from datetime import datetime,timedelta,date
import time

def calculate(func, args):
    try:
        result = func(*args)
    except:
        time.sleep(5)
        result = func(*args)
        
    return (result)

def calculatestar(args):
    try:
        return calculate(*args)
    except:
        return calculate(*args)

def URL_Mulprocessing(URLlist=None):  
    
    try:
        LogFName = ''
        
        if re.search('indeed.com',URLlist):#usa
            LogFName = 'Indeed_US'
            
        f = open((r"C:\Dell_System_Files\DellLOG\ ").strip() + LogFName + "_" + datetime.now().strftime("%m%d%Y_%H%M") + ".txt", "w")
        log_file_Path = ("C:\Dell_System_Files\DellLOG\ ").strip() + LogFName + "_" + datetime.now().strftime("%m%d%Y_%H%M") + ".txt"
        
        CompanyFinalLink    = []
        JobTitle            = []
        Company             = []
        Location            = []
        Jobdescription      = []
        PostedDate          = []
        JobPostedDate       = []
        JobCategory         = []
        DateScraped         = []
            
        
        #Retry logic for links
        re_try = 1
        while re_try < 5:        
            try:
                try:
                    Basehtml = urllib.request.urlopen(URLlist)
                except:
                    Basehtml = urllib.request.urlopen(URLlist).read()
                if Basehtml.getcode() == 200:
                    break;
            except:
                time.sleep(2)
                re_try+=1
        
            if re_try == 4:
                time.sleep(5)
                continue
        
        CategoryUrlhtmlsoup = BeautifulSoup(Basehtml,'html.parser')    
        CategoryUrlPagesVar = CategoryUrlhtmlsoup.find('div', class_='resultsTop')
        try:
            FinalCategoryUrlPagesVar = CategoryUrlPagesVar.find('div',id='searchCount')
        except:
            CompanyFinalLink = []
            
            SubCatList = ''
            for k in range(len(CompanyFinalLink)):
                SubCatList +=  '{"CompanyFinalLink": "' +CompanyFinalLink[k].strip()+ '",' \
                                '"JobTitle": "' +JobTitle[k].strip()+ '",' \
                                '"Company": "' +Company[k].strip()+ '",' \
                                '"Location": "' +Location[k].strip()+ '",' \
                                '"Jobdescription": "' +Jobdescription[k].strip()+ '",' \
                                '"PostedDate": "' +PostedDate[k].strip()+ '",' \
                                '"JobPostedDate": "' +JobPostedDate[k].strip()+ '",' \
                                '"JobCategory": "' +JobCategory[k].strip()+ '",' \
                               '"DateScraped": "' +DateScraped[k] + '"},' 
                         
            SubCatList = '{"' + 'Hiring' + '":[' + SubCatList[:-1] +']}'
            return SubCatList
            
           
        #Get total number of pages to process
        #First get raw pages 
        try:
            TJobs = FinalCategoryUrlPagesVar.text
            TotalJobsPages = re.split(' ',str(TJobs))
            
            for i in TotalJobsPages:
                try:
                    if re.search(r"(\d),(\d)",i):
                        TotalJobsPages = i
                    elif re.search(r"(\d)",i):
                        TotalJobsPages = i
                except:
                    continue  
        except:        
            TotalJobsPages = 1
        
        #Process raw pages number by 12 
        #each page displays 12 jobs
        if re.findall(",", str(TotalJobsPages)):
            TotalJobsPages = re.sub(",", "",TotalJobsPages)
            CatFinalPages = (int(TotalJobsPages)//12) + 1        
        elif re.findall(".", str(TotalJobsPages)):        
            TotalJobsPages = int(str(TotalJobsPages).replace(".", ""))
            CatFinalPages = (int(TotalJobsPages)//12) + 1
        else:
            CatFinalPages = (int(TotalJobsPages)//12) + 1     
    
        #Data capture begins based on pages to process
        if CatFinalPages == 0:
            return 
        
        #Total pages Indeed lets us process is 100 pages
        elif CatFinalPages > 100:
            #Page number increments by 10 starting with 0
            #Where 0 is page 1 and so on..
            RangeVal = 1000
        else:
            #This loop only processes less than 100 pages
            RangeVal = CatFinalPages * 10
            
        for pages in range(0, RangeVal, 10):
            f = open(log_file_Path, "a")
            f.write("\nProcessing "+ str(pages))
            
            time.sleep(random.randint(3,5))
                        
            Mainurl = URLlist + str(pages)
            
            try:
                html = urllib.request.urlopen(Mainurl)
            except:
                continue
            
            try:
                responseURL = html.geturl()
                html  = urllib.request.urlopen(responseURL)
            except:
                continue
            
            ResultsColsoup = BeautifulSoup(html,'html.parser')       
            resultslist = ResultsColsoup.find_all(id = 'resultsCol')
            
            try:
                listofjobsvar = resultslist[0].findAll('div', class_='jobsearch-SerpJobCard')
            except:
                continue
     
            for l in range(len(listofjobsvar)):
                
                #Job URL
                try:
                    CompanyFinalLink.insert(0, re.search(r'[^?]+', URLlist).group().replace('/jobs','') + listofjobsvar[l].find('a')['href'].strip())
                except:
                    CompanyFinalLink.insert(0, '')
                
                #Job Title
                try:
                    JobTitle.insert(0, listofjobsvar[l].find('a', class_='jobtitle').text.strip())
                except:
                    JobTitle.insert(0, '')
                
                #Company name
                try:
                    Company.insert(0,listofjobsvar[l].find('span', class_='company').text.strip())
                except:
                    Company.insert(0, '')
                
                #location
                try:
                    try:
                        Location.insert(0, listofjobsvar[l].find('div', class_='location').text.strip())
                    except:
                        Location.insert(0, listofjobsvar[l].find('span', class_='location').text.strip())
                except:
                    Location.insert(0, '')
                
                #Description not capturing cause its taking up lot of size
                Jobdescription.insert(0, '')
                
                #Date
                try:
                    PostedDate.insert(0, listofjobsvar[l].find('span', class_='date').text.strip())
                except:
                    PostedDate.insert(0, '') 
                
                #Tag based on country
                PostedDateTag = 'hours'
                    
                try:
                    #Gets today's date
                    if re.findall(PostedDateTag, listofjobsvar[l].find('span', class_='date').text.strip()):
                        JobPostedDate.insert(0, datetime.today().strftime('%Y-%m-%d'))
                    else:
                        #Gets value and calculates date
                        number = listofjobsvar[l].find('span', class_='date').text
                        number = number.split(' ')[0].strip()
                        JobPostedDate.insert(0, (datetime.today() - timedelta(days=int(number))).strftime('%Y-%m-%d'))
                            
                except:
                    JobPostedDate.insert(0, '')   
               
                #GermanySub Category
                try:                    
                    URLsubCat = ((re.search(r'=\b[^&]+', URLlist).group()).replace('=','')).replace('+',' ')                    
                    JobCategory.insert(0, URLsubCat + ' jobs')
                except:
                    JobCategory.insert(0, '')
                
                #Date scraped
                CurrentDatDate = date.today().strftime('%m/%d/%Y')
                DateScraped.insert(0, CurrentDatDate)
        
        f.close()
        
        # Process list for quotes within strings    
        CompanyFinalLink = [i.replace("\"",'*') if "\"" in i else i for i in CompanyFinalLink]
        Company = [i.replace("\"",'*') if "\"" in i else i for i in Company]
        JobTitle = [i.replace("\"",'*') if "\"" in i else i for i in JobTitle]
        Location = [i.replace("\"",'*') if "\"" in i else i for i in Location]
        
        #Creating a json like structure or dict for parsing captured data
        SubCatList = ''
        for k in range(len(CompanyFinalLink)):
            SubCatList +=  '{"CompanyFinalLink": "' +CompanyFinalLink[k].strip()+ '",' \
                            '"JobTitle": "' +JobTitle[k].strip()+ '",' \
                            '"Company": "' +Company[k].strip()+ '",' \
                            '"Location": "' +Location[k].strip()+ '",' \
                            '"Jobdescription": "' +Jobdescription[k].strip()+ '",' \
                            '"PostedDate": "' +PostedDate[k].strip()+ '",' \
                            '"JobPostedDate": "' +JobPostedDate[k].strip()+ '",' \
                            '"JobCategory": "' +JobCategory[k].strip()+ '",' \
                           '"DateScraped": "' +DateScraped[k] + '"},' 
                     
        SubCatList = '{"' + 'Hiring' + '":[' + SubCatList[:-1] +']}'
        
        #Removing escape characters
        jsonOBJ = ''
        decodelist = ["\\","\b","\f","\n","\r","\t"]
        for i in range(len(decodelist)):
            if decodelist[i] in SubCatList:
                jsonOBJ = SubCatList.replace(decodelist[i],'')
                SubCatList = jsonOBJ
                
        f = open(log_file_Path, "a")
        # f.write("\nFinished Processing Json")
        f.write("\n---Finished Processing " + ((re.search(r'=\b[^&]+', URLlist).group()).replace('=','')).replace('+',' '))
        f.close()
        
        return SubCatList  
    except Exception:
        f = open(log_file_Path, "a")
        f.write("\nError "+ str(traceback.format_exc()))
        return


def mprocessing(PlistArgs=None):
    PROCESSES = 5
    FinalResults= []
    
    with multiprocessing.Pool(PROCESSES) as pool:
        TASKS = [(URL_Mulprocessing, (i,)) for i in PlistArgs]
        for x in pool.map(calculatestar, TASKS):
            FinalResults.append(x)
    
    return FinalResults
        
if __name__ == '__main__':
    multiprocessing.freeze_support()
    mprocessing()