# -*- coding: utf-8 -*-
"""
@About - Indeed template
@author: Sanjay Tanneeru
"""

def JsonDataframe(listObj):
    import json,pandas as pd
    
    CompanyFinalLink    = []
    JobTitle            = []
    Company             = []
    Location            = []
    Jobdescription      = []
    PostedDate          = []
    JobPostedDate       = []
    JobCategory         = []
    DateScraped         = []
    
    if len(listObj) != 0:
        for i in range(len(listObj)): 
            if listObj[i] == None:
                continue
            else:     
                parsejson = json.loads(listObj[i])
                MainCatVal= ''
                MainCatVal = list(parsejson)[-1]            
                for j in range(len(parsejson[MainCatVal])):
                    CompanyFinalLink.append(parsejson.get(MainCatVal)[j].get('CompanyFinalLink'))
                    JobTitle.append(parsejson.get(MainCatVal)[j].get('JobTitle'))
                    Company.append(parsejson.get(MainCatVal)[j].get('Company'))
                    Location.append(parsejson.get(MainCatVal)[j].get('Location'))
                    Jobdescription.append(parsejson.get(MainCatVal)[j].get('Jobdescription'))
                    PostedDate.append(parsejson.get(MainCatVal)[j].get('PostedDate'))
                    JobPostedDate.append(parsejson.get(MainCatVal)[j].get('JobPostedDate'))
                    JobCategory.append(parsejson.get(MainCatVal)[j].get('JobCategory'))
                    DateScraped.append(parsejson.get(MainCatVal)[j].get('DateScraped'))
             
    outputForIndeed = pd.DataFrame( columns = ['CompanyJobURL', 'Company_Name', 'Job_Title', 'Job_Location', \
                                               'Job_Category','Job_Description', 'DateScraped', 'Job_Posted_Text', 
                                               'Job_Posted_Date']) 

    outputForIndeed['CompanyJobURL']= CompanyFinalLink
    outputForIndeed['Job_Title']=JobTitle
    outputForIndeed['Company_Name']= Company
    outputForIndeed['Job_Location']= Location
    outputForIndeed['Job_Description']= Jobdescription
    outputForIndeed['Job_Posted_Text']= PostedDate
    outputForIndeed['Job_Posted_Date']= JobPostedDate
    outputForIndeed['Job_Category']= JobCategory
    outputForIndeed['DateScraped']= DateScraped
    
    return outputForIndeed

def ProcessLogFile(T1=None, T2=None, msg=None, Cnty=None):
    file = open(T1 + '\\' + T2 +'.txt','a')
    if Cnty == '':
        file.write('\n'+ msg)
    else:
        file.write('\n'+ msg + ' -- ' + Cnty)
    file.close()
 
def updateCSV(ProcessingFolder,LOG_PATH, upList, filename, text):
    import pandas as pd, time, traceback
    
    TempIngestionFolder = ProcessingFolder
    SET_INDEED_LOG_PATH = LOG_PATH

    XRefSuccessful = False
    
    re_try = 1
    while re_try < 5:
        try:
            with open(TempIngestionFolder + '\\' + filename,'r') as infile:
                Cat_row_num = pd.read_csv(infile)
                for u in upList:
                    
                    try:
                        lineno = Cat_row_num.loc[Cat_row_num[Cat_row_num.keys()[1]] == u.title()].index[0] + 2
                    except:
                        try:
                            lineno = Cat_row_num.loc[Cat_row_num[Cat_row_num.keys()[1]] == u].index[0] + 2
                        except:
                            continue
                    
                    fro = open(TempIngestionFolder + '\\' + filename, "rb")
                
                    current_line = 0
                    while current_line < lineno - 1:
                        fro.readline()
                        current_line += 1
                
                    seekpoint = fro.tell()
                    frw = open(TempIngestionFolder + '\\' + filename, "r+b")
                    frw.seek(seekpoint, 0)
                    
                    # read the line we want to update
                    line = fro.readline()
                    chars = line[0: (len(line) - 1)-1] + text.encode('utf-8') + line[(len(line) - 1)-1:]
                    
                    while chars:
                        frw.write(chars)
                        chars = fro.readline()
                
                    fro.close()
                    frw.truncate()
                    frw.close()
                infile.close()
            
            XRefSuccessful = True
            return XRefSuccessful
        except Exception:
            Error = traceback.format_exc()
            ProcessLogFile(TempIngestionFolder, SET_INDEED_LOG_PATH, '\nRetry: '+ str(re_try), '')
            time.sleep(5)
            re_try+=1
            continue
        
    if XRefSuccessful == False:
        ProcessLogFile(TempIngestionFolder, SET_INDEED_LOG_PATH, '\nFile Open Error: '+ str(Error) + '\nList to update' + str(upList), '')

    return XRefSuccessful

def main():
    
    #Import packages
    import os,sys,re
    os.chdir('<Navigate to folder where modules exists>')
    import pandas as pd, urllib.request, datetime, gc, traceback, time,Indeed_Module_Category,Indeed_Module_MainJobs
    from bs4 import BeautifulSoup
    
    #Get all arguments  - when you run in command line, you need to provide 4 arguments below
    Base_URL            = str(sys.argv[1]) #Base url for the country
    XrefTable           = str(sys.argv[2]) #Temp table to track the processing
    FinalTempTable      = str(sys.argv[3]) #Temp table to store data into which is csv format
    TempIngestionFolder = str(sys.argv[4]) #Processing folder
    
    #Get processing country code
    ProcessCountry = ''
    
    if re.search('indeed.com',Base_URL):#usa
        ProcessCountry = 'US'
    
    #Create Indeed log file
    SET_INDEED_LOG_PATH = 'ProcessLog'

    ProcessLogFile(TempIngestionFolder, SET_INDEED_LOG_PATH, 'Started Processing Indeed', ProcessCountry + ' on ' + datetime.datetime.today().strftime("%m/%d/%Y") + ' at '  + datetime.datetime.today().strftime("%H:%M:%S"))
    
    #explicit garbage collection
    gc.collect()
            
    #Begin processing by getting job categories
    BaseURL             = Base_URL
    Category            = urllib.request.urlopen(BaseURL)
    soupIndeedCategory  = BeautifulSoup(Category, 'html.parser')
    AllCategory         = soupIndeedCategory.find('table', id='categories')
    FinalAllCategory    = AllCategory.findAll('td')
    
    #Final jobs category links to process
    linksprocess = [i.find('a')['href'].replace('/browsejobs','') for i in FinalAllCategory ]
    
    #Get sub-category links using category -- uses "Indeed_Module_Category.py" modules to fetch all subcategories from website
    TempRefList = Indeed_Module_Category.Mulprocessing(linksprocess,Base_URL)
    
    ProcessLogFile(TempIngestionFolder, SET_INDEED_LOG_PATH, 'Processed Category Reference', ProcessCountry)
    
    #Parse and write to dataframe
    IMainCategory = []
    IMainSubCategory = []
    IMainURL = []

    if len(TempRefList) != 0:
        for i in range(len(TempRefList)):
            if TempRefList[i] != None:
                MainCatVal= ''
                MainCatVal = list(TempRefList[i])[-1]
                for j in range(len(TempRefList[i][MainCatVal])):
                    IMainCategory.append(MainCatVal)
                    IMainSubCategory.append(TempRefList[i].get(MainCatVal)[j].get('SubCategory'))
                    IMainURL.append(TempRefList[i].get(MainCatVal)[j].get('CategoryURL'))

    CategoryXref = pd.DataFrame( columns = [ProcessCountry+'MainCategory', ProcessCountry+'MainSubCategory',\
                                              ProcessCountry+'MainCategoryURL', 'DateScraped', 'Flag']) 

    CategoryXref[ProcessCountry+'MainCategory']=IMainCategory
    CategoryXref[ProcessCountry+'MainSubCategory']=IMainSubCategory
    CategoryXref[ProcessCountry+'MainCategoryURL']=IMainURL
    CategoryXref['DateScraped']=datetime.date.today().strftime('%m/%d/%Y')
    
    #Drop duplicates from dataframe and write to CSV
    CategoryXref.drop_duplicates().to_csv(TempIngestionFolder + '\\' + XrefTable +'.csv',mode='w',index=False,header=True,encoding='utf-8-sig')
    
    ProcessLogFile(TempIngestionFolder, SET_INDEED_LOG_PATH, 'Stored Category Ref to '+ XrefTable,'')
    
    #Read from Xref
    file = open(TempIngestionFolder + '\\' + XrefTable +'.csv')
    XrefData = pd.read_csv(file, skipinitialspace=True)
    file.close()
    
    ProcessLogFile(TempIngestionFolder, SET_INDEED_LOG_PATH, 'Indeed ' + ProcessCountry +\
                       ' Category count: ' + str(len(XrefData.iloc[:, 0].unique())),'')

    if len(XrefData) > 0:
        pass
    else:
        ProcessLogFile(TempIngestionFolder, SET_INDEED_LOG_PATH, '\nError: No Urls found', '')
        return 
    
    #Build string to get URL from csv for MaincategoryURL based on country 
    CountryStr = ProcessCountry + 'MainCategoryURL'
    
    #Get position of that column in dataframe
    MainCategoryPos = XrefData.columns.get_loc(CountryStr)
     
    #Based on position of the columns, get urls
    ToProcessURL = XrefData.iloc[:, MainCategoryPos]
    
    #Reset index for processing flow
    ToProcessURL = ToProcessURL.reset_index(drop=True)
    
    ProcessLogFile(TempIngestionFolder, SET_INDEED_LOG_PATH, 'Total Urls to process: '+ str(len(ToProcessURL)), '')
    
    ProcessLogFile(TempIngestionFolder, SET_INDEED_LOG_PATH, 'Begin batch processing...', '')
    
    #Batch Process 5 urls at same time. 0-5, 5-10, 10-15 ..... until end
    URLS_TO_PROCESS = []
    Breakpoint = 0
    BATCH_LOAD_COUNTER = 5
    
    for links in range(len(ToProcessURL)):
        
        #Load 5 urls per batch for processing
        if links < BATCH_LOAD_COUNTER:
            Breakpoint +=1
            URLS_TO_PROCESS.append(ToProcessURL[links])
            
            #Check if last URL per batch is loaded for batch processing
            if (Breakpoint + 1) == 6:
                
                #Process Main urls
                gc.collect()
                
                try:
                    ProcessedJobsList = Indeed_Module_MainJobs.mprocessing(URLS_TO_PROCESS)
                except Exception:
                    ProcessLogFile(TempIngestionFolder, SET_INDEED_LOG_PATH, '\nError: '+ str(traceback.format_exc()), '')
                    return
                
                ProcessLogFile(TempIngestionFolder, SET_INDEED_LOG_PATH, \
                            '--Batch Processed ' + str(links+1) + ' url(s) at ' + datetime.datetime.today().strftime("%H:%M:%S"), '')
                            
                #Process json and write to DF
                DbObj = JsonDataframe(ProcessedJobsList)
                
                #Write to table - csv
                re_try = 1
                TableSuccessful = False
                while re_try < 5:
                    try:
                        if (os.path.isfile(TempIngestionFolder + '\\' + FinalTempTable + '.csv')) != True:
                            if BATCH_LOAD_COUNTER == 5:
                                DbObj.drop_duplicates().to_csv(TempIngestionFolder + '\\' + FinalTempTable +'.csv',mode='w',index=False,header=True,encoding='utf-8-sig')
                            else:
                                DbObj.drop_duplicates().to_csv(TempIngestionFolder + '\\' + FinalTempTable +'.csv',mode='a',index=False,header=False,encoding='utf-8-sig')
                        else:
                            DbObj.drop_duplicates().to_csv(TempIngestionFolder + '\\' + FinalTempTable +'.csv',mode='a',index=False,header=False,encoding='utf-8-sig')
                        
                        TableSuccessful = True
                        break
                    
                    except Exception:
                        Error = traceback.format_exc()
                        ProcessLogFile(TempIngestionFolder, SET_INDEED_LOG_PATH, '\nRetry: '+ str(re_try), '')
                        time.sleep(4) #wait and try again
                        re_try+=1
                        continue
                    
                if TableSuccessful != True:
                    ProcessLogFile(TempIngestionFolder, SET_INDEED_LOG_PATH, '\nFile Open Error: '+ str(Error), '')
                    return
                
                #Get processed category list to update Flag = 1 in csv
                updatecsvlist = []
                updatecsvlist = [(DbObj['Job_Category'].unique()).item(cat).replace('jobs','').strip() for cat in range(len(DbObj['Job_Category'].unique()))]
                
                #Update the csv
                Update_Val = updateCSV(TempIngestionFolder, SET_INDEED_LOG_PATH, updatecsvlist, XrefTable + '.csv', '1')
                
                if Update_Val != True:
                    return
    
                ProcessLogFile(TempIngestionFolder, SET_INDEED_LOG_PATH, \
                               '--Saved Processed Batch to '+ FinalTempTable + '.csv', '')
                
                Breakpoint = 0 #Reset variable
                BATCH_LOAD_COUNTER += len(URLS_TO_PROCESS)
                URLS_TO_PROCESS = [] #Reset variable
                continue
            
            elif links+1 == len(ToProcessURL):
                
                #Process Main urls
                gc.collect()
                
                try:
                    ProcessedJobsList = Indeed_Module_MainJobs.mprocessing(URLS_TO_PROCESS)
                except Exception:
                    ProcessLogFile(TempIngestionFolder, SET_INDEED_LOG_PATH, '\nError: '+ str(traceback.format_exc()), '')
                    return
                
                ProcessLogFile(TempIngestionFolder, SET_INDEED_LOG_PATH, \
                               '--Batch Processed ' + str(links+1) + ' url(s) at ' + datetime.datetime.today().strftime("%H:%M:%S"), '')
                
                #Process json and write to DF
                DbObj = JsonDataframe(ProcessedJobsList)
                
                #Write to table - csv
                re_try = 1
                TableSuccessful = False
                while re_try < 5:
                    try:
                        if (os.path.isfile(TempIngestionFolder + '\\' + FinalTempTable + '.csv')) != True:
                            if BATCH_LOAD_COUNTER == 5:
                                DbObj.drop_duplicates().to_csv(TempIngestionFolder + '\\' + FinalTempTable +'.csv',mode='w',index=False,header=True,encoding='utf-8-sig')
                            else:
                                DbObj.drop_duplicates().to_csv(TempIngestionFolder + '\\' + FinalTempTable +'.csv',mode='a',index=False,header=False,encoding='utf-8-sig')
                        else:
                            DbObj.drop_duplicates().to_csv(TempIngestionFolder + '\\' + FinalTempTable +'.csv',mode='a',index=False,header=False,encoding='utf-8-sig')
                        
                        TableSuccessful = True
                        break
                    
                    except Exception:
                        Error = traceback.format_exc()
                        ProcessLogFile(TempIngestionFolder, SET_INDEED_LOG_PATH, '\nRetry: '+ str(re_try), '')
                        time.sleep(4) #wait and try again
                        re_try+=1
                        continue
                    
                if TableSuccessful != True:
                    ProcessLogFile(TempIngestionFolder, SET_INDEED_LOG_PATH, '\nFile Open Error: '+ str(Error), '')
                    return
                
                #Get processed category list to update Flag = 1 in csv
                updatecsvlist = []
                updatecsvlist = [(DbObj['Job_Category'].unique()).item(cat).replace('jobs','').strip() for cat in range(len(DbObj['Job_Category'].unique()))]
                
                #Update the csv
                Update_Val = updateCSV(TempIngestionFolder, SET_INDEED_LOG_PATH, updatecsvlist, XrefTable + '.csv', '1')

                if Update_Val != True:
                    return
                
                ProcessLogFile(TempIngestionFolder, SET_INDEED_LOG_PATH, \
                               '--Saved Processed Batch to '+ FinalTempTable + '.csv', '')
                break

    ProcessLogFile(TempIngestionFolder, SET_INDEED_LOG_PATH,'Total Urls processed : ' + str(links+1), '')
    
    #To adjust backslash, replace to modify string and save to new variable
    TempIngestionFolder2 = TempIngestionFolder.replace('\\\\','\\')
    
    ReadCSV = TempIngestionFolder2.replace('\\','\\\\') + r'\\' + FinalTempTable + '.csv'
    file = open(ReadCSV)
    TempTableCount = pd.read_csv(ReadCSV , skipinitialspace=True)
    file.close()

    ProcessLogFile(TempIngestionFolder, SET_INDEED_LOG_PATH, 'Total Jobs : ' + str(len(TempTableCount)), '')
    
    ProcessLogFile(TempIngestionFolder, SET_INDEED_LOG_PATH, 'Finished processing Indeed', ProcessCountry + ' on ' + datetime.datetime.today().strftime("%m/%d/%Y") + ' at '  + datetime.datetime.today().strftime("%H:%M:%S"))
    
if __name__  == '__main__':
    from datetime import datetime
    Indeedprocess_start_date = datetime.now().strftime("%m/%d/%Y")
    main()