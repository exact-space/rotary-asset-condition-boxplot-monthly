import os,platform
# current_dir = os.getcwd()
# target_dir = '/Users/nijin/exsp'
# os.chdir(target_dir)
import app_config.app_config as cfg
config = cfg.getconfig()
import timeseries.timeseries as ts
qr = ts.timeseriesquery()
meta = ts.timeseriesmeta()
# os.chdir(current_dir)
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
# import seaborn as sns
import datetime as datetime
import calendar
from datetime import timedelta
import json, requests
from pprint import pprint
import time
from functools import reduce
from flask import Flask,jsonify,request
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
from apscheduler.schedulers.blocking import BlockingScheduler
from dateutil.relativedelta import relativedelta
import warnings
warnings.filterwarnings("ignore")
# from memory_profiler import profile
# from apscheduler.schedulers.blocking import BlockingScheduler
from logzero import logger

version = platform.python_version().split(".")[0]
if version == "3":
  import app_config.app_config as cfg
elif version == "2":
  import app_config as cfg
config = cfg.getconfig()

# base_url='https://data.exactspace.co/exactapi'
# config["api"]["meta"]=base_url
base_url = config["api"]["meta"]
unitsId = os.environ.get("UNIT_ID") if os.environ.get("UNIT_ID")!=None else None
# unitsId="628dd242c78e4c5d0f3b90cf"
if unitsId==None:
    print("no unit id passed")
    exit()

config = cfg.getconfig()[unitsId]
print(config)

##################################get unitsId ###############################################################

def getUnitsId(base_url):
    url = base_url+ '/units?filter={"where":{"name":{"nlike":"test","options":"i"},"stackDeploy":true},"fields":["id"]}'
    resp = requests.get(url)
    jjson = json.loads(resp.content)
    unitsId = [i['id'] for i in jjson]
    return unitsId

# unitsId=getUnitsId(base_url)
# sufix = unitsId_+"boxplot"
# metric_list = [prefix + str(ids) for ids in unitIds]
# print(metric_list)
########################################Fetching data#####################################################################################


def getData1(taglist,timeType,qr, key = None,unitId = None, aggregators = [{"name":"avg","sampling_value":1,"sampling_unit":"minutes"}]):

    qr.addMetrics(taglist)
    if(timeType["type"]=="date"):
        qr.chooseTimeType("date",{"start_absolute":timeType["start"], "end_absolute":timeType["end"]})

    elif(timeType["type"]=="relative"):
        qr.chooseTimeType("relative",{"start_unit":timeType["start"], "start_value":timeType["end"]})


    elif(timeType["type"]=="absolute"):
        qr.chooseTimeType("absolute",{"start_absolute":timeType["start"], "end_absolute":timeType["end"]})


    else:
        logger.info('Error')
        logger.info('Improper timetype[type]')

    if aggregators != None:
        qr.addAggregators(aggregators)

    if ((key) and (key == "simulation")):
        qr.submitQuery("simulation",unitId)
    else:
        key = None
        qr.submitQuery(key,unitId)


    qr.formatResultAsDF()
    try:

        df = qr.resultset["results"][0]["data"]
        return df
    except Exception as e:
        print('Data Not Found getData1 ', e)
        return pd.DataFrame()
    
    
    
    
###########################fetch all tags################################################################



def getallTags(unitsId,base_url):
    url=base_url +'/tagmeta?filter={"where":{"unitsId":"'+str(unitsId)+'"},"fields":["dataTagId"]}'
    # {"where":{"equipmentId":{"like":"63d2280bcf24c00007438ab9"},"unitsId":"61c1818371c20d4a206a2e35"}}
    res =requests.get(url)

    if res.status_code == 200:
        tags = json.loads(res.content)
#         print(tags)
        datatag=[]
        for i in tags:
#             print(i["dataTagId"])
            datatag.append(i["dataTagId"])
            

    print(len(set(datatag)))
    return list(set(datatag))



##########################fetch eqid#########################################################################
def fetchtagmeta(unitsId,tag,base_url):
    url = base_url +'/tagmeta?filter={"where": {"unitsId":"'+str(unitsId)+'","dataTagId":"'+str(tag)+'"},"fields":["equipmentId"]}'
    response = requests.get(url)
    #     print("response " ,response)
    # tagmeta = json.loads(response.content)
    # print(tagmeta)
    # return tagmeta[0]["equipmentId"]
    # #     return tagmeta
    if response.status_code == 200:
        # if tagmeta:
    #     print("response " ,response)
        tagmeta = json.loads(response.content)
        if tagmeta:
            if "equipmentId" in tagmeta[0]:

    #     if res.status_code == 200
            # print(tagmeta)
                return tagmeta[0]["equipmentId"]
            else:
                print("no equipmentId")
                pass
        #     return tagmeta
    else:
        return []
        pass
    
###############################box pl0t calculation###################################
def boxplot(df,tag,unitId,year):
    print("**********df*****************",df)
    if not df.empty:
        if tag in df.columns:
            print(tag)
            df=df[df[tag]<99999.0]
            if not df.empty:
                
                print("dataframe",df.head())
        #         df['year'] = pd.to_datetime(df['time'], format='%d-%m-%Y %H:%M').dt.year

            # Store the year in a variable (assuming all entries are from the same year)
        #         year = df['year'].iloc[0]

                print(df.head())
                lst=[]
                #     try:
                Min=round(float(df[tag].min()),2)
                q1 = round(float(np.quantile(df[tag], 0.25)),2)
                Med=round(float(df[tag].median()),2)
                q3=round(float(np.quantile(df[tag], 0.75)),2)
                Max=round(float(df[tag].max()),2)

                lst.append( {
                    "name":unitId+"_boxplot",
                    "datapoints":[[0,Min]],
        #             "timestamp":0,
        #             "value":Min,
                    "tags":{"dataTagId" : tag, "period":str(year),"calculationType":"Min"}
                    })
                lst.append( {
                    "name":unitId+"_boxplot",
                    "datapoints": [[0,q1]],
        #             "timestamp":0,
        #             "value":q1,
                    "tags":{"dataTagId" : tag, "period":str(year),"calculationType":"q1"}
                    })
                lst.append({
                    "name":unitId+"_boxplot",
                    "datapoints": [[0,Med]],
        #             "timestamp":0,
        #             "value":med ,
                     "tags":{"dataTagId" : tag, "period":str(year),"calculationType":"Med"}
                    })
                lst.append({
                    "name":unitId+"_boxplot",
                    "datapoints": [[0,q3]],
    #                 "timestamp":0,
    #                 "value":q3,
                     "tags":{"dataTagId" : tag, "period":str(year),"calculationType":"q3"}
                    })
                lst.append({
                    "name":unitId+"_boxplot",
                    "datapoints": [[0,Max]],
        #             "timestamp":0,
        #             "value":Max,
                     "tags":{"dataTagId" : tag, "period":str(year),"calculationType":"Max"}

                    })

        #         postscylla(lst)
            else:
                print("no data")
              
                lst=[]


        else:
            print("no data")
            
            lst=[]




    #     lst=[Min,q1,med,q3,Max]
        #     except :
        #         lst=[0,0,0,0,0]
        return lst
##################################################fetchlimits###################################################################################

def fetchlimits(unitsId,tag,base_url):
    url = base_url +'/tagmeta?filter={"where": {"unitsId":"'+str(unitsId)+'","dataTagId":"'+tag+'"},"fields":["unitsId","dataTagId","equipmentId", "limRangeHi","limRangeLo","benchmark","benchmarkLoad"]}'
    response = requests.get(url)
    #     print("response " ,response)
    dct={}
    tagmeta = json.loads(response.content)
#     for i in btagmeta[0]['benchmarkLoad']
#     bk=pd.DataFrame(tagmeta[0]['benchmarkLoad'])
#     print(tagmeta[0]['benchmarkLoad'])
    loadbkt=[int(x) for x in tagmeta[0]['benchmarkLoad'].keys() if x not in["status",'end','start','lastRunHistory','lastRun'] and  tagmeta[0]['benchmarkLoad'][x]['status'] =="valid"]
    loadbkt.sort()
    q95_values = [tagmeta[0]['benchmarkLoad'][str(key)]['q95'] for key in  loadbkt]
    q005_values=[tagmeta[0]['benchmarkLoad'][str(key)]['q005'] for key in  loadbkt]
#     print( q95_values)
#     print(q005_values)
    min_q005 = min(q005_values)
    max_q95 = max(q95_values)
    upperVlaue=max_q95
#     print(min_q005,max_q95)
    rollingSD=round(tagmeta[0]["benchmark"]["rollingSd"],2)
#     print(rollingSD)
    if "zeroLimit" in tagmeta[0]["benchmark"].keys():
        dct["zeroLimit"]=tagmeta[0]["benchmark"]["zeroLimit"]
    else:
        pass
    if "rollingSd" in tagmeta[0]["benchmark"].keys():
        upperValue=max_q95+ 50*rollingSD
        lowerValue=min_q005-50*rollingSD
        dct["upperValue"]=upperValue
        dct["lowerValue"]=lowerValue
    else:
        pass
        
        
    dct['limRangeHi']=tagmeta[0]['limRangeHi']
    dct['limRangeLo'] = tagmeta[0]['limRangeLo']
    
    print(dct)
    return dct




##################################Removing Outliers###################################################################
def removingOutliers(df,statetag,validload,unitsId,tag,base_url):
  
    try:
        df = df[(df["statetag"] == 1) & (df["validload"] == 1)]
    except:
        print("No stateTag validLoad")
      
    try:     
        lim=fetchlimits(unitsId,tag,base_url)

        limRangeHi=lim["limRangeHi"]
        limRangeLo=lim["limRangeLo"]
        zeroLimit=lim["zeroLimit"]
        upperValue=lim['upperValue']
        lowerValue=lim['lowerValue']
        if zeroLimit == "positive":
            df = df[df[tag] >= 0]
        elif  zeroLimit == "negative":
            df = df[df[tag]<= 0]


        df=df[df[tag]<=limRangeHi]
        df=df[df[tag]>=limRangeLo]
        df==df[df[tag]<=upperValue]
        df==df[df[tag]>=lowerValue]   
    except:
        df=df
        
    return df     






##################################box plot for 1 yr ,1 month, 7 days ############################################
# @profile
def boxplot_oneyrs(unitsId,tag,base_url,eqid):
  
    validload='validload__'+tag
    eqid=fetchtagmeta(unitsId,tag,base_url)
    
    if eqid !=[]:
        
        statetag='state__'+eqid
        taglist= [statetag,tag, validload]
    else:
        
        
        taglist= [tag,validload]
    
#    
    # Format datetime object to desired string format
    endtime = datetime.datetime.now()
   
    start_time="01-01-"+str(endtime.year)+ " 00:00"
    
#     s7d =endtime-datetime.timedelta(days=7)
#     sd1month=endtime-datetime.timedelta(days=30)

    endtime=endtime.strftime("%d-%m-%Y %H:%M")
    

   
    df1yr=getData1(taglist,{"type":'date',"start":str(start_time),"end":str(endtime)},qr,key = None,unitId = None,aggregators = [{"name":"avg","sampling_value":5,"sampling_unit":"minutes"}])
    if not df1yr.empty:
        df1yr.dropna(inplace=True)
        df1yr["time"]=pd.to_datetime(df1yr['time']/1000+5.5*60*60, unit='s')
        df1yr['time'] =df1yr['time'].dt.strftime('%Y-%m-%d %H:%M:%S')
        df1yr['time'] = pd.to_datetime(df1yr['time'])

        bplot1yr=removingOutliers(df1yr,statetag,validload,unitsId,tag,base_url)
        bplot1yr=boxplot(bplot1yr,tag,unitsId,"1Y")
        print("bplot1yr",bplot1yr)

        if bplot1yr!=[]:

            postscylla(bplot1yr)
        else:
            pass
    
    else:
        print("No Data for 1 year")
        pass







########################################posting to kairos##################################################################
    
def postscylla(body):
    print(body)
    url = config["api"]["datapoints"]
    # url="http://data.exactspace.co/kairos/api/v1/datapoints"   #pointing to scylla
    # url="https://data.exactspace.co/exactdata/api/v1/datapoints"
    
    res = requests.post(url = url,json = body)
    print(res,body)
    return res 




#########################################main function######################################################################

def boxplot_main_fun(unitsId,base_url):
    print("Running main function")
    tagList=getallTags(unitsId,base_url)
#     tagList=['LPG_3LAV20CY101_XQ07.OUT']
    
#     tg=get_boxplot(unitsId,tag)
    for tag in tagList:
#         res=fetch_boxplot(unitsId,tag)
#         print(res)
        eqid=fetchtagmeta(unitsId,tag,base_url)
#         if res=={}:
        
        
#             boxplot_yrs(unitsId,tag,base_url,eqid) 
#             print("**********************end of years function*********************")
        boxplot_oneyrs(unitsId,tag,base_url,eqid)
        print("****************end of one year**********************************")
#             boxplot_onemonth_sevendays(unitsId,tag,base_url,eqid)
#             print("**************end of seven days**************************************")
    print("done posting for unitsId :",unitsId)
    
    time.sleep(5)
    print("Main function execution complete")
    
boxplot_main_fun(unitsId,base_url)
