#Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#SPDX-License-Identifier: MIT-0
import os
import io
import json
import boto3
import random
import csv
import botocore
from datetime import date
from datetime import datetime
from datetime import timedelta
from urllib.parse import unquote_plus
import vars
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

S3BucketName=os.environ['S3BucketName']
s3_client = boto3.client('s3')
cloudwatch_client = boto3.client('cloudwatch')
s3_resource = boto3.resource('s3')
MetricNameSpace=os.environ['MetricsNameSpace']


def tranformDateToString(date):
    return date.strftime("%Y-%m-%d")


def getDateFromString(dateString):
    return datetime.strptime(dateString, "%Y-%m-%d").date()


def getCurrentDayRealData(currentDay):
    targetfile='target_'+tranformDateToString(currentDay)+".csv"
    targetfile_key='covid-19-daily/'+targetfile
    download_path='/tmp/'+targetfile
    try:
        s3_client.download_file(S3BucketName,targetfile_key,download_path)
    except:
        logger.info("no historical data found for " + tranformDateToString(currentDay) + " in bucket=" + S3BucketName + " , with key=" + targetfile_key)
        return None
    inputFile=open(download_path,'r')
    readerObj=csv.reader(inputFile)
    next(readerObj)
    currentDayRealData={}
    for row in readerObj:
       currentDayRealData[row[1]]=row[2]
    return currentDayRealData


def getTimestampByDate(currentDay):
    return datetime(currentDay.year, currentDay.month, currentDay.day)

def getMetricData(metricTimeStamp, modelConfig, abs, p):
    MetricData={
                'MetricName': 'ForecastPerformance',
                'Dimensions': [
                    {
                        'Name': 'ModelConfig',
                        'Value': modelConfig
                    },
                    {
                        'Name': 'P',
                        'Value': p
                    },
                ],
                'Timestamp': metricTimeStamp,
                'Value': abs,
                'Unit': 'None'
            }
    return MetricData

def publishMetrics(currentDay,currentDayRealData,config):
    total_dif=0
    metricDataList=[]
    metricTimeStamp=getTimestampByDate(currentDay)
    for p in vars.forecastPList:
      metricDataList=[]
      for item in vars.ItemList:
          if (item.upper() in currentDayRealData):
              realValue=float(currentDayRealData[item.upper()])
              forcastValue=float(vars.ForcastData[tranformDateToString(currentDay)][item.lower()][p])
              if (realValue is None or realValue==0):
                  state_abs=0
              else:
                  state_abs=(abs((forcastValue-realValue)/realValue))*100
              total_dif=total_dif+state_abs
              # currently not need by state data for model monitor
              #metricDataList.append(getMetricData(metricTimeStamp, item, state_abs, p))
      avarage_abs=total_dif/len(vars.ItemList)
      logger.info("averageABS is "+str(avarage_abs) +  "============for :"+tranformDateToString(currentDay) +"====for p=" +p)
      metricDataList.append(getMetricData(metricTimeStamp, config["modelName"], avarage_abs, p))
      putForecastMetricsData(metricDataList)

def putForecastMetricsData(metricDataList):
    response = cloudwatch_client.put_metric_data(
       Namespace=MetricNameSpace,
       MetricData=metricDataList
   )
    logger.debug("metric put: "+ str (response))


def processForecastCSV(csvPath):
    inputFile=open(csvPath,'r')
    readerObj=csv.reader(inputFile)
    # get start and end date, get Rawdata Map
    cur_startDate=date.today()
    cur_endDate=None
    colNameList=[]
    for row in readerObj:
       # header
       if(row[0]=="item_id"):
          if (len(vars.forecastPList)==0):
             for i in range(2,len(row)):
                 vars.forecastPList.append(row[i])
          continue
       tmp_date_string=row[1][:10]
       tmp_date=getDateFromString(tmp_date_string)
       tmp_item_string=row[0]
       if (not tmp_item_string in vars.ItemList):
           vars.ItemList.append(tmp_item_string)
       if (not tmp_date_string in vars.ForcastData):
           vars.ForcastData[tmp_date_string]={}
       if (not tmp_item_string in vars.ForcastData[tmp_date_string]):
           vars.ForcastData[tmp_date_string][tmp_item_string]={}
       tmp_object={}
       i=2
       for p in vars.forecastPList:
          tmp_object[p]=row[i]
          i=i+1
       vars.ForcastData[tmp_date_string][tmp_item_string]=tmp_object
    inputFile.close()

def loopAllForecastExports(exportFolder):
    response = s3_client.list_objects_v2(
            Bucket=S3BucketName,
            Prefix=exportFolder
    )
    for content in response["Contents"]:
        key=content["Key"]
        if(".csv" in key):
            filePath="/tmp/"+key.split("/")[-1]
            s3_client.download_file(S3BucketName, key, filePath)
            processForecastCSV(filePath)


def calculatePublishMetrics(forecastDatasetGroupName,config,exportFolder):
    loopAllForecastExports(exportFolder)
    forecast_starttime=config["forecast_starttime"]
    # Only evaluate the first day's forcast Performance
    currentDay=getDateFromString(forecast_starttime)
    currentDayRealData=getCurrentDayRealData(currentDay)
    if(not currentDayRealData is None):
       publishMetrics(currentDay,currentDayRealData,config)

def loadconfig(DGName):
    try:
        configFile_path = "/tmp/"+DGName+".json"
        configFile_key="DatasetGroups/"+DGName+"/config.json"
        response = s3_client.list_objects_v2(
            Bucket=S3BucketName,
            Prefix=configFile_key
        )
        for content in response["Contents"]:
            s3_client.download_file(S3BucketName, configFile_key, configFile_path)
            with open(configFile_path) as f:
                 config = json.load(f)
                 return config
    except Exception as e:
        logger.error("Failed to load json config bucket= " + S3BucketName + " with key=" + configFile_key)
        raise e

def getExistingHistoricalDataKeyList():
    dailyDataResponse = s3_client.list_objects_v2(
        Bucket=S3BucketName,
        Prefix='covid-19-daily'
        )
    existingDailyDataKeyList=[]
    for content in dailyDataResponse["Contents"]:
        existingDailyDataKeyList.append(content["Key"])
    return existingDailyDataKeyList

def checkHistoricalDataAvailable(existingDailyDataKeyList,startDate,endDate):
    currentDay=startDate
    while (currentDay<=endDate):
        historyDataKey="covid-19-daily/target_"+tranformDateToString(currentDay)+".csv"
        if (not historyDataKey in existingDailyDataKeyList):
            return False
        currentDay=currentDay+timedelta(days=1)
    return True



def onEventHandler(event, context):
    existingDailyDataKeyList=getExistingHistoricalDataKeyList()
    exportResponse = s3_client.list_objects_v2(
        Bucket=S3BucketName,
        Prefix='ForecastExports'
        )
    ## Filter out all available successful exports, generating metrics and publish to cloudwatch
    for content in exportResponse["Contents"]:
        key=content["Key"]
        try:
            if("_SUCCESS" in key):
               forecastDatasetGroupName=key.split("/")[1].replace("_Forecast","")
               config=loadconfig(forecastDatasetGroupName)
               forecast_starttime=config["forecast_starttime"]
               forecast_endtime=config["forecast_endtime"]
               #loop through to make sure all the historical data exist
               dataAvailable=checkHistoricalDataAvailable(existingDailyDataKeyList,getDateFromString(forecast_starttime),getDateFromString(forecast_endtime))
               exportFolderKey=key.replace("/_SUCCESS","")
               calculatePublishMetrics(forecastDatasetGroupName,config,exportFolderKey)
               if(dataAvailable):
                   newKey=key.replace("_SUCCESS","_ARCHIVED")
                   s3_client.put_object(Bucket=S3BucketName, Key=newKey)
                   s3_client.delete_object(Bucket=S3BucketName, Key=key)
               else:
                   logger.info(forecastDatasetGroupName + " not ready to be archived yet")
        except Exception as e:
            logger.error("Failed to evaluate forecast performance, export key= " + key + ", will skip and continue to process next export")
            continue
