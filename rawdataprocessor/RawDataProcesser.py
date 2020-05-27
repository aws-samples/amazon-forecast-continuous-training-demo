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


def tranformDateToString(date):
    return date.strftime("%Y-%m-%d")

def isSameDate(date1, date2):
    return (tranformDateToString(date1)==tranformDateToString(date2))

def transformDateStringFormat(datestring):
    return datestring[0:4]+"-"+datestring[4:6]+"-"+datestring[6:]


def getDateFromString(dateString):
    return datetime.strptime(dateString, "%Y-%m-%d").date()

# simplify backfill logic , use 0 ; remove recursive
def getRowValueForTheDay(currentDay,item,cellName):
    currentDayString=tranformDateToString(currentDay)

    # if currentDayString has no data
    if (currentDayString not in vars.RawData):
       return 0

    if (not (item in vars.RawData[currentDayString])):
       return 0

    if ((not (cellName in vars.RawData[currentDayString][item])) or (vars.RawData[currentDayString][item][cellName] is None) or (vars.RawData[currentDayString][item][cellName]=="")):
            return 0
    else:
            return vars.RawData[tranformDateToString(currentDay)][item][cellName]

def writeDataAndUpload(currentDay, dataListForCurrentTimePoint):
    # only re-write recent day's data (ignore redundant historical data)
    numOfDays=abs((vars.EndDate - currentDay).days)
    if(numOfDays>5):
        return
    targetFileName="target_"+tranformDateToString(currentDay)+".csv"
    relatedFileName="related_"+tranformDateToString(currentDay)+".csv"

    targetFile = open("/tmp/"+targetFileName,'w',newline='')
    csvWriterTarget = csv.writer(targetFile)
    relatedFile = open("/tmp/"+relatedFileName,'w',newline='')
    csvWriterRelated = csv.writer(relatedFile)

    for item in dataListForCurrentTimePoint:
        targetRow=[item[0],item[1],item[2]]
        relatedRow=[item[0],item[1],item[3]]
        csvWriterTarget.writerow(targetRow)
        csvWriterRelated.writerow(relatedRow)
    targetFile.close()
    relatedFile.close()
    s3_client.upload_file("/tmp/"+targetFileName, S3BucketName, "covid-19-daily/"+targetFileName)
    s3_client.upload_file("/tmp/"+relatedFileName, S3BucketName, "covid-19-daily/"+relatedFileName)
    logger.info("daily data uploaded to bucket="+S3BucketName+", under path key=covid-19-daily for date=" + tranformDateToString(currentDay))


# this will also fill empty data for rawdata
def generateDataForCurrentDay(currentDay):
    dataListForCurrentTimePoint=[]
    for item in vars.ItemList:
        rowItemForTheDay=[]
        rowItemForTheDay.append(tranformDateToString(currentDay))
        rowItemForTheDay.append(item)
        rowItemForTheDay.append(getRowValueForTheDay(currentDay,item,"targetValue"))
        rowItemForTheDay.append(getRowValueForTheDay(currentDay,item,"relatedValue1"))
        dataListForCurrentTimePoint.append(rowItemForTheDay)
    return dataListForCurrentTimePoint


def processRawCSV(rawDataLocalPath):
    inputFile=open(rawDataLocalPath,'r')
    readerObj=csv.reader(inputFile)
    next(readerObj)
    cur_itemList=[]
    # get start and end date, get Rawdata Map
    cur_startDate=date.today()
    cur_endDate=None
    for row in readerObj:
       tmp_date_string=transformDateStringFormat(row[0])
       tmp_date=getDateFromString(tmp_date_string)
       tmp_item_string=row[1]
       if (not tmp_item_string in cur_itemList):
           cur_itemList.append(tmp_item_string)
       if (not tmp_date_string in vars.RawData):
           vars.RawData[tmp_date_string]={}
       if (not tmp_item_string in vars.RawData[tmp_date_string]):
           vars.RawData[tmp_date_string][tmp_item_string]={}
       tmp_object={}
       tmp_object["targetValue"]=row[2]
       tmp_object["relatedValue1"]=row[17]
       vars.RawData[tmp_date_string][tmp_item_string]=tmp_object
       #process start and end date
       if(tmp_date<cur_startDate):
           cur_startDate=tmp_date
       if (cur_endDate is None):
           cur_endDate=tmp_date
       if(tmp_date>cur_endDate):
           cur_endDate=tmp_date
    inputFile.close()

    vars.StartDate=cur_startDate
    vars.EndDate=cur_endDate
    vars.ItemList=cur_itemList

def updateFullHistryItem(currentDayItems):
    for item in currentDayItems:
        vars.FullHistoryList.append(item)


def writePreparedDataForModel(mconfig):
    logger.debug(mconfig)
    #The dataset group name must have 1 to 63 characters. Valid characters: a-z, A-Z, 0-9, and _
    datasetGroupName = mconfig["modelName"]+"_"+tranformDateToString(vars.StartDate).replace("-","")+"_"+tranformDateToString(vars.EndDate).replace("-","")
    FullHistoryTargetFileName="history.target."+tranformDateToString(vars.StartDate)+"."+ tranformDateToString(vars.EndDate)+".csv"
    FullHistoryRelatedFileName="history.related."+tranformDateToString(vars.StartDate)+"."+ tranformDateToString(vars.EndDate)+".csv"
    modelconfigfile= "/tmp/"+datasetGroupName+".json"
    simulateStartDate=vars.EndDate+timedelta(days=1)
    simulateEndDate=vars.EndDate+timedelta(days=mconfig["preditor"]["ForecastHorizon"])
    with open(modelconfigfile, 'w') as outfile:
        mconfig["data_starttime"]= tranformDateToString(vars.StartDate)
        mconfig["data_endtime"]= tranformDateToString(vars.EndDate)
        mconfig["forecast_starttime"]= tranformDateToString(simulateStartDate)
        mconfig["forecast_endtime"]= tranformDateToString(simulateEndDate)
        json.dump(mconfig, outfile)
    s3_client.upload_file(modelconfigfile, S3BucketName, "DatasetGroups/"+datasetGroupName+"/config.json")
    logger.info("Dataset Group config config.json uploaded to bucket="+S3BucketName+", under path key=DatasetGroups/"+datasetGroupName)


    targetFile = open("/tmp/"+FullHistoryTargetFileName,'w',newline='')
    csvWritertarget = csv.writer(targetFile)
    relatedFile = open("/tmp/"+FullHistoryRelatedFileName,'w',newline='')
    csvWriterRelated = csv.writer(relatedFile)
    for item in vars.FullHistoryList:
        targetRow=[item[0],item[1],item[2]]
        relatedRow=[item[0],item[1],item[3]]
        csvWritertarget.writerow(targetRow)
        csvWriterRelated.writerow(relatedRow)

    currentDay=simulateStartDate
    while(currentDay<=simulateEndDate):
       for item in vars.ItemList:
          rowItemForTheDay=[]
          rowItemForTheDay.append(tranformDateToString(currentDay))
          rowItemForTheDay.append(item)
          rowItemForTheDay.append(getRowValueForTheDay(vars.EndDate,item,"relatedValue1"))
          csvWriterRelated.writerow(rowItemForTheDay)
       currentDay=currentDay+timedelta(days=1)
    targetFile.close()
    relatedFile.close()
    s3_client.upload_file("/tmp/"+FullHistoryTargetFileName, S3BucketName, "DatasetGroups/"+datasetGroupName+"/target.csv")
    s3_client.upload_file("/tmp/"+FullHistoryRelatedFileName, S3BucketName, "DatasetGroups/"+datasetGroupName+"/related.csv")
    logger.info("processed data uploaded to bucket="+S3BucketName+", under path key=DatasetGroups/"+datasetGroupName)



def loadconfig():
    try:
        configFile_path = '/tmp/config.json'
        s3_client.download_file(S3BucketName, 'forecast-model-config.json', configFile_path)
        with open(configFile_path) as f:
             config = json.load(f)
        logger.debug(config)
        return config
    except Exception as e:
        logger.error("Failed to load global model json config file. bucket= " + S3BucketName + " , key=forecast-model-config.json" )
        raise e

def onEventHandler(event, context):
  config = loadconfig()

  tmpkey=tranformDateToString(date.today())+".csv"
  #download raw data and process
  download_path = '/tmp/'+tmpkey
  s3_client.download_file('covid19-lake', 'rearc-covid-19-testing-data/csv/states_daily/states_daily.csv', download_path)
  s3_client.upload_file(download_path, S3BucketName, "covid-19-raw/states_daily_raw"+tmpkey)
  logger.info("raw data downloaded from bucket=covid19-lake, key=rearc-covid-19-testing-data/csv/states_daily/states_daily.csv, uploaded to bucket="+S3BucketName+", with key=covid-19-raw/states_daily_raw" + tmpkey)

  processRawCSV(download_path)

  currentDay=vars.StartDate
  logger.debug("raw data start from " + str(vars.StartDate) +",ending at " + str(vars.EndDate))
  while (currentDay<=vars.EndDate):
      currentDayItems=generateDataForCurrentDay(currentDay)
      updateFullHistryItem(currentDayItems)
      #everyday item will only used for metrics
      writeDataAndUpload(currentDay,currentDayItems)
      currentDay=currentDay+timedelta(days=1)
  writePreparedDataForModel(config["models"][0])
