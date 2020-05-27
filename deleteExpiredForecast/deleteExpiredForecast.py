#Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#SPDX-License-Identifier: MIT-0
import os
import io
import json
import boto3
import random
from urllib.parse import unquote_plus
import csv
from datetime import date,timezone
from datetime import datetime
from datetime import timedelta
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

forecast_client = boto3.client("forecast")
#https://docs.aws.amazon.com/forecast/latest/dg/limits.html
numberOfForecastsToKeep=int(os.environ['NumberOfForecastsToKeep'])

def isExistingDataSet(client,datasetArn):
    response=client.list_datasets()
    for dataset in response["Datasets"]:
        if(dataset["DatasetArn"]==datasetArn):
          return True
    return False

#https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/forecast.html#ForecastService.Client.list_datasets
#there's situation where dataset already deleted but still registered under datasetgroup
def noExistingDataset(client,datasetGroupArn):
    response = client.describe_dataset_group(DatasetGroupArn=datasetGroupArn)
    numOfExistingDataSets = len(response["DatasetArns"])
    hasExistingDataSet=False
    for datasetArn in response["DatasetArns"]:
        if(noExistingDataImportJobs(client, datasetArn)):
            if(isExistingDataSet(client,datasetArn)):
              client.delete_dataset(DatasetArn=datasetArn)
              logger.info("Dataset deletion triggerred, DatasetArn="+datasetArn)
              hasExistingDataSet=True
        else:
            hasExistingDataSet=True
    return (not hasExistingDataSet)

# only return true if there's no existing data import jobs, if yes, then trigger the deletion
def noExistingDataImportJobs(client,DatasetArn):
    response = client.list_dataset_import_jobs( Filters=[
         {
            'Key': 'DatasetArn',
            'Value': DatasetArn,
            'Condition': 'IS'
         },
     ])
    numOfExistingDataImportJobs = len(response["DatasetImportJobs"])
    for importjob in response["DatasetImportJobs"]:
        importjobArn=importjob["DatasetImportJobArn"]
        client.delete_dataset_import_job(DatasetImportJobArn=importjobArn)
        logger.info("DatasetImportJob deletion triggerred, DatasetImportJob="+importjobArn)
    return (numOfExistingDataImportJobs==0)


# only return true if there's no existing predictors, if yes, then trigger the deletion
def noExistingPredictors(client,datasetGroupArn):
    response = client.list_predictors( Filters=[
         {
            'Key': 'DatasetGroupArn',
            'Value': datasetGroupArn,
            'Condition': 'IS'
         },
     ])
    numOfExistingPredictors = len(response["Predictors"])
    for predictor in response["Predictors"]:
        predictorArn=predictor["PredictorArn"]
        client.delete_predictor(PredictorArn=predictorArn)
        logger.info("Predictor deletion triggerred, PredictorArn="+predictorArn)
    return (numOfExistingPredictors==0)

# only return true if there's no existing Forecasts, if yes, then trigger the deletion
def noExistingForecasts(client,datasetGroupArn):
    response = client.list_forecasts( Filters=[
         {
            'Key': 'DatasetGroupArn',
            'Value': datasetGroupArn,
            'Condition': 'IS'
         },
     ])
    numOfExistingForecasts = len(response["Forecasts"])
    for forecast in response["Forecasts"]:
        forecastArn=forecast["ForecastArn"]
        client.delete_forecast(ForecastArn=forecastArn)
        logger.info("Forecast deletion triggerred, ForecastArn="+forecastArn)
    return (numOfExistingForecasts==0)

def trigger_deleteDS(datasetGroup):
    dsGroupArn=datasetGroup["DatasetGroupArn"]
    if(noExistingForecasts(forecast_client,dsGroupArn )):
      if(noExistingPredictors(forecast_client,dsGroupArn)):
          if(noExistingDataset(forecast_client,dsGroupArn)):
              forecast_client.delete_dataset_group(DatasetGroupArn=dsGroupArn)
              logger.info("DatasetGroup deletion triggerred, DatasetGroupArn="+dsGroupArn)

def onEventHandler(event, context):
    response = forecast_client.list_dataset_groups()
    numOfDSGroup=len(response["DatasetGroups"])
    oldest_creationDate=datetime.now(timezone.utc)

    if(numOfDSGroup>numberOfForecastsToKeep):
        #delete the oldest
        for datasetGroup in response["DatasetGroups"]:
            creationTime=datasetGroup["CreationTime"]
            if(oldest_creationDate>=creationTime):
               oldest_creationDate=creationTime
               oldest_datasetGroup=datasetGroup
        logger.info ("the oldest forecast is going to be deleted, datasetGroupName="+ datasetGroup["DatasetGroupName"])
        logger.debug(datasetGroup)
        trigger_deleteDS(datasetGroup)
    else:
        logger.info("number for DatasetGroups="+str(numOfDSGroup)+",  limitation="+str(numberOfForecastsToKeep)+ ", nothing to do")
