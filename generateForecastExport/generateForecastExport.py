#Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#SPDX-License-Identifier: MIT-0
import os
import io
import json
import boto3
import random
from urllib.parse import unquote_plus
import csv
from datetime import date
from datetime import datetime
from datetime import timedelta
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

S3BucketName = os.environ['S3BucketName']
roleArn = os.environ['ForecastExecutionRole']

forecast_client = boto3.client("forecast")


def isExportJobExistforForcast(client, forecastExportJobName, forecastArn):
    response = client.list_forecast_export_jobs( Filters=[
         {
            'Key': 'ForecastArn',
            'Value': forecastArn,
            'Condition': 'IS'
         },
     ])
    jobs = response["ForecastExportJobs"]
    for job in jobs:
        if (job["ForecastExportJobName"]==forecastExportJobName):
            if(job["Status"]=="CREATE_FAILED"):
                response = client.delete_forecast_export_job(ForecastExportJobArn=job["ForecastExportJobArn"])
            return True
    return False

def createExportJob(client,jobName, forcastArn, exportFileKey):
    response = client.create_forecast_export_job(
    ForecastExportJobName=jobName,
    ForecastArn=forcastArn,
    Destination={
        'S3Config': {
            'Path': exportFileKey,
            'RoleArn': roleArn,
        }})

def onEventHandler(event, context):
    # list all the dataset Group that don't have predictor
    response = forecast_client.list_forecasts( Filters=
       [ { "Condition": "IS", "Key": "Status", "Value": "ACTIVE" } ])
    for forecast in response["Forecasts"]:
        defaultExportJob=forecast["ForecastName"]+"_export"
        if(isExportJobExistforForcast(forecast_client, defaultExportJob, forecast["ForecastArn"])):
          logger.info("default export job :" + defaultExportJob + " already exist")
          continue
        DatasetGrupName=forecast["ForecastName"].replace("_forecast","")
        exportFileKey="s3://"+S3BucketName+"/ForecastExports/"+DatasetGrupName
        createExportJob(forecast_client,defaultExportJob,forecast["ForecastArn"], exportFileKey)
        logger.info("triggerred export job :" + defaultExportJob + ", datasetGroupName=" + DatasetGrupName + "forecastArn=" + forecast["ForecastArn"])
