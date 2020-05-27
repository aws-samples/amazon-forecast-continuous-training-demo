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


s3_client = boto3.client('s3')
forecast_client = boto3.client("forecast")


def tranformDateToString(date):
    return date.strftime("%Y-%m-%d")

def isSameDate(date1, date2):
    if(tranformDateToString(date1)==tranformDateToString(date2)):
        return True
    return False

def transformDateStringFormat(datestring):
    return datestring[0:4]+"-"+datestring[4:6]+"-"+datestring[6:]


def getDateFromString(dateString):
    return datetime.strptime(dateString, "%Y-%m-%d").date()


def upsertDataImportJob(client, DataSetArn, S3Url):
    # rules to use S3Url generating data import JobName
    JobName=S3Url.split("/")[-1].replace("-","").replace(".","_")
    #check if the data import job already exsit
    response = client.list_dataset_import_jobs(
    Filters=[{
            'Key': 'DatasetArn',
            'Value': DataSetArn,
            'Condition': 'IS'
            },])
    existingJobList=response["DatasetImportJobs"]
    for job in existingJobList:
        if (JobName==job["DatasetImportJobName"]):
            logger.info("DatasetImportJob already exist: "+JobName)
            return
    # if the job not exist
    logger.info("start the data import job for " + JobName + "; for dataset "+DataSetArn+ "; with datasource " + S3Url )
    response = client.create_dataset_import_job(
        DatasetImportJobName=JobName,
        DatasetArn=DataSetArn,
        DataSource={
            'S3Config': {
                'Path': S3Url,
                'RoleArn': roleArn,
            }
        },
        TimestampFormat='yyyy-MM-dd'
    )


def isExistingDataSetGroup(client, datasetGroupName):
    response = client.list_dataset_groups()
    for dsgroup in response["DatasetGroups"]:
        if (datasetGroupName == dsgroup["DatasetGroupName"]):
            return True
    return False


def upsertDataSet(existingDataSets, client, schema, datasetName, datasetType):
    for ds in existingDataSets:
        if (ds["DatasetName"] == datasetName):
            logger.info("dataset already exist: "+datasetName)
            return ds["DatasetArn"]
    response = client.create_dataset(
        DatasetName=datasetName,
        Domain='CUSTOM',
        # DatasetType='TARGET_TIME_SERIES'|'RELATED_TIME_SERIES'|'ITEM_METADATA',
        DatasetType=datasetType,
        DataFrequency='D',
        Schema=schema
    )
    return response["DatasetArn"]


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

def onEventHandler(event, context):
    body = json.loads(event['Records'][0]['body'])
    message = json.loads(body['Message'])
    logger.debug("From SQS: " + json.dumps(message))
    s3_object_info=message["Records"][0]["s3"]
    sourceBucketName = s3_object_info["bucket"]["name"]
    sourceObjectKey = s3_object_info["object"]["key"]
    if (not "DatasetGroups" in sourceObjectKey):
        logger.debug("s3 object key do not contain DatasetGroups, ignore and skip, objectkey="+sourceObjectKey)
        return
    s3ObjectUrl = "s3://" + sourceBucketName + "/" + sourceObjectKey

    datasetGroupName = sourceObjectKey.split("/")[1]
    logger.info("upsert forecast dataset group=" + datasetGroupName)

    config=loadconfig(datasetGroupName)
    target_schema=config["target_schema"]
    related_schema=config["related_schema"]
    response = forecast_client.list_datasets()
    existingDataSets = response["Datasets"]
    # upsert data set
    targetDataSetArn = upsertDataSet(existingDataSets, forecast_client, target_schema, datasetGroupName + "_target",
                                     "TARGET_TIME_SERIES")
    relatedDataSetArn = upsertDataSet(existingDataSets, forecast_client, related_schema, datasetGroupName + "_related",
                                      "RELATED_TIME_SERIES")

    # if dataGroup not exist, create
    if (not isExistingDataSetGroup(forecast_client, datasetGroupName)):
        response = forecast_client.create_dataset_group(
            DatasetGroupName=datasetGroupName,
            Domain='CUSTOM',
            DatasetArns=[
                targetDataSetArn, relatedDataSetArn
            ]
        )
        logger.info("triggerred creation of forecast datasetgroup=" + datasetGroupName)

    # load history data
    if ("target.csv" in s3ObjectUrl):
        upsertDataImportJob(forecast_client, targetDataSetArn, s3ObjectUrl)
    if ("related.csv" in s3ObjectUrl):
        upsertDataImportJob(forecast_client, relatedDataSetArn, s3ObjectUrl)
