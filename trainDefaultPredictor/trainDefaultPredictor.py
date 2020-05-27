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

forecast_client = boto3.client("forecast")
S3BucketName = os.environ['S3BucketName']
s3_client = boto3.client('s3')

def isExistingDataSetGroup(client,  datasetGroupName):
    response = client.list_dataset_groups(
    )
    for dsgroup in response["DatasetGroups"]:
        if (datasetGroupName == dsgroup["DatasetGroupName"]):
            return True
    return False

def isPreditorExitInDataSetGroup(client, datasetGroupArn, preditorName):
    response = client.list_predictors( Filters=[
         {
            'Key': 'DatasetGroupArn',
            'Value': datasetGroupArn,
            'Condition': 'IS'
         },
     ])
    predictors = response["Predictors"]
    for preditor in predictors:
        if (preditor["PredictorName"]==preditorName):
            if(preditor["Status"]=="CREATE_FAILED"):
                response = client.delete_predictor(PredictorArn=preditor["PredictorArn"])
            return True
    return False

#https://docs.aws.amazon.com/forecast/latest/dg/related-time-series-datasets.html
#https://docs.aws.amazon.com/forecast/latest/dg/API_FeaturizationConfig.html
# Forecast doesn't support aggregations or filling missing values for related time series datasets as it does for target time series datasets.
def createPredictor(client,datagroupArn,predictorName, config):
    response = client.create_predictor(
        PredictorName=predictorName,
        ForecastHorizon=config["preditor"]["ForecastHorizon"],
        PerformAutoML=True,
        EvaluationParameters=config["preditor"]["EvaluationParameters"],
        InputDataConfig={
            'DatasetGroupArn': datagroupArn,
            'SupplementaryFeatures': config["preditor"]["InputDataConfig"]["SupplementaryFeatures"]
        },
        FeaturizationConfig=config["preditor"]["FeaturizationConfig"]
        )
    logger.debug(response)
    logger.info("triggerred Predictor training for predictor=" + predictorName)

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
    # list all the dataset Group that don't have predictor
    response = forecast_client.list_dataset_groups()
    for datasetGroup in response["DatasetGroups"]:
        DGName=datasetGroup["DatasetGroupName"]
        defaultPredictorName=datasetGroup["DatasetGroupName"]+"_Predictor"
        try:
            if(isPreditorExitInDataSetGroup(forecast_client,datasetGroup["DatasetGroupArn"],defaultPredictorName)):
               logger.info("Default predictor :" + defaultPredictorName + " already exist under DatasetGroup=" + DGName)
               continue
            config= loadconfig(DGName)
            createPredictor(forecast_client,datasetGroup["DatasetGroupArn"],defaultPredictorName, config)
        except Exception as e:
            logger.error("Failed to train dataset predictor for datasetGroup= " + DGName + ", will skip and continue")
            continue
