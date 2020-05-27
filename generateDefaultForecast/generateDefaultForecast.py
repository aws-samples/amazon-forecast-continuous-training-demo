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

def getPredictorArnByName(client, datasetGroupArn, preditorName):
    response = client.list_predictors( Filters=[
         {
            'Key': 'DatasetGroupArn',
            'Value': datasetGroupArn,
            'Condition': 'IS'
         },
     ])
    predictors = response["Predictors"]
    for preditor in predictors:
        if ((preditor["PredictorName"]==preditorName) and (preditor["Status"]=="ACTIVE")):
            return preditor["PredictorArn"]

def isForcastExistInDataSetGroup(client, datasetGroupArn, forecastName):
    response = client.list_forecasts( Filters=[
         {
            'Key': 'DatasetGroupArn',
            'Value': datasetGroupArn,
            'Condition': 'IS'
         },
     ])
    Forecasts = response["Forecasts"]
    for forcast in Forecasts:
        if (forcast["ForecastName"]==forecastName):
            if(forcast["Status"]=="CREATE_FAILED"):
                client.delete_forecast(ForecastArn=forcast["ForecastArn"])
            return True
    return False

def createForecast(client,forecastName,predictorArn):
    response = client.create_forecast(
        ForecastName= forecastName,
        PredictorArn= predictorArn,
        ForecastTypes=["0.1", "0.5", "0.9"]
    )
    logger.info("for predictor with arn= " + predictorArn + ", triggered forecast creation, forecastName=" + forecastName)

def onEventHandler(event, context):
    response = forecast_client.list_dataset_groups()
    for datasetGroup in response["DatasetGroups"]:
        datasetGroupName=datasetGroup["DatasetGroupName"]
        defaultPredictorName=datasetGroupName+"_Predictor"
        defaultForecastName=datasetGroupName+"_Forecast"
        defaultPredictorArn=getPredictorArnByName(forecast_client, datasetGroup["DatasetGroupArn"], defaultPredictorName)
        if (defaultPredictorArn is None ):
            logger.info("For DatasetGroup="+datasetGroupName+" , default predictor="+defaultPredictorName + " is not trained yet or hasn't finished training, skip")
            continue
        if(isForcastExistInDataSetGroup(forecast_client,datasetGroup["DatasetGroupArn"],defaultForecastName)):
            logger.info("For DatasetGroup="+datasetGroupName+" , default predictor="+defaultPredictorName + ", default forecast="+ defaultForecastName + " already exist, skip")
            continue
        createForecast(forecast_client,defaultForecastName,defaultPredictorArn)
