import requests
import datetime
# import pytz
# from pytz import timezone
import json
import pandas as pd
from datetime import timedelta
import time
import numpy as np
import threading
import calendar
import json
import csv
from queue import Queue
from collections import namedtuple
import sys
from concurrent.futures import ThreadPoolExecutor
from itertools import repeat 
import os
# start_date=datetime.datetime.now(timezone('US/Eastern')).replace(day=1,month=1,year=2023,hour=9, minute=30,second=0,microsecond=0)
# end_date=datetime.datetime.now(timezone('US/Eastern')).replace(day=30,month=1,year=2023,hour=16, minute=0,second=0,microsecond=0)
# ticker="BANKNIFTY"
timeframe = "/range/5/minute/"

def getHolidays():
    with open(r'holidays (2).json') as holidays:
        holidaysData = json.load(holidays)
    return holidaysData
 
def getTimeOfDay(hours, minutes, seconds, dateTimeObj = None):
    if dateTimeObj == None:
        dateTimeObj = datetime.datetime.now()
        # dateTimeObj = pytz.timezone('America/New_York').localize(dateTimeObj)
    dateTimeObj = dateTimeObj.replace(hour=hours, minute=minutes, second=seconds, microsecond=0)
    return dateTimeObj
 
def getMarketEndTime(dateTimeObj = None):
    return getTimeOfDay(15, 30, 0, dateTimeObj) 
 
def getMarketStartTime(dateTimeObj = None):
    return getTimeOfDay(9, 15, 0, dateTimeObj)       
 
def isHoliday(datetimeObj):
    dayOfWeek = calendar.day_name[datetimeObj.weekday()]
    if dayOfWeek == 'Saturday' or dayOfWeek == 'Sunday':
        return True
    dateStr = datetimeObj.strftime("%Y-%m-%d")  #Utils.convertToDateStr(datetimeObj)
    holidays = getHolidays()
    if (dateStr in holidays):
        return True
    else:
        return False



def getMonthlyExpiryDayDate(datetimeObj = None):
    if datetimeObj == None:
        datetimeObj = datetime.datetime.now()
        # dateTimeObj = pytz.timezone('America/New_York').localize(dateTimeObj)
    year = datetimeObj.year
    month = datetimeObj.month
    lastDay = calendar.monthrange(year, month)[1] # 2nd entry is the last day of the month
    datetimeExpiryDay = datetime(year, month, lastDay)
    while calendar.day_name[datetimeExpiryDay.weekday()] != 'Thursday':
        datetimeExpiryDay = datetimeExpiryDay - timedelta(days=1)
    while isHoliday(datetimeExpiryDay) == True:
        datetimeExpiryDay = datetimeExpiryDay - timedelta(days=1)
 
    datetimeExpiryDay = getTimeOfDay(0, 0, 0, datetimeExpiryDay)
    return datetimeExpiryDay
 
# def prepareMonthlyExpiryFuturesSymbol(inputSymbol):
#     expiryDateTime = getMonthlyExpiryDayDate()
#     expiryDateMarketEndTime = getMarketEndTime(expiryDateTime)
#     now = datetime.datetime.datetime.now()
#     if now > expiryDateMarketEndTime:
#       # increasing today date by 20 days to get some day in next month passing to getMonthlyExpiryDayDate()
#       expiryDateTime = getMonthlyExpiryDayDate(now + timedelta(days=20))
#     year2Digits = str(expiryDateTime.year)[2:]
#     monthShort = calendar.month_name[expiryDateTime.month].upper()[0:3]
#     futureSymbol = inputSymbol + year2Digits + monthShort + 'FUT'
#     logging.info('/home/prepareMonthlyExpiryFuturesSymbol[%s] = %s', inputSymbol, futureSymbol)  
#     return futureSymbol
 
def prepareWeeklyOptionsSymbol(date,inputSymbol, strike, optionType, numWeeksPlus = 0):
    expiryDateTime = getWeeklyExpiryDayDate(date)
    todayMarketStartTime = getMarketStartTime()
    expiryDayMarketEndTime = getMarketEndTime(expiryDateTime)
    if numWeeksPlus > 0:
        expiryDateTime = expiryDateTime + timedelta(days=numWeeksPlus * 7)
        expiryDateTime = getWeeklyExpiryDayDate(expiryDateTime)
    if todayMarketStartTime > expiryDayMarketEndTime:
        expiryDateTime = expiryDateTime + timedelta(days=6)
        expiryDateTime = getWeeklyExpiryDayDate(expiryDateTime)
    # Check if monthly and weekly expiry same
    # expiryDateTimeMonthly = getMonthlyExpiryDayDate(expiryDateTime)
    # weekAndMonthExpriySame = False
    # if expiryDateTime == expiryDateTimeMonthly:
    #     weekAndMonthExpriySame = True
    #     logging.info('Weekly and Monthly expiry is same for %s', expiryDateTime)
    year2Digits = str(expiryDateTime.year)[2:].zfill(2)
    # print('stage1')
    # optionSymbol = None
    # if weekAndMonthExpriySame == True:
    #     monthShort = calendar.month_name[expiryDateTime.month].upper()[0:3]
    #     optionSymbol = inputSymbol + str(year2Digits) + monthShort + str(strike) + optionType.upper()
    # else:
    m = str(expiryDateTime.month).zfill(2)
    d = str(expiryDateTime.day).zfill(2)
    # mStr = str(m)
    # if m == 10:
    #     mStr = "O"
    # elif m == 11:
    #     mStr = "N"
    # elif m == 12:
    #     mStr = "D"
    # dStr = ("0" + str(d)) if d < 10 else str(d)
    optionSymbol = inputSymbol + str(year2Digits) + str(m) + str(d) +str((int(strike)))+ optionType.upper() 
    # logging.info('prepareWeeklyOptionsSymbol[%s, %d, %s, %d] = %s', inputSymbol, strike, optionType, numWeeksPlus, optionSymbol)  
    return optionSymbol 
 
def getWeeklyExpiryDayDate(dateTimeObj):
    if dateTimeObj == None:
        dateTimeObj = datetime.datetime.now()
        # dateTimeObj = pytz.timezone('America/New_York').localize(dateTimeObj)
    daysToAdd = 0
    if dateTimeObj.weekday() >= 3:
        daysToAdd = -1 * (dateTimeObj.weekday() - 3)
    else:
        daysToAdd = 3 - dateTimeObj.weekday()
    datetimeExpiryDay = dateTimeObj + timedelta(days=daysToAdd)
    while isHoliday(datetimeExpiryDay) == True:
        datetimeExpiryDay = datetimeExpiryDay - timedelta(days=1)
 
    datetimeExpiryDay = getTimeOfDay(0, 0, 0, datetimeExpiryDay)
    return datetimeExpiryDay

def expiryday(numWeeksPlus=0):
##--------function will check if today is expiry or not-----------------------------##########
    expiryDateTime = getWeeklyExpiryDayDate()
    todayMarketStartTime = getMarketStartTime()
    expiryDayMarketEndTime = getMarketEndTime(expiryDateTime)
    if numWeeksPlus > 0:
        expiryDateTime = expiryDateTime + timedelta(days=numWeeksPlus * 7)
        expiryDateTime = getWeeklyExpiryDayDate(expiryDateTime)
    if todayMarketStartTime > expiryDayMarketEndTime:
        expiryDateTime = expiryDateTime + timedelta(days=6)
        expiryDateTime = getWeeklyExpiryDayDate(expiryDateTime)
    nowdate = date.today().isoformat()
    if nowdate == expiryDateTime.date():
        return "Expiry"
    else:
        return "Other"
    
# start_date=datetime.datetime(2022,5,22)

bnf_df = pd.read_csv('BNF_22-23.csv')
for i in range(len(bnf_df)): 
    first_date = bnf_df.iloc[i][0]
    strike = round(bnf_df.iloc[i][5]/100)*100
    date_string = first_date
    date_format = "%d-%m-%Y"
    first_date = datetime.datetime.strptime(date_string, date_format)
    sym_CE = prepareWeeklyOptionsSymbol(first_date,"BANKNIFTY", 40000, "CE", numWeeksPlus = 0)
    sym_PE = prepareWeeklyOptionsSymbol(first_date,"BANKNIFTY", 40000, "CE", numWeeksPlus = 0)
    