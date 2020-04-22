# Credentials required need to be updated in the inputDetails.py file
from oauth2client.client import OAuth2WebServerFlow
from oauth2client.tools import run_flow
from oauth2client.file import Storage
import json
import os
import re
import httplib2 
from oauth2client import GOOGLE_REVOKE_URI, GOOGLE_TOKEN_URI, client
import requests
import cx_Oracle
from sqlalchemy import types, create_engine
import inputDetails
import pandas as pd
from datetime import timedelta, date


'''function check whether file exist in the path or not'''

def where_json(file_name):return os.path.exists(file_name)

''' function return the refresh token '''

def get_refresh_token(client_id,client_secret):
    CLIENT_ID = client_id
    CLIENT_SECRET = client_secret
    SCOPE = 'https://www.googleapis.com/auth/analytics.readonly'
    REDIRECT_URI = 'http:localhost:8080'
  
    flow = OAuth2WebServerFlow(client_id=CLIENT_ID,client_secret=CLIENT_SECRET,scope=SCOPE,redirect_uri=REDIRECT_URI)
    if where_json('credential.json')==False:
       storage = Storage('credential.json') 
       credentials = run_flow(flow, storage)
       refresh_token=credentials.refresh_token
       
    elif where_json('credential.json')==True:
       with open('credential.json') as json_file:  
           data   = json.load(json_file)
       refresh_token=data['refresh_token']
  
    return(refresh_token)


#Everything above this is Section 1 which will generate the token.
''' function return the google analytics data for given dimension, metrics, start data, end data access token, type,goal number, condition'''


def google_analytics_reporting_api_data_extraction(viewID,dim,met,start_date,end_date,refresh_token,transaction_type,goal_number,condition,stindex):
    
    viewID=viewID;dim=dim;met=met;start_date=start_date;end_date=end_date;refresh_token=refresh_token;transaction_type=transaction_type;condition=condition
    goal_number=goal_number
    viewID="".join(['ga%3A',viewID])

    
    if transaction_type=="Goal":
        met1="%2C".join([re.sub(":","%3A",i) for i in met]).replace("XX",str(goal_number))
    elif transaction_type=="Transaction":
        met1="%2C".join([re.sub(":","%3A",i) for i in met])
        
    dim1="%2C".join([re.sub(":","%3A",i) for i in dim])
    
    if where_json('credential.json')==True:
       with open('credential.json') as json_file:  
           storage_data = json.load(json_file)
       
       client_id=storage_data['client_id']
       client_secret=storage_data['client_secret']
       credentials = client.OAuth2Credentials(
                access_token=None, client_id=client_id, client_secret=client_secret, refresh_token=refresh_token,
                token_expiry=3600,token_uri=GOOGLE_TOKEN_URI,user_agent='my-user-agent/1.0',revoke_uri=GOOGLE_REVOKE_URI)

       credentials.refresh(httplib2.Http())
       rt=(json.loads(credentials.to_json()))['access_token']
  
       api_url="https://www.googleapis.com/analytics/v3/data/ga?ids="

#Parameters are being set for the api call here, including how many results to pull.
#Add Filters https://developers.google.com/analytics/devguides/reporting/core/v3/reference#filters
       url="".join([api_url,viewID,'&start-date=',start_date,'&end-date=',end_date,'&metrics=',met1,'&dimensions=',dim1,'&start-index=',str(stindex),'&max-results=10000',condition,'&access_token=',rt])
       print(url)
       try:
         r = requests.get(url)
         return (r.json())['rows']
       except:
         print("error occured in the google analytics reporting api data extraction")


def create_excel(json_data,vid):
# Creates CSV file of data               
    try:
        data=pd.DataFrame(list(json_data))
        data['ViewID']= vid
        data.to_csv('ViewID '+vid+'.csv',header=False, mode = 'a')
        print("data extraction is successfully completed")
        return data
    except:
        print("error in saving csv file")


def import_row_to_adw(ind ,viewID, channelGrouping ,countryIsoCode ,deviceCategory ,dateHourMinute ,hostname ,pagePath ,sourceMedium ,bounceRate ,avgTimeOnPage ,bounces ,exitRate ,exits ,newUsers ,pageviews ,sessions ,uniquepageviews ,userss):
# Imports row of data into ADW table
    con = cx_Oracle.connect(inputDetails.username, inputDetails.password, inputDetails.service_name)
    cur = con.cursor()
    print("####################Added###################")
    state = "insert into GA_ADW_PY (ind ,ViewID, channelGrouping ,countryIsoCode ,deviceCategory ,dateHourMinute ,hostname ,pagePath ,sourceMedium ,bounceRate ,avgTimeOnPage ,bounces ,exitRate ,exits ,newUsers ,pageviews ,sessions ,uniquepageviews ,usersno) values (%d, %s ,'%s', '%s', '%s', '%s' ,'%s', '%s', '%s', '%s','%s', '%s', '%s', '%s','%s', '%s', '%s', '%s',%s)" % (ind ,viewID, channelGrouping ,countryIsoCode ,deviceCategory ,dateHourMinute ,hostname ,pagePath ,sourceMedium ,bounceRate ,avgTimeOnPage ,bounces ,exitRate ,exits ,newUsers ,pageviews ,sessions ,uniquepageviews ,userss)
    cur.execute(state)
    con.commit()
    con.close()


def import_data_to_adw(data):
    for i, d in enumerate(list(data)):
        try :
            print(d)
            print(i)
            import_row_to_adw(i, viewID, d[0], d[1], d[2], d[3], d[4], d[5], d[6], d[7], d[8], d[9], d[10], d[11], d[12], d[13], d[14], d[15], d[16])
        except ValueError:
            # use .encode('utf-8') for any value that's throwing an error, for example d[0].encode('utf-8')
            print("UTF-Encoding is required")


# To create/drop a custom table to consume api use this function below or create directly in ADW
def create_table_adw():
    con = cx_Oracle.connect(inputDetails.username, inputDetails.password, inputDetails.service_name)
    cur = con.cursor()
    state = "create table test (ind number, browser varchar(200), sourceMedium varchar(250), hours varchar(250), users varchar(250), revenuePerTransaction varchar(250),startdate varchar(250))"
    cur.execute(state)
    con.commit()
    con.close()

def drop_table_adw():
    con = cx_Oracle.connect(inputDetails.username, inputDetails.password, inputDetails.service_name)
    cur = con.cursor()
    state = "drop table test"
    cur.execute(state)
    con.commit()
    con.close()


#1 Information generated from the InputDetails file
client_id = inputDetails.client_id
client_secret = inputDetails.client_secret
refresh_token=get_refresh_token(client_id,client_secret)

#Information generated from the InputDetails file
viewID = inputDetails.viewID
dim = inputDetails.dim
met = inputDetails.met
start_date = inputDetails.start_date
end_date = inputDetails.end_date
transaction_type = inputDetails.transaction_type
goal_number = inputDetails.goal_number
refresh_token = refresh_token
condition = inputDetails.condition

# Retrieve data from Google API
# json_data=google_analytics_reporting_api_data_extraction(viewID,dim,met,start_date,end_date,refresh_token,transaction_type,goal_number,condition)

# Import data from Google API into ADW
# import_data_to_adw(json_data)

# Create CSV file from Google API data
# create_excel(json_data)

daterange = pd.date_range(start_date, end_date)

def result_for_dates(viewID,dim,met,sday,eday,refresh_token,transaction_type,goal_number,condition,stindex):
    isNotfinished = True
    while isNotfinished:
        json_data=google_analytics_reporting_api_data_extraction(viewID,dim,met,sday,eday,refresh_token,transaction_type,goal_number,condition,stindex)
        stindex+=10000
        create_excel(json_data,viewID)

        if len(json_data) < 10000:
            isNotfinished = False

for single_date in daterange:
        stindex = 1
        day = single_date.strftime("%Y-%m-%d")
        result_for_dates(viewID,dim,met,day,day,refresh_token,transaction_type,goal_number,condition,stindex)