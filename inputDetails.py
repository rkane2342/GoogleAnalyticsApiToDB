# Generated from google developer account API section.
client_id = '****************'
client_secret = '****************'
#To call API for specific App/Website input the ViewID which can be found using https://keyword-hero.com/documentation/finding-your-view-id-in-google-analytics
#Input Dimensions and Metrics to be pulled in dim and met variable accordingly and input the start and end date accordingly.
viewID='*************'
#dim=['ga:browser','ga:sourceMedium','ga:date']
dim=['ga:channelGrouping,ga:countryIsoCode,ga:deviceCategory,ga:date,ga:hostname,ga:pagePath,ga:sourceMedium']
met=['ga:bounceRate,ga:avgTimeOnPage,ga:bounces,ga:exitRate,ga:exits,ga:newUsers,ga:pageviews,ga:sessions,ga:uniquePageviews,ga:users']

#met=['ga:users','ga:revenuePerTransaction']
start_date='2020-03-13'
end_date='2020-03-20'
transaction_type='Transaction'
goal_number=''
condition=''

#ADW Details
username = '*********'
password = '***********'
service_name = '*************'

