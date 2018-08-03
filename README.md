# Getting Insight From SEC Log Files
In this project, I am going to process the SEC access log files to extract the user interests and search engine interest in certain companies. 

Public companies and other regulated companies submit quarterly and annual reports, as well as other periodic reports to U.S. Securities and Exchange Commission (SEC). SEC provide those document as public records for everybody. Also, SEC provide access log file for the request for those documents which includes request time and date, document name, size of document, etc.

# Why we need this?
- Based on the documents submitted to SEC and the requests for those documents we can gage how much interest are there for a company. 
- This could be beneficial for anybody interested in stocks or any venture capital company to see if it is worth it pay attention to a company and consequently to invest in that company or not.

# Size and rate of data
- There are access log files dating back up to 2003 and at least in the recent years there about 2.5 GB of access log in csv format for each day.
- This amount of day per day is equivalent to about 300 records per second.

# What are the data engineering challenges?
- One of the metrics that is interesting to look at is the unique number of requests for each document that can be used to gage users interest for a company. The challenge for unique requests is that if we try to remember all the uniques requesters it list can become large very fast and outgrow memory. I am thinking of using a statistical model like HyperLogLog for approximating the number of distinct element in a multiset.
- There are lots of aggregation for a large amount of data, which can lead to considerable amount of shuffling in the map/reduce model. I was thinking I might be able to store some of the processes data in the database or S3 to reduce the amount of recomputation. 
- Since I am planning to give a dashboard for user to be able to get result to arbitrary time windows. There could be considerable amount of redundant computation if the batch views are store only at smallest granuality scales, i.e. hours, and for example a user request for aggregation for a window of one year. I was thinking that I could store data at different granularities for example hour, day, week, month, year, so at the time of query I only require to query small amount of data from the database.

# Pipeline
![Alt text](figs/Pipeline.png?raw=true "Pipeline")