# Investment Compass
The goal of this project is to extract useful information from massive amount of log files for the end user. Here, I am processing the Security and Exchaneg Comission's access log files to get an indicator of companies popularity in the eyes of public. 

Public companies and other regulated companies submit quarterly and annual reports, as well as other periodic reports to U.S. Securities and Exchange Commission (SEC). SEC provides those document as public records for everybody. Also, SEC provide access log file for the request for those documents which includes request masked ip, time and date, document name, size of document, etc.

# Why we need this?
- Based on the documents submitted to SEC and the requests for those documents we can gage how much interest are there for a company. 
- This could be beneficial for anybody interested in stocks or any venture capital company to see if it is worth it to pay attention to a company and consequently to invest in that company or not.

# Size of data
- In this project I processed one year worth of data which is about 1 TB of csv access log file.
- There are access log files dating back up to 2003 and at least in the recent years there is about 2.5 GB of access log in csv format for each day.

# What are the data engineering challenges?
- One of the metrics that is interesting to look at is the unique number of requests for each document that can be used to gage users interest for a company. The challenge for unique requests is that if we try to remember all the unique requesters in a traditional way using sets, the can become arbitrarily large very fast and outgrow memory. This leads to two issues one at the level of parallel process which leads to a large amount of shuffle reads/writes. Additionaly, to keep this in a data base we need to store considerably more data.
**Solution**: to solve this challenge I use a statistical model called HyperLogLog which sacrifying the accuracy slightly, we can find if an item is in a set with finite amount of memory. 
- Since user should be able to get result for an arbitrary time window, there could be considerable amount of redundant computation if the batch views are store only at the smallest granuality scales, i.e. hours. For example a user request for aggregation for a window of one year results in 365x24=8,760 records to be gathered and processed. 
**Solution**: To avoid this overhead at the frontend, I compute the results a different levels: hourly, daily, and monthly and store it in the database. Consequently when a user wants to look at a year long window, only 12 records need to be queried from database instead of 8,760 records.

# Pipeline
![Alt text](figs/Pipeline.png?raw=true "Pipeline")

# Project links
Project Live Demo: [http://investmentcompass.live/](http://investmentcompass.live/)
Slides: [http://slides.investmentcompass.live/](http://slides.investmentcompass.live/)