# Days Towards the Election
In this project, I am trying to process tweets to get an estimate for user rating of each candidate for presidential election over time as we get closer to the election day.

# Why we need this?
- With the growth of people interactions online, it is good to have an understanding of people opinion about a potential election candidate using online data.
- While using traditional polling is a good approach it may not cover all the demographic.

# Size and rate of data
- Data could be acquired from different sources including twitter, Facebook, blogger,...
- Assuming 1000 per second twit with the hashtag #trump for example.
Rate of data coming in 100/s * 140 Byte = 13.7 KB
- Data Generation:
            13.7KB * 3600 seconds * 24 hours = 1.1 GB per day 
                                            or 35.5 GB per month

- The size of the data is about 26 GB daily or 20 relevant tweet per second.

# Pipeline
![Alt text](figs/Pipeline.png?raw=true "Pipeline")