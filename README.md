# Days Towards the Election
In this project, I am trying to process tweets to get an estimate for user rating of each candidate for presidential election over time as we get closer to the election day.

The size of the data is about 26 GB daily or 20 relevant tweet per second.

The pipeline would be: S3 -> Kafka -> Hadoop <--> Spark -> PostgreSQL -> Flask