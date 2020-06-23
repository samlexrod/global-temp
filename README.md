# Global Temperatures Data Pipeline Documentation
Final Capstone project of Udacity Data Engineering Nanodegree. 

## Tools and Technologies
**AWS Lambda**
This is the first data ingestion service of choice since I can allocate on demand EC2 instances to run a python script design to request the text file from http://berkeleyearth.lbl.gov/.

For example, the Lambda function can get the response from the latest text file and wrangle the data into a json file for the new month appended/partitioned by month in s3.

```python
import requests
url = "http://berkeleyearth.lbl.gov/auto/Global/Complete_TAVG_complete.txt"
response = requests.get(url)
```

**AWS EMR**
This is the second data ingestion/pre-processing service of choice since I can create a cluster of master/worker nodes with Spark, Hive, and Livy so we can aggregate, transform, and clean the data.

As data gets bigger, we are using Spark to clean and impute data into the dataset.

The following script contains the cleaning python code in pyspark:
[]()

**S3**
This is the data storage service of choice, since it is cheap and scalable. By converting the files to parquet format and setting retention (or deep storage such as Glaxcier) policies for raw and staging files, we can manage to control cost and scale.

**Redshift Spectrum**
This is the endpoint to visualization tools. By utilizing the power of clusters and parrallel processes, redshift spectrum can read data directly from our storage service of choice, which is s3.

## Data Gathering and Update Schedule

**Data Gathering Steps**
1. 

Propose how often the data should be updated and why.

## Elastic Data Handling Approach
Write a description of how you would approach the problem differently under the following scenarios:
The data was increased by 100x.

## Process Scheduler
The data populates a dashboard that must be updated on a daily basis by 7am every day.

## Database Scalability
The database needed to be accessed by 100+ people.
