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

[data-cleaning.py](https://github.com/sammyrod/global-temp/blob/master/data-cleaning.py)

**S3**

This is the data storage service of choice, since it is cheap and scalable. By converting the files to parquet format and setting retention (or deep storage such as Glaxcier) policies for raw and staging files, we can manage to control cost and scale.

**Redshift Spectrum**

It will be used to conduct the data quality checks and as the endpoint to visualization tools. By utilizing the power of clusters and parrallel processes, redshift spectrum can read data directly from our storage service of choice, which is s3.

The following script represent the Airflow cleaning section of the dag:

airflow/operators.py
```python
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class NullQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table_name,
                 grouping,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.grouping = grouping

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        table_name = self.table_name
        grouping = self.grouping
        null_exist_query = f"""
            SELECT 
                SUM(CASE WHEN LandAverageTemperature IS NULL THEN 1 ELSE 0 END) AS null_LandAverageTemperature
            FROM {table_name}
            """
        null_exist_query2 = f"""
            SELECT 
                SUM(CASE WHEN LandAverageTemperatureUncertainty IS NULL THEN 1 ELSE 0 END) AS null_LandAverageTemperatureUncertainty
            FROM {table_name}
            """
        
        dupliate_exist_query = f"""
            WITH duplicates AS (
                SELECT 
                    dt
                FROM {table_name}
                GROUP BY {grouping}
                HAVING COUNT(1) > 1
            )
            SELECT COUNT(1) AS duplicate_count
            FROM duplicates
        """
        
        self.log.info(f"Conducting data quality on table {table_name}")
        records = redshift.get_records(null_exist_query)
        records2 = redshift.get_records(null_exist_query2)
        duplicate = redshift.get_records(dupliate_exist_query)
        
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check failed. The {table_name} table might have no data.")
        
        num_records = records[0][0]        
        if num_records > 0:
            raise ValueError(f'There are LandAverageTemperature nulls in the {table_name} table.') 
            
        if len(records2) < 1 or len(records2[0]) < 1:
            raise ValueError(f"Data quality check failed. The {table_name} table might have no data.")
        
        num_records = records2[0][0]        
        if num_records > 0:
            raise ValueError(f'There are LandAverageTemperatureUncertainty nulls in the {table_name} table.')   
            
        if len(duplicate) < 1 or len(duplicate[0]) < 1:
            raise ValueError(f"Data quality check failed. The {table_name} table might have no data.")
        
        num_records = duplicate[0][0]        
        if num_records > 0:
            raise ValueError(f'There are duplicates in the {table_name} table.') 
```

Here is how the dag will be initiated: 
```python
from airflow.operators import DataQualityOperator     

run_quality_checks = DataQualityOperator(
    task_id='global_check_nulls_and_duplicates',
    dag=dag,
    redshift_conn_id="redshift",
    table_name="spectrum.GlobalTemperatures",
    grouping="dt"
)

run_quality_checks = DataQualityOperator(
    task_id='country_check_nulls_and_duplicates',
    dag=dag,
    redshift_conn_id="redshift",
    table_name="spectrum.GlobalLandTemperaturesByCountry",
    grouping="dt, Country"
)
```

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
