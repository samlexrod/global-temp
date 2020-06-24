# pyspark imports
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# starting spark application
spark = SparkSession.builder.enableHiveSupport().getOrCreate()

# reading s3 data
df_global = spark.read.csv("world-temp-data/csv-files/GlobalTemperatures.csv", header=True, inferSchema=True)
df_bycountry = spark.read.csv("world-temp-data/csv-files/GlobalLandTemperaturesByCountry.csv", header=True, inferSchema=True)

# converting to parquet for efficiency in the cleaning process
df_global.write.parquet("world-temp-data/parquet-staging/GlobalTemperatures", mode="overwrite")
df_bycountry.write.parquet("world-temp-data/parquet-stating/GlobalLandTemperaturesByCountry", mode="overwrite")

# reading the parquet files
df_global = spark.read.parquet("world-temp-data/parquet-staging/GlobalTemperatures")
df_bycountry = spark.read.parquet("world-temp-data/parquet-staging/GlobalLandTemperaturesByCountry")

## Cleaning parquet file
df_global.createOrReplaceTempView('global')
df_bycountry.createOrReplaceTempView('bycountry')

df_global = spark.sql("""
WITH cte_null_impute AS (
SELECT
    dt
    ,LandAverageTemperature
    ,AVG(LandAverageTemperature) OVER(ORDER BY dt ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING) AS preced_follow_avgtemp
    ,LandAverageTemperatureUncertainty
    ,AVG(LandAverageTemperatureUncertainty) OVER(ORDER BY dt ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING) AS preced_follow_avgunctemp
FROM global
)
SELECT 
    dt
    ,COALESCE(LandAverageTemperature, preced_follow_avgtemp) AS LandAverageTemperature
    ,COALESCE(LandAverageTemperatureUncertainty, preced_follow_avgunctemp) AS LandAverageTemperatureUncertainty
FROM cte_null_impute
""")

df_bycountry = spark.sql("""
WITH cte_null_impute AS (
SELECT
    dt
    ,Country
    ,LandAverageTemperature
    ,AVG(LandAverageTemperature) OVER(ORDER BY dt ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING) AS preced_follow_avgtemp
    ,LandAverageTemperatureUncertainty
    ,AVG(LandAverageTemperatureUncertainty) OVER(ORDER BY dt ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING) AS preced_follow_avgunctemp
FROM global
)
SELECT 
    dt
    ,Country
    ,COALESCE(LandAverageTemperature, preced_follow_avgtemp) AS LandAverageTemperature
    ,COALESCE(LandAverageTemperatureUncertainty, preced_follow_avgunctemp) AS LandAverageTemperatureUncertainty
FROM cte_null_impute
""")

# writing clean parquet files
df_global.write.parquet("world-temp-data/parquet-clean/GlobalTemperatures", mode="overwrite")
df_bycountry.write.parquet("world-temp-data/parquet-clean/GlobalLandTemperaturesByCountry", mode="overwrite")