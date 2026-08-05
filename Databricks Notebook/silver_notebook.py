%md
###Data Access
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark.conf.set("fs.azure.account.auth.type.nyctaxidatalakeishika.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.nyctaxidatalakeishika.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
# Azure ADLS Authentication
spark.conf.set("fs.azure.account.auth.type.<storage-account>.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.<storage-account>.dfs.core.windows.net",
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.<storage-account>.dfs.core.windows.net",
               "<client-id>")
spark.conf.set("fs.azure.account.oauth2.client.secret.<storage-account>.dfs.core.windows.net",
               "<client-secret>")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.<storage-account>.dfs.core.windows.net",
               "<oauth-endpoint>")

# List files in Bronze container
dbutils.fs.ls("abfss://bronze@<storage-account>.dfs.core.windows.net")

# Read Data
df_trip_zone = spark.read.format("csv") \
    .option("inferSchema", True) \
    .option("header", True) \
    .load("abfss://bronze@<storage-account>.dfs.core.windows.net/trip_zone")

df_trip_type = spark.read.format("csv") \
    .option("inferSchema", True) \
    .option("header", True) \
    .load("abfss://bronze@<storage-account>.dfs.core.windows.net/trip_type")

df_trip_data = spark.read.format("parquet") \
    .schema(my_schema) \
    .option("header", True) \
    .option("recursiveFileLookup", True) \
    .load("abfss://bronze@<storage-account>.dfs.core.windows.net/trip-data/")

# Write to Silver Layer
df_trip_type.write.format("parquet") \
    .mode("append") \
    .option("path", "abfss://silver@<storage-account>.dfs.core.windows.net/trip_type") \
    .save()

df_trip_zone.write.format("parquet") \
    .mode("append") \
    .option("path", "abfss://silver@<storage-account>.dfs.core.windows.net/trip_zone") \
    .save()

df_trip_data.write.format("parquet") \
    .mode("overwrite") \
    .option("path", "abfss://silver@<storage-account>.dfs.core.windows.net/trip_data") \
    .save()
dbutils.fs.ls("abfss://bronze@nyctaxidatalakeishika.dfs.core.windows.net")
%md
###Read Data
df_trip_zone=spark.read.format('csv')\
            .option('inferSchema',True)\
            .option('header',True)\
            .load('abfss://bronze@nyctaxidatalakeishika.dfs.core.windows.net/trip_zone')
df_trip_type=spark.read.format('csv')\
                    .option('inferschema',True)\
                    .option('header',True)\
                    .load('abfss://bronze@nyctaxidatalakeishika.dfs.core.windows.net/trip_type')
my_schema='''
            VendorId BIGINT,
            lpep_pickup_datetime TIMESTAMP,
            lpep_dropoff_datetime TIMESTAMP,
            store_and_fwd_flag STRING,
            RatecodeID BIGINT,
            PULocationID BIGINT,
            DOLocationID BIGINT,
            passenger_count BIGINT,
            trip_distance DOUBLE,
            fare_amount DOUBLE,
            extra DOUBLE,
            mta_tax DOUBLE,
            tip_amount DOUBLE,
            tolls_amount DOUBLE,
            ehail_fee DOUBLE,
            improvement_surcharge DOUBLE,
            total_amount DOUBLE,
            payment_type BIGINT,
            trip_type BIGINT,
            congestion_surcharge DOUBLE
        '''
df_trip_data=spark.read.format('parquet')\
                    .schema(my_schema)\
                    .option('header',True)\
                    .option('recursiveFileLookup',True)\
                    .load('abfss://bronze@nyctaxidatalakeishika.dfs.core.windows.net/trip-data/')
%md
### Data Transformation
df_trip_type=df_trip_type.withColumnRenamed('description','trip_type_description')
df_trip_type.display()
df_trip_type.write.format('parquet')\
            .mode('append')\
            .option('path','abfss://silver@nyctaxidatalakeishika.dfs.core.windows.net/trip_type')\
            .save()
df_trip_zone=df_trip_zone.withColumn('Zone1',split(col('Zone'),'/')[0])\
                        .withColumn('Zone2',split(col('Zone'),'/')[1])\
                        .drop('Zone')
df_trip_zone.display()
df_trip_zone.write.format('parquet')\
                .mode('append')\
                .option('path','abfss://silver@nyctaxidatalakeishika.dfs.core.windows.net/trip_zone')\
                .save()
df_trip_data=df_trip_data.withColumn('Pickup_date',to_date('lpep_pickup_datetime'))\
            .withColumn('Pickup_time',date_format('lpep_pickup_datetime','HH:mm'))\
            .withColumn('Dropoff_date',to_date('lpep_dropoff_datetime'))\
            .withColumn('Dropoff_time',date_format('lpep_dropoff_datetime','HH:mm'))\
            .select('VendorId','PULocationID','DOLocationID','fare_amount','total_amount','trip_distance','Pickup_date','Pickup_time','Dropoff_date','Dropoff_time')
df_trip_data.display()
df_trip_data.write.format('parquet')\
                .mode('overwrite')\
                .option('path','abfss://silver@nyctaxidatalakeishika.dfs.core.windows.net/trip_data')\
                .save()
%md
###Analysis(Visualizations)
df_trip_data.display()