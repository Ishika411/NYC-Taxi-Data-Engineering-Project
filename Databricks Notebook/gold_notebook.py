from pyspark.sql.functions import *
from pyspark.sql.types import *
%md
###Data Access
spark.conf.set("fs.azure.account.auth.type.nyctaxidatalakeishika.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.nyctaxidatalakeishika.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.nyctaxidatalakeishika.dfs.core.windows.net", "<client-id>")
spark.conf.set("fs.azure.account.oauth2.client.secret.nyctaxidatalakeishika.dfs.core.windows.net", "<client-secret>")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.nyctaxidatalakeishika.dfs.core.windows.net", "<oauth-endpoint>")
%sql
CREATE DATABASE gold
%md
**Storage Variables**
silver='abfss://silver@nyctaxidatalakeishika.dfs.core.windows.net'
gold='abfss://gold@nyctaxidatalakeishika.dfs.core.windows.net'
df_trip_zone=spark.read.format('parquet')\
                .option('inferschema',True)\
                .option('header',True)\
                .load(silver+'/trip_zone')
df_trip_type=spark.read.format('parquet')\
                .option('inferschema',True)\
                .option('header',True)\
                .load(silver+'/trip_type')
df_trip_data=spark.read.format('parquet')\
                .option('inferschema',True)\
                .option('header',True)\
                .load(silver+'/trip_data')
%md
### Saving data in Gold layer in Delta format
df_trip_data.write.format('delta')\
                .mode('append')\
                .option('path',f'{gold}/trip_data')\
                .saveAsTable('gold.trip_data')
df_trip_zone.write.format("delta") \
                .mode("overwrite") \
                .option("path", f'{gold}/trip_zone') \
                .saveAsTable("gold.trip_zone")
df_trip_type.write.format("delta") \
                .mode("overwrite") \
                .option("path", f'{gold}/trip_type') \
                .saveAsTable("gold.trip_type")
dbutils.fs.ls(
    "abfss://gold@nyctaxidatalakeishika.dfs.core.windows.net/"
)
%md
### Querying Delta Tables
%sql
select * from gold.trip_data;
%sql
select * from gold.trip_zone
where Borough = 'EWR';
%sql
UPDATE gold.trip_zone
SET Borough = 'EMR'
WHERE Borough = 'EWR';
%sql
SELECT * FROM gold.trip_zone
WHERE Borough = 'EMR';
%sql
DELETE FROM gold.trip_zone
WHERE Borough = 'EMR';
%sql
SELECT * FROM gold.trip_zone
WHERE Borough = 'EMR';
%sql
DESCRIBE HISTORY gold.trip_zone;
%sql
SELECT * FROM gold.trip_zone VERSION AS OF 0
WHERE Borough='EWR';
%sql
RESTORE TABLE gold.trip_zone TO VERSION AS OF 0
%sql
VACUUM gold.trip_zone;
%sql
select * from gold.trip_type;
%sql
SELECT * FROM gold.trip_data;