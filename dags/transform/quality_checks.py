# Reads the table data 
# perform quality checks
# Error when any check fails or writes success message

from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, StringType,LongType
from datetime import datetime, timedelta, date
from pyspark.sql.functions import udf, regexp_replace,dayofmonth, min, max,count,dayofweek, date_format, trim, isnull
spark.sparkContext.setLogLevel("WARN")

spark_logger = spark._jvm.org.apache.log4j.Logger
file_logger= spark_logger.getLogger('DAG')

path ='s3://udacity-immigration-project/output/'

# dict holding the tables key fields 
tables_dict ={ 
			'1' : ['cities_demographics',['City','State'], True],
			'2' : ['country_codes',['Country_code'], True],
			'3' : ['US_Visas',['Visa_Type'], True],
			'4' : ['arrival_dates',['arrival_date'], True],
			'5' : ['immigration_fact',[''], False]
			}


# Check cloumns count is grater and number of rows greater than 0
# Check key field is not null
def check_tables(path, table_name,key_list, key_check_required):
	df_test = spark.read.parquet(path +  table_name)
	if (len(df_test.columns) > 0) & (df_test.count() > 0):
		file_logger.warn('Success Counts & Columns Quality Check for table {}'.format(table_name))
	else:
		raise ValueError('Not Success Counts & Columns Quality Check for table {}'.format(table_name))

	if key_check_required:
		for key in key_list:
			if (df_test.where(isnull(key)).count() >= 1):
				raise ValueError('Null Values exsits for key {} table {}'.format(key,table_name))
			else:
				file_logger.warn('Success No Nulls for Key {}  table {}'.format(key, table_name))	


# loop through the dict to check the qulity of each table
for key, value in tables_dict.items():
	check_tables(path, value[0], value[1], value[2])