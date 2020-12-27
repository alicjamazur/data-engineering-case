import sys
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark import SparkContext, SQLContext
from pyspark.sql import Window, Row
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
import zipfile
import requests
import glob
import botocore

from pyspark.sql.types import *
import pyspark.sql.functions as F

args = getResolvedOptions(sys.argv, ['JOB_NAME', 's3_output_path'])
s3_output_path = args['s3_output_path']

sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# BUCKET_NAME = 'ala-data'
# KEY = 'source/netflix/netflix.zip'
# # TEMP_DIR = 's3://ala-artifacts/scripts/temp/'
# FILE_NAME = 'netflix.zip'

# s3 = boto3.resource('s3')

# try:
#     s3.Bucket(BUCKET_NAME).download_file(KEY, TEMP_DIR+FILE_NAME)
# except botocore.exceptions.ClientError as e:
#     if e.response['Error']['Code'] == "404":
#         print("The object does not exist.")
#     else:
#         raise
    
# print("Uzipping data...")
# with zipfile.ZipFile(TEMP_DIR+FILE_NAME, 'r') as zip_ref:
#     zip_ref.extractall('.')

# from os import listdir
# print(listdir('.'))

# Load data
s3_input_path = "s3://ala-data/source/netflix/"
rdd = sc.textFile(s3_input_path+'combined*.txt')

# Restructure data and create a DataFrame
rdd_structured = rdd.map(lambda x: Row(x[0], None, None, None) if ':' in x else 
                        Row(None, x.split(',')[0], x.split(',')[1], x.split(',')[2]))

schema = StructType([StructField("Movie ID", StringType(), True),
                     StructField("User ID", StringType(), True),
                     StructField("Rating", StringType(), True),
                     StructField("Date", StringType(), True)])

df = sqlContext.createDataFrame(rdd_structured, schema=schema)

# # Enumerate rows (to be able to use window function)
df = df.withColumn("Index", F.monotonically_increasing_id())

# Fill missing Movie ID with the last value seen
window = Window.orderBy("Index").rowsBetween(-sys.maxsize, 0)
fill_with = F.last(df['Movie ID'], True).over(window)
df = df.withColumn('Movie ID', fill_with)

# Drop unnecessary rows and columns
df = df.na.drop()
df = df.drop('Index')

# Change datatype
df = df.withColumn("Rating", F.col("Rating").cast(IntegerType()))

# data_files = sorted(glob.glob(TEMP_DIR+'combined*.txt'))


# print("Glob: \n",data_files)

# from os import listdir
# print('Files BEFORE creating data_transformed_python.csv\n', listdir('.'))

# with open('data_transformed_python.csv', 'w') as outfile:
    
#     # Write header
#     outfile.write(','.join(['Movie ID', 'Customer ID', 'Rating', 'Date']))
    
#     # Write lines
#     for file in data_files:
#         with open(file, 'r') as f:
#             outfile.write('\n')
#             for line in f.readlines():
#                 if ':' in line:
#                     movie_id = line.split(':')[0]
#                 else:
#                     outfile.write(','.join([movie_id, line]))
#             f.close()
# outfile.close()

# print('Files AFTER creating data_transformed_python.csv\n', listdir('.'))

# Create user_ratings DataFrame
# user_ratings_schema = StructType([StructField("Movie ID", IntegerType(), True),
#                                   StructField("User ID", IntegerType(), True),
#                                   StructField("Rating", IntegerType(), True),
#                                   StructField("Date", StringType(), True)])

# user_ratings = sqlContext.read.csv('data_transformed_python.csv', header=True, schema=user_ratings_schema)

# Create movie_titles DataFrame
movie_titles_schema =  StructType([StructField("Movie ID", IntegerType(), True),
                                   StructField("Year", IntegerType(), True),
                                   StructField("Title", StringType(), True)])

movie_titles = sqlContext.read.csv(s3_input_path+'movie_titles.csv', header=False, schema=movie_titles_schema)


# Join user_ratings and movie_titles DataFrames
netflix_data = df.join(movie_titles, "Movie ID", 'outer')

netflix_data = netflix_data.drop('Movie ID', 'User ID', 'Date')

netflix_data = netflix_data.withColumn("Company", F.lit("Netflix"))

netflix_data.write\
    .format('parquet')\
    .option('header', 'true')\
    .mode('overwrite')\
    .save(s3_output_path)

job.commit()
