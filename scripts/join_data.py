import sys
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import gzip
import requests

from pyspark.sql.types import *
import pyspark.sql.functions as F

args = getResolvedOptions(sys.argv, ['JOB_NAME', 's3_output_path'])
s3_output_path = args['s3_output_path']

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load data 
amazon_data = sqlContext.read.csv(s3_output_path + 'amazon_data', sep=',', header=True)
netflix_data = sqlContext.read.csv(s3_output_path + 'netflix_data', sep=',', header=True)

# Join datasets
data = amazon_data.union(netflix_data)

# Title to lowercase
data = data.withColumn("Clean Title", F.lower(F.col("Title")))

# Remove text starting from parenthesis until the end
no_parenthesis = F.regexp_extract(data["Clean Title"], r'(^[^(\(|\[)]+)', 0)
data = data.withColumn("Clean Title", no_parenthesis)

# Remove characteristic words and a word that follows
no_char_words = F.regexp_replace(data["Clean Title"], ' ?(season|volume|edition|part|set) (\w+)$', '')
data = data.withColumn("Clean Title", no_char_words)

# Check for characteristic words after special character (last observed - or :) run 3 times
after_special_char = F.regexp_extract(data["Clean Title"], r'( ?[-|:])(?!.*[-|:]).*', 0)
data = data.withColumn("After Special Char", after_special_char)

# Remove text after special character if it contains any of characteristic words
characteristic_words = 'series|movie|season|volume|edition|part|set|collection|dvd|bluray'
condition = F.col("After Special Char").rlike(characteristic_words)
data = data.withColumn('Clean Title', F.when(condition, F.regexp_replace("Clean Title", r'( ?[-|:])(?!.*[-|:]).*', ''))
                                           .otherwise(F.col("Clean Title")))

# Remove punctuation
no_punctuation = F.regexp_replace(data["Clean Title"], r'([^\s\w_]|_)+', '')
data = data.withColumn("Clean Title", no_punctuation)

# Trim leading and trailing spaces
data = data.withColumn("Clean Title", F.trim(F.col("Clean Title")))

# Remove double spaces
no_double_spaces = F.regexp_replace(data["Clean Title"], r'  +', '')
data = data.withColumn("Clean Title", no_double_spaces)

# Impute Year
window = Window.partitionBy("Clean Title").orderBy(F.col("Year").desc()).rowsBetween(Window.unboundedPreceding, 0)
fill_year = F.last(F.col("Year"), True).over(window)
data = data.withColumn("Year", fill_year)

data = data.select(F.col("Company"), F.col('Clean Title').alias('Title'), F.col("Year"), F.col("Rating"))

data.write\
    .format('parquet')\
    .option('header', 'true')\
    .mode('overwrite')\
    .save(s3_output_path)

job.commit()
