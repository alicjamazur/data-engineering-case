import sys
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
# from pyspark.context import SparkContext
from pyspark import SparkContext, SQLContext
from awsglue.context import GlueContext
from awsglue.job import Job

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

# s3_input_path = "s3://ala-data/source/amazon/amazon_reviews_us_Video_DVD_v1_00.tsv"

s3_input_path='s3://ala-data/source/amazon/'
files = ["amazon_reviews_us_Video_v1_00.tsv",
        "amazon_reviews_us_Video_DVD_v1_00.tsv",
        "amazon_reviews_us_Digital_Video_Download_v1_00.tsv"]


amazon_data = sqlContext.read.csv(s3_input_path+files[0], sep='\t', header=True)

for i, file_i in enumerate(files[1:]):
    print("Processing file", i, ":", file_i)
    # Load file "i" to DF
    part_i = sqlContext.read.csv(s3_input_path+file_i, sep='\t', header=True)
    
    # Union files
    amazon_data = amazon_data.union(part_i)


# inputGDF = glueContext.create_dynamic_frame_from_options(connection_type = "s3", connection_options = {"paths": [s3_input_path]}, format = "tsv")
# amazon_data = inputGDF.toDF()
# amazon_data = sqlContext.read.csv(s3_input_path, sep='\t', header=True)

# amazon_data = digital_video.union(video_dvd).union(video)
# print("--------------------->>>>>>>>>>>>>>>>>>>>>",amazon_data.columns)
amazon_data = amazon_data.select(F.col("product_title").alias("Title"), F.col("star_rating").alias("Rating").cast(IntegerType()))
amazon_data = amazon_data.withColumn("Company", F.lit("Amazon"))

# Parse Year 
expr = r"(\([1-2][0-9]+\))"
year_found = F.regexp_extract(amazon_data["Title"], expr, 0)
amazon_data = amazon_data.withColumn("Year", year_found[2:4].cast(IntegerType()))

# Select relevant columns
amazon_data = amazon_data.select("Rating", "Year", "Title", "Company")

amazon_data.write\
    .format("parquet")\
    .option("header", "true")\
    .mode("overwrite")\
    .save(s3_output_path)

job.commit()

# amazon_data = amazon_data.select(F.col("product_title").alias('Title'), F.col('star_rating').alias('Rating').cast(IntegerType()))
# data = data.withColumn("Clean Title", F.lower(F.col("Title")))


# outputGDF = glueContext.write_dynamic_frame.from_options(frame = inputGDF, connection_type = "s3", connection_options = {"path": out_path}, format = "parquet")

# salesForecastedByDate_DF = \
#     sales_DF\
#         .where(sales_DF['opportunity_stage'] == 'Lead')\
#         .groupBy('date')\
#         .agg({'forecasted_monthly_revenue': 'sum', 'opportunity_stage': 'count'})\
#         .orderBy('date')\
#         .withColumnRenamed('count(opportunity_stage)', 'leads_generated')\
#         .withColumnRenamed('sum(forecasted_monthly_revenue)', 'forecasted_lead_revenue')\
#         .coalesce(1)

