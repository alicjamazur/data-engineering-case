from pyspark import SparkContext, SQLContext
from pyspark.sql.types import *
from pyspark.sql import Window
from pyspark.sql import Row
from pyspark.sql import functions as F
import sys
import glob

sc = SparkContext()
sqlContext = SQLContext(sc)

def load_netflix_data(url, directory):
	'''
	A function that downloads Netflix Prize dataset from Kaggle, saves and extracts in to the 'directory'.

	Parameters
	----------
	url : str
		URL to download Netflix data
	directory : str
		directory where downloaded data will be saved and extracted
	'''

	r = requests.get(url, allow_redirects=True)
	open(directory+'files.tar', 'wb').write(r.content)

	my_tar = tarfile.open('files.tar')
	my_tar.extractall() 
	my_tar.close()

def relationize_netflix_spark(directory):

	'''
	A function used to relationize Netflix data using Pyspark. For this 
	problem this is NOT an optimal solution due to considerable runtime. 

	Parameters
	----------
	directory : str
		a working directory where raw Netflix data resides

	Returns
	-------
	df : DataFrame
		Pyspark dataframe with Netflix user ratings data with following columns: "Movie ID", "User ID", "Rating", "Date"
	'''

	# Load data
	rdd = sc.textFile(directory + 'combined*.txt')

	# Relationize data and create a DataFrame
	rdd_structured = rdd.map(lambda x: Row(x[0], None, None, None) if ':' in x else 
	                        Row(None, x.split(',')[0], x.split(',')[1], x.split(',')[2]))

	schema = StructType([StructField("Movie ID", StringType(), True),
	                     StructField("User ID", StringType(), True),
	                     StructField("Rating", StringType(), True),
	                     StructField("Date", StringType(), True)])

	df = sqlContext.createDataFrame(rdd_structured, schema=schema)

	# Enumerate rows (to be able to use window function)
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

	return df

def relationize_netflix_python(outfile, directory):

	'''
	A function used to relationize Netflix data using Python. 
	For this problem this is a more optimal solution than using Pyspark. 

	Parameters
	----------
	directory : str
		a working directory where raw Netflix data resides

	outfile : str
		name of a file with relationized Netflix user ratings

	Returns
	-------
	user_ratings : DataFrame
		Pyspark dataframe with Netflix user ratings data with following columns: "Movie ID", "User ID", "Rating", "Date"
	'''

	data_files = sorted(glob.glob(directory+'combined*.txt'))

	with open(directory+outfile, 'w') as outfile:
	    
	    # Write header
	    outfile.write(','.join(['Movie ID', 'Customer ID', 'Rating', 'Date']))
	    
	    # Write lines
	    for file in data_files:
	        with open(file, 'r') as f:
	            outfile.write('\n')
	            for line in f.readlines():
	                if ':' in line:
	                    movie_id = line.split(':')[0]
	                else:
	                    outfile.write(','.join([movie_id, line]))
	            f.close()
	outfile.close()

	user_ratings_schema = StructType([StructField("Movie ID", IntegerType(), True),
                                  StructField("User ID", IntegerType(), True),
                                  StructField("Rating", IntegerType(), True),
                                  StructField("Date", StringType(), True)])

	user_ratings = sqlContext.read.csv(directory+outfile, header=True, schema=user_ratings_schema)

	return user_ratings

def transform_netflix_data(outfile, directory):

	'''
	A function used to relationize Netflix user ratings dataset and join it with with movies metadata. 
	It drops irrelevant columns and create a new column "Company" populated with "Netflix".

	Parameters
	----------
	directory : str
		a working directory where raw Netflix data resides

	Returns
	-------
	netflix_data : DataFrame
		Pyspark dataframe with Netflix data with following columns: "Rating", "Year", "Title", "Company"
	'''

	# Relationize Netflix data 
	user_ratings = relationize_netflix_python(outfile, directory)

	movie_titles_schema =  StructType([StructField("Movie ID", IntegerType(), True),
                                   StructField("Year", IntegerType(), True),
                                   StructField("Title", StringType(), True)])

	# Load movies file
	movie_titles = sqlContext.read.csv('movie_titles.csv', header=False, schema=movie_titles_schema)

	# Join 
	netflix_data = user_ratings.join(movie_titles, "Movie ID", 'outer')\
								.drop('Movie ID', 'User ID', 'Date')\
								.withColumn("Company", F.lit("Netflix"))
	return netflix_data

def load_amazon_data(s3_input_path,
					 files = ["amazon_reviews_us_Video_v1_00.tsv.gz",
					     	 "amazon_reviews_us_Video_DVD_v1_00.tsv.gz",
						  	 "amazon_reviews_us_Digital_Video_Download_v1_00.tsv.gz"],
					 directory):

	'''
	A function that downloads Amazon dataset from S3, saves it and extracts to the 'directory'.

	Parameters
	----------
	s3_input_path : str
		S3 path with Amazon Reviews data
	files : list of str
		list of files to download
	directory : str
		directory where downloaded data will be saved and extracted

	Returns
	-------
	amazon_data : DataFrame
		Pyspark dataframe with original Amazon data
	'''

	for i, file in enumerate(files):

		# Download
		url = s3_input_path + file
		r = requests.get(url, allow_redirects=True)
		open('file.tsv.gz', 'wb').write(r.content)

		# Unzip
		f = gzip.open('file.tsv.gz', 'rb')
		file_content = f.read()
		f.close()

		# Load to DF
		part_i = sqlContext.read.csv(file_content, sep='\t', header=True)

	# Join data
	amazon_data = part_0.union(part_1).union(part_2)

	return amazon_data

def transform_amazon_data(amazon_data):

	'''
	A function used to extract year of movie release from movie title, drop irrelevant 
	columns and create a new column "Company" populated with "Amazon".

	Parameters
	----------
	amazon_data : DataFrame
		Pyspark dataframe with original Amazon data
	
	Returns
	-------
	amazon_data : DataFrame
		Pyspark dataframe with Netflix data with following columns: "Rating", "Year", "Title", "Company"
	'''

	amazon_data = amazon_data.select(F.col("product_title").alias('Title'), F.col('star_rating').alias('Rating').cast(IntegerType()))
	amazon_data = amazon_data.withColumn("Company", F.lit("Amazon"))

	expr = r"(\([1-2][0-9]+\))"
	year_found = F.regexp_extract(amazon_data["Title"], expr, 0)
	amazon_data = amazon_data.withColumn("Year", year_found[2:4].cast(IntegerType()))

	amazon_data = amazon_data.select("Rating", "Year", "Title", "Company")

	return amazon_data


def join_transform(amazon_data, netflix_data):

	'''
	A function used to join Amazon and Netflix data and perform series of transforms in 
	order to be able to compare better the same movies with differently expressed titled.

	Parameters
	----------
	amazon_data : DataFrame
		Pyspark dataframe with Netflix data with following columns: "Rating", "Year", "Title", "Company"

	netflix_data : DataFrame
		Pyspark dataframe with Netflix data with following columns: "Rating", "Year", "Title", "Company"

	Returns
	-------
	data : DataFrame
		Pyspark dataframe with final Netflix and Amazon movie data with following columns: "Rating", "Year", "Title", "Company"
	'''

	# Join data 
	data = amazon_data.union(netflix_data)

	# Title to lowercase
	data = data.withColumn("Clean Title", F.lower(F.col("Title")))

	# Remove text starting from parenthesis until the end
	no_parenthesis = F.regexp_extract(data["Clean Title"], r'(^[^(\(|\[)]+)', 0)
	data = data.withColumn("Clean Title", no_parenthesis)

	# Remove characteristic words and a word that follows
	no_char_words = F.regexp_replace(data["Clean Title"], ' ?(season|volume|edition|part|set) (\w+)$', '')
	data = data.withColumn("Clean Title", no_char_words)

	# Check for characteristic words after special character (last observed - or :) 
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

	# Impute "Year" if present in other companys' records
	window = Window.partitionBy("Clean Title").orderBy(F.col("Year").desc()).rowsBetween(Window.unboundedPreceding, 0)
	fill_year = F.last(F.col("Year"), True).over(window)
	data = data.withColumn("Year", fill_year)

	return data


def main():

	# Set up directory 
	directory = '/some_directory'

	# Load and transform Netflix data 
	load_netflix_data(url='https://www.kaggle.com/netflix-inc/netflix-prize-data/download', directory)

	netflix_data = transform_netflix(outfile='data_transformed.csv', directory)

	# Load and transform Amazon data 
	amazon_data = load_amazon_data(s3_input_path='s3://amazon-reviews-pds/tsv/',
								   files = ["amazon_reviews_us_Video_v1_00.tsv.gz",
								            "amazon_reviews_us_Video_DVD_v1_00.tsv.gz",
									  	    "amazon_reviews_us_Digital_Video_Download_v1_00.tsv.gz"],
								   directory)

	amazon_data = transform_amazon_data(amazon_data)

	# Join both datasets, transform and save final dataset to Parquet file
	data = join_transform(amazon_data, netflix_data)

	# Save final dataset
	data.write.parquet("out/data.parquet")


if __name__ == "__main__":
    main()
