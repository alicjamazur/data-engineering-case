from pyspark import SparkContext, SQLContext
from pyspark.sql.types import *
from pyspark.sql import Window, Row
from pyspark.sql.functions import *
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

def relationize_netflix_data(directory):

	'''
	A function used to relationize Netflix data using Pyspark. An expensive row enumeration 
	step is necessary to be able to use a window function.

	Parameters
	----------
	directory : str
		a working directory where raw Netflix data resides

	Returns
	-------
	user_ratings : DataFrame
		Pyspark dataframe with Netflix user ratings data with following columns: "Movie ID", "User ID", "Rating", "Date"
	'''

	# Load data
	rdd = sc.textFile(directory + 'combined*.txt')

	# Relationalize and create schema
	rdd_structured = rdd.map(lambda x: Row(movieID=int(x.split(':')[0]), rating=None) if ':' in x else 
	                        Row(movieID=None, rating=int(x.split(',')[1])))

	# Load to DataFrame
	user_ratings_schema = StructType([StructField("movieID", IntegerType(), True),
	                                   StructField("rating", IntegerType(), True)])
	                                   
	user_ratings = sqlContext.createDataFrame(rdd_structured, schema=user_ratings_schema)

	# Enumerate rows
	user_ratings = user_ratings.withColumn("index", monotonically_increasing_id())

	# Fill missing Movie ID with the last value seen
	window = Window.orderBy("index").rowsBetween(-sys.maxsize, 0)
	fill_with = last(user_ratings['movieID'], True).over(window)
	user_ratings = user_ratings.withColumn('movieID', fill_with)

	# Drop unnecessary rows/columns
	user_ratings = user_ratings.drop('index') \
	                            .na.drop()

	return user_ratings


def transform_netflix_data(directory):

	'''
	A function used to relationize Netflix user ratings dataset and join it with with movies metadata. 
	It drops irrelevant columns and create a new column "company" populated with "netflix".

	Parameters
	----------
	directory : str
		a working directory where raw Netflix data resides

	Returns
	-------
	netflix_data : DataFrame
		Pyspark dataframe with Netflix data with following columns: "rating", "year", "title", "company"
	'''

	# Relationize Netflix data 
	user_ratings = transform_netflix_data(directory)

	movie_titles_schema =  StructType([StructField("Movie ID", IntegerType(), True),
                                   StructField("Year", IntegerType(), True),
                                   StructField("Title", StringType(), True)])

	# Load movies data to DataFrame
	movie_titles_schema = StructType([StructField("movieID", IntegerType(), True),
	                                   StructField("year", IntegerType(), True),
	                                   StructField("title", StringType(), True)])

	movie_titles = sqlContext.read.csv(directory + 'movie_titles.csv', header=False, schema=movie_titles_schema)

	# Join DataFrames
	netflix_data = user_ratings.join(movie_titles, "movieID", 'left')

	# Drop irrelevant columns
	netflix_data = netflix_data.drop('movieID')

	# Add a column indicating the company
	netflix_data = netflix_data.withColumn("company", lit("netflix"))

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
	columns and create a new column "company" populated with "amazon".

	Parameters
	----------
	amazon_data : DataFrame
		Pyspark dataframe with original Amazon data
	
	Returns
	-------
	amazon_data : DataFrame
		Pyspark dataframe with Netflix data with following columns: "rating", "year", "title", "company"
	'''

	amazon_data = amazon_data.withColumn("company", lit("amazon"))

	# Parse year 
	expr = r"(\([1-2][0-9]+\))"
	year_found = regexp_extract(amazon_data["product_title"], expr, 0)
	amazon_data = amazon_data.withColumn("year", year_found[2:4].cast(IntegerType()))

	# Filter columns relevant to the problem
	amazon_data = amazon_data.select(col("star_rating").alias("rating").cast(IntegerType()), 
	                                 col('year'),
	                                 col("product_title").alias("title"),
	                                 col('company'))

	return amazon_data


def join_transform(amazon_data, netflix_data):

	'''
	A function used to join Amazon and Netflix data and perform series of transforms in 
	order to be able to compare better the same movies with differently formulated titles.
	- It removes the information about the movie/series part from the title, standardizes it and 
	appends to the end of the title.
	- It converts title to lowercasem removes double spaces, trailing/leading spaces, removes punctuation
	- It propagates movie/series release year for records that lack this information

	Parameters
	----------
	amazon_data : DataFrame
		Pyspark dataframe with Netflix data with following columns: "rating", "year", "title", "company"

	netflix_data : DataFrame
		Pyspark dataframe with Netflix data with following columns: "rating", "year", "title", "company"

	Returns
	-------
	data : DataFrame
		Pyspark dataframe with final Netflix and Amazon movie data with following columns: "rating", "year", "title", "company"
	'''

	# Join datasets
	data = amazon_data.union(netflix_data)

	data = data.repartition(100) 

	# Title to lowercase
	data = data.withColumn("cleanTitle", lower(col("title")))

	# Extract season/series/part number
	number = ('([1-9]([0-9]+)?|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|'
	          'thirteen|fourteen|fifteen|sixteen|seventeen|eighteen|nineteen|twenty|'
	          'first|second|third|fourth|fifth|sixth|seventh|eighth|nineth|tenth|'
	          'eleventh|twelfth|thirteenth|fourteenth|fifteenth|sixteenth|seventeenth|'
	          'eighteenth|nineteenth|twentieth)')

	char_words = '(season|part|series|vol(\w+)?)'

	after = '(?<=' + char_words + ' +)' + number
	before = number + '(?= +' + char_words + ')'

	# If exists, extract the number after the characteristic word, otherwise the number before
	number_after = regexp_extract(data["cleanTitle"], after, 0) 
	condition = number_after != ''
	number_before = regexp_extract(data["cleanTitle"], before, 0) 
	data = data.withColumn("part", when(condition, number_after).otherwise(number_before))

	# # Remove season/part/series info
	remove_num_after = regexp_replace(data["cleanTitle"], char_words + ' ?' + after, '')
	remove_num_before = regexp_replace(data["cleanTitle"], before + ' ' + char_words , '')
	data = data.withColumn("cleanTitle", when(condition, remove_num_after).otherwise(remove_num_before))

	# Remove text starting from parenthesis 
	no_parenthesis = regexp_extract(data["cleanTitle"], r'(^[^(\(|\[)]+)', 0)
	data = data.withColumn("cleanTitle", no_parenthesis)

	# Uniformize movie/serie part indicator
	data.cache()

	data = data.withColumn('part', when((col('part') == 'one') | (col('part') =='first'), '1') \
	                              .when((col('part') == 'two') | (col('part') == 'second'), '2') \
	                              .when((col('part') == 'three') | (col('part') == 'third'), '3') \
	                              .when((col('part') == 'four') | (col('part') == 'fourth'), '4') \
	                              .when((col('part') == 'five') | (col('part') == 'fifth'), '5') \
	                              .when((col('part') == 'six') | (col('part') == 'sixth'), '6') \
	                              .when((col('part') == 'seven') | (col('part') == 'seventh'), '7') \
	                              .when((col('part') == 'eight') | (col('part') == 'eighth'), '8') \
	                              .when((col('part') == 'nine') | (col('part') == 'nineth'), '9') \
	                              .when((col('part') == 'ten') | (col('part') == 'tenth'), '10') \
	                              .when((col('part') == 'eleven') | (col('part') == 'eleventh'), '11') \
	                              .when((col('part') == 'twelve') | (col('part') == 'twelfth'), '12') \
	                              .when((col('part') == 'thirteen') | (col('part') == 'thirteenth'), '13') \
	                              .when((col('part') == 'fourteen') | (col('part') == 'fourteenth'), '14') \
	                              .when((col('part') == 'fifteen') | (col('part') == 'fifteenth'), '15') \
	                              .when((col('part') == 'sixteen') | (col('part') == 'sixteenth'), '16') \
	                              .when((col('part') == 'seventeen') | (col('part') == 'seventeenth'), '17') \
	                              .when((col('part') == 'eighteen') | (col('part') == 'eighteenth'), '18') \
	                              .when((col('part') == 'nineteen') | (col('part') == 'nineteenth'), '19') \
	                              .when((col('part') == 'twenty') | (col('part') == 'twentieth'), '20') \
	                              .otherwise(col('part')))

	# Add info about series/movie part to the end of the string
	concat = concat(col('cleanTitle'), lit(' '), col('part'))
	data = data.withColumn('cleanTitle', concat)

	# Remove punctuation
	no_punctuation = regexp_replace(data["cleanTitle"], r'([^\s\w_]|_)+', '')
	data = data.withColumn("cleanTitle", no_punctuation)

	# Remove double spaces
	no_double_spaces = regexp_replace(data["cleanTitle"], r'  +', ' ')
	data = data.withColumn("cleanTitle", no_double_spaces)

	# Trim leading and trailing spaces
	data = data.withColumn("cleanTitle", trim(col("cleanTitle")))

	# Propagate title release 
	window = Window.partitionBy("cleanTitle").orderBy(col("year").desc()).rowsBetween(Window.unboundedPreceding, 0)
	fill_year = last(col("year"), True).over(window)
	data = data.withColumn("year", fill_year)

	data = data.select(col("company"), col('cleanTitle').alias('title'), col("year"), col("rating"))

	data = data.repartition(10)

	data.write.format('parquet') \
	          .option('header', 'true') \
	          .mode('overwrite') \
	          .save(s3_output_path + 'final_data.parquet')

	return data


def main():

	# Set up directory 
	directory = '/some_directory'

	# Load and transform Netflix data 
	load_netflix_data(url='https://www.kaggle.com/netflix-inc/netflix-prize-data/download', directory)

	netflix_data = transform_netflix_data(directory)

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
	data = data.repartition(10)

	data.write.format('parquet') \
	          .option('header', 'true') \
	          .mode('overwrite') \
	          .save('final_data.parquet')

if __name__ == "__main__":
    main()
