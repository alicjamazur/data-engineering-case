# Data Engineering Case
The goal of this project is to provide a solution for Business Inteligence team who needs to perform a market study on publicly available data of their competitors. The project features extraction, transformation and loading of Amazon Customer Reviews dataset and Netflix Prize dataset with Pyspark to a data warehouse (Amazon Redshift). The execution of task was demonstrated by using AWS Step Functions coded with AWS CloudFormation. Below I provide a shortcut to create exactly the same architecture on AWS environment.

Finally, a set of [SQL queries](https://github.com/molly-moon/data-engineering-case/blob/master/sql/business_queries.sql) was executed in order to respond to some business-related questions. [Here](https://raw.githubusercontent.com/molly-moon/data-engineering-case/master/emails.txt) you will find a business-oriented summary of the task, and also a slightly more technical one in a form of an email to stakeholders. 

### Data Extraction
Both datasets present user ratings of movies and series. 
- Netflix Prize data is a dataset publicly [available on Kaggle](https://www.kaggle.com/netflix-inc/netflix-prize-data),
- Amazon Customer Reviews is a dataset publicly [available on Amazon S3](https://s3.amazonaws.com/amazon-reviews-pds/readme.html). Only a small part refering to movies, videos, and series was used in this project.

### Data Transformation and Loading
In order to be able to compare the same movie/series titles described differently by both companies, a set of transformations was made, among other things: 
- removal of text in parenthesis,
- removal of text after a special sign ('-' or ';') if it contains any characteristic words like: season, volume, part, dvd, etc,
- removal of special signs, double/trailing spaces,
- removal of irrelevant columns for this problem.

[Here](https://github.com/molly-moon/data-engineering-case/blob/master/data-transformation.py) you will find the script to perform complete transformation. [The final dataset](https://github.com/molly-moon/data-engineering-case/tree/master/final_data) contains the following columns: Company, Title, Year, Rating. It was saved to a columnar data format Parquet and loaded to Amazon Redshift. The diagram below ilustrates all data operations performed on each particular set of data. 

<p align=center>
  <img src="https://github.com/molly-moon/data-engineering-case/blob/master/logical-diagram.png" height=600/>
  </p>
<p align=center>

### PoC: AWS Step Function 
The state machine consists of 6 steps, each performed by AWS Glue. First 3 steps execute data transformation, subsequent 2 create tables and load data to s3 and the last one executes queries to answer some business related questions. 

- Step 1: Amazon Customer Reviews dataset processing and load to S3,
- Step 2: Netflix Prize dataset processing and load to S3,
- Step 3: Dataset joinm further transformation and load to S3,
- Step 4: Creation of tables schema in Redshift,
- Step 5: Data load from S3 to Redshift (via Redshift Spectrum),
- Step 6: Business queries execution and unload to S3. 

<p align=center>
  <img src="https://github.com/molly-moon/data-engineering-case/blob/master/state-machine.png" height=400/>
  </p>
<p align=center>

### PoC: AWS CloudFormation 
This template defines all necessary infrastructure, permissions and operations to execute the whole data engineering task. It creates the following resources:
- A standard configured VPC (2 private and 2 public subnets, Internet Gateway, NAT Gateway, Route Tables),
- Amazon Lambda function (necessary only for the initial setup),
- an S3 bucket,
- Amazon Redshift cluster,
- a JDBC connection with Redshift for Glue,
- Secret Manager for Redshift secrets (password, username),
- 4 different AWS Glue jobs,
- a state machine with Step Functions,
- permissions: IAM roles, IAM policies, NACLs, security groups.

### PoC: Launch CloudFormation Stack

In order to launch the app you need to create resources with Amazon Web Services. For that you need an AWS account and optionally AWS CLI set up. Learn more [here](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html).

Download CloudFormation template
```
curl -O https://raw.githubusercontent.com/molly-moon/data-engineering-case/master/template.yaml
```

To launch the stack via the console, click on the button below. Upload the template file in the *Specify template* section. 

[<img src='https://github.com/molly-moon/app-object-detection/blob/master/images/cloudformation-launch-stack.png?raw=true'>](https://console.aws.amazon.com/cloudformation/home?region=us-east-1#/stacks/new?stackName=data-engineeing-stack) 

You can also launch the stack using AWS CLI. Note you need to substitute YOUR_PATH with your actual downloads folder path.

```
aws cloudformation create-stack --stack-name data-engineering-case --template-body file://YOUR_PATH/template.yml
```