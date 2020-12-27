from redshift_module import pygresql_redshift_common as rs_common
import sys
from awsglue.utils import getResolvedOptions
import boto3

# Get job args
args = getResolvedOptions(sys.argv,['db','db_creds','bucket','file', 'redshift_role'])
db = args['db']
db_creds = args['db_creds']
bucket = args['bucket']
file = args['file']
redshift_role = args['redshift_role']

# Get sql statements
print(f'Getting SQL Statements from {bucket}/{file}...')
s3 = boto3.client('s3') 
sqls = s3.get_object(Bucket=bucket, Key=file)['Body'].read().decode('utf-8')
sqls = sqls.split(';')

# Get database connection
print('Connecting...')
con = rs_common.get_connection(db,db_creds)

# Run each sql statement
print("Connected... Running query...")
print(f"Using the Redshift role {redshift_role}")

results = []
for sql in sqls[:-1]:
    sql = sql + ';'
    if "<SUBST-ROLE-ARN>" in sql:
        sql = sql.replace("<SUBST-ROLE-ARN>", redshift_role)
    result = rs_common.query(con, sql)
    print(result)
    results.append(result)

print(results)