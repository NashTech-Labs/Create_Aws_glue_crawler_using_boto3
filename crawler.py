import boto3
import json
import pandas
import time 
import csv


client = boto3.client('athena')
response = client.create_data_catalog()
response = client.list_data_catalogs()
print(response)


##############################################################

## LIST GLUE CRAWLERS ##

client = boto3.client('glue', region_name="us-east-1")

response = client.list_crawlers()

print(json.dumps(response, indent=4, sort_keys=True, default=str))

###########################################################################

## CREATE CRAWLER ##
client = boto3.client('glue', region_name="us-east-1")

response = client.create_crawler(
    Name='CrawlerBoto3',
    Role='arn:aws:iam::967091080535:role/service-role/AWSGlueServiceRole-3', #you have to create a proper role(having access to your s3) and then give its arn here
    DatabaseName='Boto3',
    Targets={
        'S3Targets': [
            {
                'Path': 's3://aki-aws-athena-1/data/',
            },
        ]
    },
    SchemaChangePolicy={
        'UpdateBehavior': 'UPDATE_IN_DATABASE',
        'DeleteBehavior': 'DEPRECATE_IN_DATABASE'
    },
    RecrawlPolicy={
        'RecrawlBehavior': 'CRAWL_EVERYTHING'
    },
    LineageConfiguration={
        'CrawlerLineageSettings': 'DISABLE'
    }
)

print(json.dumps(response, indent=4, sort_keys=True, default=str))

##############################################################################

## START CRAWLER ##
client = boto3.client('glue', region_name="us-east-1")
response = client.list_crawlers()

response2 = client.start_crawler(
    Name=response['CrawlerNames'][0]
)

print(json.dumps(response2, indent=4, sort_keys=True, default=str))


#################################################################

## DELETE CRAWLERS ##

client = boto3.client('glue', region_name="us-east-1")
response = client.list_crawlers()

response2 = client.delete_crawler(
   Name=response['CrawlerNames'][0]
)

print(json.dumps(response2, indent=4, sort_keys=True, default=str))
print(response)['CrawlerNames']
print(type(response))

#########################################################################
