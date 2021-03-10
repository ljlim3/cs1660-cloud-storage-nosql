import boto3
import os

s3 = boto3.resource('s3',
    aws_access_key_id = os.environ.get('ACCESS_KEY_ID'),
    aws_secret_access_key = os.environ.get('SECRET_ACCESS_KEY')
)

try:
    s3.create_bucket(Bucket='datacont-lydia', CreateBucketConfiguration={
        'LocationConstraint': 'us-east-2'
    })
except:
    print("this may already exist")

bucket = s3.Bucket("datacont-lydia")

bucket.Acl().put(ACL='public-read')

body = open('/Users/lydia/Documents/UPitt/Spring2021/IntroCloudComputing/NoSQL/exp1.csv', 'rb')

o = s3.Object('datacont-lydia', 'test').put(Body=body)

s3.Object('datacont-lydia', 'test').Acl().put(ACL='public-read')

dyndb = boto3.resource('dynamodb',
    region_name='us-east-2',
    aws_access_key_id = os.getenv('ACCESS_KEY_ID'),
    aws_secret_access_key = os.getenv('SECRET_ACCESS_KEY')
)

try:
    table = dyndb.create_table(
        TableName='DataTable',
        KeySchema=[
            {
                'AttributeName': 'PartitionKey',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'RowKey',
                'KeyType': 'RANGE'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'PartitionKey',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'RowKey',
                'AttributeType': 'S'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )
except:
    # if there is an exception, the table may already exist
    table = dyndb.Table("DataTable")

# wait for the table to be created
table.meta.client.get_waiter('table_exists').wait(TableName='DataTable')

print(table.item_count)

import csv

with open("experiments.csv") as csvfile:
    csvf = csv.reader(csvfile, delimiter=',', quotechar='|')
    next(csvf, None)
    for item in csvf:
        print(item)
        body = open('./'+item[4], 'rb')
        s3.Object('datacont-lydia', item[4]).put(ACL='public-read')
        md = s3.Object('datacont-lydia', item[4]).Acl().put(ACL='public-read')
        url = "https://s3-us-east-2.amazonaws.com/datacont-lydia"+item[4]
        metadata_item = {'PartitionKey': item[0], 'RowKey': item[1],
                    'description': item[4], 'date': item[2], 'url': url}
        try:
            table.put_item(Item=metadata_item)
        except:
            print("item may already be there or another failure")

response = table.get_item(
    Key= {
        'PartitionKey': 'experiment2',
        'RowKey': 'data2'
    }
)
item = response['Item']
print(item)

print(response)