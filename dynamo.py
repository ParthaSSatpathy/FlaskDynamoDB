import boto3
from datetime import datetime, timedelta

class Dynamo():
    def __init__(self, region_name = 'us-east-1'):
        self.dynamodb = boto3.resource('dynamodb', region_name=region_name)#endpoint_url = 'http://localhost:8000')

    def create_tables(self):
        message = "Creating Table ProductCatalog"
        #Create Table
        table = self.dynamodb.create_table(
            TableName = 'ProductCatalog',
            KeySchema = [
                {
                    'AttributeName' : 'Id',
                    'KeyType': 'HASH'
                }
            ],
            AttributeDefinitions = [
                {
                    'AttributeName' : 'Id',
                    'AttributeType': 'N'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits' : 6,
                'WriteCapacityUnits' : 5
            }
        )

        # Wait untilthe table exists
        table.meta.client.get_waiter('table_exists').wait(TableName='ProductCatalog')

        message += "...Success\nCreating Table Forum"
        table = self.dynamodb.create_table(
            TableName = 'Forum',
            KeySchema = [
                {
                    'AttributeName' : 'Name',
                    'KeyType': 'HASH'
                }
            ],
            AttributeDefinitions = [
                {
                    'AttributeName' : 'Name',
                    'AttributeType': 'S'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits' : 6,
                'WriteCapacityUnits' : 5
            }
        )

        # Wait untilthe table exists
        table.meta.client.get_waiter('table_exists').wait(TableName='Forum')

        message += "...Success\nCreating Table Thread"
        table = self.dynamodb.create_table(
            TableName = 'Thread',
            KeySchema = [
                {
                    'AttributeName' : 'ForumName',
                    'KeyType': 'HASH'
                },
                {
                    'AttributeName' : 'Subject',
                    'KeyType': 'RANGE'
                }
            ],
            AttributeDefinitions = [
                {
                    'AttributeName' : 'ForumName',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName' : 'Subject',
                    'AttributeType': 'S'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits' : 6,
                'WriteCapacityUnits' : 5
            }
        )

        # Wait untilthe table exists
        table.meta.client.get_waiter('table_exists').wait(TableName='Thread')

        message += "...Success\nCreating Table Reply"
        table = self.dynamodb.create_table(
            TableName = 'Reply',
            KeySchema = [
                {
                    'AttributeName' : 'Id',
                    'KeyType': 'HASH'
                },
                {
                    'AttributeName' : 'ReplyDateTime',
                    'KeyType': 'RANGE'
                }
            ],
            AttributeDefinitions = [
                {
                    'AttributeName' : 'Id',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName' : 'ReplyDateTime',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName' : 'PostedBy',
                    'AttributeType': 'S'
                }
            ],
            LocalSecondaryIndexes=[
                {
                    'IndexName': 'PostedBy-index',
                    'KeySchema': [
                        {
                            'AttributeName': 'Id',
                            'KeyType': 'HASH'
                        },
                        {
                            'AttributeName': 'PostedBy',
                            'KeyType': 'RANGE'
                        }
                    ],
                    'Projection': {
                        'ProjectionType': 'KEYS_ONLY'
                    }
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits' : 6,
                'WriteCapacityUnits' : 5
            }
        )

        # Wait untilthe table exists
        table.meta.client.get_waiter('table_exists').wait(TableName='Reply')
        message += "...Success\nAll tables created successfully!"
    
        return message

    def upload_data(self):
        today = datetime.utcnow()#.strftime("%Y-%m-%d %H:%M:%S")
        one_day_ago = (today - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
        seven_days_ago = (today - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
        fourteen_days_ago = (today - timedelta(days=14)).strftime("%Y-%m-%d %H:%M:%S")
        twenty_days_ago = (today - timedelta(days=20)).strftime("%Y-%m-%d %H:%M:%S")

        message = "Adding data to table ProductCatalog..."

        table = self.dynamodb.Table('ProductCatalog')
        with table.batch_writer() as batch:
            batch.put_item(
                Item={
                    "Id" : 1101,
                    "Title" : "Book 101 Title",
                    "ISBN" : "111-1111111111",
                    "Authors" : set(["Author1"]),
                    "Price" : 2,
                    "Dimensions" : '8.5 x 11.0 x 0.5',
                    "PageCount" : 500,
                    "InPublication" : 1,
                    "ProductCategory" : "Book"
                }
            )
            batch.put_item(
                Item={
                    "Id" : 102,
                    "Title" : "Book 102 Title",
                    "ISBN" : "222-2222222222",
                    "Authors" : set(["Author1", 'Author2']),
                    "Price" : 20,
                    "Dimensions" : '8.5 x 11.0 x 0.8',
                    "PageCount" : 600,
                    "InPublication" : 1,
                    "ProductCategory" : "Book"
                }
            )
            batch.put_item(
                Item={
                    "Id" : 103,
                    "Title" : "Book 103 Title",
                    "ISBN" : "333-3333333333",
                    "Authors" : set(["Author1", 'Author2']),
                    "Price" : 2000,
                    "Dimensions" : '8.5 x 11.0 x 1.5',
                    "PageCount" : 600,
                    "InPublication" : 0,
                    "ProductCategory" : "Book"
                }
            )
            batch.put_item(
                Item={
                    "Id" : 201,
                    "Title" : "18-Bike-201",
                    "Description" : "201 Description",
                    "BicycleType" : "Road",
                    "Brand" : "Mountain A",
                    "Price" : 100,
                    "Gender" : "M",
                    "Color" : set(["Red", "Black"]),
                    "ProductCategory" : "Bicycle"
                }
            )
            batch.put_item(
                Item={
                    "Id" : 202,
                    "Title" : "21-Bike-202",
                    "Description" : "202 Description",
                    "BicycleType" : "Road",
                    "Brand" : "Brand-Company A",
                    "Price" : 200,
                    "Gender" : "M",
                    "Color" : set(["Green", "Black"]),
                    "ProductCategory" : "Bicycle"
                }
            )
            batch.put_item(
                Item={
                    "Id" : 203,
                    "Title" : "19-Bike-203",
                    "Description" : "203 Description",
                    "BicycleType" : "Road",
                    "Brand" : "Brand-Company B",
                    "Price" : 300,
                    "Gender" : "W",
                    "Color" : set(["Red","Green", "Black"]),
                    "ProductCategory" : "Bicycle"
                }
            )
            batch.put_item(
                Item={
                    "Id" : 204,
                    "Title" : "18-Bike-204",
                    "Description" : "204 Description",
                    "BicycleType" : "Mountain",
                    "Brand" : "Brand-Company B",
                    "Price" : 400,
                    "Gender" : "W",
                    "Color" : set(["Red"]),
                    "ProductCategory" : "Bicycle"
                }
            )
            batch.put_item(
                Item={
                    "Id" : 205,
                    "Title" : "18-Bike-205",
                    "Description" : "205 Description",
                    "BicycleType" : "Hybrid",
                    "Brand" : "Brand-Company C",
                    "Price" : 500,
                    "Gender" : "B",
                    "Color" : set(["Red","Black"]),
                    "ProductCategory" : "Bicycle"
                }
            )
        message += "...Data Added to ProductCatalog"

        message += "\n Adding Data to Table Forum..."
        table = self.dynamodb.Table('Forum')
        with table.batch_writer() as batch:
            batch.put_item(
                Item={
                    "Name" : 'Amazon DynamoDB',
                    "Category" : 'Amazon Web Services',
                    "Threads" : 0,
                    "Messages" : 0,
                    "Views" : 1000
                }
            )
            batch.put_item(
                Item={
                    "Name" : 'Amazon S3',
                    "Category" : 'Amazon Web Services',
                    "Threads" : 0
                }
            )
        message += "...Data Added to Forum"

        message += "\n Adding Data to Table Reply..."
        table = self.dynamodb.Table('Reply')
        with table.batch_writer() as batch:
            batch.put_item(
                Item={
                    "Id" : 'Amazon DynamoDB#DynamoDB Thread 1',
                    "ReplyDateTime" : fourteen_days_ago,
                    "Message" : 'DynamoDB Thread 1 Reply 2 text',
                    "PostedBy" : 'User B'
                }
            )
            batch.put_item(
                Item={
                    "Id" : 'Amazon DynamoDB#DynamoDB Thread 2',
                    "ReplyDateTime" : twenty_days_ago,
                    "Message" : 'DynamoDB Thread 2 Reply 3 text',
                    "PostedBy" : 'User B'
                }
            )
            batch.put_item(
                Item={
                    "Id" : 'Amazon DynamoDB#DynamoDB Thread 2',
                    "ReplyDateTime" : seven_days_ago,
                    "Message" : 'DynamoDB Thread 2 Reply 2 text',
                    "PostedBy" : 'User A'
                }
            )
            batch.put_item(
                Item={
                    "Id" : 'Amazon DynamoDB#DynamoDB Thread 2',
                    "ReplyDateTime" : one_day_ago,
                    "Message" : 'DynamoDB Thread 2 Reply 1 text',
                    "PostedBy" : 'User A'
                }
            )
        message += "...Data Added to Reply"

        return message
