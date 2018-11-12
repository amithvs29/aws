import json
import boto3
from boto3.dynamodb.conditions import Key


class AWS(object):
    def __init__(self, profile_name='default'):
        self.session = boto3.session.Session(profile_name=profile_name)


class DynamoDB(object):
    def __init__(self, profile_name='default'):
        self.profile_name = profile_name
        self.session = boto3.session.Session(profile_name=profile_name)
        self.dynamodb = self.session.resource('dynamodb')

    def __getitem__(self, tableName):
        return Table(tableName, self.profile_name)


class Table(AWS):
    def __init__(self, table_name, profile_name='default'):
        super().__init__(profile_name)
        self.table_name = table_name
        self.dynamodb = self.session.resource('dynamodb')
        self.table = self.dynamodb.Table(table_name)

    def getItem(self, *args, **kwargs):
        return self.table.get_item(*args, **kwargs)['Item']

    def scanTable(self, filtering_exp=None):
        response = self.table.scan(FilterExpression=filtering_exp)

        items = response['Items']
        while True:
            if response.get('LastEvaluatedKey'):
                response = self.table.scan(
                    ExclusiveStartKey=response['LastEvaluatedKey'],
                    FilterExpression=filtering_exp)
                items += response['Items']
            else:
                break
        return items

    def queryTable(self, pkValue):
        response = self.table.query(
            KeyConditionExpression=Key(self.table.key_schema[0]['AttributeName']).eq(str(pkValue)))
        return response['Items']

    def queryTableList(self, pkValueList):
        items = []
        for pkValue in pkValueList:
            items += self.queryTable(pkValue)
        return items

    def putItem(self, itemJson):
        try:
            self.table.put_item(Item=itemJson)
        except:
            print("Unable to insert")
            return False
        return True


class S3(object):
    def __init__(self, profile_name='default'):
        self.profile_name = profile_name
        self.session = boto3.session.Session(profile_name=profile_name)
        self.s3 = self.session.resource('s3')

    def __getitem__(self, bucketName):
        if bucketName in self.getBucketList():
            return Bucket(bucketName, self.profile_name)
        else:
            raise Exception("Bucket does not exist.")

    def getBucketList(self):
        return [bucket.name for bucket in self.s3.buckets.all()]


class Bucket(AWS):
    def __init__(self, bucketName, profile_name='default'):
        super().__init__(profile_name)
        self.bucketName = bucketName
        self.s3 = self.session.resource('s3')
        self.bucket = self.s3.Bucket(bucketName)

    def getItemList(self):
        return [obj.key for obj in self.bucket.objects.all()]

    def downloadAs(self, file, path, newfilename):
        if path[-1] == '/':
            path += path + newfilename
        else:
            path = path + '/' + newfilename
        try:
            self.bucket.download_file(file, path)
        except:
            print("error")

    def download(self, file, path):
        self.downloadAs(file, path, file)

    def uploadAs(self, filepath, newfilename):
        try:
            self.s3.meta.client.upload_file(filepath, self.bucketName, newfilename)
        except e:
            print(e)

    def upload(self, filepath):
        filename = filepath.split('/')[-1]
        self.uploadAs(filepath, filename)


class SNS(object):
    def __init__(self, profile_name='default'):
        self.profile_name = profile_name
        self.session = boto3.session.Session(profile_name=profile_name)
        self.sns = self.session.client('sns')

    def __getitem__(self, snsTopicArn):
        return SNSTopic(snsTopicArn, self.profile_name)


class SNSTopic(SNS):
    def __init__(self, snsTopicArn, profile_name='default'):
        super().__init__(profile_name)
        self.snsTopicArn = snsTopicArn

    def publish(self, message):
        response = self.sns.publish(
            TargetArn=self.snsTopicArn,
            Message=json.dumps({'default': json.dumps(message)}),
            MessageStructure='json'
        )
        return response
