import boto3
from boto3.dynamodb.conditions import Key, Attr
from aws import AWS, SNS, DynamoDB, S3, Table
from botocore.client import Config
from pprint import pprint
import datetime
import configparser


def timefilter(obj):
    now = datetime.datetime.now(datetime.timezone.utc)
    now = now.replace(hour=0, minute=0, second=0, microsecond=0)
    one_day_ago = now - datetime.timedelta(hours=24)
    if one_day_ago < obj.last_modified and obj.last_modified < now:
        return True
    return False


def main():
    t = Table('pine_dmfdb_stage_sc30_record_types', 'dmf-prod')
    s = [69919,70050,70263,68770,68711,71545,71671,71627,70335,71484,70783,70604,70192,71388,66742,71631,71549,71515,71704,69817,70013,69099,70848,70966,71889,69555,70071,70441,71674,69996,73453,70215,70833,71041,70094,71503,70872,70404,71539,70132,70291,70011,70514,71045,69329,70795,71112,69282,70890,71264,66705,70827,71629,66604,72174,70714,66750,72125,69943,66617,66664,71410,71370,71169,70829,69771,69953,69962,69780,71257,70659,72183,69134,70034,69990,70801,71654,69639,71882,71701,71712,69961,71422,69816,71458,69797,70649,71618,68848,70774,71525,70948,69096,69243,71492,70484]
    s = [str(i) for i in s]
    fe = Attr('airKey').is_in(s) & Attr('itemResponseMsgId').exists()
    x = t.scanTable(fe)
    print(len(x))
    # t = db['pine_dmfdb_stage_sc30_record_types'].queryTable('airKey', '60985')
    # now = datetime.datetime.now(datetime.timezone.utc)
    # one_day_ago = now - datetime.timedelta(hours=24)
    # now = now.isoformat()
    # one_day_ago = one_day_ago.isoformat()
    # fe = Attr('lastUpdatedTs').between(one_day_ago, now)
    # t = db['pine_dmfdb_stage_sc30_record_types'].scanTable(fe)
    # print(len(t))
    # s3 = S3('dmf-prod')
    # b = s3['pine-dmf-sdqresponse']
    # t = list(filter(timefilter, b.bucket.objects.all()))
    # print(len(t))


def main2():
    a = AWS('techops-newdev')
    s3 = a.session.client('s3')
    x = s3.get_object(Bucket='cb-techops-nonprod-secure-store', Key='dev/apps/cb-techops-nonprod-ert/1.0/api_credentials.ini')
    pprint(x['Body'].read().decode('UTF-8'))

    # s = S3('techops-newdev')['cb-techops-nonprod-secure-store']
    # print(s.getItemList())
    # s.bucket.download_file('dev/apps/cb-techops-nonprod-ert/1.0/secure.properties', 'C:/Users/ashivaprakash/Desktop/dev/aws/secure.properties')

    # k = a.session.client('kms')
    # with open('secure.properties', 'r') as f:
    #     data = f.read()
    #     x = k.decrypt(CiphertextBlob=base64.b64decode(data))
    #     print(x)


if __name__ == "__main__":
    main2()
