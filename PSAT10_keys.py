from aws import aws, table
from boto3.dynamodb.conditions import Key, Attr

def itemcopy(keyList):
	with open('copy item reponses.bat', 'w') as f:
		f.write("set AWS_PROFILE=dmf-prod\n")
		for i in keyList:
			cmd = 'aws s3 cp s3://pine-dmf-itemresponse/{}.xml "C:/Users/ashivaprakash/Desktop/dev/aws/PSAT 10 item responses/{}.xml"\n'.format(i,i)
			f.write(cmd)

x = table('pine_dmfdb_stage_sc30_record_types')
filtering_exp = Attr('assessment').eq(str(3))
items = x.scanTable(filtering_exp)
keys = [str(i['airKey']) for i in items]
itemcopy(keys)