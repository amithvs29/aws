import json
from aws import SNSTopic

numberOfMessages = 100
snsTopicArn = 'arn:aws:sns:us-east-1:441226591726:cb-nonprod-sc32A-essay'

topic = SNSTopic(snsTopicArn, 'techops-dev')
# with open('Test_scenario_2.txt', 'r') as f:
with open('happyscenario.txt', 'r') as f:
    message = json.loads(f.read())


for _ in range(numberOfMessages):
    topic.publish(message)
