import json
import boto3
import logging
import os
from botocore.vendored import requests

dynamodb = boto3.resource('dynamodb')

PINPOINT_PROJECT_ID = os.environ.get('PINPOINT_PROJECT_ID')
scoring_definition_table = dynamodb.Table(os.environ.get('SCORING_DEFINITION_TABLE'))

def lambda_handler(event, context):

  global log_level
  log_level = str(os.environ.get('LOG_LEVEL')).upper()
  if log_level not in [
                          'DEBUG', 'INFO',
                          'WARNING', 'ERROR',
                          'CRITICAL'
                      ]:
    log_level = 'ERROR'
  logging.getLogger().setLevel(log_level)

  logging.info(event)

  try:
    populate_score_definition('_email.click', 50)
    populate_score_definition('_email.open', 10)
    populate_score_definition('_email.delivered', 2)
    populate_score_definition('_email.hardbounce', -1000)
    populate_score_definition('_email.complaint', -1000)
    populate_score_definition('_email.unsubscribe', -500)
    populate_score_definition('_SMS.SUCCESS', 2)
    populate_score_definition('_SMS.OPTOUT', -500)
    populate_score_definition('_campaign.send', 2)
    populate_score_definition('_campaign.opened_notification', 50)
    populate_score_definition('_campaign.received_foreground', 2)
    populate_score_definition('_campaign.received_background', 2)
    populate_score_definition('_session.start', 2)
    populate_score_definition('_userauth.sign_up', 50)
    populate_score_definition('_monetization.purchase', 100)

  except Exception as e:
    logging.error('Received Error while populating default values: %s', e)
    send(event, context, 'FAILED', {})

  else:
    send(event, context, 'SUCCESS', {})


def populate_score_definition(event_type, score_offset):
  try:
    scoring_definition_table.put_item(
      Item={
        'EventType': event_type,
        'PinpointProjectId': PINPOINT_PROJECT_ID,
        'ScoreOffset':score_offset
      }
    )
  except Exception as e:
    logging.error('Received Error while populate_score_definition: %s', e)
    raise e

######
# Following taken from: https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/cfn-lambda-function-code-cfnresponsemodule.html#w2ab1c20c25c14b9c15
######
#  Copyright 2016 Amazon Web Services, Inc. or its affiliates. All Rights Reserved.
#  This file is licensed to you under the AWS Customer Agreement (the "License").
#  You may not use this file except in compliance with the License.
#  A copy of the License is located at http://aws.amazon.com/agreement/ .
#  This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied.
#  See the License for the specific language governing permissions and limitations under the License.

def send(event, context, responseStatus, responseData, physicalResourceId=None, noEcho=False):
  responseUrl = event['ResponseURL']

  print(responseUrl)

  responseBody = {}
  responseBody['Status'] = responseStatus
  responseBody['Reason'] = 'See the details in CloudWatch Log Stream: ' + context.log_stream_name
  responseBody['PhysicalResourceId'] = physicalResourceId or context.log_stream_name
  responseBody['StackId'] = event['StackId']
  responseBody['RequestId'] = event['RequestId']
  responseBody['LogicalResourceId'] = event['LogicalResourceId']
  responseBody['NoEcho'] = noEcho
  responseBody['Data'] = responseData

  json_responseBody = json.dumps(responseBody)

  print("Response body:\n" + json_responseBody)

  headers = {
      'content-type' : '',
      'content-length' : str(len(json_responseBody))
  }

  try:
      response = requests.put(responseUrl,
                              data=json_responseBody,
                              headers=headers)
      print("Status code: " + response.reason)
  except Exception as e:
      print("send(..) failed executing requests.put(..): " + str(e))
