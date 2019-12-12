import json
import boto3
import logging
import base64
import os
from botocore.exceptions import ClientError

dynamodb = boto3.resource('dynamodb')
pinpoint = boto3.client('pinpoint')

PINPOINT_PROJECT_ID = os.environ.get('PINPOINT_PROJECT_ID')
scoring_definition_table = dynamodb.Table(os.environ.get('SCORING_DEFINITION_TABLE'))
user_score_table = dynamodb.Table(os.environ.get('USER_SCORE_TABLE'))


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

  for record in event['Records']:
    try:

      payload = json.loads(base64.b64decode(record["kinesis"]["data"]))

      if payload['event_type'] != '_test.event_stream':

        scoreDefn = get_score_definition(payload['event_type'])
        logging.info('Got Score Definition: %s', scoreDefn)

        endpoint = get_endpoint(payload['client']['client_id'])
        logging.info('Got Endpoint: %s', endpoint)

        update_user_score(endpoint['User']['UserId'], scoreDefn['ScoreOffset'])

    except Exception as e:
      logging.error('Received Error while processing payload: %s', e)


def get_score_definition(event_type):
  try:
    response = scoring_definition_table.get_item(
      Key={
        'EventType': event_type,
        'PinpointProjectId': PINPOINT_PROJECT_ID
      }
    )
  except ClientError as e:
    logging.error('get_score_definition error: %s', e)
    raise e
  else:
    item = response['Item']
    return item

def get_endpoint(endpoint_id):
  try:
    response = pinpoint.get_endpoint(
      ApplicationId=PINPOINT_PROJECT_ID,
      EndpointId=endpoint_id
    )
  except Exception as e:
    logging.error('get_endpoint error: %s', e)
    raise e
  else:
    return response['EndpointResponse']

def update_user_score(user_id, score_offset):

  logging.info(score_offset)
  try:
    user_score_table.update_item(
      Key={
        'UserId': user_id,
        'PinpointProjectId': PINPOINT_PROJECT_ID
      },
      UpdateExpression="SET Score = if_not_exists(Score, :start) + :inc",
      ExpressionAttributeValues={
          ':inc': score_offset,
          ':start': 0,
      },
      ReturnValues = 'UPDATED_NEW'
    )
  except ClientError as e:
    logging.error('update_user_score error: %s', e)
    raise e
