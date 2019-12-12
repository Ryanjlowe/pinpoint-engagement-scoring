import json
import boto3
import logging
import base64
import os
from botocore.exceptions import ClientError

pinpoint = boto3.client('pinpoint')

PINPOINT_PROJECT_ID = os.environ.get('USER_SCORE_TABLE')

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
      payload = record['dynamodb']['NewImage']
      user_id = payload['UserId']['S']
      project_id = payload['PinpointProjectId']['S']
      score = payload['Score']['N']

      endpoints = get_endpoints_for_user(user_id, project_id)
      logging.info('Got Endpoints: %s', endpoints)

      for endpoint in endpoints:
        update_score_for_endpoint(project_id, endpoint, score)

    except Exception as e:
      logging.error('Received Error while processing payload: %s', e)

def get_endpoints_for_user(user_id, project_id):
  try:
    response = pinpoint.get_user_endpoints(
      ApplicationId=project_id,
      UserId=user_id
    )
  except Exception as e:
    logging.error('get_endpoints_for_user error: %s', e)
    raise e
  else:
    return response['EndpointsResponse']['Item']

def update_score_for_endpoint(project_id, endpoint, score):
  try:
    pinpoint.update_endpoint(
      ApplicationId=project_id,
      EndpointId=endpoint['Id'],
      EndpointRequest={
        'User': {
          'UserAttributes': {
            'EngagementScore': [score]
          }
        }
      }
    )
  except Exception as e:
    logging.error('update_score_for_endpoint error: %s', e)
    raise e
