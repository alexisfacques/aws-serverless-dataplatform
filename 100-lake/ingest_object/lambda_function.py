import base64
from datetime import datetime
import logging
import os
from typing import Dict, List
import json

# From requirements.txt:
import boto3
from logmatic import JsonFormatter

# From Lambda layers:
import sqs


CLF_FORMAT = '%d/%b/%Y:%H:%M:%S %z'
FUNCTION_NAME = os.getenv('AWS_LAMBDA_FUNCTION_NAME', __name__)
LOG_LEVEL = os.environ.get('LOG_LEVEL', logging.INFO)
JSON_BUCKET_NAME = os.environ['JSON_BUCKET_NAME']

handler = logging.StreamHandler()
handler.setFormatter(JsonFormatter())

logger = logging.getLogger(FUNCTION_NAME)
logger.propagate = False
logger.addHandler(handler)
logger.setLevel(LOG_LEVEL)

s3 = boto3.client('s3')
sqs_batch = sqs.SqsBatchHandler()


@sqs_batch.on_record
def handle_event(event: Dict, *_args, **_kwargs) -> Dict:
    """
    """
    logger.debug('Got event.', extra={'event': event})

    try:
        # Get s3 object.

        s3_object = s3.get_object(Bucket=event['bucketName'],
                                  Key=(key := event['key']))

        logger.debug('Got S3 object.', extra={'s3Object': s3_object})

    except KeyError as err:
        logger.error('Missing event parameter %s. Ignoring...', err,
                     extra={'error': type(err).__name__,
                            'errorDetail': {'key': err.strip('\'')},
                            'event': event})
        return None

    except s3.exceptions.NoSuchKey as err:
        logger.error('S3 object does not exist. Ignoring...', err,
                     extra={'error': type(err).__name__,
                            'errorDetail': str(err),
                            'event': event})
        return None

    except (s3.exceptions.InvalidObjectState, Exception) as err:
        logger.exception('Unhandled exception getting the S3 object.',
                         extra={'error': type(err).__name__,
                                'errorDetail': str(err),
                                'event': event})
        raise RuntimeError('Unhandled exception getting the S3 '
                           'object.') from err

    try:
        # Get S3 object body.

        # HOTFIX AS ALL PutObject of API GATEWAY ARE BASE64 ENCODED.
        s3_body = base64.b64decode(s3_object['Body'].read().decode('utf-8'))
        s3_body = json.loads(s3_body)

        logger.debug('Got S3 object body.', extra={'s3body': s3_body})

    except KeyError as err:
        logger.error('Missing S3 object parameter %s. Ignoring...',
                     extra={'error': type(err).__name__,
                            'errorDetail': str(err),
                            's3Object': s3_object})
        return None

    except ValueError as err:
        logger.error('Invalid JSON format. Ignoring...',
                     extra={'error': type(err).__name__,
                            'errorDetail': str(err),
                            's3Body': s3_body})

        # Tag S3 Object.
        # TODO: Index in DynamoDB

        return None

    except Exception as err:
        logger.exception('Unhandled exception getting the S3 object body.',
                         extra={'error': type(err).__name__,
                                'errorDetail': str(err),
                                'event': event})

        raise RuntimeError('Unhandled exception getting the S3 object '
                           'body.') from err

    try:
        # Get partition keys from S3 object metadata.
        metadata = s3_object.get('Metadata', {})
        request_time = datetime.strptime(metadata['request-time'], CLF_FORMAT)
        table = metadata['table']

        # Repartition file given its metadata
        s3_key = '/'.join([
            'table=%s' % table,
            request_time.strftime('year=%Y/month=%m/day=%d/'
                                  'hour=%H/minute=%M/second=%S'),
            key, key
        ])

    except KeyError as err:
        logger.error('Missing required metadata %s. Ignoring...', err,
                     extra={'error': type(err).__name__,
                            'errorDetail': {'key': err.strip('\'')},
                            'event': event})

        return None

    except Exception as err:
        logger.exception('Unhandled exception getting the S3 object metadata.',
                         extra={'error': type(err).__name__,
                                'errorDetail': str(err),
                                'event': event})

        raise RuntimeError('Unhandled exception getting the S3 object '
                           'metadata.') from err

    try:
        # Put the object to S3.

        put_response = s3.put_object(**{
            'Bucket': JSON_BUCKET_NAME,
            'Key': s3_key.strip('/'),
            'Body': json.dumps(s3_body),
            'Metadata': metadata
        })

        if (code := put_response['ResponseMetadata']['HTTPStatusCode']) == 200:
            logger.debug('Successfully put the file to S3.',
                         extra={'put_response': put_response})
            return True

        logger.error('Failed to save the file to S3: Unexpected response from '
                     'the boto API.',
                     extra={'status_code': code,
                            'response_detail': put_response})
        raise

    # pylint: disable=broad-except
    except Exception as err:
        logger.exception('Unhandled exception putting the object to S3.',
                         extra={'error': type(err).__name__,
                                'errorDetail': str(err)})

        raise RuntimeError('Unexpected response from the boto API') from err

    else:
        logger.debug('Successfully copied S3 object to its new location',
                     extra={'s3Location': 's3://%s/%s' % (JSON_BUCKET_NAME,
                                                          s3_key.strip('/'))})


def lambda_handler(event: Dict, context):
    """
    Handle lambda event.

    :param event:    the lambda event;
    :param context:  the lambda context;
    """
    sqs_batch(event, context)
