import logging
import os
from typing import Dict, List
import json

# From requirements.txt:
import boto3
from flatten_json import flatten
from logmatic import JsonFormatter

# From Lambda layers:
import sqs
import events


DATAFRAME_COLUMN_SEPARATOR = os.getenv('DATAFRAME_COLUMN_SEPARATOR', '__')

# By default, we will only partition data up to the request time minute. This
# list of partition keys is content-based deduped in a FIFO queue; thus if
# multiple files of a same partition are received, the message will only be
# processed once ==> this reduces the volume of partitions to create.
#
# Choosing your partition scheme in Athena is a trade-off between:
# - Average data volume per table (more partitions = SQS publishes and Lambda
#   invocations)
# - Your rows / object average size (Athena will scan an entire partition,
#   so the more files you have in a partition, to more bytes Athena will hav
#   to scan)
# As a reminder, valid partitions keys, in order, are as follows:
# 'year', 'month', 'day', 'hour', 'minute', 'second' and 'id'
PARTITION_KEYS = [partition.strip(' ') for partition
                  in str(os.environ.get('PARTITION_KEYS', '')).split(',')
                  if partition] or ['year', 'month', 'day', 'hour', 'minute']

PARTITION_RE = r''.join(('^table=([\w-]*)\/year=(\d{4})\/month=(\d{2})'
                         '\/day=(\d{2})\/hour=(\d{2})\/minute=(\d{2})'
                         '\/second=(\d{2})\/id=([\w-]*)\/'))

FUNCTION_NAME = os.getenv('AWS_LAMBDA_FUNCTION_NAME', __name__)
LOG_LEVEL = os.environ.get('LOG_LEVEL', logging.INFO)

handler = logging.StreamHandler()
handler.setFormatter(JsonFormatter())

logger = logging.getLogger(FUNCTION_NAME)
logger.propagate = False
logger.addHandler(handler)
logger.setLevel(LOG_LEVEL)

s3 = boto3.client('s3')
sqs_batch = sqs.SqsBatchHandler()


@sqs_batch.on_record
@events.from_result()
def handle_event(event: Dict, *_args, **_kwargs) -> Dict:
    """
    """
    logger.debug('Got event.', extra={'event': event})

    try:
        # Get s3 object.
        target_bucket = event['targetBucket']
        s3_object = s3.get_object(Bucket=event['bucketName'],
                                  Key=(key := event['key']))

        logger.debug('Got S3 object.', extra={'s3Object': s3_object})

    except KeyError as err:
        logger.error('Missing event parameter %s. Ignoring...', err,
                     extra={'error': type(err).__name__,
                            'errorDetail': {'key': str(err).strip('\'')},
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

        s3_body = s3_object['Body'].read().decode('utf-8')
        s3_body = json.loads(s3_body)

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
        return None

    except Exception as err:
        logger.exception('Unhandled exception getting the S3 object body.',
                         extra={'error': type(err).__name__,
                                'errorDetail': str(err),
                                'event': event})

        raise RuntimeError('Unhandled exception getting the S3 object '
                           'body.') from err

    try:
        # Flatten the S3 document.

        s3_body = flatten(s3_body, DATAFRAME_COLUMN_SEPARATOR)

        logger.debug('Transformed S3 object.', extra={'s3body': s3_body})

    except Exception as err:
        logger.exception('Unhandled exception transforming S3 object.',
                         extra={'error': type(err).__name__,
                                'errorDetail': str(err),
                                'event': event})

        raise RuntimeError('Unhandled exception transforming the S3 '
                           'object.') from err

    try:
        # Get table partition metadata from the S3 object.  This will be used
        # to update the table with new columns and partitions in the catalog.

        key_partitions: Dict = {key: value
                                for key, value in (partition.split('=')
                                for partition in key.split('/'))}

        ret: Dict = {
            'table': {
                'name': (table_name := key_partitions['table']),
                'location': 's3://%s/table=%s' % (target_bucket, table_name)
            },
            'partition': {
                'keys': PARTITION_KEYS,
                'values': [key_partitions[k] for k in PARTITION_KEYS]
            },
            'columns': sorted(s3_body.keys())
        }

    except Exception as err:
        logger.exception('Unhandled exception getting the table partition '
                         'metadata.',
                         extra={'error': type(err).__name__,
                                'errorDetail': str(err),
                                'event': event})

        raise RuntimeError('Unhandled exception getting the table partition '
                           'metadata.') from err

    try:
        # Put the object to S3.

        put_response = s3.put_object(
            Bucket=target_bucket,
            Key=key,
            Body=json.dumps(s3_body),
            Metadata=s3_object.get('Metadata', {}),
            ContentType='application/json'
        )

        if (code := put_response['ResponseMetadata']['HTTPStatusCode']) == 200:
            logger.debug('Successfully put the file to S3.',
                         extra={'put_response': put_response})
            return ret


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
