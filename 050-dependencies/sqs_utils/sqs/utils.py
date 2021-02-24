import logging
import os
from typing import Optional

import boto3
from cachetools import cached


FUNCTION_NAME = os.environ.get('FUNCTION_NAME') or \
    os.environ.get('AWS_LAMBDA_FUNCTION_NAME') or __name__

logger = logging.getLogger(FUNCTION_NAME)


@cached(cache={})
def get_url(queue_arn: str) -> Optional[str]:
    """
    Return the queue URL of an SQS queue given its queue ARN.

    :param queue_arn: str; the queue arn.
    :return:          str; the queue url if any.
    """
    sqs = boto3.client('sqs')

    try:
        # Get queue arn parameters.

        queue_name = str(queue_arn).split(':')[-1]
        account_id = str(queue_arn).split(':')[-2]

    except (AttributeError, IndexError) as err:
        logger.warning('Failed to parse Queue ARN. Ignoring...',
                       extra={'error': err, 'queue_arn': queue_arn})
        return None

    try:
        # Get queue url.

        return sqs.get_queue_url(QueueName=queue_name,
                                 QueueOwnerAWSAccountId=account_id)['QueueUrl']

    except sqs.exceptions.QueueDoesNotExist as err:
        logger.warning('Queue does not exist. Ignoring...',
                       extra={'error': err, 'queue_arn': queue_arn})
        return None


def change_message_visibility(queue_arn: str, receipt_handle: str,
                              timeout: int) -> bool:
    """
    Given the queue arn and receipt handle change the message visibility.

    :param queue_arn:      str; the queue arn.
    :param receipt_handle: str; the message receipt handle.
    :param timeout:        int; the new visibility timeout.
    :return:          bool; whether or not the operation succeeded.
    """
    sqs = boto3.client('sqs')

    if not (queue_url := get_url(queue_arn)):
        return False

    try:
        sqs.change_message_visibility(QueueUrl=queue_url,
                                      ReceiptHandle=receipt_handle,
                                      VisibilityTimeout=int(timeout))

    # pylint: disable=broad-except
    except Exception as err:
        logger.warning('Failed to change message visibility. Ignoring...',
                       extra={'error': type(err).__name__,
                              'error_detail': str(err)})
        return False

    else:
        return True
