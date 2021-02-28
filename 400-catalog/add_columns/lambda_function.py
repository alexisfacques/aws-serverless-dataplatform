import logging
import os
from typing import Dict, List
import json

# From requirements.txt:
import boto3
from logmatic import JsonFormatter

# From Lambda layers:
import sqs


FUNCTION_NAME = os.getenv('AWS_LAMBDA_FUNCTION_NAME', __name__)
LOG_LEVEL = os.environ.get('LOG_LEVEL', logging.INFO)

handler = logging.StreamHandler()
handler.setFormatter(JsonFormatter())

logger = logging.getLogger(FUNCTION_NAME)
logger.propagate = False
logger.addHandler(handler)
logger.setLevel(LOG_LEVEL)

sqs_batch = sqs.SqsBatchHandler()


@sqs_batch.on_record
def handle_event(event: Dict, *_args, **_kwargs) -> Dict:
    """
    """
    logger.debug('Got event.', extra={'event': event})


def lambda_handler(event: Dict, context):
    """
    Handle lambda event.

    :param event:    the lambda event;
    :param context:  the lambda context;
    """
    sqs_batch(event, context)
