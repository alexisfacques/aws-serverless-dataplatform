import logging
import os
from typing import Dict, List
import json

# From requirements.txt:
import boto3
from jinja2 import Template
from logmatic import JsonFormatter

# From Lambda layers:
import sqs
import events
import utils.athena as athena


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
@events.from_result()
def handle_event(event: Dict, *_args, **_kwargs) -> Dict:
    """
    """
    logger.debug('Got event.', extra={'event': event})

    try:
        # Templatize query.
        query_string: str = Template(event['queryTemplate']) \
            .render(event.get('templateValues', {}))

    except Exception as err:
        pass

    result_set: Dict = athena.execute(query_string) \
        .wait_for_result().get('ResultSet',{})

    # 2D array.
    # First row is a list of the column headers.
    # All the following rows are actual data rows.
    rows: List = result_set.get('Rows', [])

    logger.debug('Successfully executed query.',
                 extra={'result_set': result_set})

    return {
        'query': query_string,
        'rowsCount': max(len(rows) - 1, 0),
        'rows': [{rows[0]['Data'][index]['VarCharValue']: cell['VarCharValue']
                  for index, cell in enumerate(row['Data'])}
                 for row in rows[1:]]
    }


    result


def lambda_handler(event: Dict, context):
    """
    Handle lambda event.

    :param event:    the lambda event;
    :param context:  the lambda context;
    """
    sqs_batch(event, context)
