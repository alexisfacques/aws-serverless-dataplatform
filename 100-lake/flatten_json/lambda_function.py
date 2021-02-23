from logging import StreamHandler, getLogger
import os
from typing import Dict, List


from flatten_json import flatten
from logmatic import JsonFormatter


DATAFRAME_COLUMN_SEPARATOR = os.getenv('DATAFRAME_COLUMN_SEPARATOR', '__')
FUNCTION_NAME = os.getenv('AWS_LAMBDA_FUNCTION_NAME', __name__)
LOG_LEVEL = os.environ.get('LOG_LEVEL', logging.INFO)


handler = StreamHandler()
handler.setFormatter(JsonFormatter())

logger = getLogger(FUNCTION_NAME)
logger.propagate = False
logger.addHandler(handler)
logger.setLevel(LOG_LEVEL)


def lambda_handler(event: Dict, *_args, **_kwargs) -> Dict:
    """
    """
    logger.debug('Got event.', extra={'event': event})

    # READ DOCUMENT

    flatten(dic, DATAFRAME_COLUMN_SEPARATOR)

    # SAVE DOCUMENT
