import json
import logging
import os
from typing import Callable, List, Dict

import boto3

from . import utils


FUNCTION_NAME = os.environ.get('FUNCTION_NAME') or \
    os.environ.get('AWS_LAMBDA_FUNCTION_NAME') or __name__

logger = logging.getLogger(FUNCTION_NAME)


class SqsBatchHandler:
    """Execute Lambda SQS event, handle partial batch success."""

    def __init__(self):
        """Ctor."""
        self.__on_failed_record = None
        self.__on_record = None
        self.__sqs = boto3.client('sqs')

    def on_failed_record(self, function: Callable):
        """
        Register a function to execute for all failed SQS records.

        Decorator.
        :param function: Callable; function to get the results of;
                         traditionally a lambda_handler;
        """
        self.__on_failed_record = function

    def on_record(self, function: Callable):
        """
        Register a function to handle an SQS record.

        Decorator.
        :param function: Callable; function to get the results of;
                         traditionally a lambda_handler;
        """
        self.__on_record = function

    def resolve_records(self, records: List[Dict]):
        """
        Delete records from their original queue, then raise an exception.

        :param records: list; the lambda event records.
        :raise:         a expected exception to handle partial batch failure.
        """
        for rec in records:
            if rec.get('eventSource') != 'aws:sqs':
                continue

            try:
                # Delete record from the queue.

                self.__sqs.delete_message(
                    QueueUrl=utils.get_url(rec['eventSourceArn']),
                    ReceiptHandle=rec['receiptHandle']
                )

                logger.debug('Deleted record.',
                             extra={'record': rec})

            except (self.__sqs.exceptions.ReceiptHandleIsInvalid,
                    KeyError) as err:
                logger.warning('Malformed record payload. Ignoring...',
                               extra={'error': str(err), 'record': rec})
                continue

            # pylint: disable=broad-except
            except Exception as err:
                logger.error('Failed to delete message. Ignoring...',
                             extra={'error': type(err).__name__,
                                    'error_detail': str(err), 'record': rec})
                continue

        logger.info('Encountered partial batch failure. '
                    'Purposely exiting non-zero...')

        raise Exception('Partial batch failure')

    def __call__(self, event, *args, **kwargs):
        """
        Handle a SQS event having a list of Records.

        If event is not from originating from SQS, execute the handler with the
        raw event.
        :param event:  the lambda event.
        :param args:   the remaining function positional arguments.
        :param kwargs: the function key-value arguments.
        :return:        0 if all messages are successful.
        :raise:         an Exception on partial batch failure.
        """
        if not callable(self.__on_record):
            raise RuntimeError('Missing record handling configuration '
                               '(on_record method).')

        # Not an SQS event...
        if 'Records' not in event or not isinstance(event['Records'], list):
            return self.__on_record(event, *args, event, [], **kwargs)

        previous_results = ()
        error_count = 0
        for record in event['Records']:
            try:
                logger.debug('Processing record.',
                             extra={'record': record})

                decoded_event = json.loads(record['body']) \
                    if record.get('eventSource') == 'aws:sqs' else record

                record_result = self.__on_record(decoded_event, *args,
                                                 record, previous_results,
                                                 **kwargs)

            # pylint: disable=broad-except
            except Exception as err:
                logger.error('Failed to process record... Ignoring...',
                             extra={'error': str(err)})

                if callable(self.__on_failed_record):
                    try:
                        res = self.__on_failed_record(decoded_event, *args,
                                                      record, previous_results,
                                                      **kwargs)
                        logger.debug('Called \'on_failed_record\' hook.',
                                     extra={'on_failed_record': res})

                    except Exception as err:
                        logger.error('Unhandled \'on_failed_record\' '
                                     'exception. Ignoring...',
                                     extra={'error': type(err).__name__,
                                            'error_detail': str(err)})

                record['__failed'] = True
                error_count += 1
                continue

            else:
                previous_results += (record_result,) \
                    if record_result is not None else ()

        successful_records = [r for r in event['Records']
                              if not r.get('__failed', False)]

        logger.info('Received %d record(s). %d successfully processed. '
                    'Encountered %d error(s).', len(event['Records']),
                    len(successful_records), error_count)

        if len(event['Records']) == len(successful_records):
            logger.info('Successfully processed all messages from the batch. '
                        'Exiting...')
            return 0

        return self.resolve_records(successful_records)
