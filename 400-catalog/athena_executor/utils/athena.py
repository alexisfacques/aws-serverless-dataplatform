import os
import time
import logging

import boto3


ATHENA_WORKGROUP = os.environ['ATHENA_WORKGROUP']
ATHENA_QUERY_TIMEOUT_SECONDS = \
    int(os.environ.get('ATHENA_QUERY_TIMEOUT_SECONDS', 60))

LOGGER_NAME = os.getenv('AWS_LAMBDA_FUNCTION_NAME', __name__)
LOGGER = logging.getLogger(LOGGER_NAME)


athena = boto3.client('athena')


def execute(query):
    """
    """
    try:
        # Start query execution

        query_id = athena.start_query_execution(
            QueryString=query,
            WorkGroup=ATHENA_WORKGROUP
        )['QueryExecutionId']

        LOGGER.debug('Successfully started Athena query',
                     extra={'query_id': query_id,
                            'query': query})

        return AthenaQuery(query_id, ATHENA_QUERY_TIMEOUT_SECONDS)

    except Exception as err:
        LOGGER.exception('Failed to start query execution.',
                         extra={'query': query})
        raise


class AthenaQuery():
    """Wrapper for tracking Athena query status."""

    def __init__(self, query_id, query_timeout=60):
        """
        Ctor.

        :param query_id:      athena query id.
        :param query_timeout: athena query timeout.
        """

        self.query_id = query_id
        self.__query_timeout = query_timeout

    def get_result(self):
        """
        Return the result of the Athena query, if any.

        :return: the result object of the query.
        """
        try:
            return athena.get_query_results(QueryExecutionId=self.query_id)

        except Exception as err:
            LOGGER.exception('Failed to get Athena query result.',
                             extra={'error': err, 'query_id': self.query_id})
            raise

    def get_status(self):
        """
        Return status of the query.

        :return: a tuple including, in order:
                   - the state of the query;
                   - the last reason of the state change, if any;
                   - the data scanned in bytes;
                   - the current execution time in milliseconds.
        :raise:  some exceptions.
        """
        try:
            # Get query execution info.

            __execution = athena.get_query_execution(
                QueryExecutionId=self.query_id
            )['QueryExecution']

            query_status = (
                __execution['Status']['State'],
                __execution['Status'].get('StateChangeReason', None),
                __execution['Statistics'].get('DataScannedInBytes', 0),
                __execution['Statistics'].get('EngineExecutionTimeInMillis', 0)
            )

            LOGGER.debug('Got query execution info.',
                         extra={'status': query_status[0]})

        except Exception as err:
            LOGGER.exception('Failed to get Athena query status.',
                             extra={'error': err, 'query_id': self.query_id})
            raise

        else:
            return query_status

    def wait_for_result(self, query_timeout_in_seconds=None):
        """
        Wait and return results of the query.

        :param query_timeout_in_seconds: timeout before the query fails, in
                                         seconds. NOTE: This does not cancels
                                         the query. Query might still be
                                         running.
        :return:                         the query results.
        :raise:                          - RuntimeError when query status is
                                           failed;
                                         - TimeoutError if waiting time
                                           exceeds timeout.
        """
        query_timeout_in_seconds = self.__query_timeout \
            if query_timeout_in_seconds is None \
            else int(query_timeout_in_seconds)

        __sleep = 1
        current_time = 0
        while current_time <= query_timeout_in_seconds:
            (status, reason, bytes_scanned, execution_ms) = self.get_status()

            if status in ('SUCCEEDED',):
                LOGGER.info('Athena query succeeded.',
                            extra={'query_id': self.query_id,
                                   'bytes_scanned': bytes_scanned,
                                   'execution_time_in_ms': execution_ms})

                return self.get_result()

            if status in ('CANCELLED', 'FAILED'):
                LOGGER.info('Athena query failed.',
                            extra={'query_id': self.query_id,
                                   'reason': reason,
                                   'bytes_scanned': bytes_scanned,
                                   'execution_time_in_ms': execution_ms})
                raise RuntimeError(reason)

            current_time += __sleep
            time.sleep(__sleep)

        if not self.cancel():
            raise RuntimeError('Caught timeout but failed to stop query.')

        raise TimeoutError()

    def cancel(self):
        """
        Cancel the query if it is running.

        :return: whether or not is succeeded.
        :raise:  some exceptions. NOTE: Query might still be running!
        """
        try:
            if (status := self.get_status()[0]) not in ('CANCELLED', 'FAILED',
                                                        'SUCCEEDED'):
                LOGGER.debug('Athena query is running. Attempting to cancel.',
                             extra={'status': status})

                athena.stop_query_execution(QueryExecutionId=self.query_id)

        except Exception as err:
            LOGGER.exception('Failed to cancel Athena query.',
                             extra={'error': err, 'query_id': self.query_id})
            return False

        else:
            LOGGER.debug('Successfully cancelled Athena query.')
            return True
