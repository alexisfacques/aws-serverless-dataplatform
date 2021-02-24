import json
import logging
import os
import re

import boto3


FUNCTION_NAME = os.environ.get('FUNCTION_NAME') or \
    os.environ.get('AWS_LAMBDA_FUNCTION_NAME') or __name__

DEFAULT_EVENT_BUS = os.environ.get('EVENTBRIDGE_EVENT_BUS') or 'default'
DEFAULT_SOURCE_PLACEHOLDER = 'application'
DEFAULT_SOURCE = os.environ.get('EVENTBRIDGE_SOURCE') or FUNCTION_NAME
DEFAULT_DETAIL_TYPE = os.environ.get('EVENTBRIDGE_DETAIL_TYPE') or 'event'

DEFAULT_METRICS_NAMESPACE_PLACEHOLDER = 'Application'
DEFAULT_METRICS_NAMESPACE = os.environ.get('CLOUDWATCH_METRICS_NAMESPACE') \
    or DEFAULT_METRICS_NAMESPACE_PLACEHOLDER


logger = logging.getLogger(FUNCTION_NAME)


def put_events(*details,
               detail_type: str = DEFAULT_DETAIL_TYPE,
               source: str = DEFAULT_SOURCE,
               event_bus_name: str = DEFAULT_EVENT_BUS) -> bool:
    """
    Safely put events to an EventBridge event bus.

    :param details:        a list of JSON serializable objects; function will
                           ignore any non serializable objects.
    :param detail_type:    str; free-form string used to decide what fields to
                           expect in the event detail.
    :param source:         str; identifies the service that sourced the event;
                           must not start by "aws.", otherwise this will be
                           defaulted to 'application'.
    :param event_bus_name: str; the event bus that will receive the event;
                           only the rules that are associated with this event
                           bus will be able to match the event.
    :return:               bool; whether or not the events have been
                           successfully put to the event bus.
    """
    if re.match(r'^aws.', str(source)):
        source = DEFAULT_SOURCE_PLACEHOLDER

    try:
        entries = [{'Source': str(source),
                    'DetailType': str(detail_type),
                    'Detail': detail_str,
                    'EventBusName': str(event_bus_name)}
                   for detail in details
                   if (detail_str := __to_json(detail)) is not None]

        if entries:
            boto3.client('events').put_events(Entries=entries)

        return True

    # pylint: disable=broad-except
    except Exception as err:
        logger.error('Failed to put events to EventBridge.',
                     extra={'error': err, 'detail_type': detail_type,
                            'event_bus_name': event_bus_name,
                            'details': details})
        return False


def put_metric(metric_name: str, metric_value: int = 1,
               metric_space: str = DEFAULT_METRICS_NAMESPACE,
               **metric_kvs) -> bool:
    """
    Safely put a metric to CloudWatch.

    :param metric_name:  str; the name of the metric.
    :param metric_value: int; the value for the metric.
    :param metric_space: str; the namespace for the metric data; to avoid
                         conflicts with AWS service namespaces, you should not
                         specify a namespace that begins with 'AWS/', otherwise
                         this will be defaulted to 'Application'
    :param metric_kvs:   key-values arguments that will be mapped to dimensions
                         associated with the metric.
    :return:             bool; whether or not the metric has been successfully
                         put to CloudWatch.
    """
    if re.match(r'^AWS/', str(metric_space)):
        metric_space = DEFAULT_METRICS_NAMESPACE_PLACEHOLDER

    try:
        boto3.client('cloudwatch').put_metric_data(
            Namespace=metric_space,
            MetricData=[{'MetricName': str(metric_name),
                         'Dimensions': [{'Name': 'Name',
                                         'Value': FUNCTION_NAME},
                                        *[{'Name': k, 'Value': val}
                                          for k, val in metric_kvs.items()]],
                         'Value': int(metric_value),
                         'Unit': 'Count'}]
        )

        return True

    # pylint: disable=broad-except
    except Exception as err:
        logger.error('Failed to put metrics to CloudWatch.',
                     extra={'error': err, 'metric_name': metric_name,
                            'metric_space': metric_space,
                            'metric_kvs': metric_kvs})
        return False


def __to_json(obj):
    """
    Safely return the serialized obj to a JSON formatted string.

    :param obj:  a JSON serializable object.
    :return:     a JSON string if any.
    """
    try:
        return json.dumps(obj)

    # pylint: disable=broad-except
    except Exception:
        logger.error('Object is not JSON serializable. Ignoring...',
                     extra={'obj': obj})
        return None
