from enum import Enum
import json
import logging
import os
from typing import Any, Callable, Dict, Optional

from . import utils as events

FUNCTION_NAME = os.environ.get('FUNCTION_NAME') or \
    os.environ.get('AWS_LAMBDA_FUNCTION_NAME') or __name__

logger = logging.getLogger(FUNCTION_NAME)


class EventDetailState(Enum):
    """Event states returned by this component."""

    SUCCEEDED = 'SUCCEEDED'
    FAILED = 'FAILED'


class EventRuntimeError(RuntimeError):
    """A RuntimeError to which you can add details upon returning an event."""

    def __init__(self, message: str, detail: Optional[Dict] = None):
        """
        Ctor.

        :param column_name: the failing column name.
        :param column_type: the failing column_type, if any.
        """
        super(EventRuntimeError, self).__init__(message)

        self.detail = detail

    def get_result(self) -> Dict:
        """
        Return a representation of the error result.

        :return: dict.
        """
        return self.detail if self.detail \
            else {'error': type(self).__name__, 'message': str(self)}

    def __repr__(self) -> str:
        """
        Return the object representation.

        :return: str; a json string of all the column info parameters.
        """
        return json.dumps({'message': self.__str__(), **self.get_result()})


def from_result(detail_type: str = events.DEFAULT_DETAIL_TYPE,
                source: str = events.DEFAULT_SOURCE,
                event_bus_name: str = events.DEFAULT_EVENT_BUS,
                ignore_fails: bool = True) -> Any:
    """
    Define an event bus to which send the result of a decorated function.

    Decorator factory.
    :param detail_type:    str; identifies, in combination with the source
                           field, the fields and values that appear in the
                           detail field.
    :param source:         str; identifies the service that sourced the event;
                           must not start by "aws.".
    :param event_bus_name: str; the event bus that will receive the event;
                           only the rules that are associated with this event
                           bus will be able to match the event.
    :param ignore_fails:   bool; whether or not to fail on errors from
                           the eventbridge API.
    :return:               the decorator.
    """
    def decorator(function: Callable) -> Any:
        """
        Send an event with the result of the function.

        Function decorator.
        :param function: Callable; function to get the results of;
                         traditionally a lambda_handler;
        :return:         the function wrapper.
        """
        def wrapper(event, *args, **kwargs) -> Any:
            """
            Call the function and emit an eventbridge event with the result.

            Function wrapper.
            :param event:  the lambda event.
            :param args:   the remaining function positional arguments.
            :param kwargs: the function key-value arguments.
            :return:       the function result.
            :raise:        - any exception from the wrapped function.
                           - RuntimeError on any EventBridge API exception.
            """
            try:
                # Execute lambda handler.

                result = function(event, *args, **kwargs)

                event_detail = {'state': EventDetailState.SUCCEEDED.value,
                                'event': event,
                                'result': result}

            except EventRuntimeError as err:
                event_detail = {'state': EventDetailState.FAILED.value,
                                'event': event,
                                'result': err.get_result()}
                raise

            # pylint: disable=broad-except
            except Exception as err:
                event_detail = {'state': EventDetailState.FAILED.value,
                                'event': event,
                                'result': {'error': type(err).__name__,
                                           'message': str(err)}}
                raise

            else:
                return result

            finally:
                logger.debug('Attempting to emit an event to EventBridge.',
                             extra={'event_bus_name': event_bus_name,
                                    'detail_type': detail_type,
                                    'source': source,
                                    'event_detail': event_detail})

                if not events.put_events(event_detail,
                                         event_bus_name=event_bus_name,
                                         detail_type=detail_type,
                                         source=source) and not ignore_fails:
                    raise RuntimeError('Failed to emit EventBridge event')

        return wrapper

    return decorator
