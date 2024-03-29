# Copyright 2016 Intel Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ------------------------------------------------------------------------------

from threading import Condition
from threading import RLock

from sawtooth_sdk.messaging.exceptions import ValidatorConnectionError
from sawtooth_sdk.protobuf import validator_pb2


class FutureResult:
    def __init__(self, message_type, content):
        self.message_type = message_type
        self.content = content


class FutureError:
    """Used when resolving a future, to
    set the FutureResult to an error result.
    Specifically this raises ValidatorConnectionError
    when accessing attributes, but can be made more general.
    """

    @property
    def content(self):
        raise ValidatorConnectionError()

    @property
    def message_type(self):
        raise ValidatorConnectionError()


class Future:
    def __init__(self, correlation_id, request_type=None):
        self.correlation_id = correlation_id
        self._result = None
        self._condition = Condition()
        self._request_type = request_type

    def done(self):
        return self._result is not None

    def result(self, timeout=None):
        with self._condition:
            if self._result is None:
                if not self._condition.wait(timeout):
                    message_type = validator_pb2.Message.MessageType.Name(
                        self._request_type) if self._request_type else None
                    raise FutureTimeoutError(
                        'Future timed out waiting for response to {}'.format(
                            message_type))
        return self._result

    def set_result(self, result):
        with self._condition:
            self._result = result
            self._condition.notify()


class FutureCollectionKeyError(Exception):
    pass


class FutureTimeoutError(Exception):
    pass


class FutureCollection:
    def __init__(self):
        self._futures = {}
        self._lock = RLock()

    def __eq__(self, other):
        if not isinstance(other, FutureCollection):
            return NotImplemented
        # not thread safe
        return self._futures == other._futures

    def put(self, future):
        with self._lock:
            self._futures[future.correlation_id] = future

    def set_result(self, correlation_id, result):
        with self._lock:
            future = self.get(correlation_id)
            future.set_result(result)

    def get(self, correlation_id):
        with self._lock:
            if correlation_id not in self._futures:
                raise FutureCollectionKeyError(
                    "no such correlation id: {}".format(correlation_id))
            return self._futures[correlation_id]

    def remove(self, correlation_id):
        with self._lock:
            if correlation_id not in self._futures:
                raise FutureCollectionKeyError(
                    "no such correlation id: {}".format(correlation_id))
            del self._futures[correlation_id]

    def future_values(self):
        with self._lock:
            return self._futures.values()
