#   Copyright 2021 Modelyst LLC
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

import time
from collections import defaultdict
from typing import TYPE_CHECKING, DefaultDict

if TYPE_CHECKING:
    from dbgen.providers.aws.ecs import ECSTask


def follow_logs(logs, ecs, ecs_task: 'ECSTask'):
    # this can be `boto3.client(logs)` instead of using `session`
    filter_logs_events_kwargs = {
        "logGroupName": ecs_task.task_name,
        "logStreamNames": [ecs_task.get_log_stream()],
        "interleaved": True,
    }
    try:
        yield from _do_filter_log_events(logs, ecs, filter_logs_events_kwargs, ecs_task)
    except KeyboardInterrupt:
        # The only way to exit from the --follow is to Ctrl-C. So
        # we should exit the iterator rather than having the
        # KeyboardInterrupt propogate to the rest of the command.
        return


def _get_latest_events_and_timestamp(event_ids_per_timestamp):
    if event_ids_per_timestamp:
        # Keep only ids of the events with the newest timestamp
        newest_timestamp = max(event_ids_per_timestamp.keys())
        event_ids_per_timestamp = defaultdict(
            set, {newest_timestamp: event_ids_per_timestamp[newest_timestamp]}
        )
    return event_ids_per_timestamp


def _reset_filter_log_events_params(fle_kwargs, event_ids_per_timestamp):
    # Remove nextToken and update startTime for the next request
    # with the timestamp of the newest event
    if event_ids_per_timestamp:
        fle_kwargs["startTime"] = max(event_ids_per_timestamp.keys())
    fle_kwargs.pop("nextToken", None)


def _do_filter_log_events(logs_client, ecs_client, filter_logs_events_kwargs, ecs_task: 'ECSTask'):
    event_ids_per_timestamp: DefaultDict[str, set] = defaultdict(set)
    while True:
        task_details = ecs_task.get_task_details(ecs_client)
        response = logs_client.filter_log_events(**filter_logs_events_kwargs)

        for event in response["events"]:
            # For the case where we've hit the last page, we will be
            # reusing the newest timestamp of the received events to keep polling.
            # This means it is possible that duplicate log events with same timestamp
            # are returned back which we do not want to yield again.
            # We only want to yield log events that we have not seen.
            if event["eventId"] not in event_ids_per_timestamp[event["timestamp"]]:
                event_ids_per_timestamp[event["timestamp"]].add(event["eventId"])
                yield event
        event_ids_per_timestamp = _get_latest_events_and_timestamp(event_ids_per_timestamp)
        if "nextToken" in response:
            filter_logs_events_kwargs["nextToken"] = response["nextToken"]
        else:
            _reset_filter_log_events_params(filter_logs_events_kwargs, event_ids_per_timestamp)
            time.sleep(ecs_task.interval)
        if task_details.lastStatus in ('STOPPED', 'DEPROVISIONING') and "nextToken" not in response:
            break
