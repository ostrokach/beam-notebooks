from __future__ import division, print_function

import contextlib
import gc
import json
import logging
import math
import os
import pickle
import sys
import tempfile
import time
import uuid
import weakref
from datetime import datetime

import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.io.gcp.pubsub import PubsubMessage
from apache_beam.options.pipeline_options import GoogleCloudOptions, PipelineOptions
from apache_beam.transforms import combiners, window
from apache_beam.utils import timestamp
from google.api_core import exceptions as gexc
from google.cloud import pubsub_v1

from pubsub_helpers import (
    create_pubsub_topic,
    create_pubsub_subscription,
    write_to_pubsub,
    read_from_pubsub,
)

try:
    from contextlib import ExitStack
except ImportError:
    from contextlib2 import ExitStack


options = PipelineOptions(
    runner="DataflowRunner",
    project="strokach-playground",
    temp_location="gs://strokach/dataflow_temp",
    sdk_location=os.path.expanduser(
        "~/workspace/beam/sdks/python/dist/apache-beam-2.15.0.dev0.tar.gz"
    ),
)


@contextlib.contextmanager
def create_pubsub_topic(project_id, prefix):
    pub_client = pubsub_v1.PublisherClient()
    topic_path = pub_client.topic_path(
        project_id, "{}-{}".format(prefix, uuid.uuid4().hex)
    )
    pub_client.create_topic(topic_path)
    try:
        yield topic_path
    finally:
        pub_client.delete_topic(topic_path)


@contextlib.contextmanager
def create_pubsub_subscription(topic_path, suffix=""):
    subscription_path = topic_path.replace("/topics/", "/subscriptions/") + (
        "-{}".format(suffix) if suffix else ""
    )
    sub_client.create_subscription(subscription_path, topic_path)
    try:
        yield subscription_path
    finally:
        sub_client.delete_subscription(subscription_path)


def write_to_pubsub(data_list, topic_path, attributes_fn=None, chunk_size=100):
    pub_client = pubsub_v1.PublisherClient()
    for start in range(0, len(data_list), chunk_size):
        data_chunk = data_list[start : start + chunk_size]
        if attributes_fn:
            attributes_chunk = [attributes_fn(data) for data in data_chunk]
        else:
            attributes_chunk = [{} for _ in data_chunk]
        futures = [
            pub_client.publish(
                topic_path, json.dumps(data).encode("utf-8"), **attributes
            )
            for data, attributes in zip(data_chunk, attributes_chunk)
        ]
        for future in futures:
            future.result()
        print("Finished publishing chunk of size {}.".format(len(data_chunk)))
        time.sleep(0.1)


def read_from_pubsub(subscription_path, number_of_elements):
    sub_client = pubsub_v1.SubscriberClient()
    messages = []
    while len(messages) <= number_of_elements:
        try:
            response = sub_client.pull(
                subscription_path, max_messages=number_of_elements
            )
        except gexc.RetryError:
            pass
        for msg in response.received_messages:
            sub_client.acknowledge(subscription_path, [msg.ack_id])
            data = json.loads(msg.message.data.decode("utf-8"))
            attributes = msg.message.attributes
            assert data["timestamp"] == int(attributes["timestamp"])
            messages.append(data)
            print(data)
    return messages


def timestamp_element(element):
    from apache_beam.transforms import window
    from apache_beam.utils import timestamp

    return window.TimestampedValue(
        element, timestamp.Timestamp(micros=element["timestamp"] * 1000)
    )


def encode_to_pubsub_message(element):
    import json
    from apache_beam.io.gcp.pubsub import PubsubMessage

    data = json.dumps(element).encode("utf-8")
    attributes = {"timestamp": str(element["timestamp"])}
    return PubsubMessage(data, attributes)


class AddGroupTimestamp(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        element = {"timestamp": int(window.end.micros // 1000), "batch": element}
        yield element


def run_pipeline(options):
    with ExitStack() as stack:
        input_topic = stack.enter_context(
            create_pubsub_topic(options.view_as(GoogleCloudOptions).project, "input")
        )
        input_subscription = stack.enter_context(
            create_pubsub_subscription(input_topic)
        )
        output_topic = stack.enter_context(
            create_pubsub_topic(options.view_as(GoogleCloudOptions).project, "output")
        )
        output_subscription = stack.enter_context(
            create_pubsub_subscription(output_topic)
        )

        unix_time = datetime.utcnow() - datetime.utcfromtimestamp(0)
        rounded_time = int(math.ceil(unix_time.total_seconds() / 100) * 100)
        # ReadFromPubSub expects timestamps to be in milliseconds
        rounded_time_millis = rounded_time * 1000
        print("Current time: {}".format(rounded_time_millis))
        data = [{"timestamp": rounded_time_millis + i} for i in range(0, 100000, 10)]
        write_to_pubsub(data, input_topic)

        p = beam.Pipeline(options=options)
        _ = (
            p
            | "Read"
            >> beam.io.ReadFromPubSub(
                subscription=input_subscription,
                # with_attributes=["timestamp_milliseconds"],
                # timestamp_attribute="timestamp_milliseconds",
            )
            # | beam.Create(data)
            | "Decode" >> beam.Map(lambda e: json.loads(e.decode("utf-8")))
            | "Add timestamp" >> beam.Map(timestamp_element)
            | "Add windowing" >> beam.WindowInto(window.FixedWindows(1))
            | "Group into batches"
            >> beam.CombineGlobally(combiners.ToListCombineFn()).without_defaults()
            | "Add a timestamp to each batch" >> beam.ParDo(AddGroupTimestamp())
            | "Encode" >> beam.Map(encode_to_pubsub_message)
            | "Write" >> beam.io.WriteToPubSub(output_topic, with_attributes=True)
        )
        pr = p.run()
        try:
            output = read_from_pubsub(output_subscription, number_of_elements=99)
        finally:
            pr.cancel()
    return output


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--runner", choices=["DirectRunner", "DataflowRunner"])
    args = parser.parse_args()
    # logging.basicConfig(level=logging.DEBUG)
    options = PipelineOptions(
        runner=args.runner,
        project="strokach-playground",
        streaming=True,
        temp_location="gs://strokach/dataflow_temp",
        sdk_location=os.path.expanduser(
            "~/workspace/beam/sdks/python/dist/apache-beam-2.15.0.dev0.tar.gz"
        ),
    )
    output = run_pipeline(options)
    with open("test_output.pkl", "wb") as fout:
        pickle.dump(output, fout)
    # PubSub messages may be delivered multiple times, so multiply by 0.9 to account for this
    num_messages = len(output)
    num_unique_groups = len({int(e["timestamp"] // 100 * 100) for e in output})
    assert num_unique_groups >= (0.9 * num_messages), (num_unique_groups, num_messages)
