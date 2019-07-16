from __future__ import print_function

import contextlib
import gc
import json
import logging
import os
import pickle
import sys
import tempfile
import time
import uuid
import weakref

import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.io.gcp.pubsub import PubsubMessage
from apache_beam.options.pipeline_options import GoogleCloudOptions, PipelineOptions
from apache_beam.transforms import combiners, window
from apache_beam.utils import timestamp
from google.cloud import pubsub_v1

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
    topic_path = "projects/{}/topics/{}-{}".format(project_id, prefix, uuid.uuid4().hex)
    pub_client = pubsub_v1.PublisherClient()
    pub_client.create_topic(topic_path)
    try:
        yield topic_path
    finally:
        pub_client.delete_topic(topic_path)


@contextlib.contextmanager
def create_pubsub_subscription(topic_path, suffix=""):
    subscription_path = topic_path.replace("/topics/", "/subscriptions/")
    if suffix:
        subscription_path += "-{}".format(suffix)
    sub_client = pubsub_v1.SubscriberClient()
    sub_client.create_subscription(subscription_path, topic_path)
    try:
        yield subscription_path
    finally:
        sub_client.delete_subscription(subscription_path)


def write_to_pubsub(data_list, topic_path):
    pub_client = pubsub_v1.PublisherClient()
    futures = [pub_client.publish(topic_path, data) for data in data_list]
    for future in futures:
        future.result()


def read_from_pubsub(subscription_path, number_of_elements=None):
    sub_client = pubsub_v1.SubscriberClient()
    messages = []
    while len(messages) <= number_of_elements:
        response = sub_client.pull(subscription_path, max_messages=number_of_elements)
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
        element, timestamp.Timestamp(seconds=element["timestamp"])
    )


def add_timestamp_from_first_element(element):
    return {"timestamp": element[0]["timestamp"], "batch": element}


def encode_to_pubsub_message(element):
    import json
    from apache_beam.io.gcp.pubsub import PubsubMessage

    data = json.dumps(element).encode("utf-8")
    attributes = {"timestamp": str(element["timestamp"])}
    return PubsubMessage(data, attributes)


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

        write_to_pubsub(
            [json.dumps({"timestamp": i}).encode("utf-8") for i in range(10000)],
            input_topic,
        )
        p = beam.Pipeline(options=options)
        _ = (
            p
            | beam.io.ReadFromPubSub(subscription=input_subscription)
            # | beam.Create(data)
            | beam.Map(lambda e: json.loads(e.decode("utf-8")))
            # | beam.Map(lambda e: print(e) or e)
            | beam.Map(timestamp_element)
            | beam.WindowInto(window.FixedWindows(100))
            | "Group into batches"
            >> beam.CombineGlobally(combiners.ToListCombineFn()).without_defaults()
            | "Add a timestamp to each batch"
            >> beam.Map(add_timestamp_from_first_element)
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
    # logging.basicConfig(level=logging.DEBUG)
    options = PipelineOptions(
        runner="DirectRunner",
        # runner="DataflowRunner",
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
