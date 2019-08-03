from __future__ import print_function

import contextlib
import gc
import os
import shutil
import sys
import tempfile
import time
import uuid
import weakref

import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.pipeline import PipelineOptions
from google.cloud import pubsub_v1

try:
    from contextlib import ExitStack
except ImportError:
    from contextlib2 import ExitStack


options = PipelineOptions(
    runner="DirectRunner", project="strokach-playground", streaming=True
)


@contextlib.contextmanager
def temporary_folder():
    tmpdir = tempfile.mkdtemp()
    try:
        yield tmpdir
    finally:
        shutil.rmtree(tmpdir)


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
    sub_client = pubsub_v1.SubscriberClient()
    subscription_path = topic_path.replace("/topics/", "/subscriptions/") + (
        "-{}".format(suffix) if suffix else ""
    )
    sub_client.create_subscription(subscription_path, topic_path)
    try:
        yield subscription_path
    finally:
        sub_client.delete_subscription(subscription_path)


def run_batch_pipeline():
    tmpdir = tempfile.mkdtemp()
    with ExitStack() as stack:
        tmpdir = stack.enter_context(temporary_folder())

        p = beam.Pipeline(options=options)
        p_ref = weakref.ref(p)
        _ = (
            p
            | beam.Create(range(100))
            | beam.io.WriteToText(os.path.join(tmpdir, "pipeline-gc-test"))
        )
        pr = p.run().wait_until_finish()
    return p_ref


def run_streaming_pipeline(from_topic=True):
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

        p = beam.Pipeline(options=options)
        p_ref = weakref.ref(p)
        _ = (
            p
            | "Read"
            >> (
                beam.io.ReadFromPubSub(topic=input_topic)
                if from_topic
                else beam.io.ReadFromPubSub(subscription=input_subscription)
            )
            | "Write" >> beam.io.WriteToPubSub(output_topic)
        )
        pr = p.run()
        try:
            time.sleep(5)
        finally:
            pr.cancel()
    return p_ref


if __name__ == "__main__":
    batch_p_ref = run_batch_pipeline()
    streaming_p_from_topic_ref = run_streaming_pipeline(from_topic=True)
    streaming_p_from_subscription_ref = run_streaming_pipeline(from_topic=False)
    gc.collect()

    print("Batch pipeline: {}".format(batch_p_ref()))
    print("Streaming pipeline from topic: {}".format(streaming_p_from_topic_ref()))
    print(
        "Streaming pipeline from subscription: {}".format(
            streaming_p_from_subscription_ref()
        )
    )
    # Batch pipeline: None
    # Streaming pipeline from topic: <apache_beam.pipeline.Pipeline object at 0x7f3c6e8def90>
    # Streaming pipeline from subscription: None
