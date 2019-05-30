#!/usr/bin/env python
from __future__ import print_function

import multiprocessing
import os
import subprocess
import time

import apache_beam as beam
from google.cloud import pubsub_v1


def count_open_files():
    """Count the number of files opened by current process."""
    pid = multiprocessing.current_process().pid
    lsof_out = subprocess.check_output(["lsof", "-p", str(pid)])
    num_open_files = len(lsof_out.strip().split("\n")) - 1
    return num_open_files


def start_streaming_pipeline(project_id, subscription_path):
    runner = beam.runners.direct.DirectRunner()
    pipeline_options = beam.pipeline.PipelineOptions(project=project_id, streaming=True)
    taxirides_pc = (
        #
        beam.Pipeline(runner=runner, options=pipeline_options)
        | "Read" >> beam.io.ReadFromPubSub(subscription=subscription_path)
    )
    results = taxirides_pc.pipeline.run()
    return results


def monitor():
    start_time = time.time()
    for _ in range(20):
        num_open_files = count_open_files()
        time_elapsed = time.time() - start_time
        print(
            "Time elapsed: {:<3s}s, Number of open files: {}".format(
                str(round(time_elapsed, 0)), num_open_files
            )
        )
        if num_open_files > 1000:
            break
        time.sleep(5)


if __name__ == "__main__":
    project_id = "strokach-playground"
    topic_path = "projects/pubsub-public-data/topics/taxirides-realtime"

    client = pubsub_v1.SubscriberClient()
    subscription_path = client.subscription_path(project_id, "taxirides-realtime-sub")

    subscription = client.create_subscription(subscription_path, topic_path)
    print("Subscription created: {}".format(subscription_path))
    try:
        results = start_streaming_pipeline(project_id, subscription_path)
        monitor()
    finally:
        client.delete_subscription(subscription_path)
        print("Subscription deleted: {}".format(subscription_path))
        pass
