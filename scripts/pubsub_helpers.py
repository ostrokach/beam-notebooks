import contextlib

from google.cloud import pubsub_v1


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
