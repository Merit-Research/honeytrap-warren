# warren.py - A simple RabbitMQ message consumer module and program
#
# Author: Mark Weiman <mweiman@merit.edu>

import pika


class Warren:
    def __init__(self, amqp_url, message_cb=None):
        self._connection = None
        self._channel = None
        self.message_cb = message_cb
        self._url = amqp_url

    def _default_cb(self, data):
        print("[" + data["queue"] + "] " + data["message"].decode("utf-8"))

    def _callback(self, channel, method, properties, body):
        data = {
            "queue": method.routing_key,
            "message": body,
        }

        if self.message_cb is None:
            self._default_cb(data)
        else:
            self.message_cb(data)

    def add_queue(self, queue):
        self._channel.basic_consume(queue, self._callback, auto_ack=True)

    def start_connection(self):
        parameters = pika.URLParameters(self._url)
        self._connection = pika.BlockingConnection(parameters)
        self._channel = self._connection.channel()
        self._channel.basic_qos(prefetch_count=1)

    def start_consuming(self):
        self._channel.start_consuming()


class WarrenError(Exception):
    pass


def main():
    warren = Warren("amqp://guest:guest@localhost:5672/%2F")
    warren.start_connection()
    warren.add_queue("merit-ht")

    try:
        warren.start_consuming()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
