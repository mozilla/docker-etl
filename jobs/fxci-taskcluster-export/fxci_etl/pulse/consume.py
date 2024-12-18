from kombu import Connection, Exchange, Queue
from loguru import logger

from fxci_etl.config import Config
from fxci_etl.pulse.handler import BigQueryHandler, PulseHandler


def get_connection(config: Config):
    assert config.pulse
    pulse = config.pulse
    return Connection(
        hostname=pulse.host,
        port=pulse.port,
        userid=pulse.user,
        password=pulse.password,
        ssl=True,
    )


def get_consumer(
    config: Config, connection: Connection, name: str, callbacks: list[PulseHandler]
):
    assert config.pulse
    pulse = config.pulse
    qconf = pulse.queues[name]
    exchange = Exchange(qconf.exchange, type="topic")
    exchange(connection).declare(
        passive=True
    )  # raise an error if exchange doesn't exist

    queue = Queue(
        name=f"queue/{pulse.user}/{name}",
        exchange=exchange,
        routing_key=qconf.routing_key,
        durable=pulse.durable,
        exclusive=False,
        auto_delete=not pulse.durable,
    )

    consumer = connection.Consumer(queue, auto_declare=False, callbacks=callbacks)
    qinfo = consumer.queues[0].queue_declare()
    logger.debug(f"{qinfo.message_count} pending messages")
    consumer.queues[0].queue_bind()
    return consumer


def drain(config: Config, name: str, callbacks: list[PulseHandler]):
    logger.info(f"Draining pulse queue {name}")
    with get_connection(config) as connection:
        with get_consumer(config, connection, name, callbacks) as consumer:
            while True:
                try:
                    connection.drain_events(timeout=1)
                except TimeoutError:
                    count = consumer.queues[0].queue_declare(passive=True).message_count
                    if count < 100:
                        break

    for callback in callbacks:
        callback.process_buffer()
