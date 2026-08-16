import logging
from . import config
from .admin_server import serve
from .kafka_worker import KafkaWorker
from .repository import Repository


def main():
    logging.basicConfig(level=logging.INFO,format="%(asctime)s %(levelname)s %(name)s %(message)s")
    repo=Repository(config.DATABASE_URL)
    worker=KafkaWorker(config.KAFKA_BOOTSTRAP_SERVERS,config.KAFKA_TOPIC,config.STUB_CONSUMER_GROUP,repo)
    worker.start(); serve(config.ADMIN_PORT,repo,worker)


if __name__ == "__main__": main()
