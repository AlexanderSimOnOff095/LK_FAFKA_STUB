import json
import logging
import threading
import time
from confluent_kafka import Consumer, Producer
from .domain import REQUEST_TYPE, build_result, choose_status, decode_message_key, fingerprint, validate_request

log = logging.getLogger(__name__)


class KafkaWorker(threading.Thread):
    daemon = True
    def __init__(self, bootstrap: str, topic: str, group: str, repository):
        super().__init__(name="kafka-worker")
        self.topic=topic; self.repository=repository; self.stop_event=threading.Event()
        self.consumer=Consumer({"bootstrap.servers":bootstrap,"group.id":group,"enable.auto.commit":False,"auto.offset.reset":"earliest"})
        self.producer=Producer({"bootstrap.servers":bootstrap,"acks":"all","enable.idempotence":True})

    def stop(self): self.stop_event.set()

    def publish(self, key: str, value: dict):
        self.producer.produce(self.topic,key=key.encode(),value=json.dumps(value,ensure_ascii=False).encode())
        self.producer.flush(10)

    def run(self):
        self.consumer.subscribe([self.topic])
        try:
            while not self.stop_event.is_set():
                msg=self.consumer.poll(1.0)
                if msg is None: continue
                if msg.error(): log.error("Kafka error: %s",msg.error()); continue
                try:
                    event=json.loads(msg.value()); key=decode_message_key(msg.key())
                    if event.get("eventType") != REQUEST_TYPE or event.get("producer") != "eapo-cab":
                        self.consumer.commit(msg); continue
                    validate_request(event,key); fp=fingerprint(event); old=self.repository.find_processed(event["requestId"])
                    if old:
                        if old[0] != fp: result=build_result(event,None,("REQUEST_ID_CONFLICT","requestId already used with another payload",False))
                        else: result=old[1]
                    else:
                        settings=self.repository.settings()
                        if settings.error_simulation_enabled:
                            result=build_result(event,None,(settings.error_code,settings.error_message,settings.retryable))
                        else:
                            try: result=build_result(event,choose_status(event["currentStatus"],settings,self.repository.transitions()))
                            except LookupError: result=build_result(event,None,("UNKNOWN_STATUS","Status transition is not configured",False))
                        self.repository.save_processed(event["requestId"],fp,result)
                    delay=self.repository.settings().result_publish_delay_ms
                    if delay: time.sleep(delay/1000)
                    self.publish(key,result); self.consumer.commit(msg)
                except Exception: log.exception("Message processing failed")
        finally:
            self.consumer.close()
