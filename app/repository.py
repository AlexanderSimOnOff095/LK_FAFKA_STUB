import json
import psycopg
from .domain import Settings


class Repository:
    def __init__(self, dsn: str): self.dsn = dsn

    def connect(self): return psycopg.connect(self.dsn)

    def settings(self) -> Settings:
        with self.connect() as c, c.cursor() as q:
            q.execute("SELECT processing_mode,result_publish_delay_ms,error_simulation_enabled,error_code,error_message,retryable FROM service_settings WHERE id=1")
            r=q.fetchone(); return Settings(*r)

    def transitions(self) -> dict[str, str]:
        with self.connect() as c, c.cursor() as q:
            q.execute("SELECT incoming_status,target_status FROM application_status_transition WHERE is_active ORDER BY priority,id")
            result={}
            for incoming,target in q.fetchall(): result.setdefault(incoming,target)
            return result

    def find_processed(self, request_id: str):
        with self.connect() as c, c.cursor() as q:
            q.execute("SELECT request_fingerprint,result_payload FROM processed_request WHERE request_id=%s",(request_id,))
            return q.fetchone()

    def save_processed(self, request_id: str, fp: str, payload: dict):
        with self.connect() as c, c.cursor() as q:
            q.execute("INSERT INTO processed_request(request_id,request_fingerprint,result_payload) VALUES(%s,%s,%s::jsonb) ON CONFLICT(request_id) DO NOTHING",(request_id,fp,json.dumps(payload)))

    def save_transition(self, current: str, nxt: str):
        with self.connect() as c, c.cursor() as q:
            q.execute("INSERT INTO application_status_transition(application_id,application_type,incoming_status,target_status) VALUES('*','DEFAULT',%s,%s) ON CONFLICT(application_type,incoming_status,target_status) DO UPDATE SET is_active=true,updated_at=now()",(current,nxt))

    def delete_transition(self, current: str):
        with self.connect() as c, c.cursor() as q: q.execute("DELETE FROM application_status_transition WHERE incoming_status=%s",(current,))

    def update_settings(self, **values):
        allowed={"processing_mode","result_publish_delay_ms","error_simulation_enabled","error_code","error_message","retryable"}
        values={k:v for k,v in values.items() if k in allowed}
        if not values: return
        sql=",".join(f"{k}=%s" for k in values)
        with self.connect() as c, c.cursor() as q: q.execute(f"UPDATE service_settings SET {sql},updated_at=now() WHERE id=1",tuple(values.values()))

    def reset(self):
        with self.connect() as c, c.cursor() as q:
            q.execute("UPDATE service_settings SET processing_mode='STATUS_TRANSITION',result_publish_delay_ms=0,error_simulation_enabled=false,error_code='INTERNAL_ERROR',error_message='Test processing error',retryable=false,updated_at=now() WHERE id=1")
            q.execute("DELETE FROM application_status_transition")
            q.execute("INSERT INTO application_status_transition(application_id,application_type,incoming_status,target_status,priority) VALUES ('*','DEFAULT','NEW','PROCESSING',1),('*','DEFAULT','PROCESSING','COMPLETED',1),('*','DEFAULT','COMPLETED','COMPLETED',1),('*','DEFAULT','ERROR','PROCESSING',1)")
