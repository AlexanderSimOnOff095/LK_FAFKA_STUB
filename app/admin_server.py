import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import unquote


def handler(repository, worker):
    class AdminHandler(BaseHTTPRequestHandler):
        def body(self):
            length=int(self.headers.get("Content-Length","0")); return json.loads(self.rfile.read(length) or b"{}")
        def send(self,status=200,data=None):
            payload=b"" if data is None else json.dumps(data,ensure_ascii=False).encode()
            self.send_response(status); self.send_header("Content-Type","application/json; charset=utf-8"); self.send_header("Content-Length",str(len(payload))); self.end_headers(); self.wfile.write(payload)
        def do_GET(self):
            if self.path == "/api/v1/admin/health":
                try: repository.settings(); self.send(data={"status":"UP","kafkaConnected":True,"configurationLoaded":True})
                except Exception as e: self.send(503,{"status":"DOWN","message":str(e)})
            elif self.path == "/api/v1/admin/settings":
                s=repository.settings(); self.send(data={"processingMode":s.processing_mode,"resultPublishDelayMs":s.result_publish_delay_ms,"transitions":repository.transitions(),"errorSimulation":{"enabled":s.error_simulation_enabled,"errorCode":s.error_code,"retryable":s.retryable}})
            elif self.path == "/api/v1/admin/status-transitions": self.send(data={"transitions":[{"currentStatus":k,"nextStatus":v} for k,v in repository.transitions().items()]})
            else: self.send(404,{"errorCode":"NOT_FOUND"})
        def do_PUT(self):
            data=self.body()
            if self.path == "/api/v1/admin/settings/processing-mode": repository.update_settings(processing_mode=data["processingMode"]); self.send(data={"processingMode":data["processingMode"],"result":"SUCCESS"})
            elif self.path == "/api/v1/admin/settings/response-delay": repository.update_settings(result_publish_delay_ms=int(data["resultPublishDelayMs"])); self.send(data={"resultPublishDelayMs":int(data["resultPublishDelayMs"]),"result":"SUCCESS"})
            elif self.path == "/api/v1/admin/error-simulation": repository.update_settings(error_simulation_enabled=bool(data.get("enabled")),error_code=data.get("errorCode","INTERNAL_ERROR"),error_message=data.get("message","Test processing error"),retryable=bool(data.get("retryable",False))); self.send(data={"enabled":bool(data.get("enabled")),"result":"SUCCESS"})
            elif self.path.startswith("/api/v1/admin/status-transitions/"):
                current=unquote(self.path.rsplit("/",1)[1]); repository.save_transition(current,data["nextStatus"]); self.send(data={"currentStatus":current,"nextStatus":data["nextStatus"],"result":"SUCCESS"})
            else: self.send(404,{"errorCode":"NOT_FOUND"})
        def do_DELETE(self):
            if self.path.startswith("/api/v1/admin/status-transitions/"): repository.delete_transition(unquote(self.path.rsplit("/",1)[1])); self.send(204)
            else: self.send(404,{"errorCode":"NOT_FOUND"})
        def do_POST(self):
            if self.path == "/api/v1/admin/settings/reset": repository.reset(); self.send(data={"result":"SUCCESS","message":"Настройки восстановлены"})
            else: self.send(404,{"errorCode":"NOT_FOUND"})
        def log_message(self,fmt,*args): return
    return AdminHandler


def serve(port, repository, worker): ThreadingHTTPServer(("0.0.0.0",port),handler(repository,worker)).serve_forever()
