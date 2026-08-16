import unittest
from app.domain import Settings, build_result, choose_status, decode_message_key, encode_message_key, fingerprint, validate_request


def request(**overrides):
    event={"eventId":"e1","eventType":"APPLICATION_STATUS_CHANGE_REQUESTED","eventVersion":1,"requestId":"r1","correlationId":"c1","producer":"eapo-cab","applicationId":"a1","currentStatus":"NEW"}
    event.update(overrides); return event


class DomainTest(unittest.TestCase):
    def test_transition(self): self.assertEqual("PROCESSING",choose_status("NEW",Settings(),{"NEW":"PROCESSING"}))
    def test_always_completed(self): self.assertEqual("COMPLETED",choose_status("NEW",Settings(processing_mode="ALWAYS_COMPLETED"),{}))
    def test_keep_current(self): self.assertEqual("NEW",choose_status("NEW",Settings(processing_mode="KEEP_CURRENT_STATUS"),{}))
    def test_unknown_status(self):
        with self.assertRaises(LookupError): choose_status("UNKNOWN",Settings(),{})
    def test_validation(self): validate_request(request(),"a1")
    def test_key_mismatch(self):
        with self.assertRaisesRegex(ValueError,"APPLICATION_KEY_MISMATCH"): validate_request(request(),"a2")
    def test_rest_proxy_json_key(self): self.assertEqual("a1",decode_message_key(b'"a1"'))
    def test_plain_kafka_key(self): self.assertEqual("a1",decode_message_key(b'a1'))
    def test_rest_proxy_key_round_trip(self): self.assertEqual("a1",decode_message_key(encode_message_key("a1")))
    def test_fingerprint_is_payload_based(self): self.assertEqual(fingerprint(request(eventId="x")),fingerprint(request(eventId="y")))
    def test_success_result(self):
        result=build_result(request(),"PROCESSING"); self.assertEqual("SUCCESS",result["result"]); self.assertEqual("e1",result["causationId"])
    def test_error_result(self):
        result=build_result(request(),None,("UNKNOWN_STATUS","missing",False)); self.assertEqual("ERROR",result["result"]); self.assertIsNone(result["status"])


if __name__ == "__main__": unittest.main()
