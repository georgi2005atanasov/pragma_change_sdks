from pathlib import Path
import sys
import unittest
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'python'))
from pragma_change import Client

class SDKTests(unittest.TestCase):
 def setUp(self):
  self.client=Client('http://127.0.0.1:1','test','app',['duration_ms']);self.client.stop.set();self.client.thread.join();self.addCleanup(self.client.close)
 def event(self):return dict(actor='u',action='read',session_id='s',attributes={'password':'SECRET','duration_ms':5})
 def test_privacy_and_bounds(self):
  for _ in range(1001):self.client.record(**self.event())
  self.assertEqual(self.client.health()['queued'],1000);self.assertEqual(self.client.health()['dropped'],1)
  self.assertNotIn('password',self.client.queue[0][0]['attributes'])
 def test_retry_identity_and_expiry(self):
  self.client.record(**self.event());identifier=self.client.queue[0][0]['event_id']
  with patch.object(self.client,'_request',side_effect=OSError('outage')):
   self.client.flush();self.assertEqual(self.client.queue[0][0]['event_id'],identifier)
   for _ in range(4):self.client.flush()
  self.assertEqual(self.client.health()['dropped'],1)
 def test_unavailable_is_check(self):
  self.assertEqual(self.client.evaluate(**self.event())['action'],'check')
