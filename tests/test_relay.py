import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import relay


class FakeGitHub:
    issues = []
    fail_after_accept = False

    def __init__(self, config):
        self.config = config

    def publish(self, row):
        pinned = json.loads(row['wire'])
        wire = json.loads(pinned['body'])
        n = len(self.issues) + 1
        result = dict(number=n, html_url=f'https://github.com/test/relay/issues/{n}',
                      body=relay.PREFIX + relay.canonical(wire), user={'login': 'alice'})
        self.issues.append(result)
        if self.fail_after_accept:
            self.fail_after_accept = False
            raise RuntimeError('response lost')
        return result

    def poll(self, after):
        return [x for x in self.issues if x['number'] > after]

    def test(self):
        return {'full_name': self.config['repo']}


class RelayTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.sender = relay.Store(Path(self.tmp.name) / 'sender')
        self.receiver = relay.Store(Path(self.tmp.name) / 'receiver')
        config = dict(repo='test/relay', channel='test', secret='test-secret', github_token='fake', enabled=True)
        self.sender.save_config(dict(config, role='sender'))
        self.receiver.save_config(dict(config, role='receiver', allowed_senders=['alice']))
        FakeGitHub.issues = []
        FakeGitHub.fail_after_accept = False
        self.fake = patch.object(relay, 'GitHub', FakeGitHub)
        self.fake.start()
        self.addCleanup(self.fake.stop)

    def events(self, store):
        with store.db() as db:
            return [dict(x) for x in db.execute('SELECT * FROM events')]

    def test_end_to_end_and_restart_dedup(self):
        event_id = self.sender.enqueue('notify', {'message': 'hello 世界'})
        relay.Agent(self.sender).cycle()
        self.assertEqual(self.events(self.sender)[0]['status'], 'published')
        relay.Agent(self.receiver).cycle()
        self.assertEqual(self.events(self.receiver)[0]['status'], 'done')
        # Force replay after restart: a duplicate transport delivery stays one local event.
        FakeGitHub.issues.append(dict(FakeGitHub.issues[0], number=2))
        receiver2 = relay.Store(self.receiver.directory)
        relay.Agent(receiver2).cycle()
        self.assertEqual(len(self.events(receiver2)), 1)
        self.assertEqual(self.events(receiver2)[0]['id'], event_id)

    def test_publish_accepted_but_response_lost(self):
        self.sender.enqueue('notify', {})
        client = FakeGitHub(self.sender.read_config())
        client.fail_after_accept = True
        with patch.object(relay, 'GitHub', return_value=client):
            with self.assertRaises(RuntimeError):
                relay.Agent(self.sender).cycle()
            with self.sender.db() as db:
                db.execute('UPDATE events SET next_try=0')
            relay.Agent(self.sender).cycle()
        self.assertEqual(len(FakeGitHub.issues), 2)
        relay.Agent(self.receiver).cycle()
        self.assertEqual(len(self.events(self.receiver)), 1)

    def test_invalid_signature_sender_and_channel(self):
        config = self.receiver.read_config()
        wire = relay.envelope('test', 'notify', {}, 'test-secret')
        body = relay.PREFIX + relay.canonical(wire)
        self.assertEqual(relay.verify(body, config, 'alice')['type'], 'notify')
        with self.assertRaises(ValueError):
            relay.verify(body, config, 'mallory')
        wire['event']['payload']['message'] = 'tampered'
        with self.assertRaises(ValueError):
            relay.verify(relay.PREFIX + relay.canonical(wire), config, 'alice')
        other = relay.envelope('other', 'notify', {}, 'test-secret')
        with self.assertRaises(ValueError):
            relay.verify(relay.PREFIX + relay.canonical(other), config, 'alice')

    def test_real_command_receives_payload_without_shell_expansion(self):
        output = Path(self.tmp.name) / 'output.json'
        script = Path(self.tmp.name) / 'action.py'
        script.write_text('import os,pathlib\npathlib.Path(os.environ["OUTPUT_FILE"]).write_text(os.environ["RELAY_PAYLOAD_JSON"])\n')
        self.receiver.save_config({'handlers': {'model.ready': {'argv': [sys.executable, str(script)], 'timeout': 10}}})
        value = {'message': '$(touch SHOULD_NOT_EXIST); `id`'}
        self.sender.enqueue('model.ready', value)
        relay.Agent(self.sender).cycle()
        with patch.dict(os.environ, OUTPUT_FILE=str(output)):
            relay.Agent(self.receiver).cycle()
        self.assertEqual(json.loads(output.read_text()), value)
        self.assertEqual(self.events(self.receiver)[0]['status'], 'done')

    def test_failed_command_is_not_automatically_rerun(self):
        self.receiver.save_config({'handlers': {'test.fail': {'argv': [sys.executable, '-c', 'print("failed output");raise SystemExit(7)'], 'timeout': 10}}})
        self.sender.enqueue('test.fail', {})
        relay.Agent(self.sender).cycle()
        a = relay.Agent(self.receiver)
        a.cycle()
        a.cycle()
        row = self.events(self.receiver)[0]
        self.assertEqual(row['status'], 'failed')
        self.assertEqual(row['attempts'], 1)
        self.assertIn('failed output', row['output'])

    def test_expired_event_does_not_execute(self):
        wire = relay.envelope('test', 'notify', {}, 'test-secret')
        wire['event']['expires_at'] = int(time.time()) - 1
        self.receiver.insert(wire['event'], 'in', relay.canonical(wire['event']))
        relay.Agent(self.receiver).cycle()
        self.assertEqual(self.events(self.receiver)[0]['status'], 'expired')

    def test_watch_recovers_without_duplicate_event(self):
        watch_id = 'a4d2a9ba-1924-4631-9daf-6dba477f069e'
        with self.sender.db() as db:
            db.execute("INSERT INTO watches VALUES(?,?,?,?,?,'waiting','',?)", (watch_id,'http://localhost/health','model.ready','{}',time.time()+60,time.time()))
        class Response:
            status = 200
            def __enter__(self): return self
            def __exit__(self, *args): pass
        with patch('urllib.request.urlopen', return_value=Response()):
            a = relay.Agent(self.sender)
            a.watches()
            # Simulate a crash between the event enqueue and marking the watch ready.
            with self.sender.db() as db:
                db.execute("UPDATE watches SET status='waiting'")
            a.watches()
        self.assertEqual(len(self.events(self.sender)), 1)
        self.assertEqual(self.events(self.sender)[0]['id'], watch_id)

    def test_config_credentials_not_cleared_and_destination_pinned(self):
        self.sender.enqueue('notify', {})
        self.sender.save_config({'repo': 'new/repo', 'secret': '', 'github_token': ''})
        self.assertEqual(self.sender.read_config()['secret'], 'test-secret')
        self.assertEqual(json.loads(self.events(self.sender)[0]['wire'])['repo'], 'test/relay')
        self.assertEqual(self.sender.config_path.stat().st_mode & 0o777, 0o600)

    def test_timeout_marks_failure(self):
        self.receiver.save_config({'handlers': {'test.timeout': {'argv': [sys.executable, '-c', 'import time;time.sleep(30)'], 'timeout': 1}}})
        self.sender.enqueue('test.timeout', {})
        relay.Agent(self.sender).cycle()
        relay.Agent(self.receiver).cycle()
        self.assertEqual(self.events(self.receiver)[0]['status'], 'failed')
        self.assertIn('超时', self.events(self.receiver)[0]['error'])


if __name__ == '__main__':
    unittest.main()
