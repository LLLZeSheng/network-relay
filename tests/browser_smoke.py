"""Optional browser end-to-end test; uses a fake mailbox, never writes to GitHub.

Run with an environment containing playwright and Chromium:
  python tests/browser_smoke.py /path/to/chrome
"""
import json
import os
from pathlib import Path
import sys
import tempfile
import threading
import time
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import relay
from test_relay import FakeGitHub
from playwright.sync_api import sync_playwright

screens = Path(__file__).resolve().parents[1] / 'previews'
screens.mkdir(exist_ok=True)
with tempfile.TemporaryDirectory() as temp, patch.object(relay, 'GitHub', FakeGitHub):
    agents = []
    for role, port in [('sender', 18787), ('receiver', 18788)]:
        store = relay.Store(Path(temp) / role)
        store.save_config({'role': role, 'poll_seconds': 5})
        agent = relay.Agent(store)
        threading.Thread(target=agent.run, daemon=True).start()
        threading.Thread(target=relay.serve, args=(store, agent, port), daemon=True).start()
        agents.append(agent)
    time.sleep(.4)
    with sync_playwright() as p:
        executable = sys.argv[1] if len(sys.argv)>1 else None
        firefox = executable is not None and Path(executable).name == 'firefox'
        engine = p.firefox if firefox else p.chromium
        browser = engine.launch(executable_path=executable, headless=True,
                                args=[] if firefox else ['--no-sandbox'])
        sender = browser.new_page(viewport={'width': 1440, 'height': 1100}, device_scale_factor=1)
        receiver = browser.new_page(viewport={'width': 1440, 'height': 1100}, device_scale_factor=1)
        errors = []
        for page in [sender, receiver]:
            page.on('pageerror', lambda e: errors.append(str(e)))
        for page, port in [(sender,18787),(receiver,18788)]:
            page.goto(f'http://127.0.0.1:{port}')
            page.locator('#settingsTop').click()
            page.locator('#cfgRepo').fill('example/network-events')
            page.locator('#cfgToken').fill('fake-local-test-only')
            page.locator('#cfgSecret').fill('local-shared-test-key')
            page.locator('#cfgChannel').fill('model-operations')
            page.locator('#cfgAuthors').fill('alice')
            page.locator('#cfgEnabled').check()
            handlers = {'notify': {'argv': [], 'timeout': 30}, 'model.ready': {
                'argv': [sys.executable, str(relay.HERE / 'examples/on_model_ready.py')], 'timeout': 10}}
            page.locator('#cfgHandlers').fill(json.dumps(handlers))
            page.locator('#settingsForm button[type=submit]').click()
            page.wait_for_function("!document.querySelector('#settings').open")
        # Validate user-facing JSON error without sending an event.
        sender.locator('#eventPayload').fill('{bad json')
        sender.locator('#sendButton').click()
        sender.wait_for_function("document.querySelector('#toast').classList.contains('error')")
        sender.locator('#eventPayload').fill('{"model":"MG46", "run":"eval-001"}')
        sender.locator('#eventType').fill('model.ready')
        sender.locator('#eventMessage').fill('MG46 已就绪，可以开始评测。')
        sender.locator('#sendButton').click()
        sender.wait_for_function("document.querySelector('#countDone').textContent === '1'", timeout=25000)
        receiver.wait_for_function("document.querySelector('#countDone').textContent === '1'", timeout=25000)
        receiver.locator('.event').first.click()
        receiver.wait_for_function("document.querySelector('#detailLog').textContent.includes('MG46')")
        assert '收到模型就绪事件' in receiver.locator('#detailLog').inner_text()
        receiver.locator('[data-close=detail]').click()
        # Capture the actual two dashboards after a full simulated delivery.
        sender.evaluate('window.scrollTo(0,0)')
        receiver.evaluate('window.scrollTo(0,0)')
        sender.wait_for_timeout(4600)
        sender.screenshot(path=str(screens/'sender.png'), full_page=True)
        receiver.screenshot(path=str(screens/'receiver.png'), full_page=True)
        # Exercise mobile layout and ensure it doesn't overflow horizontally.
        sender.set_viewport_size({'width':390,'height':844})
        sender.screenshot(path=str(screens/'sender-mobile.png'), full_page=True)
        assert sender.evaluate('document.documentElement.scrollWidth <= innerWidth')
        sender.locator('[data-mode=watch]').click()
        assert sender.locator('#healthUrl').is_visible()
        assert not errors, errors
        browser.close()
    for agent in agents:
        agent.stop.set()
    time.sleep(.3)
print('PASS: both dashboards, settings, invalid JSON, delivery, command logs, mobile layout, watch form; no JavaScript errors')
