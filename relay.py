#!/usr/bin/env python3
"""Two outbound-only agents, a GitHub Issues mailbox, and a local web console."""
import argparse
import contextlib
import datetime as dt
import hashlib
import hmac
import json
import logging
import os
from pathlib import Path
import re
import signal
import sqlite3
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PREFIX = "network-relay/v1\n"
HERE = Path(__file__).resolve().parent
DEFAULTS = {"role": "sender", "repo": "", "channel": "default", "github_token": "",
            "secret": "", "allowed_senders": [], "poll_seconds": 10, "enabled": False,
            "handlers": {"notify": {"argv": [], "timeout": 30}}}
LOG = logging.getLogger("relay")


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def envelope(channel, kind, payload, secret, ttl=86400):
    now = int(time.time())
    event = dict(version=1, id=str(uuid.uuid4()), channel=channel, type=kind,
                 payload=payload, created_at=now, expires_at=now + ttl)
    return {"event": event, "signature": hmac.new(secret.encode(), canonical(event).encode(), hashlib.sha256).hexdigest()}


def verify(body, config, author):
    if not isinstance(body, str) or not body.startswith(PREFIX) or len(body.encode()) > 40000:
        raise ValueError("不是有效的 Relay 事件")
    value = json.loads(body[len(PREFIX):])
    event = value["event"]
    signature = hmac.new(config["secret"].encode(), canonical(event).encode(), hashlib.sha256).hexdigest()
    if not config["secret"] or not hmac.compare_digest(signature, str(value.get("signature", ""))):
        raise ValueError("事件签名不匹配")
    if config["allowed_senders"] and author.lower() not in [s.lower() for s in config["allowed_senders"]]:
        raise ValueError("发送者不在允许列表")
    if event["version"] != 1 or event["channel"] != config["channel"]:
        raise ValueError("协议或通道不匹配")
    uuid.UUID(event["id"])
    if not isinstance(event["type"], str) or not isinstance(event["payload"], dict):
        raise ValueError("事件字段无效")
    if not isinstance(event["expires_at"], int) or not isinstance(event["created_at"], int):
        raise ValueError("事件时间无效")
    if event["expires_at"] <= event["created_at"] or event["created_at"] > time.time() + 300:
        raise ValueError("事件时间无效")
    return event


class Store:
    def __init__(self, directory):
        self.directory = Path(directory).resolve()
        self.directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.config_path = self.directory / "config.json"
        self.lock = threading.RLock()
        self.config = dict(DEFAULTS)
        if self.config_path.exists():
            self.config.update(json.loads(self.config_path.read_text()))
        with self.db() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS events (
                  id TEXT PRIMARY KEY, direction TEXT, kind TEXT, payload TEXT, wire TEXT,
                  status TEXT, attempts INTEGER DEFAULT 0, next_try REAL DEFAULT 0,
                  issue_url TEXT DEFAULT '', error TEXT DEFAULT '', output TEXT DEFAULT '',
                  created REAL, updated REAL);
                CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);
                CREATE TABLE IF NOT EXISTS watches (id TEXT PRIMARY KEY, url TEXT, kind TEXT,
                  payload TEXT, deadline REAL, status TEXT, error TEXT DEFAULT '', created REAL);
            """)
        os.chmod(self.directory / "state.db", 0o600)

    @contextlib.contextmanager
    def db(self):
        conn = sqlite3.connect(self.directory / "state.db", timeout=15)
        conn.row_factory = sqlite3.Row
        try:
            with conn:
                yield conn
        finally:
            conn.close()

    def read_config(self):
        with self.lock:
            return json.loads(json.dumps(self.config))

    def save_config(self, change):
        with self.lock:
            c = self.read_config()
            for key in DEFAULTS:
                if key in change:
                    if key in ("github_token", "secret") and not change[key]:
                        continue  # Blank fields preserve credentials.
                    c[key] = change[key]
            if c["role"] not in ("sender", "receiver"):
                raise ValueError("角色无效")
            for name in ("github_token", "secret"):
                if not isinstance(c[name], str) or len(c[name]) > 4096 or any(x in c[name] for x in ("\r", "\n")):
                    raise ValueError("凭据格式无效")
            if c["repo"] and not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", c["repo"]):
                raise ValueError("仓库格式应为 owner/repository")
            if not re.fullmatch(r"[A-Za-z0-9_.-]{1,64}", c["channel"]):
                raise ValueError("通道只支持字母、数字、点、下划线和短横线")
            c["poll_seconds"] = int(c["poll_seconds"])
            if not 5 <= c["poll_seconds"] <= 3600:
                raise ValueError("轮询间隔须为 5–3600 秒")
            if not isinstance(c["enabled"], bool) or not isinstance(c["allowed_senders"], list):
                raise ValueError("配置格式无效")
            if any(not isinstance(s, str) for s in c["allowed_senders"]):
                raise ValueError("发送者须为 GitHub 用户名")
            if not isinstance(c["handlers"], dict):
                raise ValueError("操作映射须为 JSON 对象")
            for kind, handler in c["handlers"].items():
                if not re.fullmatch(r"[A-Za-z0-9_.-]{1,64}", kind) or not isinstance(handler, dict):
                    raise ValueError("操作映射格式无效")
                argv = handler.get("argv", [])
                if not isinstance(argv, list) or any(not isinstance(x, str) for x in argv):
                    raise ValueError("argv 须为字符串数组")
                if not 1 <= float(handler.get("timeout", 300)) <= 86400:
                    raise ValueError("操作超时须为 1–86400 秒")
            tmp = self.config_path.with_suffix(".tmp")
            fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
            with os.fdopen(fd, "w") as f:
                json.dump(c, f, ensure_ascii=False, indent=2)
            os.replace(tmp, self.config_path)
            self.config = c
            return c

    def enqueue(self, kind, payload, ttl=86400):
        c = self.read_config()
        if not c["secret"] or not c["repo"]:
            raise ValueError("请先配置仓库与共享密钥")
        if not re.fullmatch(r"[A-Za-z0-9_.-]{1,64}", kind) or not isinstance(payload, dict):
            raise ValueError("事件类型或内容无效")
        ttl = int(ttl)
        if not 60 <= ttl <= 604800:
            raise ValueError("事件有效期须为 60 秒至 7 天")
        wire = envelope(c["channel"], kind, payload, c["secret"], ttl)
        if len(canonical(wire).encode()) > 32000:
            raise ValueError("事件内容须小于 32 KB")
        self.insert(wire["event"], "out", canonical(wire), repo=c["repo"])
        return wire["event"]["id"]

    def insert(self, event, direction, wire="", issue_url="", repo=""):
        now = time.time()
        # Outbox pins its destination: later settings changes cannot redirect it.
        if direction == "out":
            wire = canonical({"repo": repo, "body": wire})
        with self.db() as db:
            db.execute("INSERT OR IGNORE INTO events(id,direction,kind,payload,wire,status,issue_url,created,updated) VALUES(?,?,?,?,?,?,?,?,?)",
                       (event["id"], direction, event["type"], canonical(event["payload"]), wire,
                        "pending", issue_url, now, now))


class GitHub:
    def __init__(self, config):
        self.config = config

    def request(self, method, path, data=None):
        token = (self.config["github_token"] or os.environ.get("GITHUB_TOKEN", "")).strip()
        if not token:
            raise ValueError("请配置 GitHub Token")
        req = urllib.request.Request("https://api.github.com" + path,
            data=json.dumps(data).encode() if data is not None else None, method=method,
            headers={"Authorization": "Bearer " + token, "Accept": "application/vnd.github+json",
                     "X-GitHub-Api-Version": "2026-03-10", "User-Agent": "network-relay/0.1",
                     "Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=20) as response:
                return json.loads(response.read(8 * 1024 * 1024))
        except urllib.error.HTTPError as e:
            raise RuntimeError(f"GitHub HTTP {e.code}；请检查网络、Token 权限或 API 限流") from None

    def test(self):
        info = self.request("GET", f"/repos/{self.config['repo']}")
        if not info.get("has_issues", True):
            raise ValueError("该仓库尚未启用 Issues")
        self.request("GET", f"/repos/{self.config['repo']}/issues?per_page=1&state=all")
        return info

    def publish(self, row):
        pinned = json.loads(row["wire"])
        wire = json.loads(pinned["body"])
        return self.request("POST", f"/repos/{pinned['repo']}/issues", {
            "title": f"[relay:{wire['event']['channel']}] {row['kind']} · {row['id'][:8]}",
            "body": PREFIX + canonical(wire)})

    def poll(self, after):
        rows = []
        for page in range(1, 101):
            query = urllib.parse.urlencode(dict(state="all", sort="created", direction="desc", per_page=100, page=page))
            batch = self.request("GET", f"/repos/{self.config['repo']}/issues?{query}")
            rows.extend(x for x in batch if x["number"] > after)
            if len(batch) < 100 or any(x["number"] <= after for x in batch):
                return sorted(rows, key=lambda x: x["number"])
        raise RuntimeError("待扫描记录超过 10000 条；请使用专用中转仓库")


class Agent:
    def __init__(self, store):
        self.store = store
        self.stop = threading.Event()
        self.message = "尚未连接"
        self.last_poll = None
        self.connected = False
        self.error = False

    def ingest(self, github, config):
        key = "cursor:" + config["repo"] + ":" + config["channel"]
        with self.store.db() as db:
            row = db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        cursor = int(row[0]) if row else 0
        issues = github.poll(cursor)
        # Store all fetched events and the watermark together: restart cannot lose a batch.
        with self.store.db() as db:
            for issue in issues:
                cursor = max(cursor, issue["number"])
                if "pull_request" in issue:
                    continue
                try:
                    event = verify(issue.get("body"), config, issue["user"]["login"])
                except (ValueError, KeyError, TypeError, AttributeError, RecursionError):
                    continue
                now = time.time()
                db.execute("INSERT OR IGNORE INTO events(id,direction,kind,payload,wire,status,issue_url,created,updated) VALUES(?,?,?,?,?,?,?,?,?)",
                           (event["id"], "in", event["type"], canonical(event["payload"]), canonical(event),
                            "pending", issue.get("html_url", ""), now, now))
            db.execute("INSERT OR REPLACE INTO meta VALUES(?,?)", (key, str(cursor)))

    def update(self, event_id, **fields):
        fields["updated"] = time.time()
        with self.store.db() as db:
            db.execute("UPDATE events SET " + ",".join(k + "=?" for k in fields) + " WHERE id=?", (*fields.values(), event_id))

    def dispatch(self, row, config):
        event = json.loads(row["wire"])
        if event["expires_at"] <= time.time():
            self.update(row["id"], status="expired", error="事件已过期")
            return
        handler = config["handlers"].get(row["kind"])
        if handler is None:
            self.update(row["id"], status="blocked", error="尚未配置此事件对应的操作")
            return
        self.update(row["id"], status="running", attempts=row["attempts"] + 1, error="")
        argv = handler.get("argv", [])
        if not argv:
            self.update(row["id"], status="done", output=canonical(event["payload"]))
            return
        env = dict(os.environ, RELAY_EVENT_ID=event["id"], RELAY_EVENT_TYPE=event["type"],
                   RELAY_PAYLOAD_JSON=canonical(event["payload"]))
        log_path = self.store.directory / (event["id"] + ".log")
        process = None
        try:
            with open(log_path, "ab", buffering=0) as out:
                process = subprocess.Popen(argv, stdin=subprocess.DEVNULL, stdout=out, stderr=subprocess.STDOUT,
                                           env=env, start_new_session=True)
                try:
                    code = process.wait(timeout=float(handler.get("timeout", 300)))
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait()
                    raise RuntimeError("操作执行超时") from None
            if code:
                raise RuntimeError(f"操作退出码 {code}")
            self.update(row["id"], status="done", output=self.tail(log_path))
        except Exception as e:
            # Do not automatically rerun side effects. An operator can explicitly retry.
            self.update(row["id"], status="failed", error=str(e), output=self.tail(log_path))

    @staticmethod
    def tail(path):
        if not path.exists():
            return ""
        with open(path, "rb") as f:
            f.seek(max(0, path.stat().st_size - 16000))
            return f.read().decode(errors="replace")

    def watches(self):
        with self.store.db() as db:
            rows = db.execute("SELECT * FROM watches WHERE status='waiting'").fetchall()
        for row in rows:
            status, error = "waiting", ""
            if row["deadline"] < time.time():
                status, error = "expired", "等待模型就绪超时"
            else:
                try:
                    with urllib.request.urlopen(row["url"], timeout=3) as r:
                        if r.status == 200:
                            # Stable watch ID makes recovery after enqueue idempotent.
                            c = self.store.read_config()
                            if not c['secret'] or not c['repo']:
                                raise ValueError('请先配置仓库与共享密钥')
                            wire = envelope(c['channel'], row['kind'], json.loads(row['payload']), c['secret'])
                            wire['event']['id'] = row['id']
                            wire['signature'] = hmac.new(c['secret'].encode(), canonical(wire['event']).encode(), hashlib.sha256).hexdigest()
                            self.store.insert(wire['event'], 'out', canonical(wire), repo=c['repo'])
                            status = "ready"
                except Exception:
                    error = "健康检查尚未返回 HTTP 200"
            with self.store.db() as db:
                db.execute("UPDATE watches SET status=?,error=? WHERE id=?", (status, error, row["id"]))

    def cycle(self):
        c = self.store.read_config()
        if not c["enabled"]:
            self.connected = False
            self.message = "已暂停"
            return
        github = GitHub(c)
        if c["role"] == "sender":
            self.watches()
            with self.store.db() as db:
                pending = db.execute("SELECT * FROM events WHERE direction='out' AND status='pending' AND next_try<=? ORDER BY created LIMIT 20", (time.time(),)).fetchall()
            for row in pending:
                if not self.store.read_config()["enabled"]:
                    break
                event = json.loads(json.loads(row["wire"])["body"])["event"]
                if event["expires_at"] <= time.time():
                    self.update(row["id"], status="expired", error="发送前事件已过期")
                    continue
                try:
                    result = github.publish(row)
                    self.update(row["id"], status="published", issue_url=result["html_url"], error="", attempts=row["attempts"] + 1)
                except Exception as e:
                    self.update(row["id"], attempts=row["attempts"] + 1, error=str(e), next_try=time.time() + min(300, 10 * 2 ** min(row["attempts"], 5)))
                    raise
            self.message = "发送队列运行中"
        else:
            self.ingest(github, c)
            self.message = "正在监听新事件"
            with self.store.db() as db:
                pending = db.execute("SELECT * FROM events WHERE direction='in' AND status='pending' ORDER BY created LIMIT 20").fetchall()
            for row in pending:
                if self.stop.is_set() or not self.store.read_config()["enabled"]:
                    break
                self.dispatch(row, c)
        self.last_poll = time.time()
        # A quiet sender has not necessarily contacted GitHub; connection test is separate.
        self.connected = c["role"] == "receiver"

    def run(self):
        # A crash after executing a command cannot prove its effects: require review.
        with self.store.db() as db:
            db.execute("UPDATE events SET status='uncertain',error='上次进程中断，请核对操作结果后重试' WHERE status='running'")
        while not self.stop.is_set():
            try:
                self.cycle()
                self.error = False
            except Exception as e:
                self.connected = False
                self.error = True
                self.message = str(e)
                LOG.warning("Agent: %s", e)
            self.stop.wait(self.store.read_config()["poll_seconds"])


def serve(store, agent, port):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt, *args):
            pass

        def reply(self, code, value, content_type="application/json; charset=utf-8"):
            raw = value if isinstance(value, bytes) else json.dumps(value, ensure_ascii=False).encode()
            self.send_response(code)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(raw)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()
            self.wfile.write(raw)

        def do_GET(self):
            if self.headers.get("Host", "") not in {f"127.0.0.1:{port}", f"localhost:{port}"}:
                return self.reply(403, {"error": "Host 不匹配"})
            if self.path in ("/", "/sender", "/receiver"):
                return self.reply(200, (HERE / "web/index.html").read_bytes(), "text/html; charset=utf-8")
            if self.path != "/api/state":
                return self.reply(404, {"error": "Not found"})
            c = store.read_config()
            c["has_token"] = bool(c.pop("github_token") or os.environ.get("GITHUB_TOKEN"))
            c["has_secret"] = bool(c.pop("secret"))
            with store.db() as db:
                rows = [dict(r) for r in db.execute("SELECT id,direction,kind,payload,status,attempts,issue_url,error,output,created,updated FROM events ORDER BY created DESC LIMIT 100")]
                counts = dict(db.execute("SELECT status,count(*) FROM events GROUP BY status").fetchall())
                watches = [dict(r) for r in db.execute("SELECT * FROM watches ORDER BY created DESC LIMIT 20")]
            return self.reply(200, dict(config=c, events=rows, watches=watches, counts=counts,
                agent=dict(message=agent.message, last_poll=agent.last_poll, connected=agent.connected, error=agent.error)))

        def do_POST(self):
            # Loopback only, plus Origin/Host validation to prevent browser cross-site writes.
            host = self.headers.get("Host", "")
            if host not in {f"127.0.0.1:{port}", f"localhost:{port}"}:
                return self.reply(403, {"error": "Host 不匹配"})
            origin = self.headers.get("Origin")
            if origin and origin not in {f"http://127.0.0.1:{port}", f"http://localhost:{port}"}:
                return self.reply(403, {"error": "Origin 不匹配"})
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if not 0 < length <= 100000:
                    raise ValueError("请求大小无效")
                data = json.loads(self.rfile.read(length))
                if self.path == "/api/config":
                    store.save_config(data)
                    result = {"ok": True}
                elif self.path == "/api/test":
                    c = store.read_config()
                    info = GitHub(c).test()
                    result = {"ok": True, "message": "仓库连接成功：" + info["full_name"]}
                elif self.path == "/api/send":
                    if store.read_config()["role"] != "sender":
                        raise ValueError("请在发送端发布事件")
                    result = {"id": store.enqueue(data["type"], data["payload"], data.get("ttl", 86400))}
                elif self.path == "/api/watch":
                    if store.read_config()["role"] != "sender":
                        raise ValueError("请在发送端添加监控")
                    parsed = urllib.parse.urlsplit(data["url"])
                    if parsed.scheme not in ("http", "https") or not parsed.hostname:
                        raise ValueError("健康检查地址须为 HTTP(S)")
                    timeout = int(data.get("timeout", 1800))
                    if not 5 <= timeout <= 86400 or not isinstance(data["payload"], dict):
                        raise ValueError("等待时长或事件内容无效")
                    if not re.fullmatch(r"[A-Za-z0-9_.-]{1,64}", data["type"]):
                        raise ValueError("事件类型无效")
                    watch_id = str(uuid.uuid4())
                    with store.db() as db:
                        db.execute("INSERT INTO watches(id,url,kind,payload,deadline,status,created) VALUES(?,?,?,?,?,'waiting',?)",
                            (watch_id, data["url"], data["type"], canonical(data["payload"]), time.time()+timeout, time.time()))
                    result = {"id": watch_id}
                elif self.path == "/api/retry":
                    with store.db() as db:
                        n = db.execute("UPDATE events SET status='pending',next_try=0,error='' WHERE id=? AND status IN ('failed','blocked','uncertain')", (data["id"],)).rowcount
                    result = {"ok": bool(n)}
                else:
                    return self.reply(404, {"error": "Not found"})
                self.reply(200, result)
            except Exception as e:
                self.reply(400, {"error": str(e)})

    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    LOG.info("Console: http://127.0.0.1:%s", port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        agent.stop.set()
        server.server_close()


def main():
    parser = argparse.ArgumentParser(description="跨内网事件桥：GitHub 中转 + 双端控制台")
    parser.add_argument("--data", default="./relay-data")
    parser.add_argument("--port", type=int, default=8787)
    parser.add_argument("--role", choices=("sender", "receiver"))
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    store = Store(args.data)
    if args.role:
        store.save_config({"role": args.role})
    # One worker per state directory; publishers use the local HTTP API.
    import fcntl
    lock = open(store.directory / "agent.lock", "w")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        parser.error("该数据目录已有运行中的 agent")
    agent = Agent(store)
    threading.Thread(target=agent.run, daemon=True).start()
    serve(store, agent, args.port)


if __name__ == "__main__":
    main()
