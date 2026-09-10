"""SEC-only HTTP with one shared request clock and bounded responses."""
from __future__ import annotations
import hashlib
import json
import os
from pathlib import Path
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request


def atomic_write(path, body):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + f'.{os.getpid()}.{threading.get_ident()}.tmp')
    with temporary.open('wb') as stream:
        stream.write(body)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def check_url(url):
    p = urllib.parse.urlsplit(url)
    if (p.scheme != 'https' or p.hostname not in {'www.sec.gov', 'data.sec.gov', 'archives.sec.gov'}
            or p.username or p.password or p.port not in (None, 443)):
        raise ValueError('Only HTTPS SEC URLs are accepted')


class SecClient:
    def __init__(self, user_agent, interval=.4):
        if not re.search(r'[^\s@]+@[^\s@]+\.[^\s@]+', user_agent):
            raise ValueError('SEC_USER_AGENT must include an application name and contact email')
        self.user_agent = user_agent
        self.interval = max(.4, interval)
        self.lock = threading.Lock()
        self.next_request = 0.0
        self.requests = 0
        self.download_bytes = 0
        self.blocked = threading.Event()
        client = self

        class Redirect(urllib.request.HTTPRedirectHandler):
            def redirect_request(self, req, fp, code, msg, headers, newurl):
                check_url(newurl)
                client.pace()
                return super().redirect_request(req, fp, code, msg, headers, newurl)

        self.opener = urllib.request.build_opener(Redirect())

    def pace(self):
        with self.lock:
            if self.blocked.is_set():
                raise RuntimeError('SEC access circuit is paused after HTTP 403')
            time.sleep(max(0, self.next_request - time.monotonic()))
            self.next_request = time.monotonic() + self.interval
            self.requests += 1

    def get(self, url, max_bytes=150_000_000):
        check_url(url)
        for attempt in range(5):
            self.pace()
            try:
                request = urllib.request.Request(url, headers={
                    'User-Agent': self.user_agent, 'Accept-Encoding': 'identity'})
                with self.opener.open(request, timeout=90) as response:
                    body = response.read(max_bytes + 1)
                    if len(body) > max_bytes:
                        raise ValueError('SEC response exceeds configured size bound')
                    expected = response.headers.get('Content-Length')
                    if expected and int(expected) != len(body):
                        raise ValueError('Incomplete SEC response')
                with self.lock:
                    self.download_bytes += len(body)
                return body
            except urllib.error.HTTPError as exc:
                if exc.code == 403:
                    self.blocked.set()
                if exc.code not in {429, 500, 502, 503, 504} or attempt == 4:
                    raise
                delay = exc.headers.get('Retry-After', '')
                delay = min(300, max(5, float(delay))) if delay.isdigit() else 2 ** (attempt + 2)
                with self.lock:
                    self.next_request = max(self.next_request, time.monotonic() + delay)
            except (urllib.error.URLError, TimeoutError, ConnectionError):
                if attempt == 4:
                    raise
                time.sleep(2 ** (attempt + 1))
        raise RuntimeError('Retry exhaustion')

    def cached(self, url, directory, refresh=False):
        directory = Path(directory)
        stem = hashlib.sha256(url.encode()).hexdigest()
        path = directory / (stem + '.body')
        metadata_path = directory / (stem + '.json')
        if path.exists() and metadata_path.exists() and not refresh:
            body = path.read_bytes()
            meta = json.loads(metadata_path.read_text())
            if meta['url'] != url or meta['sha256'] != hashlib.sha256(body).hexdigest():
                raise ValueError('SEC discovery cache checksum mismatch')
            return body, meta
        body = self.get(url)
        meta = {'url': url, 'sha256': hashlib.sha256(body).hexdigest(), 'bytes': len(body),
                'retrieved_at_utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}
        atomic_write(path, body)
        atomic_write(metadata_path, json.dumps(meta, sort_keys=True).encode())
        return body, meta
