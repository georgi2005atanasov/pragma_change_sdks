"""Server-side collection; pass identities from verified authentication context."""
from collections import deque
import json
import threading
import time
from urllib import request, error
from uuid import uuid4


class NoRedirect(request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs): return None


class Client:
    def __init__(self, url, token, application_id, allowed_attributes=()):
        self.url, self.token, self.application_id = url.rstrip('/'), token, application_id
        self.allowed = set(allowed_attributes)
        self.queue, self.lock, self.flush_lock = deque(), threading.Lock(), threading.Lock()
        self.sent, self.dropped = 0, 0
        self.closed = False
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self._worker, daemon=True); self.thread.start()

    def event(self, **values):
        event = dict(version=1, event_id=str(uuid4()), application_id=self.application_id,
                     resource=None, workflow_id=None, correlation_id=None, timestamp_ms=int(time.time()*1000),
                     outcome='succeeded', cohort='', sequence_start=False, sequence_number=None)
        event.update(values)
        if event['application_id'] != self.application_id or not all(event.get(k) for k in ('actor','action','session_id')):
            raise ValueError('authenticated actor, stable action and session_id are required')
        event['attributes'] = {k:v for k,v in event.get('attributes',{}).items() if k in self.allowed}
        if len(json.dumps(event).encode()) > 16384: raise ValueError('event exceeds 16 KiB')
        return event

    def record(self, **values):
        event = self.event(**values)
        with self.lock:
            if self.closed or len(self.queue) >= 1000: self.dropped += 1; return False
            self.queue.append((event, 0)); return True

    def _request(self, path, body):
        req = request.Request(self.url+path, data=json.dumps(body).encode(), headers={'Authorization':'Bearer '+self.token,'Content-Type':'application/json'})
        with request.build_opener(NoRedirect).open(req, timeout=2) as response:
            return json.loads(response.read(1024*1024))

    def flush(self):
        with self.flush_lock:
            with self.lock: batch = [self.queue.popleft() for _ in range(min(100,len(self.queue)))]
            if not batch: return
            try:
                self._request('/v1/events', {'events':[event for event,_ in batch]})
                with self.lock: self.sent += len(batch)
            except Exception as exc:
                permanent = isinstance(exc,error.HTTPError) and 400 <= exc.code < 500 and exc.code != 429
                with self.lock:
                    for event, attempt in reversed(batch):
                        if permanent or attempt >= 4 or len(self.queue)>=1000: self.dropped += 1
                        else: self.queue.appendleft((event,attempt+1))

    def evaluate(self, **values):
        try:
            result = self._request('/v1/evaluate', self.event(**{**values,'outcome':'attempted'}))
            if result.get('action') in ('allow','check','block'): return result
        except Exception: pass
        return dict(action='check', recommended_action='check', shadow=False, surprise=None, model_id=None, reasons=['behavior_service_unavailable'])

    def health(self):
        with self.lock: return dict(queued=len(self.queue),sent=self.sent,dropped=self.dropped)

    def _worker(self):
        while not self.stop.wait(1): self.flush()

    def close(self):
        with self.lock: self.closed = True
        self.stop.set(); self.thread.join(timeout=3)
        deadline = time.monotonic()+5
        while self.health()['queued'] and time.monotonic() < deadline: self.flush()
        with self.lock:
            self.dropped += len(self.queue); self.queue.clear()

    def asgi_middleware(self, app, resolve):
        """resolve(scope) returns server-authenticated context after upstream auth instrumentation."""
        async def middleware(scope, receive, send):
            if scope['type'] != 'http': return await app(scope,receive,send)
            started, status, failed = time.monotonic(), 500, False
            async def capture(message):
                nonlocal status
                if message['type']=='http.response.start': status=message['status']
                await send(message)
            try: await app(scope,receive,capture)
            except BaseException:
                failed = True; raise
            finally:
                try:
                    context=resolve(scope)
                    if context: self.record(**{**context,'outcome':'succeeded' if not failed and status<400 else 'failed','attributes':{**context.get('attributes',{}),'status_code':status,'duration_ms':(time.monotonic()-started)*1000}})
                except Exception:
                    with self.lock: self.dropped += 1
        return middleware

from .recording import DatasetRecorder
