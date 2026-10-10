#!/usr/bin/env python3
"""Capture actual requests from all four SDKs and compare the v1 event contract."""
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
ROOT=Path(__file__).resolve().parents[1]
fixture=ROOT/'contract/event.json'
received=[]
class Capture(BaseHTTPRequestHandler):
 def do_POST(self):
  assert self.path=='/v1/events'
  assert self.headers['Authorization']=='Bearer contract-test'
  if self.headers.get('Transfer-Encoding')=='chunked':
   data=b''
   while True:
    size=int(self.rfile.readline().split(b';')[0],16)
    if not size:
     self.rfile.readline();break
    data+=self.rfile.read(size);self.rfile.read(2)
  else:data=self.rfile.read(int(self.headers['Content-Length']))
  received.extend(json.loads(data)['events'])
  self.send_response(200);self.end_headers();self.wfile.write(b'{"accepted":1}')
 def log_message(self,*args):pass
server=ThreadingHTTPServer(('127.0.0.1',0),Capture)
threading.Thread(target=server.serve_forever,daemon=True).start()
url=f'http://127.0.0.1:{server.server_port}'
node="import fs from 'node:fs';import {PragmaChange} from './node/index.js';const e=JSON.parse(fs.readFileSync(process.argv[2]));const c=new PragmaChange({url:process.argv[1],token:'contract-test',applicationId:e.application_id,allowedAttributes:['duration_ms']});c.record(e);await c.close();"
python="import json,sys;sys.path.insert(0,'python');from pragma_change import Client;e=json.load(open(sys.argv[2]));c=Client(sys.argv[1],'contract-test',e['application_id'],['duration_ms']);c.record(**e);c.close()"
commands=[['node','--input-type=module','-e',node,url,str(fixture)], [sys.executable,'-c',python,url,str(fixture)], [os.environ.get('DOTNET','dotnet'),'run','--project','contract/csharp/Contract.csproj','--',url,str(fixture)],['cargo','run','-q','-p','pragmachange','--example','contract','--',url,str(fixture)]]
try:
 expected=json.loads(fixture.read_text());del expected['attributes']['secret']
 for name,command in zip(['Node.js','Python','C#','Rust'],commands):
  before=len(received)
  subprocess.run(command,cwd=ROOT,check=True,timeout=120,stdout=subprocess.DEVNULL)
  assert len(received)==before+1,(name,'missing or duplicate request')
  assert received[-1]==expected,(name,'contract mismatch',received[-1])
  print(name,'v1 event parity and privacy filtering passed',flush=True)
finally:server.shutdown()
