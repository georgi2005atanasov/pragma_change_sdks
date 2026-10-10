import { randomUUID } from 'node:crypto';
const unavailable = () => ({ action:'check', recommended_action:'check', shadow:false, surprise:null, model_id:null, reasons:['behavior_service_unavailable'] });
export class PragmaChange {
  constructor({url, token, applicationId, allowedAttributes=[]}) {
    this.url=url.replace(/\/$/,''); this.token=token; this.applicationId=applicationId;
    this.allowed=new Set(allowedAttributes); this.queue=[]; this.sent=0; this.dropped=0; this.flushing=null;this.closed=false;
    this.timer=setInterval(()=>{this.flush().catch(()=>{});},1000);this.timer.unref?.();
  }
  event(input) {
    const value={version:1,event_id:randomUUID(),application_id:this.applicationId,resource:null,workflow_id:null,correlation_id:null,timestamp_ms:Date.now(),outcome:'succeeded',cohort:'',sequence_start:false,sequence_number:null,...input};
    if(value.application_id!==this.applicationId || !value.actor || !value.action || !value.session_id) throw Error('Authenticated actor, stable action and session_id are required');
    value.attributes=Object.fromEntries(Object.entries(input.attributes||{}).filter(([k])=>this.allowed.has(k)));
    if(Buffer.byteLength(JSON.stringify(value))>16384) throw Error('Event exceeds 16 KiB');
    return value;
  }
  record(input) {
    const event=this.event(input);
    if(this.closed||this.queue.length>=1000){this.dropped++;return false;}
    this.queue.push({event,attempt:0});return true;
  }
  async request(path,body) {
    const response=await fetch(this.url+path,{method:'POST',headers:{Authorization:`Bearer ${this.token}`,'Content-Type':'application/json'},body:JSON.stringify(body),signal:AbortSignal.timeout(2000),redirect:'error'});
    if(!response.ok){const error=Error('Behavior request failed');error.status=response.status;throw error;}
    return response.json();
  }
  flush() {
    if(this.flushing)return this.flushing;
    this.flushing=this.flushBatch().finally(()=>{this.flushing=null;});return this.flushing;
  }
  async flushBatch() {
    const batch=this.queue.splice(0,100);if(!batch.length)return;
    try{await this.request('/v1/events',{events:batch.map(x=>x.event)});this.sent+=batch.length;}
    catch(error){const retry=[];for(const item of batch){item.attempt++;if(item.attempt>=5||(error.status>=400&&error.status<500&&error.status!==429))this.dropped++;else retry.push(item);}const combined=[...retry,...this.queue];this.dropped+=Math.max(0,combined.length-1000);this.queue=combined.slice(0,1000);}
  }
  async evaluate(input) {
    try {const result=await this.request('/v1/evaluate',this.event({...input,outcome:'attempted'}));return ['allow','check','block'].includes(result.action)?result:unavailable();} catch{return unavailable();}
  }
  health(){return {queued:this.queue.length,sent:this.sent,dropped:this.dropped};}
  async close(){
    this.closed=true;clearInterval(this.timer);
    const deadline=Date.now()+5000;
    do{await this.flush();}while(this.queue.length&&Date.now()<deadline);
    this.dropped+=this.queue.length;this.queue=[];
  }
  /** Express-compatible response observer. resolve(req) must use verified server-side identity. */
  middleware(resolve) {
    return (req,res,next)=>{const start=performance.now();let context;
      try{context=resolve(req);}catch{this.dropped++;next();return;}
      res.once('finish',()=>{try{if(context)this.record({...context,outcome:res.statusCode<400?'succeeded':'failed',attributes:{...context.attributes,status_code:res.statusCode,duration_ms:performance.now()-start}});}catch{this.dropped++;}});next();};
  }
}
export { DatasetRecorder } from './recording.js';
