import test from 'node:test';
import assert from 'node:assert/strict';
import {PragmaChange} from './index.js';
const event={actor:'user',action:'document.read',session_id:'s',attributes:{password:'SECRET',duration_ms:2}};
test('queue is bounded and sensitive fields are dropped',async()=>{const client=new PragmaChange({url:'http://127.0.0.1:1',token:'test',applicationId:'app',allowedAttributes:['duration_ms']});try{for(let i=0;i<1001;i++)client.record(event);assert.equal(client.health().queued,1000);assert.equal(client.health().dropped,1);assert.equal(client.queue[0].event.attributes.password,undefined);}finally{clearInterval(client.timer);}});
test('retries preserve event identity and expire',async()=>{const client=new PragmaChange({url:'http://127.0.0.1:1',token:'test',applicationId:'app'});try{client.record(event);const id=client.queue[0].event.event_id;client.request=async()=>{throw Error('outage');};await client.flush();assert.equal(client.queue[0].event.event_id,id);for(let i=0;i<4;i++)await client.flush();assert.equal(client.health().dropped,1);assert.equal(client.health().queued,0);}finally{clearInterval(client.timer);}});
test('unavailable decisions require verification',async()=>{const client=new PragmaChange({url:'http://127.0.0.1:1',token:'test',applicationId:'app'});try{assert.equal((await client.evaluate(event)).action,'check');}finally{await client.close();}});
