import test from 'node:test';
import assert from 'node:assert/strict';
import worker, {contentSecurityPolicy} from '../cloudflare/csp-worker.mjs';

const html='<html><head></head><body>Unmodified origin body</body></html>';
const realFetch=globalThis.fetch;
test.after(()=>{globalThis.fetch=realFetch;});

function origin(contentType='text/html; charset=utf-8', status=200) {
  const response=new Response(html,{status,headers:{'content-type':contentType,
    'etag':'"origin"','last-modified':'Sat, 05 Sep 2026 00:00:00 GMT',
    'cache-control':'public, max-age=600','x-frame-options':'SAMEORIGIN'}});
  globalThis.fetch=async()=>response;
  return response;
}

test('policy nonce is random per response and does not permit inline handlers or eval',async()=>{
  const seen=new Set();
  for(let i=0;i<5;i++){
    origin();
    const response=await worker.fetch(new Request('https://13f.wesleyyon.com/'),{CSP_MODE:'enforce'});
    const policy=response.headers.get('content-security-policy');
    const nonce=policy.match(/'nonce-([^']+)'/)[1];
    assert.equal(Buffer.from(nonce,'base64').length,16);
    seen.add(nonce);
    assert.match(policy,/script-src-attr 'none'/);
    assert.match(policy,/frame-ancestors 'self'/);
    assert.doesNotMatch(policy.split(';').find(x=>x.trim().startsWith('script-src ')),/unsafe-inline|unsafe-eval/);
    assert.equal(response.headers.get('cache-control'),'no-store');
    assert.equal(response.headers.get('etag'),null);
    assert.equal(response.headers.get('last-modified'),null);
    assert.equal(response.headers.get('x-frame-options'),'SAMEORIGIN');
    assert.equal(await response.text(),html);
  }
  assert.equal(seen.size,5);
});

test('defaults to report-only until the owner enables enforcement',async()=>{
  origin();
  const response=await worker.fetch(new Request('https://13f.wesleyyon.com/'));
  assert.equal(response.headers.get('content-security-policy'),null);
  assert.ok(response.headers.get('content-security-policy-report-only'));
});

test('leaves datasets, assets, challenges, other hosts and methods untouched',async()=>{
  for(const [url,type,status,method] of [
    ['https://13f.wesleyyon.com/data/x.json','application/json',200,'GET'],
    ['https://13f.wesleyyon.com/app.js','application/javascript',200,'GET'],
    ['https://13f.wesleyyon.com/','text/html',403,'GET'],
    ['https://elsewhere.example/','text/html',200,'GET'],
    ['http://13f.wesleyyon.com/','text/html',200,'GET'],
    ['https://13f.wesleyyon.com/','text/html',200,'POST'],
  ]){
    const original=origin(type,status);
    assert.equal(await worker.fetch(new Request(url,{method}),{CSP_MODE:'enforce'}),original);
  }
});

test('rejects malformed nonce input',()=>{
  assert.throws(()=>contentSecurityPolicy("x'; script-src *"));
});
