import assert from 'node:assert/strict';
import test from 'node:test';
import {auditEtfDescriptions} from '../scripts/audit_etf_descriptions.mjs';

function fixture() {
  return {index: {data_contract_version: 5, tickers: [
    {cusip:'78464A409',ticker:'SPYG',instrument_type:'EQUITY',stock_id:'78464A409'},
    {cusip:'78464A508',ticker:'SPYV',instrument_type:'EQUITY',stock_id:'78464A508'},
  ]}, labels: {data_contract_version: 5, fund_identities: ['78464A409','78464A508'],
    labels: {'78464A409':'SPDR SERIES TRUST — ETF','78464A508':'SPDR SERIES TRUST — ETF'},
    kinds: {'78464A409':'ETF','78464A508':'ETF'}, product_names: {},
    identity_history: {schema_version:1, reviewed_as_of:'2026-09-08', groups:{}}}};
}

test('rejects duplicated fallback descriptions using the actual search renderer', () => {
  const {index, labels} = fixture();
  const result = auditEtfDescriptions(index, labels);
  assert.equal(result.ok, false);
  assert.equal(result.collisions.length, 1);
  assert.deepEqual(result.collisions[0].map(entry => entry.ticker), ['SPYG','SPYV']);
});

test('accepts distinct full names and distinct SEC class fallbacks', () => {
  const {index, labels} = fixture();
  labels.product_names = {'78464A409':'SPDR Portfolio S&P 500 Growth ETF',
    '78464A508':'SPDR Portfolio S&P 500 Value ETF'};
  assert.equal(auditEtfDescriptions(index, labels).ok, true);
  labels.product_names['78464A508'] = labels.product_names['78464A409'];
  assert.equal(auditEtfDescriptions(index, labels).ok, false);
  labels.product_names = {};
  labels.labels = {'78464A409':'SPDR SERIES TRUST — S&P 500 GROWTH',
    '78464A508':'SPDR SERIES TRUST — S&P 500 VALUE'};
  assert.equal(auditEtfDescriptions(index, labels).ok, true);
});

test('includes exact display-only ETF identities while keeping options separate', () => {
  const {index, labels} = fixture();
  index.tickers[0].ticker = null;
  labels.reviewed_displays = {'78464A409|EQUITY': {
    ticker:'SPYG',match_kind:'exact_cusip',confidence_tier:'A'}};
  index.tickers.push({...index.tickers[1],instrument_type:'CALL',stock_id:'78464A508|CALL'});
  const result = auditEtfDescriptions(index, labels);
  assert.equal(result.etf_entries_checked, 2);
  assert.equal(result.ok, false);
});

test('rejects missing metadata instead of reporting an empty successful audit', () => {
  const {index, labels} = fixture();
  assert.throws(() => auditEtfDescriptions({...index,tickers:null}, labels), /lacks ticker entries/);
  assert.throws(() => auditEtfDescriptions(index, {...labels,product_names:null}), /unavailable or invalid/);
});
