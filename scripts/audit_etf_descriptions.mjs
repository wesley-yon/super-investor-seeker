/** Audit the real search descriptions before a generated dataset is released. */
import fs from 'node:fs';
import path from 'node:path';
import vm from 'node:vm';
import {fileURLToPath, pathToFileURL} from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

export function auditEtfDescriptions(index, labels) {
  const app = fs.readFileSync(path.join(root, 'app.js'), 'utf8');
  const start = app.indexOf('// ---------- init ----------');
  const end = app.indexOf('// ---------- URL routing ----------', start);
  if (start < 0 || end < start) throw new Error('Cannot isolate search logic from browser startup');
  const logic = app.slice(0, start) + app.slice(end);
  const audit = `
    assertCompatibleDataContract(indexPayload, 'index.json');
    assertCompatibleDataContract(labelPayload, 'security_labels.json');
    assertRequiredSecurityMetadata(labelPayload, 'security_labels.json');
    if (!Array.isArray(indexPayload.tickers)) throw new Error('index.json lacks ticker entries');
    idx = indexPayload;
    securityLabels = normalizeSecurityTextMap(labelPayload.labels);
    securityKinds = normalizeSecurityKindPayload(labelPayload);
    securityProductNames = normalizeSecurityTextMap(labelPayload.product_names);
    securityFundIdentities = normalizeSecurityFundIdentityPayload(labelPayload);
    securityInstrumentNames = normalizeSecurityTextMap(labelPayload.instrument_names);
    securityIdentityHistory = normalizeIdentityHistory(labelPayload.identity_history);
    securityReviewedDisplays = normalizeReviewedDisplays(labelPayload.reviewed_displays);
    const eligible = idx.tickers.filter(entry => isCommonStockSearchEntry(entry)
      && securityKindForCusip(entry.cusip) === 'ETF' && tickerSearchSymbol(entry));
    const entries = dedupeVisuallyIdenticalTickerMatches(eligible, idx.tickers).map(entry => ({
      cusip: entry.cusip, ticker: tickerSearchSymbol(entry),
      description: searchResultDescription(entry),
      full_name: securityProductNameForCusip(entry.cusip),
    }));
    const grouped = new Map();
    for (const entry of entries) {
      const key = entry.description.trim().toLocaleLowerCase('en-US');
      if (!grouped.has(key)) grouped.set(key, []);
      grouped.get(key).push(entry);
    }
    const collisions = [...grouped.values()].filter(group =>
      new Set(group.map(entry => entry.ticker.toUpperCase())).size > 1);
    const missing = entries.filter(entry => !entry.description.trim());
    JSON.stringify({ok: !collisions.length && !missing.length,
      ticker_entries_checked: idx.tickers.length, etf_entries_checked: entries.length,
      full_names: entries.filter(entry => entry.full_name).length, collisions, missing, entries});
  `;
  return JSON.parse(vm.runInNewContext(logic + audit,
    {indexPayload: index, labelPayload: labels}, {timeout: 30000}));
}

if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  const dataDir = process.argv[2] ? path.resolve(process.argv[2]) : path.join(root, 'data');
  const result = auditEtfDescriptions(
    JSON.parse(fs.readFileSync(path.join(dataDir, 'index.json'), 'utf8')),
    JSON.parse(fs.readFileSync(path.join(dataDir, 'security_labels.json'), 'utf8')),
  );
  const {entries, ...summary} = result;
  console.log(JSON.stringify(summary));
  if (!result.ok) process.exitCode = 1;
}
