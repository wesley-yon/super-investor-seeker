"""Export a pinned collection snapshot into loss-preserving, queryable SQLite.

Each normalized source record is stored whole. SQL views expose useful fields;
financial strings retain their precision, and joint owners never multiply rows.
This module performs no network requests and does not replace the source archive.
"""
from __future__ import annotations

import argparse
from collections import Counter, OrderedDict
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import tempfile
import time

from .audit_batches import file_hash, readonly
from .discovery_refresh import iso_date
from .document_index import bounded_gzip, MAX_DOCUMENT_BYTES
from .http import atomic_write
from .increment import frozen_hash, shard_path
from .inventory import canonical
from .locking import writer_lock
from .package import encode_row
from .parser import FILING_FIELDS, OWNER_FIELDS, ROW_FIELDS

ENTITIES = ('filing', 'owners', 'transactions', 'holdings', 'footnotes', 'signatures')
DATABASE = 'normalized.sqlite3'
MANIFEST = 'query-export.json'
MAX_OPEN_SHARDS = 8
RESERVE_BYTES = 2 * 1024 ** 3
COMMON = ('filing_date', 'source_url')
FINANCIAL = (*ROW_FIELDS, 'row_id', 'row_number', 'document_type', 'is_amendment',
             'issuer_cik', 'issuer_name', 'issuer_trading_symbol', 'period_of_report',
             'owner_names', 'owner_ciks', 'transaction_value', 'transaction_value_basis',
             'classification', 'market_scope')
DECIMAL_FIELDS = ('transaction_shares', 'transaction_price_per_share', 'transaction_value',
                  'reported_transaction_total_value', 'shares_owned_following', 'value_owned_following',
                  'conversion_or_exercise_price', 'underlying_security_shares', 'underlying_security_value')
VIEWS = {
    'filings': ('filing', (*FILING_FIELDS, 'parser_version', 'is_amendment', 'owner_names', 'owner_ciks',
                          'reporting_owner_count', 'transaction_count', 'holding_count', 'footnote_count', 'signature_count')),
    'owners': ('owners', (*OWNER_FIELDS, 'owner_id', 'owner_number')),
    'transactions': ('transactions', FINANCIAL),
    'holdings': ('holdings', FINANCIAL),
    'footnotes': ('footnotes', ('footnote_id', 'text')),
    'signatures': ('signatures', ('signature_number', 'signature_name', 'signature_date')),
}


def create_database(path):
    db = sqlite3.connect(path)
    db.execute('PRAGMA foreign_keys=ON')
    db.executescript('''
        PRAGMA journal_mode=DELETE;
        PRAGMA synchronous=FULL;
        CREATE TABLE documents(
            sequence INTEGER PRIMARY KEY, accession TEXT NOT NULL UNIQUE,
            filing_date TEXT NOT NULL, issuer_cik INTEGER, form TEXT NOT NULL,
            status TEXT NOT NULL, source_url TEXT NOT NULL,
            source_sha256 TEXT NOT NULL, parsed_sha256 TEXT NOT NULL,
            is_parsed INTEGER NOT NULL CHECK(is_parsed IN (0,1)),
            inventory_json TEXT NOT NULL CHECK(json_valid(inventory_json)),
            audit_json TEXT NOT NULL CHECK(json_valid(audit_json)),
            UNIQUE(sequence,accession));
        CREATE INDEX documents_issuer_date ON documents(issuer_cik,filing_date,accession);
        CREATE TABLE records(
            entity TEXT NOT NULL, document_order INTEGER NOT NULL,
            accession TEXT NOT NULL, ordinal INTEGER NOT NULL CHECK(ordinal>0),
            record_json TEXT NOT NULL CHECK(json_valid(record_json)),
            PRIMARY KEY(entity,document_order,ordinal), UNIQUE(entity,accession,ordinal),
            FOREIGN KEY(document_order,accession) REFERENCES documents(sequence,accession));
        CREATE TABLE export_metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL);
    ''')
    for name, (entity, fields) in VIEWS.items():
        expressions = ['accession', 'ordinal', 'record_json']
        for field in (*COMMON, *fields):
            if not re.fullmatch(r'[a-z][a-z0-9_]*', field):
                raise ValueError('Unsupported normalized query field')
            expressions.append(f'''json_extract(record_json,'$.{field}') AS "{field}"''')
        if entity in ('transactions', 'holdings'):
            expressions.append("json_extract(record_json,'$.table') AS table_kind")
        db.execute('CREATE VIEW ' + name + ' AS SELECT ' + ','.join(expressions)
                   + " FROM records WHERE entity='" + entity + "'")
    return db


def decoded_document(inventory, document):
    if (document is None or inventory['accession'] != document['accession']
            or inventory['source_url'] != document['source_url']
            or inventory['status'] != document['status']):
        raise ValueError('Selected document identity or collection status differs')
    decoded = {}
    for name in ('source', 'parsed'):
        body = bounded_gzip(document[name + '_gzip'], MAX_DOCUMENT_BYTES, document[name + '_bytes'])
        if hashlib.sha256(body).hexdigest() != document[name + '_sha256'] or document[name + '_sha256'] != inventory[name + '_sha256']:
            raise ValueError('Selected original or normalized document checksum differs')
        if name == 'parsed':
            decoded = json.loads(body)
    if decoded is None:
        if document['status'] != 'review':
            raise ValueError('Unparsed documents must remain explicitly in review')
        return None
    if (not isinstance(decoded, dict) or set(decoded) != set(ENTITIES)
            or not isinstance(decoded['filing'], dict)
            or any(not isinstance(decoded[key], list) for key in ENTITIES[1:])):
        raise ValueError('Unsupported normalized document structure')
    for key, field in (('owners', 'reporting_owner_count'), ('transactions', 'transaction_count'),
                       ('holdings', 'holding_count'), ('footnotes', 'footnote_count'), ('signatures', 'signature_count')):
        if decoded['filing'].get(field) != len(decoded[key]):
            raise ValueError('Normalized filing counts differ from the complete entity rows')
    for key in ENTITIES:
        for row in [decoded[key]] if key == 'filing' else decoded[key]:
            if (not isinstance(row, dict) or any(row.get(field) != inventory[field]
                    for field in ('accession', 'filing_date', 'source_url'))):
                raise ValueError('Normalized row identity or provenance differs')
            if key in ('transactions', 'holdings') and any(not isinstance(row.get(field), str) for field in DECIMAL_FIELDS):
                raise ValueError('Normalized financial values must remain exact source strings')
    return decoded


def readback(path, expected_counts, expected_hashes, document_count, parsed_count, document_hash):
    db = readonly(path)
    try:
        if db.execute('PRAGMA integrity_check').fetchone()[0] != 'ok' or list(db.execute('PRAGMA foreign_key_check')):
            raise ValueError('Queryable export database integrity failed')
        if tuple(db.execute('SELECT count(*),coalesce(sum(is_parsed),0) FROM documents').fetchone()) != (document_count, parsed_count):
            raise ValueError('Queryable export document membership differs')
        source_hash = hashlib.sha256()
        for row in db.execute('SELECT * FROM documents ORDER BY sequence'):
            source_hash.update(canonical(list(row)) + b'\n')
        if source_hash.hexdigest() != document_hash:
            raise ValueError('Queryable export document provenance differs')
        actual_counts = Counter(); hashes = {key: hashlib.sha256() for key in ENTITIES}
        for entity, accession, ordinal, payload in db.execute(
                'SELECT entity,accession,ordinal,record_json FROM records ORDER BY entity,document_order,ordinal'):
            if entity not in hashes:
                raise ValueError('Unsupported exported entity')
            row = json.loads(payload)
            hashes[entity].update(canonical([accession, ordinal, row]) + b'\n')
            actual_counts[entity] += 1
        if (any(actual_counts[key] != expected_counts[key] for key in ENTITIES)
                or {key: value.hexdigest() for key, value in hashes.items()} != expected_hashes):
            raise ValueError('Queryable export rows differ from every selected normalized source record')
    finally:
        db.close()


def build(root, inventory, inventory_pin, output, *, issuer_cik=None, filed_from='', filed_through=''):
    started = time.monotonic()
    root, inventory, output = Path(root).resolve(), Path(inventory).absolute(), Path(output).absolute()
    if (not isinstance(inventory_pin, str) or not re.fullmatch(r'[0-9a-f]{64}', inventory_pin)
            or inventory.is_symlink() or frozen_hash(inventory) != inventory_pin):
        raise ValueError('Provide the independently pinned frozen inventory file')
    if issuer_cik is not None and (type(issuer_cik) is not int or not 1 <= issuer_cik <= 9999999999):
        raise ValueError('Issuer CIK must be a positive integer of at most ten digits')
    source = readonly(inventory); shards = OrderedDict()
    try:
        scope = dict(source.execute("SELECT key,value FROM settings WHERE key IN ('window_start','window_end')"))
        start, through = iso_date(filed_from or scope['window_start']), iso_date(filed_through or scope['window_end'])
        if not scope['window_start'] <= start <= through <= scope['window_end']:
            raise ValueError('Export filing dates must stay inside the frozen inventory scope')
        conditions, params = ['filing_date>=?', 'filing_date<=?'], [start, through]
        if issuer_cik is not None:
            conditions.append('issuer_cik=?'); params.append(issuer_cik)
        where = ' AND '.join(conditions)
        queue = dict(source.execute('SELECT status,count(*) FROM filings WHERE ' + where + ' GROUP BY status', params))
        expected_documents = sum(queue.get(key, 0) for key in ('verified', 'review'))
        if output.exists() or output.is_symlink():
            raise ValueError('Queryable export requires a new output directory')
        output.parent.mkdir(parents=True, exist_ok=True)
        with writer_lock(output.parent / ('.' + output.name + '.query-export-lock')):
            if output.exists() or output.is_symlink():
                raise ValueError('Queryable export destination already exists')
            with tempfile.TemporaryDirectory(prefix='.' + output.name + '.building-', dir=output.parent) as directory:
                work = Path(directory); database = work / DATABASE
                db = create_database(database)
                count = parsed_count = 0; counts = Counter(); hashes = {key: hashlib.sha256() for key in ENTITIES}
                document_hash = hashlib.sha256()
                try:
                    rows = source.execute("SELECT * FROM filings WHERE status IN ('verified','review') AND " + where
                                          + ' ORDER BY filing_date,accession', params)
                    for row in rows:
                        item = dict(row); relative = item['shard']
                        if relative not in shards:
                            if len(shards) >= MAX_OPEN_SHARDS:
                                _, old = shards.popitem(last=False); old.close()
                            shards[relative] = readonly(shard_path(root, relative))
                        shards.move_to_end(relative)
                        document = shards[relative].execute('SELECT * FROM documents WHERE accession=?', (item['accession'],)).fetchone()
                        decoded = decoded_document(item, document)
                        if shutil.disk_usage(work).free < RESERVE_BYTES + 4 * document['parsed_bytes']:
                            raise ValueError('Insufficient free disk for the complete queryable export')
                        count += 1; parsed_count += decoded is not None
                        values = (
                            count, item['accession'], item['filing_date'], item['issuer_cik'], item['form'], item['status'],
                            item['source_url'], item['source_sha256'], item['parsed_sha256'], int(decoded is not None),
                            canonical(encode_row(item)).decode(), document['audit_json'])
                        db.execute('INSERT INTO documents VALUES(?,?,?,?,?,?,?,?,?,?,?,?)', values)
                        document_hash.update(canonical(values) + b'\n')
                        if decoded is not None:
                            for entity in ENTITIES:
                                records = [decoded[entity]] if entity == 'filing' else decoded[entity]
                                for ordinal, record in enumerate(records, 1):
                                    db.execute('INSERT INTO records VALUES(?,?,?,?,?)', (
                                        entity, count, item['accession'], ordinal, canonical(record).decode()))
                                    hashes[entity].update(canonical([item['accession'], ordinal, record]) + b'\n')
                                    counts[entity] += 1
                        if count % 1000 == 0:
                            db.commit()
                            print(json.dumps({'query_export_documents': count, 'selected_documents': expected_documents}), flush=True)
                    if count != expected_documents:
                        raise ValueError('Queryable export selected-document coverage differs')
                    metadata = {'query_export_schema': 1, 'inventory_file_sha256': inventory_pin, 'source_scope': scope,
                                'selection': {'issuer_cik': issuer_cik, 'filed_from': start, 'filed_through': through},
                                'matching_inventory_status_counts': queue, 'selected_documents': count,
                                'parsed_documents': parsed_count, 'unparsed_documents': count - parsed_count,
                                'entity_counts': {key: counts[key] for key in ENTITIES},
                                'entity_sha256': {key: value.hexdigest() for key, value in hashes.items()},
                                'document_provenance_sha256': document_hash.hexdigest(),
                                'financial_values_preserved_as_text': True, 'all_normalized_fields_retained': True,
                                'amendments_kept_as_separate_filings': True, 'joint_owners_do_not_multiply_rows': True,
                                'raw_originals_in_source_archive': True, 'complete_backfill': False}
                    db.execute('INSERT INTO export_metadata VALUES(?,?)', ('manifest', canonical(metadata).decode()))
                    db.commit()
                    db.execute('ANALYZE'); db.commit()
                finally:
                    db.close()
                readback(database, counts, metadata['entity_sha256'], count, parsed_count, metadata['document_provenance_sha256'])
                if frozen_hash(inventory) != inventory_pin:
                    raise ValueError('Frozen inventory changed during queryable export')
                result = {**metadata, 'database': {'file': DATABASE, 'bytes': database.stat().st_size, 'sha256': file_hash(database)},
                          'every_normalized_record_readback_verified': True, 'private_archive_uploaded': False,
                          'elapsed_seconds': round(time.monotonic() - started, 3)}
                atomic_write(work / MANIFEST, canonical(result))
                if output.exists() or output.is_symlink():
                    raise ValueError('Queryable export destination changed before publication')
                os.rename(work, output)
                return result
    finally:
        source.close()
        for db in shards.values():
            db.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--inventory-snapshot', type=Path, required=True)
    parser.add_argument('--inventory-sha256', required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--issuer-cik', type=int)
    parser.add_argument('--filed-from', default=''); parser.add_argument('--filed-through', default='')
    args = parser.parse_args()
    print(json.dumps(build(args.root, args.inventory_snapshot, args.inventory_sha256, args.output,
                           issuer_cik=args.issuer_cik, filed_from=args.filed_from, filed_through=args.filed_through)), flush=True)


if __name__ == '__main__':
    main()
