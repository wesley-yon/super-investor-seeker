"""Prepare, audit, and privately archive a bounded incremental maintenance run.

Preparation uses archive read access. Publication is a separate command so a
workflow can issue a fresh, narrowly scoped write token only after verification.
No schedule, mutable latest-checkpoint pointer, or completed-backfill claim is
created by this module.
"""
from __future__ import annotations

import argparse
import contextlib
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import re
import shutil
import sys
import time
from zoneinfo import ZoneInfo

from . import document_index, github_documents, github_increment_stage, github_document_index_stage
from . import maintenance_refresh, runner
from .audit_batches import file_hash, readonly
from .discovery_refresh import iso_date
from .github_chain import valid_hash, validate_locator
from .github_session import open_inventory
from .http import SecClient, atomic_write
from .increment import build as build_increment, frozen_hash, source_path, verify as verify_increment
from .inventory import canonical
from .inventory_archive import capture_snapshot
from .inventory_delta import schema, state

MANIFEST = 'prepared-maintenance.json'
MAX_FILINGS = 1000
MAX_SECONDS = 900


def completed_calendar_date(now=None):
    now = now or datetime.now(ZoneInfo('America/New_York'))
    return (now.astimezone(ZoneInfo('America/New_York')).date() - timedelta(days=1)).isoformat()


def step(work, name, operation):
    print(json.dumps({'maintenance_stage': name, 'free_bytes': shutil.disk_usage(work).free}), flush=True)
    directory = work / 'logs'; directory.mkdir(exist_ok=True)
    with (directory / (name + '.log')).open('w') as log, contextlib.redirect_stdout(log), contextlib.redirect_stderr(log):
        try:
            return operation()
        except Exception as error:
            atomic_write(work / 'failure.json', canonical({'stage': name, 'error_type': type(error).__name__,
                                                          'error': str(error), 'complete_backfill': False}))
            raise


def collection_selection(inventory, maximum, start, through, now=None):
    db = readonly(inventory)
    try:
        scope = dict(db.execute("SELECT key,value FROM settings WHERE key IN ('window_start','window_end')"))
        start = start or scope['window_start']
        if not scope['window_start'] <= start <= through <= scope['window_end']:
            raise ValueError('Collection dates must stay within the complete candidate scope')
        rows = [dict(row) for row in db.execute('''SELECT * FROM filings WHERE source_url IS NOT NULL
            AND filing_date>=? AND filing_date<=?
            AND (status IN ('pending','inflight') OR (status='retry' AND retry_after<=?))
            ORDER BY filing_date,accession LIMIT ?''', (start, through, time.time() if now is None else now, maximum))]
        if any(row['source_sha256'] or row['parsed_sha256'] for row in rows):
            raise ValueError('Recover interrupted committed originals before cloud collection')
        return rows
    finally:
        db.close()


def collection_sources(candidate, restored_parent, selected):
    """Retain inherited ZIPs needed by the bounded pending-document audit."""
    db = readonly(candidate / 'inventory.sqlite3')
    try:
        for key in sorted({row['bulk_source'] for row in selected if row['bulk_source']}):
            source = dict(db.execute('SELECT * FROM sources WHERE source_key=?', (key,)).fetchone())
            destination = source_path(candidate, 'sources', source)
            if not destination.exists():
                parent_path = source_path(restored_parent, 'sources', source)
                if (parent_path.is_symlink() or not parent_path.is_file()
                        or parent_path.stat().st_size != source['bytes'] or file_hash(parent_path) != source['sha256']):
                    raise ValueError('A pending-document bulk source must be recovered before collection')
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(parent_path, destination)
            if (destination.is_symlink() or destination.stat().st_size != source['bytes']
                    or file_hash(destination) != source['sha256']):
                raise ValueError('Pending-document source bytes differ from the target inventory')
    finally:
        db.close()


def prepare(output, tag, transport_pin, document_index_pin, *, client, through=None,
            collection_start='', max_filings=250, seconds=600, workers=2):
    through = iso_date(through or completed_calendar_date())
    if through > completed_calendar_date():
        raise ValueError('Cloud maintenance must end before the current New York calendar day')
    if collection_start:
        collection_start = iso_date(collection_start)
        if collection_start > through:
            raise ValueError('Collection start must not follow the cutoff')
    if (type(max_filings) is not int or not 1 <= max_filings <= MAX_FILINGS
            or type(seconds) is not int or not 1 <= seconds <= MAX_SECONDS
            or type(workers) is not int or not 1 <= workers <= 4 or not valid_hash(document_index_pin)):
        raise ValueError('Provide bounded collection limits and a pinned complete document index')
    locator = validate_locator({'layout': 'content_addressed', 'tag': tag, 'sha256': transport_pin})
    work = Path(output).absolute()
    if work.exists() or work.is_symlink():
        raise ValueError('Maintenance preparation requires a new workspace')
    work.mkdir(parents=True)
    started = time.monotonic()
    session = step(work, 'restore-inventory', lambda: open_inventory(tag, transport_pin, work / 'recovery'))
    parent = session.root / 'inventory.sqlite3'
    cached = step(work, 'load-complete-document-index',
                  lambda: github_documents.cache_index(session.chain, session.downloader, document_index_pin))
    document_index.verify_inventory(cached['index_path'], cached['manifest'], parent)
    plan = step(work, 'refresh-sec-sources', lambda: maintenance_refresh.prepare(
        parent, work / 'refresh', through, client=client, workers=workers))
    selected = collection_selection(work / 'refresh/inventory.sqlite3', max_filings, collection_start, through)
    accessions = sorted(row['accession'] for row in selected)
    selection_body = canonical(accessions); atomic_write(work / 'collection-accessions.json', selection_body)
    extra_sources = sorted({row['bulk_source'] for row in selected if row['bulk_source']} - set(plan['bulk_quarters']))
    recovered = step(work, 'recover-affected-originals-and-sources', lambda: session.recover_maintenance(
        work / 'refresh', plan['plan_sha256'], document_index_pin=document_index_pin,
        additional_source_quarters=extra_sources or None))
    candidate = work / 'candidate'
    refreshed = step(work, 'materialize-refreshed-candidate', lambda: maintenance_refresh.materialize(
        work / 'refresh', plan['plan_sha256'], session.root, candidate))
    collection_sources(candidate, session.root, selected)
    collected = step(work, 'collect-bounded-originals', lambda: runner.run(
        candidate, client, workers=workers, limit=max_filings, seconds=seconds,
        accessions=accessions, attempt_limit=max_filings))
    if collected['attempted_this_run'] > max_filings or collected['completed_this_run'] > len(accessions):
        raise ValueError('Collection exceeded its frozen work selection')
    target = work / 'target-inventory.sqlite3'
    step(work, 'freeze-collected-inventory', lambda: capture_snapshot(candidate, target))
    node = session.chain[-1]
    increment = step(work, 'audit-and-build-increment', lambda: build_increment(
        candidate, node['directory'] / node['manifest_name'], node['reference']['manifest_sha256'],
        parent, target, work / 'increment', workers=workers))
    indexed = step(work, 'extend-complete-document-index', lambda: document_index.build(
        work / 'increment/increment.json', increment['increment_manifest_sha256'], target, work / 'document-index',
        ancestors=[node['directory'] / node['manifest_name'] for node in session.chain],
        parent_index=cached['directory'] / document_index.MANIFEST, parent_index_pin=document_index_pin))
    session.check_parent()
    source_audit = json.loads((work / 'increment/source-audit.json').read_text())
    if (source_audit['semantic_sha256'] != increment['changed_source_audit_sha256']
            or source_audit['selected_documents'] != increment['changed_committed_documents']
            or any(source_audit['counts'].get(key, 0) for key in ('document_failure', 'original_xml_field_failure'))):
        raise ValueError('The complete changed-document source audit did not pass')
    db = readonly(target)
    try:
        # A small private readback selection accompanies the complete index.
        # It does not limit the source audit or archived changed-document set.
        touched = set(accessions) | set(json.loads((work / 'refresh/committed-accessions.json').read_text()))
        changed = sorted(a for a in touched if db.execute(
            "SELECT 1 FROM filings WHERE accession=? AND status IN ('verified','review')", (a,)).fetchone())
        if len(changed) != increment['changed_committed_documents']:
            raise ValueError('Archived documents differ from the refreshed and collected original union')
        sample = changed[:1000] or [row[0] for row in db.execute(
            "SELECT accession FROM filings WHERE status IN ('verified','review') ORDER BY accession LIMIT 1")]
        queue = dict(db.execute('SELECT status,count(*) FROM filings GROUP BY status'))
    finally:
        db.close()
    atomic_write(work / 'readback-accessions.json', canonical(sample))
    report = {'cloud_maintenance_preparation_schema': 1, 'maintenance_run_prepared': True,
              'parent_locator': locator, 'parent_document_index_sha256': document_index_pin,
              'target_scope': plan['target_scope'], 'collection_start': collection_start or plan['target_scope']['window_start'],
              'max_filings': max_filings, 'collection_seconds_limit': seconds, 'workers': workers,
              'collection_accessions_sha256': file_hash(work / 'collection-accessions.json'),
              'collection_selected_documents': len(accessions),
              'readback_accessions_sha256': file_hash(work / 'readback-accessions.json'),
              'refresh_plan_sha256': plan['plan_sha256'], 'refresh': plan,
              'originals_rechecked': refreshed['rechecked_committed_documents'], 'collection': collected,
              'archive_recovery': recovered, 'target_inventory_file_sha256': frozen_hash(target),
              'target_inventory_state_sha256': increment['target_inventory_state']['state_sha256'],
              'increment_manifest_sha256': increment['increment_manifest_sha256'],
              'document_index_sha256': indexed['document_index_manifest_sha256'],
              'changed_documents_archived': increment['changed_committed_documents'],
              'indexed_documents': indexed['committed_documents'], 'queue_counts': queue,
              'source_audit_semantic_sha256': source_audit['semantic_sha256'],
              'source_audit_counts': source_audit['counts'],
              'original_source_checks': source_audit['counts'].get('original_xml_fields_checked', 0),
              'financial_table_comparisons': source_audit['financial_table_comparisons'],
              'SEC_requests': client.requests, 'SEC_payload_bytes': client.download_bytes,
              'elapsed_seconds': round(time.monotonic() - started, 3),
              'private_archive_uploaded': False, 'current_cutoff_completeness_verified': False,
              'cloud_daily_maintenance_active': False, 'complete_backfill': False}
    atomic_write(work / MANIFEST, canonical(report))
    return {**report, 'prepared_manifest_sha256': file_hash(work / MANIFEST)}


def read_prepared(directory, pin):
    work = Path(directory).resolve(); path = work / MANIFEST
    if (not valid_hash(pin) or path.is_symlink() or not 0 < path.stat().st_size <= 2_000_000 or file_hash(path) != pin):
        raise ValueError('Prepared maintenance manifest checksum or bounds differ')
    report = json.loads(path.read_text())
    if (report.get('cloud_maintenance_preparation_schema') != 1 or report.get('maintenance_run_prepared') is not True
            or any(report.get(key) is not False for key in ('private_archive_uploaded', 'current_cutoff_completeness_verified',
                                                           'cloud_daily_maintenance_active', 'complete_backfill'))):
        raise ValueError('Unsupported prepared maintenance report')
    validate_locator(report['parent_locator'])
    target = work / 'target-inventory.sqlite3'
    if target.is_symlink() or frozen_hash(target) != report['target_inventory_file_sha256']:
        raise ValueError('Prepared maintenance inventory changed before publication')
    increment = verify_increment(work / 'increment', report['increment_manifest_sha256'])
    if (increment['target_inventory_state']['state_sha256'] != report['target_inventory_state_sha256']
            or increment['changed_committed_documents'] != report['changed_documents_archived']
            or increment['changed_source_audit_sha256'] != report['source_audit_semantic_sha256']):
        raise ValueError('Prepared maintenance checkpoint differs from the verified result')
    audit = json.loads((work / 'increment/source-audit.json').read_text())
    if (report['source_audit_counts'] != audit['counts']
            or report['financial_table_comparisons'] != audit['financial_table_comparisons']
            or report['original_source_checks'] != audit['counts'].get('original_xml_fields_checked', 0)):
        raise ValueError('Prepared maintenance metrics differ from the archived source audit')
    db = readonly(target)
    try:
        if state(db, schema(db)) != increment['target_inventory_state']:
            raise ValueError('Prepared maintenance inventory differs from every archived table row')
    finally:
        db.close()
    index = document_index.pinned_json(work / 'document-index' / document_index.MANIFEST, report['document_index_sha256'])
    if report['indexed_documents'] != index['committed_documents']:
        raise ValueError('Prepared maintenance index coverage differs')
    document_index.verify_inventory(work / 'document-index' / index['index_file']['file'], index, target)
    for name in ('collection', 'readback'):
        path = work / (name + '-accessions.json')
        if path.is_symlink() or not 0 < path.stat().st_size <= 2_000_000 or file_hash(path) != report[name + '_accessions_sha256']:
            raise ValueError('Prepared maintenance accession selection changed')
        values = json.loads(path.read_text())
        if (not isinstance(values, list) or len(values) > MAX_FILINGS
                or any(not isinstance(a, str) or not re.fullmatch(document_index.ACCESSION, a) for a in values)
                or values != sorted(set(values))):
            raise ValueError('Prepared maintenance accession selection has invalid members')
        if name == 'collection' and len(values) != report['collection_selected_documents']:
            raise ValueError('Prepared maintenance collection count differs')
    return work, report


def publish(directory, prepared_pin, bucket_tag):
    work, prepared = read_prepared(directory, prepared_pin)
    if not re.fullmatch(r'insider-archives-\d{6}-\d{3}', bucket_tag):
        raise ValueError('Publication needs an explicit insider archive bucket')
    # Refresh the normal dataset pointer immediately before publication. Other
    # dataset jobs may legitimately have advanced it during long preparation.
    latest = github_increment_stage.api('repos/' + github_increment_stage.REPOSITORY + '/releases/latest')
    if not latest['tag_name'].startswith('dataset-'):
        raise ValueError('The normal dataset release must remain selected')
    staged = step(work, 'stage-and-readback-increment', lambda: github_increment_stage.stage_increment(
        work / 'increment', work / 'publication/increment', prepared['increment_manifest_sha256'],
        prepared['parent_locator'], bucket_tag, latest['id']))
    selected = json.loads((work / 'readback-accessions.json').read_text())
    indexed = step(work, 'stage-and-readback-document-index', lambda: github_document_index_stage.stage_index(
        work / 'document-index', work / 'publication/index', prepared['document_index_sha256'], selected,
        work / 'target-inventory.sqlite3', bucket_tag, staged['transport_sha256'], staged['latest_dataset_release_after']))
    # Existing published buckets remain published. Newly created buckets remain
    # verified private drafts until explicitly published; never move latest.
    report = {'maintenance_checkpoint_archived': True, 'prepared_manifest_sha256': prepared_pin,
              'parent_locator': prepared['parent_locator'], 'locator': staged['locator'],
              'document_index_sha256': indexed['document_index_sha256'],
              'filing_selection_sha256': indexed['filing_selection_sha256'],
              'increment_manifest_sha256': prepared['increment_manifest_sha256'],
              'target_inventory_state_sha256': prepared['target_inventory_state_sha256'],
              'target_scope': prepared['target_scope'], 'collection_selected_documents': prepared['collection_selected_documents'],
              'collection_completed_documents': prepared['collection']['completed_this_run'],
              'collection_attempted_documents': prepared['collection']['attempted_this_run'],
              'collection_network_errors': prepared['collection']['network_errors_this_run'],
              'originals_rechecked': prepared['originals_rechecked'],
              'changed_documents_archived': prepared['changed_documents_archived'],
              'indexed_documents': prepared['indexed_documents'], 'queue_counts': prepared['queue_counts'],
              'original_source_checks': prepared['original_source_checks'],
              'source_audit_semantic_sha256': prepared['source_audit_semantic_sha256'],
              'source_audit_counts': prepared['source_audit_counts'],
              'financial_table_comparisons': prepared['financial_table_comparisons'],
              'SEC_requests': prepared['SEC_requests'], 'SEC_payload_bytes': prepared['SEC_payload_bytes'],
              'increment_assets_uploaded': staged['new_assets_uploaded'], 'index_assets_uploaded': indexed['new_assets_uploaded'],
              'bucket_draft': staged['draft'], 'independent_archive_and_index_readback_verified': True,
              'latest_dataset_release_before': latest['id'], 'latest_dataset_release_after': indexed['latest_dataset_release_after'],
              'latest_release_unchanged': latest['id'] == indexed['latest_dataset_release_after'],
              'current_cutoff_completeness_verified': False, 'cloud_daily_maintenance_active': False, 'complete_backfill': False}
    atomic_write(work / 'maintenance-publication.json', canonical(report))
    return report


def main():
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest='command', required=True)
    build = commands.add_parser('prepare')
    build.add_argument('--output', type=Path, required=True); build.add_argument('--tag', required=True)
    build.add_argument('--transport-sha256', required=True); build.add_argument('--document-index-sha256', required=True)
    build.add_argument('--through'); build.add_argument('--collection-start', default='')
    build.add_argument('--max-filings', type=int, default=250); build.add_argument('--seconds', type=int, default=600)
    build.add_argument('--workers', type=int, choices=range(1, 5), default=2)
    send = commands.add_parser('publish')
    send.add_argument('--work', type=Path, required=True); send.add_argument('--prepared-sha256', required=True)
    send.add_argument('--bucket-tag', required=True)
    args = parser.parse_args()
    try:
        if args.command == 'prepare':
            result = prepare(args.output, args.tag, args.transport_sha256, args.document_index_sha256,
                             client=SecClient(os.environ.get('SEC_USER_AGENT', '')), through=args.through,
                             collection_start=args.collection_start, max_filings=args.max_filings,
                             seconds=args.seconds, workers=args.workers)
            if os.environ.get('GITHUB_OUTPUT'):
                with open(os.environ['GITHUB_OUTPUT'], 'a') as output:
                    output.write('prepared_sha256=' + result['prepared_manifest_sha256'] + '\n')
        else:
            result = publish(args.work, args.prepared_sha256, args.bucket_tag)
        print(json.dumps(result), flush=True)
    except Exception as error:
        # Raw filing identifiers and parser errors stay in the private workspace,
        # not in the public code repository's Actions log.
        print(json.dumps({'maintenance_command_failed': args.command, 'error_type': type(error).__name__,
                          'complete_backfill': False}), flush=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
