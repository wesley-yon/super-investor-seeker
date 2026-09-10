"""Stage adaptive recovery after one complete inventory restore.

One session owns one verified download cache and accepts one final recovery
selection. Discovery can run between these stages without publishing accession
lists or rereading the inventory archives. A failed recovery is terminal.
"""
from __future__ import annotations

from pathlib import Path
import shutil
import time

from . import github_chain as transport
from . import github_documents, github_sources
from .http import atomic_write
from .increment import frozen_hash
from .inventory import canonical


def open_inventory(tag, transport_pin, output, *, source_quarters=None):
    return transport._read_chain(tag, transport_pin, output, inventory_only=True,
                                 source_quarters=source_quarters, keep_session=True)


class InventorySession:
    def __init__(self, chain, downloader, output, report, started):
        self.chain, self.downloader, self.output = chain, downloader, Path(output)
        self.root = self.output / 'restored'
        self.report, self.started = dict(report), started
        self.inventory_hash = frozen_hash(self.root / 'inventory.sqlite3')
        self.used = False

    def check_parent(self):
        path = self.root / 'inventory.sqlite3'
        if (self.root.is_symlink() or path.is_symlink() or not path.is_file()
                or frozen_hash(path) != self.inventory_hash):
            raise ValueError('Staged recovery parent inventory changed after verification')

    def recover_refresh(self, directory, pin, *, document_index_pin=None):
        from .discovery_refresh import read_plan
        return self._recover_plan(directory, pin, document_index_pin, read_plan,
                                   'discovery_plan', 'local_discovery_plan')

    def recover_bulk_refresh(self, directory, pin, *, document_index_pin=None):
        from .bulk_refresh import read_plan
        return self._recover_plan(directory, pin, document_index_pin, read_plan,
                                   'bulk_refresh_plan', 'local_bulk_refresh_plan')

    def _recover_plan(self, directory, pin, document_index_pin, read_plan, prefix, selection_source):
        if self.used:
            raise ValueError('A staged recovery session accepts only one final selection')
        try:
            self.check_parent()
            _, plan, accessions = read_plan(directory, pin)
            if plan['parent_inventory_state'] != self.chain[-1]['manifest']['target_inventory_state']:
                raise ValueError('Discovery plan parent differs from the pinned recovery session')
            result = self.recover(accessions, document_index_pin=document_index_pin,
                                  source_quarters=plan['required_bulk_quarters'] or None)
        except Exception:
            self.used = True
            (self.output / 'cloud-verification.json').unlink(missing_ok=True)
            raise
        self.report = {**result, prefix + '_sha256': pin, prefix + '_parent_verified': True,
                       'filing_selection_source': selection_source}
        atomic_write(self.output / 'cloud-verification.json', canonical(self.report))
        return dict(self.report)

    def recover(self, accessions, *, document_index_pin=None, source_quarters=None):
        if self.used:
            raise ValueError('A staged recovery session accepts only one final selection')
        self.used = True
        # No final success artifact survives a failed adaptive stage.
        (self.output / 'cloud-verification.json').unlink()
        self.check_parent()
        if not isinstance(accessions, (list, tuple)):
            raise ValueError('Recovery accessions must be a bounded explicit list')
        if accessions:
            github_documents.local_selection(document_index_pin, accessions)
        elif document_index_pin is not None and not transport.valid_hash(document_index_pin):
            raise ValueError('Invalid optional document index checksum')
        quarters = self.report.get('source_quarters', [])
        if source_quarters is not None:
            quarters = sorted(set(quarters + github_sources.quarter_keys(source_quarters)))
        before_assets = len(self.downloader.downloaded)
        before_bytes = sum(self.downloader.planned[key][0] for key in self.downloader.downloaded)
        documents = github_documents.plan_local(self.chain, self.downloader, document_index_pin, accessions) if accessions else None
        sources = github_sources.plan(self.chain, self.downloader, quarters) if quarters else None
        remaining = sum(size for key, (size, _) in self.downloader.planned.items() if key not in self.downloader.downloaded)
        decoded = (4 * documents['decoded_bytes'] if documents else 0) + (sources['raw_bytes'] if sources else 0)
        if shutil.disk_usage(self.output).free < 2 * remaining + decoded + 2 * 1024**3:
            raise ValueError('Insufficient free disk for adaptive archive recovery')
        # Source reuse validates existing bytes and metadata before loading data.
        source_report = github_sources.restore(sources, self.downloader, self.root, reuse_verified=True) if sources else {}
        document_report = github_documents.restore(documents, self.downloader, self.root) if documents else {
            'selected_documents_verified': 0, 'selected_document_chunks': 0, 'includes_original_documents': False}
        self.check_parent()
        latest = transport.api('repos/' + transport.REPOSITORY + '/releases/latest')
        if not latest['tag_name'].startswith('dataset-'):
            raise ValueError('The normal dataset release pointer is not selected')
        total_bytes = sum(self.downloader.planned[key][0] for key in self.downloader.downloaded)
        self.report = {**self.report, **source_report, **document_report,
                       'adaptive_recovery_verified': True, 'parent_inventory_unchanged': True,
                       'inventory_archive_replay_count': 1, 'assets': len(self.downloader.downloaded),
                       'asset_bytes': total_bytes, 'recovery_added_assets': len(self.downloader.downloaded) - before_assets,
                       'recovery_added_asset_bytes': total_bytes - before_bytes,
                       'latest_dataset_release_after': latest['id'],
                       'latest_release_unchanged': self.report['latest_dataset_release_before'] == latest['id'],
                       'elapsed_seconds': round(time.monotonic() - self.started, 3),
                       'remaining_free_bytes': shutil.disk_usage(self.output).free}
        atomic_write(self.output / 'cloud-verification.json', canonical(self.report))
        return dict(self.report)
