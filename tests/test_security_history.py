"""Dated continuity is display metadata, never a position conversion."""
import copy
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import reviewed_ticker_map
from security_history import history_review, historical_security_keys, validate_history, REVIEW_SHA256
from scripts.refresh_security_history import refresh


class SecurityHistoryTests(unittest.TestCase):
    def test_all_reviewed_groups_have_ordered_exact_identities(self):
        document = history_review()
        self.assertEqual(45, len(document['groups']))
        self.assertEqual(46, sum(len(g['events']) for g in document['groups'].values()))
        self.assertEqual(46, len(historical_security_keys()))
        self.assertEqual(['74319X207', '74319X306', '74319X405'],
                         next(g['cusips'] for g in document['groups'].values() if g['ticker'] == 'PFSA'))

    def test_invalid_graphs_fail_closed(self):
        for mutate in [
            lambda d: d.update(schema_version=2),
            lambda d: next(iter(d['groups'].values()))['cusips'].reverse(),
            lambda d: next(iter(d['groups'].values()))['events'][0].update(effective_date='2027-01-01'),
            lambda d: next(iter(d['groups'].values()))['events'][0].update(sources=['javascript:alert(1)']),
            lambda d: next(iter(d['groups'].values())).update(instrument_type='CALL'),
        ]:
            document = copy.deepcopy(history_review())
            mutate(document)
            with self.assertRaises(ValueError):
                validate_history(document)

    def test_quote_guard_is_typed_and_raw_master_is_unchanged(self):
        records = {f'{cusip}|{kind}': {'price_lookup_allowed': True}
                   for cusip in ['38259P508', '02079K305'] for kind in ['EQUITY', 'CALL', 'PUT']}
        master = {'records': records}
        before = copy.deepcopy(master)
        with patch.object(reviewed_ticker_map, 'load_review', return_value=None):
            projected = reviewed_ticker_map.apply_review(master, Path('.'))
        self.assertEqual(before, master)
        for key, record in projected['records'].items():
            self.assertEqual(key != '38259P508|EQUITY', record['price_lookup_allowed'])

    def test_completion_is_written_only_after_success(self):
        pipeline = Mock()
        pipeline.load_state.return_value = {}
        pipeline.rebuild_registry_backed_outputs.side_effect = RuntimeError('failed')
        with self.assertRaises(RuntimeError):
            refresh(pipeline)
        pipeline.save_state.assert_not_called()
        pipeline.rebuild_registry_backed_outputs.side_effect = None
        refresh(pipeline)
        pipeline.rebuild_registry_backed_outputs.assert_called_with(preserve_position_economics=True)
        self.assertEqual(REVIEW_SHA256, pipeline.save_state.call_args.args[0]['security_history_review_sha256'])
        pipeline.rebuild_registry_backed_outputs.reset_mock()
        pipeline.load_state.return_value = {'security_history_review_sha256': REVIEW_SHA256}
        self.assertTrue(refresh(pipeline, pending_only=True)['already_applied'])
        pipeline.rebuild_registry_backed_outputs.assert_not_called()
