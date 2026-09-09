import copy
import unittest

from pipeline import issuer_display_symbols


class IssuerDisplaySymbolsTests(unittest.TestCase):
    def common(self, name='EXAMPLE INC', ticker='EXM'):
        return dict(name=name, label_source='sec_13f_list', security_kind='COMMON',
                    mapping_status='resolved', ticker_source='sec_ftd', ticker=ticker)

    def test_display_only_same_issuer_all_instruments(self):
        registry = {'123456100': self.common()}
        for number, kind in enumerate(['BOND', 'PREFERRED', 'WARRANT', 'RIGHT', 'UNIT']):
            registry[f'123456A{number}0'] = dict(name='EXAMPLE INC', label_source='sec_13f_list', security_kind=kind)
        before = copy.deepcopy(registry)
        self.assertEqual({key: 'EXM' for key in registry if key != '123456100'}, issuer_display_symbols(registry))
        self.assertEqual(before, registry)

    def test_reject_cross_issuer_prefix_name_ambiguity_and_untrusted(self):
        for change in ['prefix', 'name', 'ambiguous', 'untrusted', 'fund', 'historical']:
            with self.subTest(change=change):
                stock = self.common()
                registry = {'123456100': stock, '123456AB1': dict(name='EXAMPLE INC', label_source='sec_13f_list', security_kind='BOND')}
                if change == 'prefix':
                    registry['999999100'] = registry.pop('123456100')
                elif change == 'name':
                    stock['name'] = 'EXAMPLE FINANCE INC'
                elif change == 'ambiguous':
                    registry['123456200'] = self.common(ticker='EXM.B')
                elif change == 'untrusted':
                    stock['ticker_source'] = 'issuer_name_guess'
                elif change == 'fund':
                    stock['security_kind'] = 'ETF'
                else:
                    stock.update(mapping_status='unresolved', display_mappings={'EQUITY': dict(ticker='OLD', match_kind='exact_cusip', ticker_temporality='historical_only')})
                self.assertEqual({}, issuer_display_symbols(registry))
