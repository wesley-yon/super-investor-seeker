from __future__ import annotations

import unittest

import data_contract
from insider_contract import (
    INSIDER_CONTRACT_VERSION,
    InsiderContractError,
    canonical_decimal_string,
    classify_transaction_code,
)


class InsiderContractTests(unittest.TestCase):
    def test_private_contract_canonicalizes_exact_decimals_without_public_bump(
        self,
    ) -> None:
        self.assertEqual(1, INSIDER_CONTRACT_VERSION)
        self.assertEqual(5, data_contract.DATA_CONTRACT_VERSION)
        self.assertEqual("123.45", canonical_decimal_string("00123.4500"))
        self.assertEqual("0", canonical_decimal_string("-0.000"))
        self.assertIsNone(canonical_decimal_string(None))

    def test_unknown_transaction_code_is_preserved_and_never_defaults_to_ps(
        self,
    ) -> None:
        purchase = classify_transaction_code("P")
        unknown = classify_transaction_code("Q")

        self.assertEqual("purchase", purchase["normalized_category"])
        self.assertTrue(purchase["is_meaningful_ps"])
        self.assertEqual("Q", unknown["raw_code"])
        self.assertEqual("unknown", unknown["normalized_category"])
        self.assertFalse(unknown["is_meaningful_ps"])
        self.assertTrue(unknown["requires_review"])

    def test_decimal_canonicalization_rejects_unbounded_lexical_expansion(
        self,
    ) -> None:
        for value in ("1e100000", "0." + "1" * 1025):
            with self.subTest(value_length=len(value)):
                with self.assertRaisesRegex(
                    InsiderContractError,
                    "decimal",
                ):
                    canonical_decimal_string(value)


if __name__ == "__main__":
    unittest.main()
