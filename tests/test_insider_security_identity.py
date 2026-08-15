from __future__ import annotations

import unittest

from security_identity import (
    SECTION16_SECURITY_KEY_VERSION,
    normalize_sec_cik,
    normalize_section16_cik,
    normalize_section16_security_title,
    section16_owner_group_key,
    section16_security_class_key,
)


class Section16IdentityTests(unittest.TestCase):
    def test_owner_group_key_uses_sorted_canonical_ciks(self) -> None:
        expected = section16_owner_group_key(["1", "0000000002"])

        self.assertEqual(
            "5ae8275d38651a1299c45439edf220d2584c95603ed4ec6d9e2cdf645c775470",
            expected,
        )
        self.assertEqual(
            expected,
            section16_owner_group_key(["0000000002", "0000000001"]),
        )
        self.assertEqual("0000000001", normalize_sec_cik("1"))
        self.assertEqual("0000000001", normalize_section16_cik("1"))
        self.assertEqual(1, SECTION16_SECURITY_KEY_VERSION)
        self.assertRegex(expected, r"^[0-9a-f]{64}$")

    def test_security_class_key_preserves_share_class_and_derivative_grain(
        self,
    ) -> None:
        class_a = section16_security_class_key(
            "1",
            "  Class A   Common Stock ",
            is_derivative=False,
        )

        self.assertEqual(
            "CLASS A COMMON STOCK",
            normalize_section16_security_title("Class A Common Stock"),
        )
        self.assertEqual(
            "e1193f85ddf0defe7212df0e045dbcd09befceffbbbe6f2ce0b5c05be50cf9ca",
            class_a,
        )
        self.assertEqual(
            class_a,
            section16_security_class_key(
                "0000000001",
                "class a common stock",
                is_derivative=False,
            ),
        )
        self.assertNotEqual(
            class_a,
            section16_security_class_key(
                "1",
                "Class B Common Stock",
                is_derivative=False,
            ),
        )
        self.assertNotEqual(
            class_a,
            section16_security_class_key(
                "1",
                "Class A Common Stock",
                is_derivative=True,
            ),
        )


if __name__ == "__main__":
    unittest.main()
