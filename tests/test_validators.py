import unittest
from datetime import date, datetime

from Tepilora._validators import coerce_date, validate_date, validate_date_range


class TestValidators(unittest.TestCase):
    def test_validate_date_valid(self) -> None:
        self.assertEqual(validate_date("2024-01-15"), "2024-01-15")

    def test_validate_date_invalid_format(self) -> None:
        with self.assertRaises(ValueError):
            validate_date("01/15/2024")

    def test_validate_date_invalid_month(self) -> None:
        with self.assertRaises(ValueError):
            validate_date("2024-13-01")

    def test_validate_date_range_valid(self) -> None:
        validate_date_range("2024-01-01", "2024-02-01")

    def test_validate_date_range_invalid(self) -> None:
        with self.assertRaises(ValueError):
            validate_date_range("2024-03-01", "2024-02-01")

    def test_validate_date_range_partial(self) -> None:
        validate_date_range("2024-01-01", None)
        validate_date_range(None, "2024-02-01")

    def test_coerce_date_from_datetime(self) -> None:
        self.assertEqual(coerce_date(datetime(2024, 1, 15, 8, 30, 0)), "2024-01-15")

    def test_coerce_date_from_date(self) -> None:
        self.assertEqual(coerce_date(date(2024, 1, 15)), "2024-01-15")

    def test_coerce_date_from_string(self) -> None:
        self.assertEqual(coerce_date("2024-01-15"), "2024-01-15")

    def test_coerce_date_none(self) -> None:
        self.assertIsNone(coerce_date(None))
