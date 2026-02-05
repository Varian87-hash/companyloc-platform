import unittest

from fastapi import HTTPException

from api.main import _parse_month, _validate_month_range


class ApiValidationTests(unittest.TestCase):
    def test_parse_month_ok(self):
        d = _parse_month("2026-02")
        self.assertEqual(d.year, 2026)
        self.assertEqual(d.month, 2)
        self.assertEqual(d.day, 1)

    def test_parse_month_bad(self):
        with self.assertRaises(HTTPException):
            _parse_month("2026/02")

    def test_month_range_bad(self):
        from_dt = _parse_month("2026-03")
        to_dt = _parse_month("2026-02")
        with self.assertRaises(HTTPException):
            _validate_month_range(from_dt, to_dt)


if __name__ == "__main__":
    unittest.main()
