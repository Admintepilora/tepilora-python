import math
import os
import statistics
import unittest
from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Tuple

import Tepilora as T


def _env_flag(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _today() -> date:
    return date.today()


def _fmt(d: date) -> str:
    return d.isoformat()


def _extract_result(obj: Any) -> Any:
    if isinstance(obj, dict) and "result" in obj:
        return obj["result"]
    return obj


def _series_from_result_rows(rows: Any, value_key: Optional[str] = None) -> List[Tuple[str, float]]:
    if not isinstance(rows, list) or not rows:
        return []
    out: List[Tuple[str, float]] = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        d = r.get("D") or r.get("date")
        if not isinstance(d, str):
            continue
        if value_key is None:
            candidates = [k for k in r.keys() if k not in {"D", "date"}]
            if not candidates:
                continue
            k = candidates[0]
        else:
            k = value_key
        v = r.get(k)
        if isinstance(v, (int, float)) and math.isfinite(float(v)):
            out.append((d, float(v)))
    return out


def _median(values: List[float]) -> float:
    if not values:
        return float("nan")
    return statistics.median(values)


@unittest.skipUnless(_env_flag("TEPILORA_E2E"), "set TEPILORA_E2E=1 to run end-to-end analytics validation")
class TestAnalyticsQuantE2E(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        api_key = os.getenv("TEPILORA_API_KEY")
        if not api_key:
            raise unittest.SkipTest("TEPILORA_API_KEY not set")

        base_url = os.getenv("TEPILORA_BASE_URL") or "http://testserver"
        cls.client = T.TepiloraClient(api_key=api_key, base_url=base_url, timeout=60.0)

        cls.asset = "IE00B4L5Y983EURXMIL"  # iShares Core MSCI World UCITS ETF (Acc)
        cls.bench = "IE00B5BMR087EURXMIL"  # iShares Core S&P 500 UCITS ETF (Acc)

        # 3Y window: enough data for most rolling stats, still reasonably fast.
        end = _today()
        start = end - timedelta(days=365 * 3)
        cls.start_date = _fmt(start)
        cls.end_date = _fmt(end)

    def test_core_consistency_single(self) -> None:
        c = self.client
        tc = self.asset

        rets = c.analytics.returns(identifiers=tc, start_date=self.start_date, end_date=self.end_date)
        log_rets = c.analytics.log_returns(identifiers=tc, start_date=self.start_date, end_date=self.end_date)

        rets_rows = _extract_result(rets)
        log_rows = _extract_result(log_rets)
        self.assertIsInstance(rets_rows, list)
        self.assertIsInstance(log_rows, list)

        rets_series = _series_from_result_rows(rets_rows, value_key=tc)
        log_series = _series_from_result_rows(log_rows, value_key=tc)
        self.assertGreater(len(rets_series), 200)
        self.assertGreater(len(log_series), 200)

        # Align by date and verify log(1+ret) ~= log_ret for a sample.
        log_map = {d: v for d, v in log_series}
        pairs = []
        for d, r in rets_series:
            if d in log_map and r > -0.999999:
                pairs.append((r, log_map[d]))
        self.assertGreater(len(pairs), 200)
        sample = pairs[-200:]
        max_err = 0.0
        for r, lr in sample:
            err = abs(math.log1p(r) - lr)
            max_err = max(max_err, err)
        self.assertLess(max_err, 1e-6)

        vol = c.analytics.rolling_volatility(identifiers=tc, start_date=self.start_date, end_date=self.end_date, Period=60)
        var = c.analytics.rolling_variance(identifiers=tc, start_date=self.start_date, end_date=self.end_date, Period=60)
        vol_series = _series_from_result_rows(_extract_result(vol), value_key=tc)
        var_series = _series_from_result_rows(_extract_result(var), value_key=tc)
        self.assertGreater(len(vol_series), 100)
        self.assertEqual(len(vol_series), len(var_series))

        # variance ~= volatility^2 (sample last 200 points)
        pairs2 = [(v, w) for (_, v), (_, w) in zip(vol_series[-200:], var_series[-200:])]
        max_rel = 0.0
        for v, w in pairs2:
            if v < 0 or w < 0:
                self.fail("volatility/variance must be non-negative")
            denom = max(1e-12, w)
            max_rel = max(max_rel, abs((v * v) - w) / denom)
        self.assertLess(max_rel, 1e-3)

        dd = c.analytics.drawdown(identifiers=tc, start_date=self.start_date, end_date=self.end_date)
        dd_series = _series_from_result_rows(_extract_result(dd), value_key=tc)
        self.assertGreater(len(dd_series), 200)
        self.assertLessEqual(max(v for _, v in dd_series[-500:]), 1e-9)

        mdd = c.analytics.max_drawdown(identifiers=tc, start_date=self.start_date, end_date=self.end_date)
        mdd_rows = _extract_result(mdd)
        self.assertIsInstance(mdd_rows, list)
        self.assertGreaterEqual(len(mdd_rows), 1)
        # The API returns a single row with [identifier, D]
        row = mdd_rows[0]
        self.assertIn(tc, row)
        self.assertIn("D", row)
        mdd_value = float(row[tc])
        self.assertLessEqual(mdd_value, 1e-9)
        self.assertAlmostEqual(mdd_value, min(v for _, v in dd_series), places=4)

    def test_core_consistency_multi(self) -> None:
        c = self.client
        asset = self.asset
        bench = self.bench

        beta = c.analytics.rolling_beta(
            identifiers=[asset, bench],
            Obs=252,
            Period=60,
            start_date=self.start_date,
            end_date=self.end_date,
        )
        beta_series = _series_from_result_rows(_extract_result(beta), value_key="beta")
        self.assertGreater(len(beta_series), 50)
        betas = [v for _, v in beta_series]
        self.assertTrue(all(math.isfinite(x) for x in betas))
        # Equity beta should be positive and not insane (loose bounds).
        self.assertGreater(_median(betas[-200:]), 0.0)
        self.assertLess(_median(betas[-200:]), 5.0)

        te = c.analytics.tracking_error(
            identifiers=[asset, bench],
            Obs=252,
            Period=60,
            start_date=self.start_date,
            end_date=self.end_date,
        )
        te_series = _series_from_result_rows(_extract_result(te), value_key="tracking_error")
        self.assertGreater(len(te_series), 50)
        self.assertGreaterEqual(min(v for _, v in te_series[-200:]), -1e-12)

        rs = c.analytics.relative_strength(
            identifiers=[asset, bench],
            Obs=252,
            Period=60,
            start_date=self.start_date,
            end_date=self.end_date,
        )
        rs_series = _series_from_result_rows(_extract_result(rs), value_key="relative_strength")
        self.assertGreater(len(rs_series), 50)
        self.assertTrue(all(v > 0 for _, v in rs_series[-200:]))

    def test_all_functions_smoke_and_sanity(self) -> None:
        """
        Full-coverage smoke: call every function with real identifiers and
        basic parameter completion based on analytics.info.

        This test is meant to catch runtime errors and obvious numeric issues.
        """
        c = self.client
        listing = c.analytics.list()
        funcs = listing.get("functions", [])
        self.assertIsInstance(funcs, list)
        self.assertGreaterEqual(len(funcs), 50)

        failures: List[Tuple[str, str]] = []

        for fn in funcs:
            if not isinstance(fn, str):
                continue
            info = c.analytics.info(fn)
            category = info.get("category")
            params: Dict[str, Any] = {}

            # Common filters (if supported)
            params["start_date"] = self.start_date
            params["end_date"] = self.end_date

            if category == "multi":
                params["identifiers"] = [self.asset, self.bench]
                params["Obs"] = 252
            else:
                params["identifiers"] = self.asset

            # If function defines Period, set a moderate window for speed.
            schema = c.analytics.schema(fn)
            all_specs = []
            if isinstance(schema, dict):
                all_specs = (schema.get("common") or []) + (schema.get("specific") or [])
            if any(isinstance(p, dict) and p.get("name") == "Period" for p in all_specs):
                params["Period"] = 60

            # Common multi regression knobs
            if any(isinstance(p, dict) and p.get("name") == "y" for p in all_specs):
                params["y"] = self.asset
            if any(isinstance(p, dict) and p.get("name") == "x" for p in all_specs):
                params["x"] = self.bench

            try:
                res = getattr(c.analytics, fn)(strict=True, **params)
            except Exception as e:
                failures.append((fn, f"{type(e).__name__}: {str(e)[:200]}"))
                continue

            payload = _extract_result(res)
            if payload is None:
                failures.append((fn, "empty result"))
                continue

            if isinstance(payload, list) and payload:
                # numeric sanity: at least one finite numeric in last row
                last = payload[-1]
                if isinstance(last, dict):
                    nums = [v for v in last.values() if isinstance(v, (int, float))]
                    if nums and not any(math.isfinite(float(v)) for v in nums):
                        failures.append((fn, "no finite numeric values in last row"))
            elif isinstance(payload, dict):
                # ok
                pass
            else:
                # allow scalar number/boolean/string
                if isinstance(payload, (int, float)):
                    if not math.isfinite(float(payload)):
                        failures.append((fn, "non-finite scalar"))

        if failures:
            msg = "\n".join([f"- {fn}: {err}" for fn, err in failures])
            self.fail(f"Analytics failures ({len(failures)}):\n{msg}")

