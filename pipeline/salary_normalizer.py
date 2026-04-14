import re
import pandas as pd
from typing import Optional

_HOURS_PER_YEAR = 2080   # 40 hrs/wk × 52 weeks

def _clean(text: str) -> str:
    """Strip HTML, collapse whitespace, lowercase."""
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _to_annual(value: float, is_hourly: bool) -> int:
    if is_hourly:
        return int(round(value * _HOURS_PER_YEAR))
    return int(round(value))


def _parse_num(raw: str) -> float:
    """
    Convert a raw matched number string to a float.
    Handles:  '120'  '120,000'  '120k'  '120K'
    """
    raw = raw.replace(",", "").strip()
    if raw.lower().endswith("k"):
        return float(raw[:-1]) * 1000
    return float(raw)

# Number fragment: digits, optional comma-groups, optional k/K suffix
_N = r"[\d,]+(?:\.\d+)?[kK]?"

_PATTERNS = [

    # $80k–$100k/hr   or   $80–$100/hr   (hourly range)
    (
        re.compile(
            rf"\$\s*({_N})\s*[-–—to]+\s*\$?\s*({_N})\s*/\s*h(?:r|our)?",
            re.IGNORECASE,
        ),
        "range_hourly",
    ),
    # $80k–$100k   80k-100k   $80,000–$100,000   80000-100000
    (
        re.compile(
            rf"\$?\s*({_N})\s*[-–—to]+\s*\$?\s*({_N})",
            re.IGNORECASE,
        ),
        "range_annual",
    ),

    # up to $95k/hr
    (
        re.compile(
            rf"up\s+to\s+\$?\s*({_N})\s*/\s*h(?:r|hour)?",
            re.IGNORECASE,
        ),
        "upto_hourly",
    ),
    # up to $95k   up to 95k/yr   up to $95,000
    (
        re.compile(
            rf"up\s+to\s+\$?\s*({_N})(?:\s*/\s*yr)?",
            re.IGNORECASE,
        ),
        "upto_annual",
    ),

    # $120k+   $120,000+
    (
        re.compile(rf"\$?\s*({_N})\s*\+\s*/\s*h(?:r|hour)?", re.IGNORECASE),
        "plus_hourly",
    ),
    (
        re.compile(rf"\$?\s*({_N})\+", re.IGNORECASE),
        "plus_annual",
    ),

    # $45/hr   $45/hour   $45 per hour
    (
        re.compile(
            rf"\$?\s*({_N})\s*(?:/\s*h(?:r|our)?|per\s+hour)",
            re.IGNORECASE,
        ),
        "single_hourly",
    ),

    # $120k   $120,000   120k
    (
        re.compile(rf"\$\s*({_N})", re.IGNORECASE),
        "single_annual",
    ),
]

# Words that indicate no real salary data
_NO_DATA_RE = re.compile(
    r"\b(competitive|negotiable|doe|tbd|tbc|commensurate|n/?a|"
    r"not\s+disclosed|undisclosed|attractive)\b",
    re.IGNORECASE,
)

# Sanity bounds — outside these → almost certainly a parsing error
_MIN_PLAUSIBLE = 15_000
_MAX_PLAUSIBLE = 2_000_000


def _plausible(value: int) -> bool:
    return _MIN_PLAUSIBLE <= value <= _MAX_PLAUSIBLE


def normalize_salary(
    text: Optional[str],
) -> tuple[Optional[int], Optional[int], Optional[str]]:
    """
    Parse a free-text salary string and return structured integers.

    Parameters
    ----------
    text : str | None
        Raw salary field from a job listing.

    Returns
    -------
    (salary_min, salary_max, salary_raw)
        salary_min  : int | None — lower bound, annualised USD
        salary_max  : int | None — upper bound, annualised USD
        salary_raw  : str | None — the matched substring (for audit trail)

    Notes
    -----
    - Both min and max are None when no parseable salary is found.
    - For "up to X", min is None and max is X.
    - For "X+",      min is X   and max is None.
    - For a single figure, both min and max equal that figure.
    - Hourly rates are annualised at 2,080 hrs/yr (40 hrs × 52 weeks).
    - Values outside [$15k, $2M] are discarded as parsing errors.
    """
    if not text or not isinstance(text, str):
        return None, None, None

    cleaned = _clean(text)

    # Fast exit for "no data" phrases
    if _NO_DATA_RE.search(cleaned):
        return None, None, None

    for pattern, kind in _PATTERNS:
        m = pattern.search(cleaned)
        if not m:
            continue

        raw_match = m.group(0)
        groups = m.groups()
        is_hourly = "hourly" in kind

        try:
            if kind in ("range_annual", "range_hourly"):
                lo = _to_annual(_parse_num(groups[0]), is_hourly)
                hi = _to_annual(_parse_num(groups[1]), is_hourly)
                sal_min, sal_max = min(lo, hi), max(lo, hi)

            elif kind in ("upto_annual", "upto_hourly"):
                sal_min = None
                sal_max = _to_annual(_parse_num(groups[0]), is_hourly)

            elif kind in ("plus_annual", "plus_hourly"):
                sal_min = _to_annual(_parse_num(groups[0]), is_hourly)
                sal_max = None

            elif kind in ("single_annual", "single_hourly"):
                val = _to_annual(_parse_num(groups[0]), is_hourly)
                sal_min = sal_max = val

            else:
                continue

        except (ValueError, IndexError):
            continue

        # Sanity check — discard implausible values
        if sal_min is not None and not _plausible(sal_min):
            continue
        if sal_max is not None and not _plausible(sal_max):
            continue

        return sal_min, sal_max, raw_match

    return None, None, None


def normalize_salary_series(series: "pd.Series") -> "pd.DataFrame":
    """
    Apply normalize_salary to a pandas Series of raw salary strings.

    Returns a DataFrame with columns: salary_min, salary_max, salary_raw.

    Example
    -------
        result = normalize_salary_series(df["salary_text"])
        df = pd.concat([df, result], axis=1)
    """
    parsed = series.apply(lambda x: pd.Series(
        normalize_salary(x),
        index=["salary_min", "salary_max", "salary_raw"],
    ))
    parsed["salary_min"] = pd.array(parsed["salary_min"], dtype="Int64")
    parsed["salary_max"] = pd.array(parsed["salary_max"], dtype="Int64")
    return parsed

if __name__ == "__main__":
    cases = [
        # (input,                        expected_min,  expected_max)
        ("$80k–$100k",                   80_000,        100_000),
        ("80k-100k",                     80_000,        100_000),
        ("$80,000–$100,000",             80_000,        100_000),
        ("80000-100000",                 80_000,        100_000),
        ("up to $95k/yr",                None,          95_000),
        ("up to 95k",                    None,          95_000),
        ("$120k+",                       120_000,       None),
        ("$120k",                        120_000,       120_000),
        ("$45/hr",                       93_600,        93_600),
        ("$45–$55/hr",                   93_600,        114_400),
        ("salary: $150,000 - $200,000",  150_000,       200_000),
        ("competitive salary",           None,          None),
        ("DOE",                          None,          None),
        ("",                             None,          None),
        (None,                           None,          None),
        ("$200k to $250k base",          200_000,       250_000),
        ("100k-120k USD",                100_000,       120_000),
        ("EUR 60k-80k",                  None,          None),   # EUR — no $ match
    ]

    passed = 0
    failed = 0
    print(f"{'Input':<35} {'Got min':>10} {'Got max':>10}  {'Raw match'}")
    print("-" * 75)
    for text, exp_min, exp_max in cases:
        got_min, got_max, raw = normalize_salary(text)
        ok = (got_min == exp_min) and (got_max == exp_max)
        status = "✓" if ok else "✗"
        if ok:
            passed += 1
        else:
            failed += 1
        display = str(text)[:34] if text else "(None)"
        print(
            f"{status} {display:<34} {str(got_min):>10} {str(got_max):>10}"
            f"  {raw or '—'}"
        )

    print("-" * 75)
    print(f"{passed}/{passed+failed} tests passed")
