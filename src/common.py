"""Shared casts and cutoffs for the IMDb prep job."""

import pyspark.sql.functions as F

# Release years before this are not movies. Roundhay Garden Scene is 1888;
# a few experimental titles sit just before that, so the floor is 1870.
CURRENT_YEAR = 2026
MIN_TITLE_YEAR = 1870
# A few titles are already dated 2031-2032. 2115 is not a release year.
MAX_START_YEAR = CURRENT_YEAR + 10
# Series sometimes carry a planned end a few years out (2027, 2028, ...).
MAX_END_YEAR = CURRENT_YEAR + 10
MIN_PERSON_YEAR = 1
# Logistics (2012) is a real 857-hour film, about 51420 minutes.
# Runtimes past 100000 minutes are not.
MAX_RUNTIME = 100000
MAX_SEASON = 300
MIN_EPISODE = 0
MAX_EPISODE = 100000
MAX_AGE = 125
# IMDb stores years BC as positive numbers, so 43 then 17 can mean 43 BC to AD 17.
BC_YEAR_CUTOFF = 1000

SERIES_TYPES = ("tvSeries", "tvMiniSeries")
TCONST_RE = r"^tt\d+$"
NCONST_RE = r"^nm\d+$"


def as_int(name):
    value = F.col(name)
    return F.when(value.rlike(r"^\d+$"), value.cast("int"))


def as_double(name):
    value = F.col(name)
    return F.when(value.rlike(r"^\d+(\.\d+)?$"), value.cast("double"))


def present_but_not_int(name):
    value = F.col(name)
    return value.isNotNull() & ~value.rlike(r"^\d+$")


def present_but_not_double(name):
    value = F.col(name)
    return value.isNotNull() & ~value.rlike(r"^\d+(\.\d+)?$")


def token_array(name):
    """Split a comma-separated IMDb list, drop blanks, keep first-seen order."""
    parsed = F.expr(
        "array_distinct(filter(transform(split("
        + name
        + ", ','), x -> trim(x)), x -> x <> ''))"
    )
    # size(null) is -1, size(empty) is 0. Both should stay null.
    return F.when(F.size(parsed) > 0, parsed)


def count_when(condition):
    return F.sum(F.when(condition, 1).otherwise(0))
