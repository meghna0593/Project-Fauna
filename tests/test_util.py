from animals_etl.utils import split_friends, epoch_to_iso8601_utc, validate_iso8601_utc
from datetime import datetime, timezone

def test_split_friends_basic():
    assert split_friends("Dog, Kangaroo, Sea Lions") == ["Dog", "Kangaroo", "Sea Lions"]
    assert split_friends("") == []
    assert split_friends(None) == []

def test_epoch_conversions():
    # seconds
    assert epoch_to_iso8601_utc(0) == "1970-01-01T00:00:00Z"
    # milliseconds (1577836800000 -> 2020-01-01T00:00:00Z)
    assert epoch_to_iso8601_utc(1_577_836_800_000) == "2020-01-01T00:00:00Z"
    # microseconds
    assert epoch_to_iso8601_utc(1_577_836_800_000_000) == "2020-01-01T00:00:00Z"
    # nanoseconds
    assert epoch_to_iso8601_utc(1_577_836_800_000_000_000) == "2020-01-01T00:00:00Z"

def test_validate_iso():
    good = ["2020-01-01T00:00:00Z", "1999-12-31T23:59:59.123Z"]
    bad = ["2020-01-01T00:00:00+00:00", "2020-01-01", None]
    assert all(validate_iso8601_utc(s) for s in good)
    # None is allowed (field omitted), so test only non-Nones
    assert all(not validate_iso8601_utc(s) for s in bad if s is not None)
