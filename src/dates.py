"""Inclusive UTC publication cutoff, shared by API and public extraction."""
from datetime import datetime, time, timedelta, timezone
import re


def publication_time(value):
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
        return parsed.astimezone(timezone.utc) if parsed.tzinfo else None
    except (ValueError, OverflowError):
        return None


def resolve_cutoff(value, now=None):
    if value is None or value == '':
        return None
    if not isinstance(value, str):
        raise ValueError('publishedAfter must be a date (YYYY-MM-DD) or an age such as 2 days.')
    value = value.strip()
    if not value:
        return None
    age = re.fullmatch(r'([1-9][0-9]*)\s+days?', value, re.IGNORECASE)
    try:
        if age:
            today = (now or datetime.now(timezone.utc)).astimezone(timezone.utc).date()
            parsed = datetime.combine(today - timedelta(days=int(age[1]) - 1), time.min, timezone.utc)
        elif re.fullmatch(r'\d{4}-\d{2}-\d{2}', value):
            parsed = datetime.combine(datetime.strptime(value, '%Y-%m-%d').date(), time.min, timezone.utc)
        else:
            raise ValueError
    except (ValueError, OverflowError):
        raise ValueError('publishedAfter must be a valid date (YYYY-MM-DD) or a positive age such as 1 day or 2 days.') from None
    return parsed.isoformat().replace('+00:00', 'Z')


def matches_cutoff(value, cutoff):
    if cutoff is None:
        return True
    published = publication_time(value)
    return published is not None and published >= publication_time(cutoff)
