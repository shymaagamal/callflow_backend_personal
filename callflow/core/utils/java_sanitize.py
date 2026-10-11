# callflow/core/utils/java_sanitize.py
"""
Safety net for "Can't pickle <java class '...'>" errors.

The real fix for THIS specific crash is attaching worker threads to the
JVM (see smartcare_query_runner.py) -- that's what actually stops
jaydebeapi from handing back raw boxed Java objects in the first place.

This sanitizer is a second layer of defense: anything that still slips
through (a code path that builds derived data straight from a cursor
result outside the query runner, a future addition, a different driver
quirk) gets caught here, right before the one place that actually breaks:
pickling, used by Django's cache.set() (and multiprocessing, if you ever
go there).
"""

import logging
import pandas as pd

logger = logging.getLogger(__name__)


def _is_java_object(value) -> bool:
    """jpype-boxed Java objects stringify their type as '<java ...>'."""
    try:
        return str(type(value)).startswith("<java")
    except Exception:
        return False


def _convert_scalar(value):
    if _is_java_object(value):
        try:
            converted = str(value)
            logger.debug(f"[java_sanitize] converted leftover Java object -> {converted!r}")
            return converted
        except Exception:
            return None
    return value


def sanitize_for_cache(obj):
    """
    Recursively walks dicts, lists/tuples, and pandas DataFrames, converting
    any leftover Java-boxed object into a plain Python str. Numeric Java
    types (Long, BigDecimal, etc.) should already be handled upstream by
    convert_java_types -- this only catches whatever isn't.

    Covers, for a DataFrame: cell values, column headers, AND the index --
    pickle has to serialize all three, and column headers in particular
    are an easy thing to miss (cursor.description column names can be raw
    Java String objects just as easily as cell values can).

    Call this immediately before anything gets pickled:
        result = sanitize_for_cache(result)
        cache.set(cache_key, result, timeout=timeout_seconds)
    """
    if isinstance(obj, pd.DataFrame):
        # Work on an explicit copy -- avoids SettingWithCopyWarning and
        # guarantees we're not silently mutating a view of some other
        # DataFrame that's still in use elsewhere.
        obj = obj.copy()
        obj.columns = [_convert_scalar(c) for c in obj.columns]
        obj.index = [_convert_scalar(i) for i in obj.index]
        for col in obj.columns:
            if obj[col].dtype == object:
                obj.loc[:, col] = obj[col].map(_convert_scalar)
        return obj

    if isinstance(obj, dict):
        return {sanitize_for_cache(k): sanitize_for_cache(v) for k, v in obj.items()}

    if isinstance(obj, (list, tuple)):
        return [sanitize_for_cache(v) for v in obj]

    return _convert_scalar(obj)