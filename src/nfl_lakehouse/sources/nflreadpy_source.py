from __future__ import annotations

from typing import Iterable, List, Callable, Any
import nflreadpy


def _as_list(seasons: Iterable[int] | int) -> List[int]:
    return [seasons] if isinstance(seasons, int) else list(seasons)


def _call_first(mod: Any, fn_names: Iterable[str], *args, **kwargs):
    """
    Call the first function that exists on the module.
    """
    for name in fn_names:
        fn = getattr(mod, name, None)
        if callable(fn):
            return fn(*args, **kwargs)
    raise AttributeError(
        f"nflreadpy: none of these functions were found: {list(fn_names)}. "
        f"Available funcs containing keywords: "
        f"{[n for n in dir(mod) if any(k in n.lower() for k in ['sched','roster','pbp'])]}"
    )


def load_schedules(seasons: Iterable[int] | int):
    seasons = _as_list(seasons)
    # nflreadpy naming can vary by version
    return _call_first(nflreadpy, ("load_schedules", "schedules"), seasons)


def load_rosters(seasons: Iterable[int] | int):
    seasons = _as_list(seasons)
    return _call_first(nflreadpy, ("load_rosters", "rosters"), seasons)


def load_pbp(seasons: Iterable[int] | int):
    seasons = _as_list(seasons)
    return _call_first(nflreadpy, ("load_pbp", "pbp", "load_pbp_data"), seasons)
