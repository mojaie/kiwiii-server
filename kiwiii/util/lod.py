#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#
"""List of dict (LOD) utility"""

import functools


def values(key, lod):
    return map(lambda x: x[key], lod)


def find(key, value, lod):
    """Returns the record found by the given key-value pair.
    if not found, return None
    """
    return next(filter(lambda x: x[key] == value, lod), None)


def filter_(key, value, lod):
    """Yields filtered records by the given key-value pair.
    """
    return filter(lambda x: x[key] == value, lod)


def uniq_add(rcd, lod, key="key"):
    """Add a new record if the key is not exist"""
    if find(key, rcd[key], lod) is None:
        return lod + [rcd]
    return lod


def unique(lod, key="key"):
    """Skips records with duplicated key"""
    return functools.reduce(lambda x, y: uniq_add(y, x, key), lod, [])


def unique_added(lod, rcd, key="key"):
    return uniq_add(rcd, lod, key)


def extend(old, new, key="key"):
    """Update old LOD by the unique key (in-place)"""
    list(functools.reduce(lambda x, y: unique_added(x, y, key), new, old))


def record_updated(lod, rcd, key="key"):
    """Add a new record or overwrite when the key is exist"""
    lod_copy = lod[:]
    for i, r in enumerate(lod):
        if r[key] == rcd[key]:
            lod_copy[i].update(rcd)
            return lod_copy
    return lod + [rcd]


def update(old, new, key="key"):
    """Update old LOD by the unique key (in-place)"""
    list(functools.reduce(lambda x, y: record_updated(x, y, key), new, old))


def join(key, left, right, full_join=False):
    """Joins right LOD to the left LOD (in-place)"""
    idx = {}
    for L in left:
        idx[L[key]] = L
    for r in right:
        if r[key] in idx:
            idx[r[key]].update(r)
        elif full_join:
            left.append(r)
