#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#
"""List of dict (LOD) module"""


class ListOfDict(list):
    def values(self, key):
        """List of values"""
        return map(lambda x: x[key], self)

    def filter(self, key, value):
        """Filtered records by the given key-value pair"""
        return filter(lambda x: x[key] == value, self)

    def find(self, key, value):
        """Returns the record found by the given key-value pair.
        if not found, return None
        """
        return next(self.filter(key, value), None)

    def add(self, rcd, key="key", dupkey="replace"):
        """Adds a new record.
        Args:
            dupkey:
                replace - replace existing record with the new one (default)
                update - update exisiting record (dict.update)
                skip - add no record when the key is duplicated
                replace or update will be applied for only the first one found
        """
        for i, r in enumerate(self):
            if r[key] == rcd[key]:
                if dupkey == "skip":
                    return
                if dupkey == "update":
                    self[i].update(rcd)
                    return
                if dupkey == "replace":
                    self[i] = rcd
                    return
        self.append(rcd)

    def reduce(self, key="key", dupkey="update"):
        """Removes records with duplicated key
        Args:
            dupkey:
                update - update exisiting record (dict.update)
                skip - add no record when the key is duplicated
        """
        new = ListOfDict()
        for r in self:
            new.add(r, key=key, dupkey=dupkey)
        self.clear()
        self.extend(new)

    def unique(self, key="key"):
        """Aliase of reduce(dupkey="skip")"""
        return self.reduce(key, dupkey="skip")

    def merge(self, rcds, key="key", dupkey="skip"):
        """Adds records"""
        for r in rcds:
            self.add(r, key=key, dupkey=dupkey)

    def join(self, rcds, key, full_join=False):
        """Joins records"""
        idx = {}
        for L in self:
            idx[L[key]] = L
        for r in rcds:
            if r[key] in idx:
                idx[r[key]].update(r)
            elif full_join:
                self.append(r)


LOD = ListOfDict  # aliase
