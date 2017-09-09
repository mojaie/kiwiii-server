#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#


from kiwiii import tablecolumn as tc


def resource_format(data):
    for tbl in data["tables"]:
        if data["domain"] == "chemical":
            tc.add_calc_cols(tbl)
        tbl["domain"] = data["domain"]
        for col in tbl["columns"]:
            if "name" not in col:
                col["name"] = col["key"]
            if "dataColumn" not in col:
                col["dataColumn"] = col["key"]
            if "method" not in col:
                # if domain=chemical: method: chemsql
                col["method"] = "sql"
            col["visible"] = True


def screener_format(data):
    for rsrc in data["resources"]:
        rsrc["domain"] = data["domain"]
        rsrc["url"] = data["url"]
        rsrc["entity"] = "{}:{}".format(rsrc["qcsRefId"], rsrc["layerIndex"])
