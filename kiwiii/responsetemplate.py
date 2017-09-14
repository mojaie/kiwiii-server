#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#


from kiwiii import tablecolumn as tc


def response_template():
    return {
        "id": ssid,
        "format": "datatable",
        "columns": [
            {"key": "_index", "name": "Index", "sort": "numeric"}
        ]
        "records": []

    }


def structure_column():
    return {"key": "_structure", "name": "Structure", "sort": "numeric"}

def structure_column():
    return {"key": "_structure", "name": "Structure", "sort": "numeric"}
