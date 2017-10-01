
Query API
=======================

**SQLite datatable query**::

    {
        "type": "chemsearch",
        "tables": ["DRUGBANKFDA"],
        "resourceFile": "sdf_demo.sqlite3",
        "key": "id",
        "values": ["DB00189", "DB00193", "DB00203", "DB00865", "DB01143"]
    }

:Attributes:
    * **type**\ (*string*) - query task type
    * **tables**\ (*object*) - list of target resource table
    * **resourceFile**\ (*string*) - target SQLite db file name
    * **key**\ (*string*) - key
    * **values**\ (*object*) - list of values
    * **operator**\ (*string*) - query operator
    * **queryMol**\ (*object*) - query molecule object
        * **format**\ (*string*) - "dbid", "molfile" or "smiles"
        * **table**\ (*string*) - query resource table
        * **resourceFile**\ (*string*) - query SQLite db file name
        * **value**\ (*string*) - value
    * **params**\ (*object*) - optional parameters
        * **ignoreHs**\ (*bool*) - ignore hydrogens in substructure match
        * **measure**\ (*int*) - similarity measure
        * **threshold**\ (*int*) - similarity threshold
        * **diameter**\ (*int*) - MCS-DR diameter
        * **maxTreeSize**\ (*int*) - MCS-DR max tree size
        * **molSizeCutoff**\ (*int*) - molecule size cutoff
        * **timeout**\ (*float*) - timeout for MCS calculation



Response JSON format
-----------------------


**SQLite datatable response**::

    {
        "id": "hoge",
        "format": "datatable",
        "progress": 100
    }


:Attributes:
    * **id**\ (*string*) - datatable ID
    * **name**\ (*string*) - datatable name
    * **format**\ (*string*) - "datatable"
    * **query**\ (*object*) - query JSON object
    * **fields**\ (*object*) - list of fields
    * **records**\ (*object*) - list of record
    * **status**\ (*string*) - task status
    * **created**\ (*string*) - date computation job created
    * **resultCount**\ (*int*) - total number of result rows
    * **taskCount**\ (*int*) - total number of row tasks to be processed
    * **doneCount**\ (*int*) - total number of done tasks
    * **progress**\ (*float*) - doneCount / taskCount * 100

    * **responseDate**\ (*string*) - date of response sent from server
    * **execTime**\ (*float*) - execution time


* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
