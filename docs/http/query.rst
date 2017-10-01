
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


**SDFile import query (POST)**::

:Attributes:
    * **contents**\ (*file*) - SDFile contents (binary)
    * **params**\ (*object*) - optional parameters


**SDFile import query (POST)**::

:Attributes:
    * **json**\ (*file*) - JSON datafile (binary, stringified)
    * **params**\ (*object*) - optional parameters
        * **fields**\ (*object*) - list of fields to be imported
        * **implh**\ (*bool*) - make hydrogens implicit or not
        * **recalc**\ (*bool*) - recalculate 2D coordinates or not


**Similarity network query (POST)**::

:Attributes:
    * **json**\ (*file*) - JSON datafile (binary, stringified)
    * **params**\ (*object*) - optional parameters


**Job result query (GET)**::

    {
        "id": "",
        "command": "abort"
    }

:Attributes:
    * **id**\ (*string*) - job ID
    * **command**\ (*string*) - command to the server task



* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
