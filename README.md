# Debugging Tools
## Enable all verbose logs
Running `./logging.sh` will enable all verbose logs for the queries in the provider.
More details in the script itself.

## Querying the database
There are 2 different ways to easily query the database, `qc` and `contatcsproviderutils.sh`.

### QC usage
`qc` queries the deivce directly. For usage, append the query in single quotes after the command:

e.g.
```
qc/qc 'select * from raw_contacts'
```

or to get all the tables
```
qc/qc '.tables'
```
QC support SQLite language, but it might have some limitations working with complex nested queries.

### contactsproviderutils.sh usage

This script downlaods the database locally and logins into a local version. It is also possible to push any change back to the device.

* Add tools to path
    ```
    source contactsproviderutils.sh
    ```
* Pull `contacts2.db` and query:
    ```
    sqlite3-pull
    ```
    This will open a sql terminal with `rlwrap` which can be easily used for queries.
* Pull `contacts2.db` and query with a graphical interface:
    ```
    sqlitebrowser-pull
    ```
* Push local updates to the device:
    ```
    sqlite3-push
    ```

