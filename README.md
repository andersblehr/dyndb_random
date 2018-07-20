# dyndb_random

Generate random test data for AWS DynamoDB.

```text
Generate random test data for a DynamoDB table.

Usage:
  dyndb_random.py -t <table> [-n <num rows>] [-o <file>] [-d] [-c] [-x]

Options:
  -t: Generate data for DynamoDB table with name <table>. A value
      definitions file <table>.vdef.json must be present in the local
      directory or in $SHOTRUN_HOME/test/data/vdef.
  -n: Generate <num rows> rows of data (default: 10).
  -o: Write data to <file>.json (default: ./<table>[.<timestamp>].json).
  -d: Write data in DynamoDB's internal JSON format (default: plain JSON).
  -c: Also write data to CSV file (./<table>[.timestamp].csv), or to
      <file>.csv if the -o option has been given.
  -x: Do not link tables.
  -h: Show this help text.

Requires:
  loremipsum (http://pythonhosted.org/loremipsum/)
  python-geohash (https://github.com/hkwi/python-geohash)
```
