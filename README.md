# data-warehouse-client

To run:
1. Clone the repo
1. In the repo's root dir, create a `db-credentials.json` file containing database credentials (substituting all `<VARS>`):
   ```
   {"user": "<USER>", "pass": "<PASSWORD>", "IP": "<IP>", "port": <PORT>}
   ```
1. Create an `output` dir in the repo's root dir
1. Run the sample_queries file via
   ```
   python sample_queries.py
   ```

A range of sample queries will be run against the database and the results will be stored in the `output` dir and printed to console.
