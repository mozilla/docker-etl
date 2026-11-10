Copied from bigquery-etl's generated-sql branch at c65533248ce1df364545ffd9f300ea5a8ff493d2
(2026-10-15), which is public and MPL-2.0 licensed.

`sql/` holds the persistent UDFs the tests expand, laid out as in that branch so
`persistent_udfs()` reads it unchanged. `bqetl/` holds whole queries, named
`<dataset>.<table>.sql`. Add a file here when a test needs it; a copy that
falls behind upstream is fine, since the tests check the resolver, not
bigquery-etl. Redash, Looker and ad hoc query text never goes here; write a
synthetic query with the same pattern instead.
