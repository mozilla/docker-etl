# Vendored dashboard frontend (for local verification)

This is a copy of the Firefox Graphics Telemetry dashboard's static site, used
to eyeball the JSON this job produces before it ever reaches production. It is
the page served at <https://firefoxgraphics.github.io/telemetry/>.

## Provenance

Copied from <https://github.com/FirefoxGraphics/telemetry/tree/master/www> at
commit `bb0cc53a61a30acb22b3973f32f1721c572d02bc` (2023-11-13). MPL 2.0, same as
this job.

This is a point-in-time snapshot and will drift from upstream if it ever changes. It is only
here for local verification, not as the source of truth for the deployed site.

The only local change is in `chart-impl.js`: `USE_S3_FOR_CHART_DATA` is set to
`false` so the page loads chart data from the local `data/` directory instead of
the live GCS endpoint. Set it back to `true` to point at production data.

## Verifying a dry run

1. Generate the JSON into `data/` with a dry run (it reads BigQuery but writes
   locally):

   ```bash
   # from the job root (jobs/graphics-dashboard)
   python -m graphics_dashboard.dashboard --dry-run --test-output-dir frontend/data
   python -m graphics_dashboard.trends    --dry-run --test-output-dir frontend/data
   ```

   The generated `*.json` files in `data/` are git-ignored, so they won't be
   committed and go stale.

2. Serve this directory over HTTP and open it. A file:// URL will not work; the
   page fetches the JSON over AJAX, which browsers block for local files.

   ```bash
   python -m http.server -d frontend 8000
   # then open http://localhost:8000/
   ```

`layers-failureid-statistics.json` is intentionally not produced by this job
(see the top-level README), so the failure-id chart that depends on it will be
empty or partial. Everything else should render.
