# river-data-vaisala

**Sync service for the Vaisala viewLinc logger network.**

Polls the viewLinc REST API and pushes readings into
[river-data](https://github.com/RIVER-EPFL/river-data-api), one stream per viewLinc location,
incrementally from the cursor the server reports for each stream. Device status (battery,
signal, reachability) rides the same cycle and is emitted at most every
`STATUS_INTERVAL_SECONDS`, with repeated values collapsed. Locations are discovered on startup
and on a full sync triggered from the dashboard.

Built on [river-data-core](https://github.com/RIVER-EPFL/river-data-core), which handles
enrollment, heartbeats, cursors, retries and the commands sent from the dashboard.

## Run

```bash
docker build -t river-data-vaisala . && docker run --env-file .env river-data-vaisala
```

| Variable | Description | Default |
|----------|-------------|---------|
| `VAISALA_BASE_URL` | viewLinc REST base, ie. `https://host/rest/v1` | required |
| `VAISALA_BEARER_TOKEN` | viewLinc API token | required |
| `VAISALA_SKIP_TLS_VERIFY` | Accept the appliance's self-signed certificate | `false` |
| `MAX_HISTORY_DAYS` | Backfill window for a stream with no cursor | `90` |
| `STATUS_INTERVAL_SECONDS` | Minimum time between device status emissions | `1800` |
| `API_BASE_URL` | river-data API URL | required |
| `SERVICE_CLIENT_ID`, `SERVICE_CLIENT_SECRET` | Enrollment credentials, issued from the dashboard | required |
| `INSTANCE_ID` | Distinguishes instances of one service | `default` |
| `SYNC_INTERVAL_SECONDS` | Time between cycles, overridden by the dashboard when set there | `300` |

The remaining runner variables are in the
[river-data-core README](https://github.com/RIVER-EPFL/river-data-core#configuration).

Newly registered streams store readings straight away but belong to no site until an
administrator pairs them on the dashboard's Streams page, which backfills their history.

## License

MIT
