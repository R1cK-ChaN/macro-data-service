# Development Workflow

This workflow keeps local development in the dev/read-only client lane and
ships writer work through the VPS.

## Local setup

Create `~/.macro-data/dev.env` with the production API endpoint and token:

```bash
ANALYST_MACRO_DATA_BASE_URL=https://<prod-api-host>
ANALYST_MACRO_DATA_API_TOKEN=<token>
```

`MACRO_DATA_PROFILE` defaults to `dev`. Read checks use the prod API:

```bash
macro-data-service resolve CPI_US --json
```

Local writer commands require the explicit debug flag:

```bash
macro-data-service refresh-source --source fred --allow-local-write --db-path /tmp/macro-data-dev.db
```

Use that path for fixture/debug runs with a temporary DB. Keep routine
ingestion, release-aware refresh, launch-gate/data-quality filing, and backups
on the VPS.

## New fetcher to production

1. Add or update the fetcher/parser code under `src/ingestion/...`.
2. Add fixture-backed parser/projector tests under `tests/fixtures/...` and
   `tests/...`.
3. Run the focused local tests against fixtures and temporary DBs:

   ```bash
   pytest tests/test_<provider>.py
   ```

4. Push the branch:

   ```bash
   git push -u origin <branch>
   ```

5. Deploy on the VPS:

   ```bash
   ssh vps 'git -C /opt/macro-data pull && systemctl --user restart <unit>'
   ```

6. Verify the production unit:

   ```bash
   ssh vps 'journalctl --user -u <unit> -n 100 --no-pager'
   ```

7. Verify a read path from local dev:

   ```bash
   macro-data-service resolve <CONCEPT_ID> --json
   ```

The production systemd units carry `Environment=MACRO_DATA_PROFILE=prod`; local
commands stay in `dev` unless a fixture/debug run opts into
`--allow-local-write`.
