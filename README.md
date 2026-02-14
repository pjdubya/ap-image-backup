# ap-image-backup

## GUI (PySide6)

This repository includes a desktop GUI for comparison and queued actions:

- It compares local files to NAS backups folder-by-folder.
- It shows which folders are likely safe to delete locally (`Safe To Delete = Yes`).
- It can run queued actions (pull from NAS, push to NAS, move NAS files to _Trash).
- It does **not** delete files.

Existing command-line scripts remain unchanged.

### Install GUI dependencies

```bash
pip install -r requirements-gui.txt
```

### Run GUI

```bash
python ap-image-backup-gui.py
```

### Processing machine workflow (find what to pull)

In the GUI:

- `Capture Machine View: Local → NAS safety (review later)` is for validating capture-machine backup/delete safety.
- `Processing Machine View: NAS → Local pull candidates + actions` is for deciding what to pull to processing.

1. Enter `Server`, `Username`, `Password`, `Server Path`, and local processing path.
2. Click `Refresh File Status`.
3. Review `NAS → Local pull-candidates view`:
	- `Not pulled`: target exists on NAS but not locally.
	- `Partially pulled`: some NAS files are missing locally.
	- `Local differs`: local copies exist but differ from NAS metadata.
	- `Up to date`: local appears synchronized with NAS for that target.
4. Use `Recent Date` (derived from targets like `DATE_2026-02-12...`) to identify newest captures quickly.
5. Prioritize targets with `Recommended Action = Pull from NAS`.

### NAS DB index

- Pull-candidate scans use a NAS SQLite index plus a refreshed local SQLite index for faster compare operations.
- Click `Rebuild NAS DB` to force a full NAS rescan and regenerate the NAS index file.
- During local-to-NAS writes via this app, copied files are upserted into the NAS index.

### Queue actions from GUI

1. Optionally enable `Include flats (_FlatWizard) for queued pull actions`.
2. In `Processing Machine View: NAS → Local pull candidates + actions`, use `Queue Pull`, `Queue Push`, or `Queue Delete`.
3. Review the `Action queue` section for order, options, and status.
4. Click `Start Queue`.
5. Watch progress in `Action queue` (`Status`, `Progress %`, `Copied`, `Skipped`, `Errors`) and `Next` target label.

`Add to Queue` is disabled for targets already in the queue to prevent accidental duplicate entries.

### Table-2 actions

`Processing Machine View: NAS → Local pull candidates + actions` now recommends one of:

- `Pull to Local`
- `Push to NAS`
- `Delete on NAS (_Trash)`
- `No action`

Queue execution uses the recommended action for each queued target.

Queue rows also show split counters:

- `Lights C/S/E` (copied / skipped / errors)
- `Flats C/S/E` (copied / skipped / errors)

After each queue item, a `Skip Log` button opens the per-target skip-reason log file.

### Compare modes

- `Images`: excludes paths containing `WBPP` or `Processing`.
- `WIP`: only includes paths under `WBPP` or `Processing`.

### Safety note

Comparison features are read-only. Queue actions run only what you explicitly queued per target.