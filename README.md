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
pip install -r requirements.txt
```

### Run GUI

```bash
python ap-image-backup-gui.py
```

### Credentials via .env (recommended)

Copy `.env.example` to `.env` and fill in your values. GUI fields and CLI argument defaults are populated from `.env`.

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

## Prebuilt Windows executable

A standalone `ap-image-backup.exe` (no Python install required) is built automatically by GitHub Actions — see [.github/workflows/build.yml](.github/workflows/build.yml).

- Trigger: every push to `main`.
- The workflow generates the icon (`build_icon.py`), then runs PyInstaller against `ap-image-backup-gui.py`.
- The resulting exe is **not** committed to the repo (`dist/` and `build/` are gitignored) — it's attached to a new GitHub Release tagged `build-<run number>`.
- To get the latest build, download it from the repo's **Releases** page.
- To build it yourself locally, install `pyinstaller` and `pillow`, then run the same steps as the workflow:

  ```bash
  python build_icon.py
  pyinstaller --onefile --windowed --icon "icon.ico" --add-data "icon.ico;." --name "ap-image-backup" ap-image-backup-gui.py
  ```

  The exe will be in `dist/`.

### Installing the exe on a machine

1. Create a folder for the app, e.g. `C:\Program Files\ap-image-backup` (needs admin) or `%LOCALAPPDATA%\Programs\ap-image-backup` (no admin needed).
2. Copy `ap-image-backup.exe` into that folder.
3. Copy `.env.example` into that same folder, rename it to `.env`, and fill in that machine's values. The exe looks for `.env` next to itself (not next to any Python source), so it must live alongside the exe.
4. Add a Start Menu shortcut to the exe:
   - Right-click `ap-image-backup.exe` → `Show more options` → `Create shortcut`.
   - Move the resulting `.lnk` file into the Start Menu, either by dragging it into the Start Menu itself or by moving it into `%APPDATA%\Microsoft\Windows\Start Menu\Programs` (open that path via `Win+R`).

   Or create the shortcut from PowerShell:

   ```powershell
   $exe = "C:\Program Files\ap-image-backup\ap-image-backup.exe"
   $lnk = "$env:APPDATA\Microsoft\Windows\Start Menu\Programs\ap-image-backup.lnk"
   $shell = New-Object -ComObject WScript.Shell
   $shortcut = $shell.CreateShortcut($lnk)
   $shortcut.TargetPath = $exe
   $shortcut.WorkingDirectory = Split-Path $exe
   $shortcut.Save()
   ```
5. Launch it from the Start Menu like any other installed app.