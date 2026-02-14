from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Callable, Dict, Iterable
from datetime import datetime
import os
import re
import json
import sqlite3
import tempfile
import time
import smbclient


class CompareMode(Enum):
    IMAGES = "IMAGES"
    WIP = "WIP"


WIP_PATTERNS = ("WBPP", "Processing")
NAS_DB_FILENAME = "__ap_image_backup_nas_index.sqlite"
LOCAL_DB_FILENAME = "local_index.sqlite"
TARGET_STATE_FILENAME = "target_state.json"


@dataclass
class FolderResult:
    folder: str
    local_files: int = 0
    backed_up_files: int = 0
    missing_on_nas_files: int = 0
    different_files: int = 0

    @property
    def safe_to_delete(self) -> bool:
        return self.local_files > 0 and self.missing_on_nas_files == 0 and self.different_files == 0


@dataclass
class CompareSummary:
    total_local_files: int
    total_backed_up_files: int
    total_missing_on_nas_files: int
    total_different_files: int


@dataclass
class PullTargetResult:
    target: str
    recent_date: str = ""
    nas_files: int = 0
    local_files: int = 0
    matched_files: int = 0
    missing_locally_files: int = 0
    different_files: int = 0
    local_only_files: int = 0
    wip_local_only_files: int = 0
    missing_latest_mtime: int = 0
    last_pull_timestamp: int = 0

    @property
    def status(self) -> str:
        if self.nas_files == 0:
            return "Empty on NAS"
        if self.local_files == 0 and self.missing_locally_files > 0:
            return "Not pulled"
        if self.missing_locally_files > 0:
            return "Partially pulled"
        if self.different_files > 0:
            return "Local differs"
        return "Up to date"

    @property
    def recommended_action(self) -> str:
        if self.missing_locally_files > 0:
            if self.last_pull_timestamp > 0 and self.missing_latest_mtime > 0 and self.missing_latest_mtime <= self.last_pull_timestamp:
                return "Delete on NAS (_Trash)"
            return "Pull to Local"
        if self.local_only_files > 0:
            return "Push to NAS"
        if self.status == "Local differs":
            return "Pull to Local"
        return "No action"


@dataclass
class PullScanSummary:
    total_targets: int
    pull_candidates: int
    up_to_date_targets: int


@dataclass
class PullExecutionResult:
    target: str
    total_files: int
    copied_files: int
    skipped_files: int
    error_files: int
    lights_copied_files: int = 0
    lights_skipped_files: int = 0
    lights_error_files: int = 0
    flats_copied_files: int = 0
    flats_skipped_files: int = 0
    flats_error_files: int = 0
    skip_log_path: str = ""
    action: str = ""

    @property
    def success(self) -> bool:
        return self.error_files == 0


def _parse_recent_date_from_target(target: str) -> str:
    match = re.match(r"^DATE_(\d{4}-\d{2}-\d{2})", target)
    if not match:
        return ""
    return match.group(1)


def _normalize_rel_path(path: str) -> str:
    return path.replace("\\", "/")


def _target_from_rel_path(rel_path: str) -> str:
    normalized = _normalize_rel_path(rel_path)
    first = normalized.split("/", 1)[0]
    return first if first else "(root)"


def _ensure_index_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS file_index (
            rel_path TEXT PRIMARY KEY,
            target TEXT NOT NULL,
            size_bytes INTEGER NOT NULL,
            mtime_int INTEGER NOT NULL
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_file_index_target ON file_index(target)")
    conn.commit()


def _create_local_index_db_path(local_root: str) -> Path:
    cache_dir = Path(local_root) / ".ap-image-backup"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir / LOCAL_DB_FILENAME


def _target_state_path(local_root: str) -> Path:
    cache_dir = Path(local_root) / ".ap-image-backup"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir / TARGET_STATE_FILENAME


def _load_target_state(local_root: str) -> dict:
    state_path = _target_state_path(local_root)
    if not state_path.exists():
        return {}
    try:
        with open(state_path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
            if isinstance(data, dict):
                return data
    except Exception:
        return {}
    return {}


def _save_target_state(local_root: str, data: dict) -> None:
    state_path = _target_state_path(local_root)
    with open(state_path, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2)


def mark_target_pulled(local_root: str, target: str, timestamp: int | None = None) -> None:
    state = _load_target_state(local_root)
    pulls = state.get("last_successful_pull", {})
    if not isinstance(pulls, dict):
        pulls = {}

    pulls[target] = int(timestamp if timestamp is not None else time.time())
    state["last_successful_pull"] = pulls
    _save_target_state(local_root, state)


def _is_wip_path(path: Path) -> bool:
    parts = [part.lower() for part in path.parts]
    return any(pattern.lower() in part for part in parts for pattern in WIP_PATTERNS)


def _include_file(path: Path, mode: CompareMode) -> bool:
    if mode == CompareMode.IMAGES:
        return not _is_wip_path(path)
    if mode == CompareMode.WIP:
        return _is_wip_path(path)
    return True


def _is_wip_rel_path(rel_path: str) -> bool:
    return _is_wip_path(Path(rel_path))


def _relative_folder_from_root(relative_file: Path) -> str:
    if len(relative_file.parts) <= 1:
        return "(root)"
    return relative_file.parts[0]


def _is_missing_smb_exception(exc: Exception) -> bool:
    message = str(exc).upper()
    missing_markers = (
        "STATUS_OBJECT_NAME_NOT_FOUND",
        "STATUS_NO_SUCH_FILE",
        "STATUS_OBJECT_PATH_NOT_FOUND",
        "NO SUCH FILE",
        "CANNOT FIND THE FILE",
    )
    return any(marker in message for marker in missing_markers)


def _same_file(local_path: Path, smb_file_path: str) -> tuple[bool, str]:
    try:
        smb_stat = smbclient.stat(smb_file_path)
    except FileNotFoundError:
        return False, "missing"
    except Exception as exc:
        if _is_missing_smb_exception(exc):
            return False, "missing"
        return False, "error"

    local_stat = local_path.stat()
    same_size = local_stat.st_size == smb_stat.st_size
    same_mtime = int(local_stat.st_mtime) == int(smb_stat.st_mtime)

    if same_size and same_mtime:
        return True, "same"
    return False, "different"


def _same_file_nas_to_local(smb_file_path: str, local_path: Path) -> tuple[bool, str]:
    if not local_path.exists():
        return False, "missing"

    try:
        smb_stat = smbclient.stat(smb_file_path)
    except FileNotFoundError:
        return False, "missing-on-nas"
    except Exception as exc:
        if _is_missing_smb_exception(exc):
            return False, "missing-on-nas"
        return False, "error"

    local_stat = local_path.stat()
    same_size = local_stat.st_size == smb_stat.st_size
    same_mtime = int(local_stat.st_mtime) == int(smb_stat.st_mtime)

    if same_size and same_mtime:
        return True, "same"
    return False, "different"


def _iter_local_files(local_root: Path, mode: CompareMode) -> Iterable[Path]:
    for root, _, files in os.walk(local_root):
        root_path = Path(root)
        for file_name in files:
            path = root_path / file_name
            if _include_file(path.relative_to(local_root), mode):
                yield path


def _count_smb_files(smb_root: str) -> int:
    total = 0
    for _, _, files in smbclient.walk(smb_root):
        total += len(files)
    return total


def _smb_makedirs(path: str) -> None:
    norm = path.replace("\\", "/")
    if "/" not in norm:
        return
    parts = norm.split("/")
    current = parts[0]
    for part in parts[1:]:
        current = f"{current}/{part}"
        try:
            if not smbclient.path.isdir(current):
                smbclient.mkdir(current)
        except Exception:
            pass


def _build_skip_log_path(local_root: str, target: str) -> Path:
    cache_dir = Path(local_root) / ".ap-image-backup" / "logs"
    cache_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_target = re.sub(r"[^A-Za-z0-9_.-]+", "_", target).strip("_") or "target"
    return cache_dir / f"skip_reasons_{safe_target}_{timestamp}.log"


def _copy_smb_tree_to_local(
    *,
    smb_root: str,
    local_root: Path,
    total_files: int,
    processed: int,
    copied: int,
    skipped: int,
    errors: int,
    progress_callback: Callable[[int, int, str], None] | None,
    context_label: str,
    skip_logger: Callable[[str], None] | None,
) -> tuple[int, int, int, int]:
    os.makedirs(local_root, exist_ok=True)

    for current_smb_root, _, files in smbclient.walk(smb_root):
        rel_dir = os.path.relpath(current_smb_root, smb_root)
        local_dir = local_root if rel_dir == "." else local_root / Path(rel_dir)
        os.makedirs(local_dir, exist_ok=True)

        for file_name in files:
            smb_file_path = os.path.join(current_smb_root, file_name)
            local_file_path = local_dir / file_name

            try:
                smb_stat = smbclient.stat(smb_file_path)

                if local_file_path.exists():
                    local_stat = local_file_path.stat()
                    same_size = local_stat.st_size == smb_stat.st_size
                    same_mtime = int(local_stat.st_mtime) == int(smb_stat.st_mtime)
                    if same_size and same_mtime:
                        skipped += 1
                        if skip_logger is not None:
                            skip_logger(
                                f"[{context_label}] SKIP same size+mtime: {smb_file_path}"
                            )
                        processed += 1
                        if progress_callback is not None:
                            progress_callback(processed, total_files, str(local_file_path))
                        continue

                with smbclient.open_file(smb_file_path, mode="rb") as smb_file:
                    with open(local_file_path, "wb") as local_file:
                        local_file.write(smb_file.read())

                os.utime(local_file_path, (smb_stat.st_mtime, smb_stat.st_mtime))
                copied += 1
            except Exception:
                errors += 1

            processed += 1
            if progress_callback is not None:
                progress_callback(processed, total_files, str(local_file_path))

    return processed, copied, skipped, errors


def refresh_local_index(local_root: str, progress_callback: Callable[[str], None] | None = None) -> Path:
    local_root_path = Path(local_root)
    if not local_root_path.exists():
        raise FileNotFoundError(f"Local path does not exist: {local_root}")

    local_db_path = _create_local_index_db_path(local_root)
    conn = sqlite3.connect(local_db_path)
    try:
        _ensure_index_schema(conn)
        conn.execute("DELETE FROM file_index")

        rows: list[tuple[str, str, int, int]] = []
        for root, _, files in os.walk(local_root_path):
            root_path = Path(root)
            for file_name in files:
                full_path = root_path / file_name
                rel_path = _normalize_rel_path(str(full_path.relative_to(local_root_path)))
                if rel_path.startswith(".ap-image-backup/"):
                    continue

                stat = full_path.stat()
                target = _target_from_rel_path(rel_path)
                rows.append((rel_path, target, int(stat.st_size), int(stat.st_mtime)))
                if progress_callback is not None:
                    progress_callback(rel_path)

        conn.executemany(
            "INSERT INTO file_index(rel_path, target, size_bytes, mtime_int) VALUES(?, ?, ?, ?)",
            rows,
        )
        conn.commit()
    finally:
        conn.close()

    return local_db_path


def _load_index_rows(db_path: Path) -> dict[str, tuple[str, int, int]]:
    conn = sqlite3.connect(db_path)
    try:
        _ensure_index_schema(conn)
        cursor = conn.execute("SELECT rel_path, target, size_bytes, mtime_int FROM file_index")
        return {row[0]: (row[1], int(row[2]), int(row[3])) for row in cursor.fetchall()}
    finally:
        conn.close()


def _download_nas_db_to_temp(
    *,
    server: str,
    username: str,
    password: str,
    share_root: str,
) -> Path:
    smb_db_path = os.path.join(share_root, NAS_DB_FILENAME)
    local_temp = Path(tempfile.gettempdir()) / f"ap_image_backup_nas_{int(time.time() * 1000)}.sqlite"

    smbclient.register_session(server, username=username, password=password)
    try:
        with smbclient.open_file(smb_db_path, mode="rb") as smb_file:
            with open(local_temp, "wb") as local_file:
                local_file.write(smb_file.read())
    finally:
        smbclient.reset_connection_cache()

    return local_temp


def _upload_temp_db_to_nas(
    *,
    server: str,
    username: str,
    password: str,
    share_root: str,
    local_db_path: Path,
) -> None:
    smb_db_path = os.path.join(share_root, NAS_DB_FILENAME)

    smbclient.register_session(server, username=username, password=password)
    try:
        with open(local_db_path, "rb") as local_file:
            with smbclient.open_file(smb_db_path, mode="wb") as smb_file:
                smb_file.write(local_file.read())

        now = int(time.time())
        smbclient.utime(smb_db_path, (now, now))
    finally:
        smbclient.reset_connection_cache()


def rebuild_nas_index(
    *,
    server: str,
    username: str,
    password: str,
    share_root: str,
    progress_callback: Callable[[str], None] | None = None,
) -> int:
    local_temp = Path(tempfile.gettempdir()) / f"ap_image_backup_nas_rebuild_{int(time.time() * 1000)}.sqlite"
    conn = sqlite3.connect(local_temp)
    try:
        _ensure_index_schema(conn)
        conn.execute("DELETE FROM file_index")
        rows: list[tuple[str, str, int, int]] = []

        smbclient.register_session(server, username=username, password=password)
        try:
            for root, _, files in smbclient.walk(share_root):
                for file_name in files:
                    smb_file_path = os.path.join(root, file_name)
                    rel_path = _normalize_rel_path(os.path.relpath(smb_file_path, share_root))
                    if rel_path == NAS_DB_FILENAME:
                        continue

                    smb_stat = smbclient.stat(smb_file_path)
                    target = _target_from_rel_path(rel_path)
                    rows.append((rel_path, target, int(smb_stat.st_size), int(smb_stat.st_mtime)))

                    if progress_callback is not None:
                        progress_callback(rel_path)
        finally:
            smbclient.reset_connection_cache()

        conn.executemany(
            "INSERT INTO file_index(rel_path, target, size_bytes, mtime_int) VALUES(?, ?, ?, ?)",
            rows,
        )
        conn.commit()
    finally:
        conn.close()

    _upload_temp_db_to_nas(
        server=server,
        username=username,
        password=password,
        share_root=share_root,
        local_db_path=local_temp,
    )

    try:
        local_temp.unlink(missing_ok=True)
    except Exception:
        pass

    return len(rows)


def _get_or_build_nas_db(
    *,
    server: str,
    username: str,
    password: str,
    share_root: str,
    force_rebuild: bool,
    progress_callback: Callable[[str], None] | None = None,
) -> Path:
    if force_rebuild:
        rebuild_nas_index(
            server=server,
            username=username,
            password=password,
            share_root=share_root,
            progress_callback=progress_callback,
        )

    try:
        return _download_nas_db_to_temp(
            server=server,
            username=username,
            password=password,
            share_root=share_root,
        )
    except Exception:
        rebuild_nas_index(
            server=server,
            username=username,
            password=password,
            share_root=share_root,
            progress_callback=progress_callback,
        )
        return _download_nas_db_to_temp(
            server=server,
            username=username,
            password=password,
            share_root=share_root,
        )


def upsert_nas_index_entries(
    *,
    server: str,
    username: str,
    password: str,
    share_root: str,
    entries: list[tuple[str, int, int]],
) -> None:
    if not entries:
        return

    nas_db_temp = _get_or_build_nas_db(
        server=server,
        username=username,
        password=password,
        share_root=share_root,
        force_rebuild=False,
    )

    conn = sqlite3.connect(nas_db_temp)
    try:
        _ensure_index_schema(conn)
        normalized_rows = []
        for rel_path, size_bytes, mtime_int in entries:
            normalized_rel = _normalize_rel_path(rel_path)
            normalized_rows.append(
                (normalized_rel, _target_from_rel_path(normalized_rel), int(size_bytes), int(mtime_int))
            )

        conn.executemany(
            """
            INSERT INTO file_index(rel_path, target, size_bytes, mtime_int)
            VALUES(?, ?, ?, ?)
            ON CONFLICT(rel_path)
            DO UPDATE SET
                target=excluded.target,
                size_bytes=excluded.size_bytes,
                mtime_int=excluded.mtime_int
            """,
            normalized_rows,
        )
        conn.commit()
    finally:
        conn.close()

    _upload_temp_db_to_nas(
        server=server,
        username=username,
        password=password,
        share_root=share_root,
        local_db_path=nas_db_temp,
    )

    try:
        nas_db_temp.unlink(missing_ok=True)
    except Exception:
        pass


def compare_local_to_nas(
    *,
    server: str,
    username: str,
    password: str,
    share_root: str,
    local_root: str,
    mode: CompareMode,
    progress_callback: Callable[[str], None] | None = None,
) -> tuple[list[FolderResult], CompareSummary]:
    local_root_path = Path(local_root)
    if not local_root_path.exists():
        raise FileNotFoundError(f"Local path does not exist: {local_root}")

    folder_results: Dict[str, FolderResult] = {}

    smbclient.register_session(server, username=username, password=password)
    try:
        for local_file in _iter_local_files(local_root_path, mode):
            relative_file = local_file.relative_to(local_root_path)
            folder_key = _relative_folder_from_root(relative_file)
            folder_result = folder_results.setdefault(folder_key, FolderResult(folder=folder_key))
            folder_result.local_files += 1

            smb_file_path = os.path.join(share_root, str(relative_file))
            same, reason = _same_file(local_file, smb_file_path)

            if same:
                folder_result.backed_up_files += 1
            elif reason == "missing":
                folder_result.missing_on_nas_files += 1
            else:
                folder_result.different_files += 1

            if progress_callback is not None:
                progress_callback(str(relative_file))
    finally:
        smbclient.reset_connection_cache()

    ordered_results = sorted(folder_results.values(), key=lambda item: item.folder.lower())
    summary = CompareSummary(
        total_local_files=sum(result.local_files for result in ordered_results),
        total_backed_up_files=sum(result.backed_up_files for result in ordered_results),
        total_missing_on_nas_files=sum(result.missing_on_nas_files for result in ordered_results),
        total_different_files=sum(result.different_files for result in ordered_results),
    )

    return ordered_results, summary


def scan_nas_pull_candidates(
    *,
    server: str,
    username: str,
    password: str,
    share_root: str,
    local_root: str,
    progress_callback: Callable[[str], None] | None = None,
    force_rebuild_nas_db: bool = False,
) -> tuple[list[PullTargetResult], PullScanSummary]:
    local_root_path = Path(local_root)
    if not local_root_path.exists():
        raise FileNotFoundError(f"Local path does not exist: {local_root}")

    local_db_path = refresh_local_index(local_root, progress_callback=progress_callback)
    nas_db_path = _get_or_build_nas_db(
        server=server,
        username=username,
        password=password,
        share_root=share_root,
        force_rebuild=force_rebuild_nas_db,
        progress_callback=progress_callback,
    )

    try:
        local_rows = _load_index_rows(local_db_path)
        nas_rows = _load_index_rows(nas_db_path)
        state = _load_target_state(local_root)
        last_pull_map = state.get("last_successful_pull", {}) if isinstance(state, dict) else {}

        local_counts: dict[str, int] = {}
        for _rel, (target, _size, _mtime) in local_rows.items():
            local_counts[target] = local_counts.get(target, 0) + 1

        results_map: dict[str, PullTargetResult] = {}

        for rel_path, (target, nas_size, nas_mtime) in nas_rows.items():
            if target.startswith("_"):
                continue

            target_result = results_map.setdefault(
                target,
                PullTargetResult(target=target, recent_date=_parse_recent_date_from_target(target)),
            )
            target_result.nas_files += 1

            local_entry = local_rows.get(rel_path)
            if local_entry is None:
                target_result.missing_locally_files += 1
                target_result.missing_latest_mtime = max(target_result.missing_latest_mtime, int(nas_mtime))
            else:
                _local_target, local_size, local_mtime = local_entry
                if local_size == nas_size and local_mtime == nas_mtime:
                    target_result.matched_files += 1
                else:
                    target_result.different_files += 1

        for rel_path, (local_target, _size, _mtime) in local_rows.items():
            if local_target.startswith("_"):
                continue
            if local_target not in results_map:
                results_map[local_target] = PullTargetResult(
                    target=local_target,
                    recent_date=_parse_recent_date_from_target(local_target),
                )
            if rel_path not in nas_rows:
                results_map[local_target].local_only_files += 1
                if _is_wip_rel_path(rel_path):
                    results_map[local_target].wip_local_only_files += 1

        for target, result in results_map.items():
            result.local_files = local_counts.get(target, 0)
            try:
                result.last_pull_timestamp = int(last_pull_map.get(target, 0)) if isinstance(last_pull_map, dict) else 0
            except Exception:
                result.last_pull_timestamp = 0

        ordered_results = sorted(results_map.values(), key=lambda item: item.target.lower())
        summary = PullScanSummary(
            total_targets=len(ordered_results),
            pull_candidates=sum(1 for result in ordered_results if result.recommended_action != "No action"),
            up_to_date_targets=sum(1 for result in ordered_results if result.status == "Up to date"),
        )

        return ordered_results, summary
    finally:
        try:
            nas_db_path.unlink(missing_ok=True)
        except Exception:
            pass


def pull_target_from_nas(
    *,
    server: str,
    username: str,
    password: str,
    share_root: str,
    local_root: str,
    target: str,
    include_flats: bool = False,
    progress_callback: Callable[[int, int, str], None] | None = None,
) -> PullExecutionResult:
    local_root_path = Path(local_root)
    if not local_root_path.exists():
        raise FileNotFoundError(f"Local path does not exist: {local_root}")

    smb_target_root = os.path.join(share_root, target)
    local_target_root = local_root_path / target
    skip_log_path = _build_skip_log_path(local_root, target)

    with open(skip_log_path, "w", encoding="utf-8") as skip_log:
        skip_log.write(f"Skip log for target: {target}\n")
        skip_log.write(f"Generated: {datetime.now().isoformat()}\n\n")

    smbclient.register_session(server, username=username, password=password)
    try:
        total_files = _count_smb_files(smb_target_root)

        flat_sources: list[tuple[str, Path]] = []
        if include_flats:
            try:
                top_entries = smbclient.listdir(smb_target_root)
            except Exception:
                top_entries = []

            for entry in top_entries:
                entry_smb_path = os.path.join(smb_target_root, entry)
                if not smbclient.path.isdir(entry_smb_path):
                    continue
                flat_smb_root = os.path.join(share_root, "_FlatWizard", entry)
                if smbclient.path.isdir(flat_smb_root):
                    flat_local_root = local_root_path / "_FlatWizard" / entry
                    flat_sources.append((flat_smb_root, flat_local_root))
                    total_files += _count_smb_files(flat_smb_root)

        processed = 0

        lights_copied = 0
        lights_skipped = 0
        lights_errors = 0

        flats_copied = 0
        flats_skipped = 0
        flats_errors = 0

        def _write_skip_line(text: str) -> None:
            with open(skip_log_path, "a", encoding="utf-8") as skip_log_file:
                skip_log_file.write(text + "\n")

        processed, lights_copied, lights_skipped, lights_errors = _copy_smb_tree_to_local(
            smb_root=smb_target_root,
            local_root=local_target_root,
            total_files=total_files,
            processed=processed,
            copied=lights_copied,
            skipped=lights_skipped,
            errors=lights_errors,
            progress_callback=progress_callback,
            context_label="LIGHTS",
            skip_logger=_write_skip_line,
        )

        for flat_smb_root, flat_local_root in flat_sources:
            processed, copied_delta, skipped_delta, errors_delta = _copy_smb_tree_to_local(
                smb_root=flat_smb_root,
                local_root=flat_local_root,
                total_files=total_files,
                processed=processed,
                copied=0,
                skipped=0,
                errors=0,
                progress_callback=progress_callback,
                context_label="FLATS",
                skip_logger=_write_skip_line,
            )
            flats_copied += copied_delta
            flats_skipped += skipped_delta
            flats_errors += errors_delta

        copied = lights_copied + flats_copied
        skipped = lights_skipped + flats_skipped
        errors = lights_errors + flats_errors

        return PullExecutionResult(
            target=target,
            total_files=total_files,
            copied_files=copied,
            skipped_files=skipped,
            error_files=errors,
            lights_copied_files=lights_copied,
            lights_skipped_files=lights_skipped,
            lights_error_files=lights_errors,
            flats_copied_files=flats_copied,
            flats_skipped_files=flats_skipped,
            flats_error_files=flats_errors,
            skip_log_path=str(skip_log_path),
            action="pull",
        )
    finally:
        smbclient.reset_connection_cache()


def push_target_to_nas(
    *,
    server: str,
    username: str,
    password: str,
    share_root: str,
    local_root: str,
    target: str,
    progress_callback: Callable[[int, int, str], None] | None = None,
) -> PullExecutionResult:
    local_root_path = Path(local_root)
    local_target_root = local_root_path / target
    if not local_target_root.exists():
        raise FileNotFoundError(f"Local target does not exist: {local_target_root}")

    skip_log_path = _build_skip_log_path(local_root, f"push_{target}")
    with open(skip_log_path, "w", encoding="utf-8") as skip_log:
        skip_log.write(f"Skip log for push target: {target}\n")
        skip_log.write(f"Generated: {datetime.now().isoformat()}\n\n")

    total_files = 0
    for _, _, files in os.walk(local_target_root):
        total_files += len(files)

    processed = 0
    copied = 0
    skipped = 0
    errors = 0
    nas_upserts: list[tuple[str, int, int]] = []

    smbclient.register_session(server, username=username, password=password)
    try:
        for root, _, files in os.walk(local_target_root):
            root_path = Path(root)
            rel_dir = _normalize_rel_path(str(root_path.relative_to(local_root_path)))
            smb_dir = os.path.join(share_root, rel_dir)
            _smb_makedirs(smb_dir)

            for file_name in files:
                local_file = root_path / file_name
                rel_path = _normalize_rel_path(str(local_file.relative_to(local_root_path)))
                smb_file = os.path.join(share_root, rel_path)

                try:
                    local_stat = local_file.stat()
                    local_size = int(local_stat.st_size)
                    local_mtime = int(local_stat.st_mtime)

                    try:
                        smb_stat = smbclient.stat(smb_file)
                        if int(smb_stat.st_size) == local_size and int(smb_stat.st_mtime) == local_mtime:
                            skipped += 1
                            with open(skip_log_path, "a", encoding="utf-8") as skip_log_file:
                                skip_log_file.write(f"[PUSH] SKIP same size+mtime: {smb_file}\n")
                            processed += 1
                            if progress_callback is not None:
                                progress_callback(processed, total_files, str(local_file))
                            continue
                    except Exception:
                        pass

                    _smb_makedirs(os.path.dirname(smb_file))
                    with open(local_file, "rb") as local_fh:
                        with smbclient.open_file(smb_file, mode="wb") as smb_fh:
                            smb_fh.write(local_fh.read())

                    smbclient.utime(smb_file, (local_mtime, local_mtime))
                    nas_upserts.append((rel_path, local_size, local_mtime))
                    copied += 1
                except Exception:
                    errors += 1

                processed += 1
                if progress_callback is not None:
                    progress_callback(processed, total_files, str(local_file))
    finally:
        smbclient.reset_connection_cache()

    if nas_upserts:
        upsert_nas_index_entries(
            server=server,
            username=username,
            password=password,
            share_root=share_root,
            entries=nas_upserts,
        )

    return PullExecutionResult(
        target=target,
        total_files=total_files,
        copied_files=copied,
        skipped_files=skipped,
        error_files=errors,
        lights_copied_files=copied,
        lights_skipped_files=skipped,
        lights_error_files=errors,
        flats_copied_files=0,
        flats_skipped_files=0,
        flats_error_files=0,
        skip_log_path=str(skip_log_path),
        action="push",
    )


def delete_nas_only_to_trash(
    *,
    server: str,
    username: str,
    password: str,
    share_root: str,
    local_root: str,
    target: str,
    require_pull_checkpoint: bool = True,
    progress_callback: Callable[[int, int, str], None] | None = None,
) -> PullExecutionResult:
    state = _load_target_state(local_root)
    pulls = state.get("last_successful_pull", {}) if isinstance(state, dict) else {}
    last_pull = int(pulls.get(target, 0)) if isinstance(pulls, dict) else 0
    if require_pull_checkpoint and last_pull <= 0:
        raise RuntimeError(f"Cannot delete NAS-only files for {target}: no last successful pull timestamp found")

    local_db_path = refresh_local_index(local_root)
    nas_db_path = _get_or_build_nas_db(
        server=server,
        username=username,
        password=password,
        share_root=share_root,
        force_rebuild=False,
    )

    local_rows = _load_index_rows(local_db_path)
    nas_rows = _load_index_rows(nas_db_path)

    candidates: list[str] = []
    for rel_path, (row_target, _size, mtime_int) in nas_rows.items():
        if row_target != target:
            continue
        if rel_path in local_rows:
            continue
        if (not require_pull_checkpoint) or int(mtime_int) <= last_pull:
            candidates.append(rel_path)

    skip_log_path = _build_skip_log_path(local_root, f"delete_{target}")
    with open(skip_log_path, "w", encoding="utf-8") as skip_log:
        skip_log.write(f"Delete-to-trash log for target: {target}\n")
        skip_log.write(f"Generated: {datetime.now().isoformat()}\n\n")

    date_tag = datetime.now().strftime("%Y-%m-%d")
    trash_root = os.path.join(share_root, "_Trash", date_tag)

    processed = 0
    moved = 0
    errors = 0
    moved_parents: set[str] = set()

    smbclient.register_session(server, username=username, password=password)
    try:
        for rel_path in candidates:
            src = os.path.join(share_root, rel_path)
            dst = os.path.join(trash_root, rel_path)
            try:
                _smb_makedirs(os.path.dirname(dst))
                smbclient.rename(src, dst)
                moved += 1
                parent_dir = os.path.dirname(src)
                if parent_dir:
                    moved_parents.add(parent_dir)
                with open(skip_log_path, "a", encoding="utf-8") as skip_log_file:
                    skip_log_file.write(f"[DELETE->TRASH] MOVED {src} -> {dst}\n")
            except Exception:
                errors += 1

            processed += 1
            if progress_callback is not None:
                progress_callback(processed, len(candidates), rel_path)

        local_target_root = Path(local_root) / target
        dirs_to_check: set[str] = set()
        for parent in moved_parents:
            current = parent
            target_root = os.path.join(share_root, target)
            while current.startswith(target_root):
                dirs_to_check.add(current)
                if current == target_root:
                    break
                current = os.path.dirname(current)

        for nas_dir in sorted(dirs_to_check, key=len, reverse=True):
            rel_dir = os.path.relpath(nas_dir, os.path.join(share_root, target))
            if rel_dir == ".":
                local_dir = local_target_root
            else:
                local_dir = local_target_root / Path(_normalize_rel_path(rel_dir))

            if local_dir.exists():
                continue

            try:
                if smbclient.path.isdir(nas_dir) and len(smbclient.listdir(nas_dir)) == 0:
                    smbclient.rmdir(nas_dir)
                    with open(skip_log_path, "a", encoding="utf-8") as skip_log_file:
                        skip_log_file.write(f"[DELETE->TRASH] REMOVED EMPTY DIR {nas_dir}\n")
            except Exception:
                pass
    finally:
        smbclient.reset_connection_cache()

    if candidates:
        nas_db_temp = _get_or_build_nas_db(
            server=server,
            username=username,
            password=password,
            share_root=share_root,
            force_rebuild=False,
        )
        conn = sqlite3.connect(nas_db_temp)
        try:
            _ensure_index_schema(conn)
            conn.executemany("DELETE FROM file_index WHERE rel_path = ?", [(rel_path,) for rel_path in candidates])
            conn.commit()
        finally:
            conn.close()

        _upload_temp_db_to_nas(
            server=server,
            username=username,
            password=password,
            share_root=share_root,
            local_db_path=nas_db_temp,
        )

        try:
            nas_db_temp.unlink(missing_ok=True)
        except Exception:
            pass

    try:
        nas_db_path.unlink(missing_ok=True)
    except Exception:
        pass

    return PullExecutionResult(
        target=target,
        total_files=len(candidates),
        copied_files=moved,
        skipped_files=0,
        error_files=errors,
        lights_copied_files=moved,
        lights_skipped_files=0,
        lights_error_files=errors,
        flats_copied_files=0,
        flats_skipped_files=0,
        flats_error_files=0,
        skip_log_path=str(skip_log_path),
        action="delete",
    )
