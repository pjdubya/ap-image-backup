from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass

from PySide6.QtCore import QObject, QThread, Signal, Slot
from PySide6.QtWidgets import (
    QApplication,
    QAbstractItemView,
    QCheckBox,
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QFileDialog,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from compare_engine import (
    CompareMode,
    PullExecutionResult,
    compare_local_to_nas,
    delete_local_target,
    delete_nas_only_to_trash,
    list_action_candidate_files,
    mark_target_pulled,
    pull_target_from_nas,
    push_target_to_nas,
    rebuild_nas_index,
    scan_nas_pull_candidates,
)


def load_dotenv_file(dotenv_path: str) -> dict[str, str]:
    values: dict[str, str] = {}

    if not os.path.exists(dotenv_path):
        return values

    with open(dotenv_path, "r", encoding="utf-8") as dotenv_file:
        for raw_line in dotenv_file:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue

            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key:
                values[key] = value

    return values


def get_app_config() -> dict[str, str]:
    config = dict(os.environ)
    dotenv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    for key, value in load_dotenv_file(dotenv_path).items():
        config.setdefault(key, value)
    return config


def config_bool(config: dict[str, str], key: str, default: bool) -> bool:
    raw_value = config.get(key)
    if raw_value is None:
        return default

    normalized = raw_value.strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    return default


def normalize_path(value: str) -> str:
    return os.path.expandvars(os.path.expanduser(value.strip()))


def get_local_path_presets(config: dict[str, str]) -> list[str]:
    raw_presets = config.get("AP_BACKUP_LOCAL_PATH_PRESETS", "").strip()
    parsed_presets: list[str] = []

    if raw_presets:
        try:
            presets_value = json.loads(raw_presets)
            if isinstance(presets_value, list):
                for item in presets_value:
                    if isinstance(item, str) and item.strip():
                        parsed_presets.append(normalize_path(item))
        except json.JSONDecodeError:
            bracketed = raw_presets
            if raw_presets.startswith("[") and raw_presets.endswith("]"):
                bracketed = raw_presets[1:-1]

            for item in bracketed.split(","):
                cleaned = item.strip().strip('"').strip("'")
                if cleaned:
                    parsed_presets.append(normalize_path(cleaned))

            if not parsed_presets:
                for item in raw_presets.split(";"):
                    cleaned = item.strip().strip('"').strip("'")
                    if cleaned:
                        parsed_presets.append(normalize_path(cleaned))

    if parsed_presets:
        return parsed_presets

    default_home_path = os.path.join(os.path.expanduser("~"), "Pictures", "NINA")
    legacy_candidates = [
        config.get("AP_BACKUP_LOCAL_PATH_HOME", default_home_path),
        config.get("AP_BACKUP_LOCAL_PATH_D", r"D:\Pictures\NINA"),
    ]

    deduped_presets: list[str] = []
    for candidate in legacy_candidates:
        if not candidate:
            continue
        normalized_candidate = normalize_path(candidate)
        if normalized_candidate and normalized_candidate.lower() not in {path.lower() for path in deduped_presets}:
            deduped_presets.append(normalized_candidate)

    if deduped_presets:
        return deduped_presets

    return [normalize_path(default_home_path), normalize_path(r"D:\Pictures\NINA")]


@dataclass
class CompareRequest:
    server: str
    username: str
    password: str
    server_path: str
    local_path: str
    mode: CompareMode


@dataclass
class TargetScanRequest:
    server: str
    username: str
    password: str
    server_path: str
    local_path: str
    force_rebuild_nas_db: bool = False


@dataclass
class QueueEntry:
    target: str
    include_flats: bool
    action: str = "pull"
    estimated_files: int = -1
    preview_text: str = ""
    status: str = "Queued"
    progress_percent: int = 0
    copied_files: int = 0
    skipped_files: int = 0
    error_files: int = 0
    lights_counts: str = "0/0/0"
    flats_counts: str = "0/0/0"
    total_files: int = 0
    skip_log_path: str = ""


@dataclass
class QueueRequest:
    server: str
    username: str
    password: str
    server_path: str
    local_path: str
    queue_entries: list[QueueEntry]


class CompareWorker(QObject):
    finished = Signal(list, object)
    failed = Signal(str)
    progress = Signal(str)

    def __init__(self, request: CompareRequest):
        super().__init__()
        self.request = request

    @Slot()
    def run(self) -> None:
        try:
            share_root = rf"\\{self.request.server}{self.request.server_path}"
            results, summary = compare_local_to_nas(
                server=self.request.server,
                username=self.request.username,
                password=self.request.password,
                share_root=share_root,
                local_root=self.request.local_path,
                mode=self.request.mode,
                progress_callback=lambda text: self.progress.emit(text),
            )
            self.finished.emit(results, summary)
        except Exception as exc:
            self.failed.emit(str(exc))


class TargetScanWorker(QObject):
    finished = Signal(list, object)
    failed = Signal(str)
    progress = Signal(str)

    def __init__(self, request: TargetScanRequest):
        super().__init__()
        self.request = request

    @Slot()
    def run(self) -> None:
        try:
            share_root = rf"\\{self.request.server}{self.request.server_path}"
            results, summary = scan_nas_pull_candidates(
                server=self.request.server,
                username=self.request.username,
                password=self.request.password,
                share_root=share_root,
                local_root=self.request.local_path,
                progress_callback=lambda text: self.progress.emit(text),
                force_rebuild_nas_db=self.request.force_rebuild_nas_db,
            )
            self.finished.emit(results, summary)
        except Exception as exc:
            self.failed.emit(str(exc))


class RebuildNasDbWorker(QObject):
    progress = Signal(str)
    finished = Signal(int)
    failed = Signal(str)

    def __init__(self, request: TargetScanRequest):
        super().__init__()
        self.request = request

    @Slot()
    def run(self) -> None:
        try:
            share_root = rf"\\{self.request.server}{self.request.server_path}"
            rows = rebuild_nas_index(
                server=self.request.server,
                username=self.request.username,
                password=self.request.password,
                share_root=share_root,
                progress_callback=lambda text: self.progress.emit(text),
            )
            self.finished.emit(rows)
        except Exception as exc:
            self.failed.emit(str(exc))


class QueueWorker(QObject):
    queue_position = Signal(str, str)
    item_progress = Signal(str, int, int)
    item_finished = Signal(str, object)
    failed = Signal(str)
    finished = Signal()

    def __init__(self, request: QueueRequest):
        super().__init__()
        self.request = request

    @Slot()
    def run(self) -> None:
        try:
            share_root = rf"\\{self.request.server}{self.request.server_path}"
            queue_targets = [entry.target for entry in self.request.queue_entries]

            for index, entry in enumerate(self.request.queue_entries):
                next_target = queue_targets[index + 1] if index + 1 < len(queue_targets) else "(none)"
                self.queue_position.emit(entry.target, next_target)

                if entry.action == "pull":
                    result = pull_target_from_nas(
                        server=self.request.server,
                        username=self.request.username,
                        password=self.request.password,
                        share_root=share_root,
                        local_root=self.request.local_path,
                        target=entry.target,
                        include_flats=entry.include_flats,
                        progress_callback=lambda processed, total, _path, t=entry.target: self.item_progress.emit(t, processed, total),
                    )
                elif entry.action == "push":
                    result = push_target_to_nas(
                        server=self.request.server,
                        username=self.request.username,
                        password=self.request.password,
                        share_root=share_root,
                        local_root=self.request.local_path,
                        target=entry.target,
                        include_flats=entry.include_flats,
                        progress_callback=lambda processed, total, _path, t=entry.target: self.item_progress.emit(t, processed, total),
                    )
                elif entry.action == "delete":
                    result = delete_nas_only_to_trash(
                        server=self.request.server,
                        username=self.request.username,
                        password=self.request.password,
                        share_root=share_root,
                        local_root=self.request.local_path,
                        target=entry.target,
                        require_pull_checkpoint=True,
                        progress_callback=lambda processed, total, _path, t=entry.target: self.item_progress.emit(t, processed, total),
                    )
                elif entry.action == "delete_force":
                    result = delete_nas_only_to_trash(
                        server=self.request.server,
                        username=self.request.username,
                        password=self.request.password,
                        share_root=share_root,
                        local_root=self.request.local_path,
                        target=entry.target,
                        require_pull_checkpoint=False,
                        progress_callback=lambda processed, total, _path, t=entry.target: self.item_progress.emit(t, processed, total),
                    )
                elif entry.action == "delete_local":
                    delete_local_target(
                        local_root=self.request.local_path,
                        target=entry.target,
                    )
                    result = PullExecutionResult(
                        target=entry.target,
                        total_files=0,
                        copied_files=0,
                        skipped_files=0,
                        error_files=0,
                        action="delete_local",
                    )
                else:
                    raise ValueError(f"Unsupported queue action: {entry.action}")

                self.item_finished.emit(
                    result.target,
                    result,
                )
        except Exception as exc:
            self.failed.emit(str(exc))
        finally:
            self.finished.emit()


class BackupCompareWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("AP Image Backup Manager")
        self.resize(1100, 700)

        self.app_config = get_app_config()

        self.thread: QThread | None = None
        self.worker: QObject | None = None
        self.queue_running = False

        server_host = self.app_config.get("AP_BACKUP_SERVER", "nasbox.rhino-kitefin.ts.net")
        server_path = self.app_config.get("AP_BACKUP_SERVER_PATH", r"\home\NINA")

        self.server_edit = QLineEdit(server_host)
        self.username_edit = QLineEdit(self.app_config.get("AP_BACKUP_USERNAME", ""))
        self.password_edit = QLineEdit(self.app_config.get("AP_BACKUP_PASSWORD", ""))
        self.password_edit.setEchoMode(QLineEdit.Password)
        self.server_path_edit = QLineEdit(server_path)

        self.local_path_presets = get_local_path_presets(self.app_config)
        self.custom_local_path = normalize_path(self.app_config.get("AP_BACKUP_LOCAL_PATH_CUSTOM", ""))
        self.custom_local_preset_value = "__custom__"

        self.local_path_preset_combo = QComboBox()
        for preset_path in self.local_path_presets:
            self.local_path_preset_combo.addItem(preset_path, preset_path)
        self.local_path_preset_combo.addItem("Custom (Browse)...", self.custom_local_preset_value)
        self.local_path_preset_combo.currentIndexChanged.connect(self.on_local_path_preset_changed)

        default_local_path_raw = self.app_config.get("AP_BACKUP_LOCAL_PATH_DEFAULT", "").strip()
        default_local_path = normalize_path(default_local_path_raw) if default_local_path_raw else ""

        selected_index = 0 if self.local_path_presets else self.local_path_preset_combo.findData(self.custom_local_preset_value)
        if default_local_path_raw.lower() == "custom":
            selected_index = self.local_path_preset_combo.findData(self.custom_local_preset_value)
            if self.custom_local_path:
                self.local_path_edit = QLineEdit(self.custom_local_path)
            elif self.local_path_presets:
                self.local_path_edit = QLineEdit(self.local_path_presets[0])
            else:
                self.local_path_edit = QLineEdit("")
        else:
            if default_local_path:
                for idx, preset_path in enumerate(self.local_path_presets):
                    if preset_path.lower() == default_local_path.lower():
                        selected_index = idx
                        break
                else:
                    selected_index = self.local_path_preset_combo.findData(self.custom_local_preset_value)
                    self.local_path_edit = QLineEdit(default_local_path)
            if not hasattr(self, "local_path_edit"):
                initial_path = self.local_path_presets[selected_index] if self.local_path_presets else self.custom_local_path
                self.local_path_edit = QLineEdit(initial_path)

        if selected_index < 0:
            selected_index = 0
        self.local_path_preset_combo.setCurrentIndex(selected_index)

        self.mode_combo = QComboBox()
        self.mode_combo.addItem("Images (exclude WBPP/Processing)", CompareMode.IMAGES)
        self.mode_combo.addItem("WIP only (WBPP/Processing)", CompareMode.WIP)
        compare_mode_default = self.app_config.get("AP_BACKUP_COMPARE_MODE", "images").strip().lower()
        if compare_mode_default == "wip":
            self.mode_combo.setCurrentIndex(1)

        self.machine_mode_combo = QComboBox()
        self.machine_mode_combo.addItem("Processing PC", "processing")
        self.machine_mode_combo.addItem("Creation PC", "creation")
        self.machine_mode_combo.currentIndexChanged.connect(self.on_machine_mode_changed)
        machine_mode_default = self.app_config.get("AP_BACKUP_MACHINE_MODE", "processing").strip().lower()
        if machine_mode_default == "creation":
            self.machine_mode_combo.setCurrentIndex(1)

        self.only_action_needed_checkbox = QCheckBox("Only show folders needing action")
        self.only_action_needed_checkbox.stateChanged.connect(self.apply_filter)
        self.only_action_needed_checkbox.setChecked(
            config_bool(self.app_config, "AP_BACKUP_ONLY_ACTION_NEEDED", False)
        )

        self.include_flats_checkbox = QCheckBox("Include flats (_FlatWizard) for queued pull actions")
        self.include_flats_checkbox.setChecked(
            config_bool(self.app_config, "AP_BACKUP_INCLUDE_FLATS", False)
        )

        self.include_flats_push_checkbox = QCheckBox("Include flats (_FlatWizard) for queued push actions")
        self.include_flats_push_checkbox.setChecked(False)

        self.status_label = QLabel("Ready.")
        self.summary_label = QLabel("Summary: -")
        self.queue_status_label = QLabel("Queue: idle")
        self.queue_next_label = QLabel("Next: -")

        self.compare_button = QPushButton("Run Comparison")
        self.compare_button.clicked.connect(self.start_comparison)

        self.refresh_status_button = QPushButton("Refresh File Status")
        self.refresh_status_button.clicked.connect(self.start_target_scan)

        self.rebuild_nas_db_button = QPushButton("Rebuild NAS DB")
        self.rebuild_nas_db_button.clicked.connect(self.start_rebuild_nas_db)

        self.start_queue_button = QPushButton("Start Queue")
        self.start_queue_button.clicked.connect(self.start_queue)

        self.remove_queue_item_button = QPushButton("Remove Selected Queue Item")
        self.remove_queue_item_button.clicked.connect(self.remove_selected_queue_item)

        self.clear_queue_button = QPushButton("Clear Queue")
        self.clear_queue_button.clicked.connect(self.clear_queue)

        self.browse_button = QPushButton("Browse...")
        self.browse_button.clicked.connect(self.select_local_folder)

        self.results_table = QTableWidget(0, 6)
        self.results_table.setHorizontalHeaderLabels(
            [
                "Folder",
                "Local Files",
                "Backed Up",
                "Missing On NAS",
                "Different",
                "Safe To Delete",
            ]
        )
        self.results_table.horizontalHeader().setStretchLastSection(True)

        self.pull_table = QTableWidget(0, 11)
        self.pull_table.setHorizontalHeaderLabels(
            [
                "Target Folder",
                "Recent Date",
                "NAS Files",
                "Local Files",
                "Matched",
                "Missing Locally",
                "Local Only",
                "Different",
                "Status",
                "Safe To Delete",
                "Actions",
            ]
        )
        self.pull_table.horizontalHeader().setStretchLastSection(True)

        self.queue_table = QTableWidget(0, 11)
        self.queue_table.setHorizontalHeaderLabels(
            [
                "Target Folder",
                "Action",
                "How Many Files",
                "Status",
                "Progress %",
                "Lights C/S/E",
                "Flats C/S/E",
                "Copied",
                "Skipped",
                "Errors",
                "Skip Log",
            ]
        )
        self.queue_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.queue_table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.queue_table.horizontalHeader().setStretchLastSection(True)
        self.queue_table.cellClicked.connect(self.on_queue_table_cell_clicked)

        root = QWidget()
        self.setCentralWidget(root)

        form_layout = QGridLayout()
        form_layout.addWidget(QLabel("Machine Mode"), 0, 0)
        form_layout.addWidget(self.machine_mode_combo, 0, 1)

        form_layout.addWidget(QLabel("Server"), 1, 0)
        form_layout.addWidget(self.server_edit, 1, 1)
        form_layout.addWidget(QLabel("Server Path"), 1, 2)
        form_layout.addWidget(self.server_path_edit, 1, 3)

        form_layout.addWidget(QLabel("Username"), 2, 0)
        form_layout.addWidget(self.username_edit, 2, 1)
        form_layout.addWidget(QLabel("Password"), 2, 2)
        form_layout.addWidget(self.password_edit, 2, 3)

        form_layout.addWidget(QLabel("Local Path Preset"), 3, 0)
        form_layout.addWidget(self.local_path_preset_combo, 3, 1)

        form_layout.addWidget(QLabel("Local Path"), 4, 0)
        form_layout.addWidget(self.local_path_edit, 4, 1, 1, 2)
        form_layout.addWidget(self.browse_button, 4, 3)

        self.compare_mode_label = QLabel("Compare Mode")
        form_layout.addWidget(self.compare_mode_label, 5, 0)
        form_layout.addWidget(self.mode_combo, 5, 1)

        controls_layout = QHBoxLayout()
        controls_layout.addWidget(self.compare_button)
        controls_layout.addWidget(self.refresh_status_button)
        controls_layout.addWidget(self.rebuild_nas_db_button)
        controls_layout.addWidget(self.only_action_needed_checkbox)
        controls_layout.addWidget(self.include_flats_checkbox)
        controls_layout.addWidget(self.include_flats_push_checkbox)
        controls_layout.addStretch(1)

        queue_controls_layout = QHBoxLayout()
        queue_controls_layout.addWidget(self.start_queue_button)
        queue_controls_layout.addWidget(self.remove_queue_item_button)
        queue_controls_layout.addWidget(self.clear_queue_button)
        queue_controls_layout.addStretch(1)

        layout = QVBoxLayout(root)
        layout.addLayout(form_layout)
        layout.addLayout(controls_layout)

        self.capture_section_label = QLabel("Capture Machine View: Local → NAS safety (review later)")
        layout.addWidget(self.capture_section_label)
        layout.addWidget(self.summary_label)
        layout.addWidget(self.results_table)

        self.processing_section_label = QLabel("Folders needing upload to NAS")
        layout.addWidget(self.processing_section_label)
        layout.addWidget(self.pull_table)
        layout.addLayout(queue_controls_layout)
        self.queue_section_label = QLabel("Action queue")
        layout.addWidget(self.queue_section_label)
        layout.addWidget(self.queue_status_label)
        layout.addWidget(self.queue_next_label)
        layout.addWidget(self.queue_table)
        layout.addWidget(self.status_label)

        self._all_results = []
        self._all_target_results = []
        self._queue_entries: list[QueueEntry] = []

        self.on_machine_mode_changed()

    @Slot()
    def on_machine_mode_changed(self) -> None:
        current_mode = self.machine_mode_combo.currentData()
        processing_mode = current_mode == "processing"

        self.compare_button.setVisible(False)
        self.compare_mode_label.setVisible(not processing_mode)
        self.mode_combo.setVisible(not processing_mode)
        self.capture_section_label.setVisible(False)
        self.results_table.setVisible(False)

        self.refresh_status_button.setVisible(True)
        self.rebuild_nas_db_button.setVisible(processing_mode)
        self.include_flats_checkbox.setVisible(processing_mode)
        self.include_flats_push_checkbox.setVisible(not processing_mode)
        self.only_action_needed_checkbox.setVisible(processing_mode)
        self.processing_section_label.setVisible(True)
        self.pull_table.setVisible(True)
        self.start_queue_button.setVisible(True)
        self.remove_queue_item_button.setVisible(True)
        self.clear_queue_button.setVisible(True)
        self.queue_section_label.setVisible(True)
        self.queue_status_label.setVisible(True)
        self.queue_next_label.setVisible(True)
        self.queue_table.setVisible(True)

        if processing_mode:
            self.processing_section_label.setText("Processing Machine View: NAS → Local pull candidates + actions")
        else:
            self.processing_section_label.setText("Folders needing upload to NAS")
        self.populate_target_table(self._all_target_results)

    @Slot()
    def select_local_folder(self) -> None:
        selected = QFileDialog.getExistingDirectory(self, "Select Local Path", self.local_path_edit.text())
        if selected:
            self.local_path_edit.setText(selected)
            custom_index = self.local_path_preset_combo.findData(self.custom_local_preset_value)
            if custom_index >= 0:
                self.local_path_preset_combo.setCurrentIndex(custom_index)

    @Slot()
    def on_local_path_preset_changed(self) -> None:
        selected_value = self.local_path_preset_combo.currentData()
        if selected_value == self.custom_local_preset_value:
            return
        if isinstance(selected_value, str):
            self.local_path_edit.setText(selected_value)

    @Slot()
    def start_comparison(self) -> None:
        if self.queue_running:
            QMessageBox.warning(self, "Queue running", "Wait for the queue to finish before running comparison.")
            return

        server = self.server_edit.text().strip()
        username = self.username_edit.text().strip()
        password = self.password_edit.text()
        server_path = self.server_path_edit.text().strip()
        local_path = self.local_path_edit.text().strip()

        if not server or not username or not password or not server_path or not local_path:
            QMessageBox.warning(self, "Missing fields", "Please provide server, username, password, server path, and local path.")
            return

        if not os.path.exists(local_path):
            QMessageBox.warning(self, "Invalid local path", "The selected local path does not exist.")
            return

        selected_mode = self.mode_combo.currentData()
        if not isinstance(selected_mode, CompareMode):
            QMessageBox.warning(self, "Invalid mode", "Please select a valid compare mode.")
            return

        request = CompareRequest(
            server=server,
            username=username,
            password=password,
            server_path=server_path,
            local_path=local_path,
            mode=selected_mode,
        )

        self.set_busy_state(True)
        self.status_label.setText("Comparing local files to NAS...")
        self.summary_label.setText("Summary: running...")

        self.thread = QThread()
        self.worker = CompareWorker(request)
        self.worker.moveToThread(self.thread)

        self.thread.started.connect(self.worker.run)
        self.worker.progress.connect(self.on_progress)
        self.worker.finished.connect(self.on_finished)
        self.worker.failed.connect(self.on_failed)

        self.worker.finished.connect(self.thread.quit)
        self.worker.failed.connect(self.thread.quit)
        self.thread.finished.connect(self.thread.deleteLater)

        self.thread.start()

    @Slot()
    def start_target_scan(self) -> None:
        if self.queue_running:
            QMessageBox.warning(self, "Queue running", "Wait for the queue to finish before refreshing file status.")
            return

        server = self.server_edit.text().strip()
        username = self.username_edit.text().strip()
        password = self.password_edit.text()
        server_path = self.server_path_edit.text().strip()
        local_path = self.local_path_edit.text().strip()

        if not server or not username or not password or not server_path or not local_path:
            QMessageBox.warning(self, "Missing fields", "Please provide server, username, password, server path, and local path.")
            return

        if not os.path.exists(local_path):
            QMessageBox.warning(self, "Invalid local path", "The selected local path does not exist.")
            return

        request = TargetScanRequest(
            server=server,
            username=username,
            password=password,
            server_path=server_path,
            local_path=local_path,
            force_rebuild_nas_db=False,
        )

        self.set_busy_state(True)
        self.status_label.setText("Scanning NAS targets for pull candidates...")

        self.thread = QThread()
        self.worker = TargetScanWorker(request)
        self.worker.moveToThread(self.thread)

        self.thread.started.connect(self.worker.run)
        self.worker.progress.connect(self.on_target_scan_progress)
        self.worker.finished.connect(self.on_target_scan_finished)
        self.worker.failed.connect(self.on_failed)

        self.worker.finished.connect(self.thread.quit)
        self.worker.failed.connect(self.thread.quit)
        self.thread.finished.connect(self.thread.deleteLater)

        self.thread.start()

    @Slot()
    def start_rebuild_nas_db(self) -> None:
        if self.queue_running:
            QMessageBox.warning(self, "Queue running", "Wait for the queue to finish before rebuilding NAS DB.")
            return

        server = self.server_edit.text().strip()
        username = self.username_edit.text().strip()
        password = self.password_edit.text()
        server_path = self.server_path_edit.text().strip()
        local_path = self.local_path_edit.text().strip()

        if not server or not username or not password or not server_path or not local_path:
            QMessageBox.warning(self, "Missing fields", "Please provide server, username, password, server path, and local path.")
            return

        confirm = QMessageBox.question(
            self,
            "Rebuild NAS DB",
            "Rebuild NAS DB now? This rescans the NAS share and may take a while.",
        )
        if confirm != QMessageBox.Yes:
            return

        request = TargetScanRequest(
            server=server,
            username=username,
            password=password,
            server_path=server_path,
            local_path=local_path,
            force_rebuild_nas_db=True,
        )

        self.set_busy_state(True)
        self.status_label.setText("Rebuilding NAS DB...")

        self.thread = QThread()
        self.worker = RebuildNasDbWorker(request)
        self.worker.moveToThread(self.thread)

        self.thread.started.connect(self.worker.run)
        self.worker.progress.connect(self.on_target_scan_progress)
        self.worker.finished.connect(self.on_rebuild_nas_db_finished)
        self.worker.failed.connect(self.on_failed)

        self.worker.finished.connect(self.thread.quit)
        self.worker.failed.connect(self.thread.quit)
        self.thread.finished.connect(self.thread.deleteLater)

        self.thread.start()

    @Slot(str)
    def on_progress(self, file_path: str) -> None:
        self.status_label.setText(f"Comparing: {file_path}")

    @Slot(str)
    def on_target_scan_progress(self, target: str) -> None:
        self.status_label.setText(f"Scanning target: {target}")

    @Slot(int)
    def on_rebuild_nas_db_finished(self, rows: int) -> None:
        self.status_label.setText("Rebuild NAS DB complete.")
        self.summary_label.setText(f"Summary: Rebuilt NAS DB with {rows} indexed file(s)")
        self.set_busy_state(False)

    @Slot(list, object)
    def on_finished(self, results, summary) -> None:
        self._all_results = results
        self.populate_table(results)

        safe_folders = sum(1 for result in results if result.safe_to_delete)
        self.summary_label.setText(
            f"Summary: Local={summary.total_local_files}, BackedUp={summary.total_backed_up_files}, "
            f"Missing={summary.total_missing_on_nas_files}, Different={summary.total_different_files}, "
            f"SafeFolders={safe_folders}/{len(results)}"
        )
        self.status_label.setText("Comparison complete. Read-only mode made no file changes.")
        self.set_busy_state(False)

    @Slot(list, object)
    def on_target_scan_finished(self, results, summary) -> None:
        self._all_target_results = results
        self.populate_target_table(results)

        if self.machine_mode_combo.currentData() == "processing":
            self.summary_label.setText(
                f"Summary: Targets={summary.total_targets}, PullCandidates={summary.pull_candidates}, "
                f"UpToDate={summary.up_to_date_targets}"
            )
            self.status_label.setText("NAS pull-candidate scan complete. Read-only mode made no file changes.")
        else:
            upload_candidates = sum(1 for result in results if (result.local_only_files > 0 or result.different_files > 0))
            self.summary_label.setText(
                f"Summary: Targets={summary.total_targets}, NeedUpload={upload_candidates}"
            )
            self.status_label.setText("Scan complete. Queue Push for folders with upload differences.")
        self.set_busy_state(False)

    @Slot(str)
    def on_failed(self, message: str) -> None:
        self.status_label.setText("Comparison failed.")
        self.summary_label.setText("Summary: error")
        self.set_busy_state(False)
        QMessageBox.critical(self, "Comparison failed", message)

    @Slot()
    def apply_filter(self) -> None:
        if self.only_action_needed_checkbox.isChecked():
            filtered = [
                result
                for result in self._all_results
                if (result.missing_on_nas_files > 0 or result.different_files > 0)
            ]
            self.populate_table(filtered)

            target_filtered = [
                result
                for result in self._all_target_results
                if result.recommended_action != "No action"
            ]
            self.populate_target_table(target_filtered)
            return
        self.populate_table(self._all_results)
        self.populate_target_table(self._all_target_results)

    def populate_table(self, results) -> None:
        self.results_table.setRowCount(0)

        for row_index, result in enumerate(results):
            self.results_table.insertRow(row_index)
            self.results_table.setItem(row_index, 0, QTableWidgetItem(result.folder))
            self.results_table.setItem(row_index, 1, QTableWidgetItem(str(result.local_files)))
            self.results_table.setItem(row_index, 2, QTableWidgetItem(str(result.backed_up_files)))
            self.results_table.setItem(row_index, 3, QTableWidgetItem(str(result.missing_on_nas_files)))
            self.results_table.setItem(row_index, 4, QTableWidgetItem(str(result.different_files)))
            self.results_table.setItem(row_index, 5, QTableWidgetItem("Yes" if result.safe_to_delete else "No"))

        self.results_table.resizeColumnsToContents()

    def _build_action_preview_lines(self, result, action: str, include_flats: bool = False) -> list[str]:
        action_titles = {
            "pull": "Pull from NAS",
            "push": "Push to NAS",
            "delete": "Delete on NAS (_Trash)",
            "delete_force": "Delete on NAS (_Trash, force)",
            "delete_local": "Delete Local Folder",
        }
        action_title = action_titles.get(action, action)

        detail_lines = [
            f"Target: {result.target}",
            f"Action: {action_title}",
            "",
            "Current status snapshot:",
            f"- Status: {result.status}",
            f"- NAS files: {result.nas_files}",
            f"- Local files: {result.local_files}",
            f"- Missing locally: {result.missing_locally_files}",
            f"- Local only: {result.local_only_files}",
            f"- Different: {result.different_files}",
        ]

        if action == "pull":
            detail_lines.extend(
                [
                    "",
                    "Pull behavior:",
                    f"- Includes flats: {'Yes' if include_flats else 'No'}",
                    "- Copies NAS files into local target for this folder.",
                ]
            )
        elif action == "push":
            detail_lines.extend(
                [
                    "",
                    "Push behavior:",
                    "- Copies local files for this target up to NAS.",
                    "- Existing NAS files with same size+mtime are skipped.",
                ]
            )
        elif action in ("delete", "delete_force"):
            detail_lines.extend(
                [
                    "",
                    "Delete behavior:",
                    "- Moves NAS-only files to _Trash (does not hard-delete).",
                ]
            )
            if action == "delete":
                detail_lines.append("- Requires a successful pull checkpoint for safety.")
            else:
                detail_lines.append("- Ignores pull checkpoint safety requirement.")

        return detail_lines

    def _build_action_preview_with_files(
        self,
        result,
        action: str,
        include_flats: bool,
        candidate_files: list[str],
    ) -> list[str]:
        detail_lines = self._build_action_preview_lines(result, action, include_flats)
        detail_lines.extend(["", f"Files to be acted on ({len(candidate_files)}):"])
        if candidate_files:
            detail_lines.extend(candidate_files)
        else:
            detail_lines.append("(none)")
        return detail_lines

    def _estimate_action_files(self, result, action: str) -> int:
        if action == "pull":
            return int(result.missing_locally_files) + int(result.different_files)
        if action == "push":
            return int(result.local_only_files) + int(result.different_files)
        if action in ("delete", "delete_force"):
            return int(result.missing_locally_files)
        return -1

    def confirm_queue_action(
        self,
        result,
        action: str,
        include_flats: bool = False,
        preview_text: str | None = None,
    ) -> bool:
        action_titles = {
            "pull": "Pull from NAS",
            "push": "Push to NAS",
            "delete": "Delete on NAS (_Trash)",
            "delete_force": "Delete on NAS (_Trash, force)",
            "delete_local": "Delete Local Folder",
        }
        action_title = action_titles.get(action, action)
        if preview_text is None:
            detail_lines = self._build_action_preview_lines(result, action, include_flats)
        else:
            detail_lines = preview_text.splitlines()

        dialog = QDialog(self)
        dialog.setWindowTitle("Confirm Queue Action")
        dialog.resize(680, 420)

        layout = QVBoxLayout(dialog)
        title_label = QLabel(f"Queue {action_title} for '{result.target}'?")
        layout.addWidget(title_label)

        details_box = QTextEdit()
        details_box.setReadOnly(True)
        details_box.setPlainText("\n".join(detail_lines))
        layout.addWidget(details_box)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        queue_button = buttons.button(QDialogButtonBox.Ok)
        if queue_button is not None:
            queue_button.setText("Queue Action")
        buttons.accepted.connect(dialog.accept)
        buttons.rejected.connect(dialog.reject)
        layout.addWidget(buttons)

        return dialog.exec() == QDialog.Accepted

    def confirm_and_add_target_to_queue(self, result, action: str, include_flats: bool = False) -> None:
        server = self.server_edit.text().strip()
        username = self.username_edit.text().strip()
        password = self.password_edit.text()
        server_path = self.server_path_edit.text().strip()
        local_path = self.local_path_edit.text().strip()

        if not server or not username or not password or not server_path or not local_path:
            QMessageBox.warning(self, "Missing fields", "Please provide server, username, password, server path, and local path.")
            return

        share_root = rf"\\{server}{server_path}"
        try:
            candidate_files = list_action_candidate_files(
                server=server,
                username=username,
                password=password,
                share_root=share_root,
                local_root=local_path,
                target=result.target,
                action=action,
                include_flats=include_flats,
                require_pull_checkpoint=True,
            )
        except Exception as exc:
            QMessageBox.warning(self, "Preview failed", f"Could not generate action file preview:\n{exc}")
            return

        preview_text = "\n".join(self._build_action_preview_with_files(result, action, include_flats, candidate_files))
        if not self.confirm_queue_action(
            result=result,
            action=action,
            include_flats=include_flats,
            preview_text=preview_text,
        ):
            return
        self.add_target_to_queue(
            target=result.target,
            include_flats=include_flats,
            action=action,
            estimated_files=len(candidate_files),
            preview_text=preview_text,
        )

    def populate_target_table(self, results) -> None:
        self.pull_table.setRowCount(0)
        queued_targets = {entry.target for entry in self._queue_entries}
        processing_mode = self.machine_mode_combo.currentData() == "processing"
        display_results = (
            results
            if processing_mode
            else [result for result in results if (result.local_only_files > 0 or result.different_files > 0)]
        )

        for row_index, result in enumerate(display_results):
            self.pull_table.insertRow(row_index)
            self.pull_table.setItem(row_index, 0, QTableWidgetItem(result.target))
            self.pull_table.setItem(row_index, 1, QTableWidgetItem(result.recent_date))
            self.pull_table.setItem(row_index, 2, QTableWidgetItem(str(result.nas_files)))
            self.pull_table.setItem(row_index, 3, QTableWidgetItem(str(result.local_files)))
            self.pull_table.setItem(row_index, 4, QTableWidgetItem(str(result.matched_files)))
            self.pull_table.setItem(row_index, 5, QTableWidgetItem(str(result.missing_locally_files)))
            self.pull_table.setItem(row_index, 6, QTableWidgetItem(str(result.local_only_files)))
            self.pull_table.setItem(row_index, 7, QTableWidgetItem(str(result.different_files)))
            self.pull_table.setItem(row_index, 8, QTableWidgetItem(result.status))
            self.pull_table.setItem(row_index, 9, QTableWidgetItem("Yes" if result.safe_to_delete else "No"))

            action_widget = QWidget()
            action_layout = QHBoxLayout(action_widget)
            action_layout.setContentsMargins(0, 0, 0, 0)

            is_queued = result.target in queued_targets

            if processing_mode:
                queue_pull_button = QPushButton("Queue Pull")
                queue_pull_button.setEnabled((result.missing_locally_files > 0 or result.different_files > 0) and not is_queued)
                queue_pull_button.clicked.connect(
                    lambda _checked=False, row_result=result: self.confirm_and_add_target_to_queue(
                        row_result,
                        action="pull",
                        include_flats=self.include_flats_checkbox.isChecked(),
                    )
                )
                action_layout.addWidget(queue_pull_button)

            queue_push_button = QPushButton("Queue Push")
            if processing_mode:
                queue_push_button.setEnabled(result.local_only_files > 0 and not is_queued)
            else:
                queue_push_button.setEnabled((result.local_only_files > 0 or result.different_files > 0) and not is_queued)
            queue_push_button.clicked.connect(
                lambda _checked=False, row_result=result: self.confirm_and_add_target_to_queue(
                    row_result,
                    action="push",
                    include_flats=self.include_flats_push_checkbox.isChecked(),
                )
            )
            action_layout.addWidget(queue_push_button)

            if processing_mode:
                queue_delete_button = QPushButton("Queue Delete")
                queue_delete_button.setEnabled(result.missing_locally_files > 0 and not is_queued)
                queue_delete_button.clicked.connect(
                    lambda _checked=False, row_result=result: self.confirm_and_add_target_to_queue(
                        row_result,
                        action="delete",
                        include_flats=False,
                    )
                )
                action_layout.addWidget(queue_delete_button)
            else:
                queue_delete_local_button = QPushButton("Queue Delete Local")
                queue_delete_local_button.setEnabled(result.safe_to_delete and not is_queued)
                queue_delete_local_button.clicked.connect(
                    lambda _checked=False, row_result=result: self.confirm_and_add_target_to_queue(
                        row_result,
                        action="delete_local",
                        include_flats=False,
                    )
                )
                action_layout.addWidget(queue_delete_local_button)

            self.pull_table.setCellWidget(row_index, 10, action_widget)

        self.pull_table.resizeColumnsToContents()

    def set_busy_state(self, busy: bool) -> None:
        if self.queue_running:
            self.compare_button.setEnabled(False)
            self.refresh_status_button.setEnabled(False)
            self.rebuild_nas_db_button.setEnabled(False)
            self.start_queue_button.setEnabled(False)
            self.remove_queue_item_button.setEnabled(False)
            self.clear_queue_button.setEnabled(False)
            self.only_action_needed_checkbox.setEnabled(False)
            self.include_flats_checkbox.setEnabled(False)
            self.include_flats_push_checkbox.setEnabled(False)
            return

        self.compare_button.setEnabled(not busy)
        self.refresh_status_button.setEnabled(not busy)
        self.rebuild_nas_db_button.setEnabled(not busy)
        self.start_queue_button.setEnabled(not busy)
        self.remove_queue_item_button.setEnabled(not busy)
        self.clear_queue_button.setEnabled(not busy)
        self.only_action_needed_checkbox.setEnabled(not busy)
        self.include_flats_checkbox.setEnabled(not busy)
        self.include_flats_push_checkbox.setEnabled(not busy)

    def add_target_to_queue(
        self,
        target: str,
        include_flats: bool | None = None,
        action: str | None = None,
        estimated_files: int | None = None,
        preview_text: str | None = None,
    ) -> None:
        if self.queue_running:
            QMessageBox.warning(self, "Queue running", "Cannot modify queue while it is running.")
            return

        if include_flats is None:
            include_flats = self.include_flats_checkbox.isChecked()

        if action is None:
            action_key = "pull"
        else:
            action_key = action

        existing_entry = next((entry for entry in self._queue_entries if entry.target == target), None)
        if existing_entry is not None:
            existing_entry.include_flats = include_flats
            existing_entry.action = action_key
            if estimated_files is not None:
                existing_entry.estimated_files = int(estimated_files)
            if preview_text is not None:
                existing_entry.preview_text = preview_text
            self.populate_queue_table()
            self.populate_target_table(self._all_target_results)
            QMessageBox.information(
                self,
                "Queue updated",
                f"{target} was already queued. Action={action_key}, Include flats={'Yes' if include_flats else 'No'}.",
            )
            return

        self._queue_entries.append(
            QueueEntry(
                target=target,
                include_flats=include_flats,
                action=action_key,
                estimated_files=int(estimated_files) if estimated_files is not None else -1,
                preview_text=preview_text or "",
            )
        )
        self.populate_queue_table()
        self.populate_target_table(self._all_target_results)
        self.queue_status_label.setText(f"Queue: {len(self._queue_entries)} item(s) queued")
        self.status_label.setText(
            f"Queued {target}: action={action_key}, {'with flats' if include_flats else 'without flats'}"
        )

    @Slot()
    def remove_selected_queue_item(self) -> None:
        if self.queue_running:
            QMessageBox.warning(self, "Queue running", "Cannot modify queue while it is running.")
            return

        selected_rows = self.queue_table.selectionModel().selectedRows()
        if not selected_rows:
            return

        row_index = selected_rows[0].row()
        if 0 <= row_index < len(self._queue_entries):
            self._queue_entries.pop(row_index)
            self.populate_queue_table()
            self.populate_target_table(self._all_target_results)
            self.queue_status_label.setText(f"Queue: {len(self._queue_entries)} item(s) queued")

    @Slot()
    def clear_queue(self) -> None:
        if self.queue_running:
            QMessageBox.warning(self, "Queue running", "Cannot clear queue while it is running.")
            return

        self._queue_entries = []
        self.populate_queue_table()
        self.populate_target_table(self._all_target_results)
        self.queue_status_label.setText("Queue: idle")
        self.queue_next_label.setText("Next: -")

    @Slot()
    def start_queue(self) -> None:
        if self.queue_running:
            QMessageBox.warning(self, "Queue running", "Queue is already running.")
            return

        if not self._queue_entries:
            QMessageBox.warning(self, "Queue empty", "Add at least one target to the queue first.")
            return

        server = self.server_edit.text().strip()
        username = self.username_edit.text().strip()
        password = self.password_edit.text()
        server_path = self.server_path_edit.text().strip()
        local_path = self.local_path_edit.text().strip()

        if not server or not username or not password or not server_path or not local_path:
            QMessageBox.warning(self, "Missing fields", "Please provide server, username, password, server path, and local path.")
            return

        if not os.path.exists(local_path):
            QMessageBox.warning(self, "Invalid local path", "The selected local path does not exist.")
            return

        if self.machine_mode_combo.currentData() == "creation":
            non_push_actions = [entry.target for entry in self._queue_entries if entry.action != "push"]
            if non_push_actions:
                QMessageBox.warning(
                    self,
                    "Push-only queue",
                    "Creation mode queue is push-only. Remove any non-push entries first.",
                )
                return

        pull_count = sum(1 for entry in self._queue_entries if entry.action == "pull")
        flats_count = sum(1 for entry in self._queue_entries if entry.action == "pull" and entry.include_flats)
        if self.machine_mode_combo.currentData() == "creation":
            prompt = (
                f"Start queue with {len(self._queue_entries)} target(s)?\n"
                "Action: Push to NAS"
            )
        else:
            prompt = (
                f"Start queue with {len(self._queue_entries)} target(s)?\n"
                f"Pull actions: {pull_count} (include flats on {flats_count})."
            )
        confirm = QMessageBox.question(self, "Start queue", prompt)
        if confirm != QMessageBox.Yes:
            return

        for entry in self._queue_entries:
            entry.status = "Queued"
            entry.progress_percent = 0
            entry.copied_files = 0
            entry.skipped_files = 0
            entry.error_files = 0
            entry.lights_counts = "0/0/0"
            entry.flats_counts = "0/0/0"
            entry.total_files = 0
            entry.skip_log_path = ""

        request = QueueRequest(
            server=server,
            username=username,
            password=password,
            server_path=server_path,
            local_path=local_path,
            queue_entries=[QueueEntry(**entry.__dict__) for entry in self._queue_entries],
        )

        self.queue_running = True
        self.set_busy_state(True)
        self.status_label.setText("Running queue...")
        self.queue_status_label.setText("Queue: running")
        self.populate_queue_table()

        self.thread = QThread()
        self.worker = QueueWorker(request)
        self.worker.moveToThread(self.thread)

        self.thread.started.connect(self.worker.run)
        self.worker.queue_position.connect(self.on_queue_position)
        self.worker.item_progress.connect(self.on_queue_item_progress)
        self.worker.item_finished.connect(self.on_queue_item_finished)
        self.worker.failed.connect(self.on_failed)
        self.worker.finished.connect(self.on_queue_finished)

        self.worker.finished.connect(self.thread.quit)
        self.thread.finished.connect(self.thread.deleteLater)

        self.thread.start()

    @Slot(str, str)
    def on_queue_position(self, current_target: str, next_target: str) -> None:
        for entry in self._queue_entries:
            if entry.target == current_target:
                entry.status = "Running"
                break

        self.queue_status_label.setText(f"Queue: running {current_target}")
        self.queue_next_label.setText(f"Next: {next_target}")
        self.populate_queue_table()

    @Slot(str, int, int)
    def on_queue_item_progress(self, target: str, processed: int, total: int) -> None:
        action_label = "Processing"
        for entry in self._queue_entries:
            if entry.target == target:
                entry.total_files = total
                if total > 0:
                    entry.progress_percent = int((processed / total) * 100)
                else:
                    entry.progress_percent = 100
                if entry.action == "pull":
                    action_label = "Pulling"
                elif entry.action == "push":
                    action_label = "Pushing"
                elif entry.action in ("delete", "delete_force"):
                    action_label = "Deleting"
                break

        self.status_label.setText(f"{action_label} {target}: {processed}/{total}")
        self.populate_queue_table()

    @Slot(str, object)
    def on_queue_item_finished(self, target: str, result) -> None:
        for entry in self._queue_entries:
            if entry.target == target:
                entry.total_files = int(result.total_files)
                entry.copied_files = int(result.copied_files)
                entry.skipped_files = int(result.skipped_files)
                entry.error_files = int(result.error_files)
                entry.lights_counts = (
                    f"{int(result.lights_copied_files)}/{int(result.lights_skipped_files)}/{int(result.lights_error_files)}"
                )
                entry.flats_counts = (
                    f"{int(result.flats_copied_files)}/{int(result.flats_skipped_files)}/{int(result.flats_error_files)}"
                )
                entry.skip_log_path = str(result.skip_log_path or "")
                entry.progress_percent = 100
                entry.status = "Done" if int(result.error_files) == 0 else "Done with errors"
                if entry.action == "pull" and int(result.error_files) == 0:
                    mark_target_pulled(self.local_path_edit.text().strip(), target)
                break

        self.populate_queue_table()

    @Slot()
    def on_queue_finished(self) -> None:
        self.queue_running = False
        self.set_busy_state(False)
        self.queue_status_label.setText("Queue: finished")
        self.queue_next_label.setText("Next: (none)")
        self.status_label.setText("Queue finished.")

    def populate_queue_table(self) -> None:
        self.queue_table.setRowCount(0)

        action_labels = {
            "pull": "Pull",
            "push": "Push",
            "delete": "Delete",
            "delete_force": "Delete (Force)",
            "delete_local": "Delete Local",
        }

        for row_index, entry in enumerate(self._queue_entries):
            self.queue_table.insertRow(row_index)
            self.queue_table.setItem(row_index, 0, QTableWidgetItem(entry.target))
            action_text = action_labels.get(entry.action, entry.action)
            if entry.action == "pull":
                action_text = f"{action_text} ({'Flats' if entry.include_flats else 'No Flats'})"
            self.queue_table.setItem(row_index, 1, QTableWidgetItem(action_text))

            if entry.total_files > 0:
                files_text = str(entry.total_files)
            elif entry.estimated_files >= 0:
                files_text = f"~{entry.estimated_files}"
            else:
                files_text = "-"

            files_item = QTableWidgetItem(files_text)
            self.queue_table.setItem(row_index, 2, files_item)

            self.queue_table.setItem(row_index, 3, QTableWidgetItem(entry.status))
            self.queue_table.setItem(row_index, 4, QTableWidgetItem(str(entry.progress_percent)))
            self.queue_table.setItem(row_index, 5, QTableWidgetItem(entry.lights_counts))
            self.queue_table.setItem(row_index, 6, QTableWidgetItem(entry.flats_counts))
            self.queue_table.setItem(row_index, 7, QTableWidgetItem(str(entry.copied_files)))
            self.queue_table.setItem(row_index, 8, QTableWidgetItem(str(entry.skipped_files)))
            self.queue_table.setItem(row_index, 9, QTableWidgetItem(str(entry.error_files)))

            if entry.skip_log_path:
                open_log_button = QPushButton("Open Log")
                open_log_button.setEnabled(os.path.exists(entry.skip_log_path))

                def _open_log(path: str) -> None:
                    if os.path.exists(path):
                        os.startfile(path)
                    else:
                        QMessageBox.warning(self, "Log missing", f"Skip log file not found: {path}")

                open_log_button.clicked.connect(
                    lambda _checked=False, log_path=entry.skip_log_path: _open_log(log_path)
                )
                self.queue_table.setCellWidget(row_index, 10, open_log_button)

        self.queue_table.resizeColumnsToContents()

    @Slot(int, int)
    def on_queue_table_cell_clicked(self, row: int, column: int) -> None:
        how_many_files_column = 2
        if column != how_many_files_column:
            return
        if not (0 <= row < len(self._queue_entries)):
            return

        preview_text = self._queue_entries[row].preview_text
        if not preview_text:
            return

        self.show_preview_action_dialog(preview_text)

    def show_preview_action_dialog(self, preview_text: str) -> None:
        dialog = QDialog(self)
        dialog.setWindowTitle("Preview Action")
        dialog.resize(760, 520)

        layout = QVBoxLayout(dialog)

        details_box = QTextEdit()
        details_box.setReadOnly(True)
        details_box.setPlainText(preview_text)
        layout.addWidget(details_box)

        buttons = QDialogButtonBox(QDialogButtonBox.Close)
        close_button = buttons.button(QDialogButtonBox.Close)
        if close_button is not None:
            close_button.setText("Close")
        buttons.rejected.connect(dialog.reject)
        buttons.accepted.connect(dialog.accept)
        layout.addWidget(buttons)

        dialog.exec()


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = BackupCompareWindow()
    window.show()
    sys.exit(app.exec())
