from __future__ import annotations

import os
import sys
from dataclasses import dataclass

from PySide6.QtCore import QObject, QThread, Signal, Slot
from PySide6.QtWidgets import (
    QApplication,
    QAbstractItemView,
    QCheckBox,
    QComboBox,
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
    QVBoxLayout,
    QWidget,
)

from compare_engine import (
    CompareMode,
    compare_local_to_nas,
    delete_nas_only_to_trash,
    mark_target_pulled,
    pull_target_from_nas,
    push_target_to_nas,
    rebuild_nas_index,
    scan_nas_pull_candidates,
)


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

        self.thread: QThread | None = None
        self.worker: QObject | None = None
        self.queue_running = False

        self.server_edit = QLineEdit("nasbox")
        self.username_edit = QLineEdit("apBackup")
        self.password_edit = QLineEdit()
        self.password_edit.setEchoMode(QLineEdit.Password)
        self.server_path_edit = QLineEdit(r"\home\NINA")

        self.home_local_path = os.path.join(os.path.expanduser("~"), "Pictures", "NINA")
        self.alt_local_path = r"D:\Pictures\NINA"
        self.custom_local_preset_value = "__custom__"

        self.local_path_preset_combo = QComboBox()
        self.local_path_preset_combo.addItem("Home Pictures\\NINA", self.home_local_path)
        self.local_path_preset_combo.addItem("D:\\Pictures\\NINA", self.alt_local_path)
        self.local_path_preset_combo.addItem("Custom (Browse)...", self.custom_local_preset_value)
        self.local_path_preset_combo.currentIndexChanged.connect(self.on_local_path_preset_changed)

        self.local_path_edit = QLineEdit(self.home_local_path)

        self.mode_combo = QComboBox()
        self.mode_combo.addItem("Images (exclude WBPP/Processing)", CompareMode.IMAGES)
        self.mode_combo.addItem("WIP only (WBPP/Processing)", CompareMode.WIP)

        self.machine_mode_combo = QComboBox()
        self.machine_mode_combo.addItem("Processing PC", "processing")
        self.machine_mode_combo.addItem("Creation PC", "creation")
        self.machine_mode_combo.currentIndexChanged.connect(self.on_machine_mode_changed)

        self.only_action_needed_checkbox = QCheckBox("Only show folders needing action")
        self.only_action_needed_checkbox.stateChanged.connect(self.apply_filter)

        self.include_flats_checkbox = QCheckBox("Include flats (_FlatWizard) for queued pull actions")

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

        self.pull_table = QTableWidget(0, 10)
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
                "Actions",
            ]
        )
        self.pull_table.horizontalHeader().setStretchLastSection(True)

        self.queue_table = QTableWidget(0, 10)
        self.queue_table.setHorizontalHeaderLabels(
            [
                "Target Folder",
                "Include Flats",
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

        self.processing_section_label = QLabel("Processing Machine View: NAS → Local pull candidates + actions")
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

        self.machine_mode_combo.setCurrentIndex(0)
        self.on_machine_mode_changed()

    @Slot()
    def on_machine_mode_changed(self) -> None:
        current_mode = self.machine_mode_combo.currentData()
        processing_mode = current_mode == "processing"

        self.compare_button.setVisible(not processing_mode)
        self.compare_mode_label.setVisible(not processing_mode)
        self.mode_combo.setVisible(not processing_mode)
        self.capture_section_label.setVisible(not processing_mode)
        self.results_table.setVisible(not processing_mode)

        self.refresh_status_button.setVisible(processing_mode)
        self.rebuild_nas_db_button.setVisible(processing_mode)
        self.include_flats_checkbox.setVisible(processing_mode)
        self.only_action_needed_checkbox.setVisible(processing_mode)
        self.processing_section_label.setVisible(processing_mode)
        self.pull_table.setVisible(processing_mode)
        self.start_queue_button.setVisible(processing_mode)
        self.remove_queue_item_button.setVisible(processing_mode)
        self.clear_queue_button.setVisible(processing_mode)
        self.queue_section_label.setVisible(processing_mode)
        self.queue_status_label.setVisible(processing_mode)
        self.queue_next_label.setVisible(processing_mode)
        self.queue_table.setVisible(processing_mode)

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

        self.summary_label.setText(
            f"Summary: Targets={summary.total_targets}, PullCandidates={summary.pull_candidates}, "
            f"UpToDate={summary.up_to_date_targets}"
        )
        self.status_label.setText("NAS pull-candidate scan complete. Read-only mode made no file changes.")
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

    def populate_target_table(self, results) -> None:
        self.pull_table.setRowCount(0)
        queued_targets = {entry.target for entry in self._queue_entries}

        for row_index, result in enumerate(results):
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

            action_widget = QWidget()
            action_layout = QHBoxLayout(action_widget)
            action_layout.setContentsMargins(0, 0, 0, 0)

            is_queued = result.target in queued_targets

            queue_pull_button = QPushButton("Queue Pull")
            queue_pull_button.setEnabled((result.missing_locally_files > 0 or result.different_files > 0) and not is_queued)
            queue_pull_button.clicked.connect(
                lambda _checked=False, target=result.target: self.add_target_to_queue(
                    target,
                    include_flats=self.include_flats_checkbox.isChecked(),
                    action="pull",
                )
            )

            queue_push_button = QPushButton("Queue Push")
            queue_push_button.setEnabled(result.local_only_files > 0 and not is_queued)
            queue_push_button.clicked.connect(
                lambda _checked=False, target=result.target: self.add_target_to_queue(
                    target,
                    include_flats=False,
                    action="push",
                )
            )

            queue_delete_button = QPushButton("Queue Delete")
            queue_delete_button.setEnabled(result.missing_locally_files > 0 and not is_queued)
            queue_delete_button.clicked.connect(
                lambda _checked=False, target=result.target: self.add_target_to_queue(
                    target,
                    include_flats=False,
                    action="delete_force",
                )
            )

            action_layout.addWidget(queue_pull_button)
            action_layout.addWidget(queue_push_button)
            action_layout.addWidget(queue_delete_button)
            self.pull_table.setCellWidget(row_index, 9, action_widget)

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
            return

        self.compare_button.setEnabled(not busy)
        self.refresh_status_button.setEnabled(not busy)
        self.rebuild_nas_db_button.setEnabled(not busy)
        self.start_queue_button.setEnabled(not busy)
        self.remove_queue_item_button.setEnabled(not busy)
        self.clear_queue_button.setEnabled(not busy)
        self.only_action_needed_checkbox.setEnabled(not busy)
        self.include_flats_checkbox.setEnabled(not busy)

    def add_target_to_queue(self, target: str, include_flats: bool | None = None, action: str | None = None) -> None:
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

        pull_count = sum(1 for entry in self._queue_entries if entry.action == "pull")
        flats_count = sum(1 for entry in self._queue_entries if entry.action == "pull" and entry.include_flats)
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

        for row_index, entry in enumerate(self._queue_entries):
            self.queue_table.insertRow(row_index)
            self.queue_table.setItem(row_index, 0, QTableWidgetItem(entry.target))
            self.queue_table.setItem(
                row_index,
                1,
                QTableWidgetItem(f"{'Yes' if entry.include_flats else 'No'} ({entry.action})"),
            )
            self.queue_table.setItem(row_index, 2, QTableWidgetItem(entry.status))
            self.queue_table.setItem(row_index, 3, QTableWidgetItem(str(entry.progress_percent)))
            self.queue_table.setItem(row_index, 4, QTableWidgetItem(entry.lights_counts))
            self.queue_table.setItem(row_index, 5, QTableWidgetItem(entry.flats_counts))
            self.queue_table.setItem(row_index, 6, QTableWidgetItem(str(entry.copied_files)))
            self.queue_table.setItem(row_index, 7, QTableWidgetItem(str(entry.skipped_files)))
            self.queue_table.setItem(row_index, 8, QTableWidgetItem(str(entry.error_files)))

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
                self.queue_table.setCellWidget(row_index, 9, open_log_button)

        self.queue_table.resizeColumnsToContents()


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = BackupCompareWindow()
    window.show()
    sys.exit(app.exec())
