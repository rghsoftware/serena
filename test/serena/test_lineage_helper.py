"""Tests for lineage helper function used by symbol tools."""

import sqlite3
import tempfile
from pathlib import Path

import pytest

from serena.tools.symbol_tools import _record_to_lineage


class TestRecordToLineageHelper:
    """Test the _record_to_lineage helper function."""

    @pytest.fixture
    def lineage_db(self, tmp_path, monkeypatch):
        """Create a temporary lineage database."""
        # Create .spectrena/lineage.db
        spectrena_dir = tmp_path / ".spectrena"
        spectrena_dir.mkdir()
        db_file = spectrena_dir / "lineage.db"

        # Create schema
        conn = sqlite3.connect(db_file)
        try:
            conn.execute("""
                CREATE TABLE specs (
                    spec_id TEXT PRIMARY KEY,
                    title TEXT NOT NULL
                )
            """)
            conn.execute("""
                CREATE TABLE plans (
                    plan_id TEXT PRIMARY KEY,
                    spec_id TEXT NOT NULL,
                    title TEXT NOT NULL,
                    FOREIGN KEY (spec_id) REFERENCES specs(spec_id)
                )
            """)
            conn.execute("""
                CREATE TABLE tasks (
                    task_id TEXT PRIMARY KEY,
                    plan_id TEXT NOT NULL,
                    title TEXT NOT NULL,
                    FOREIGN KEY (plan_id) REFERENCES plans(plan_id)
                )
            """)
            conn.execute("""
                CREATE TABLE phase_state (
                    id INTEGER PRIMARY KEY,
                    current_task_id TEXT,
                    FOREIGN KEY (current_task_id) REFERENCES tasks(task_id)
                )
            """)
            conn.execute("""
                CREATE TABLE code_changes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id TEXT NOT NULL,
                    file_path TEXT NOT NULL,
                    symbol_fqn TEXT,
                    change_type TEXT NOT NULL,
                    tool_used TEXT NOT NULL,
                    old_content_hash TEXT,
                    new_content_hash TEXT,
                    timestamp TEXT NOT NULL
                )
            """)

            # Insert test data for active task
            conn.execute("INSERT INTO specs VALUES ('SPEC-001', 'Test Spec')")
            conn.execute("INSERT INTO plans VALUES ('PLAN-001', 'SPEC-001', 'Test Plan')")
            conn.execute("INSERT INTO tasks VALUES ('TASK-ACTIVE', 'PLAN-001', 'Active Task')")
            conn.execute("INSERT INTO phase_state VALUES (1, 'TASK-ACTIVE')")
            conn.commit()
        finally:
            conn.close()

        monkeypatch.chdir(tmp_path)
        return {"db_file": db_file, "tmp_path": tmp_path}

    def test_records_with_explicit_task_id(self, lineage_db):
        """Test recording with explicit task_id parameter."""
        _record_to_lineage(
            task_id="TASK-EXPLICIT",
            file_path="src/test.py",
            change_type="modify",
            tool_used="replace_symbol_body",
            symbol_fqn="src/test.py:TestClass.method",
            old_content="old code",
            new_content="new code",
        )

        # Verify change was recorded
        conn = sqlite3.connect(lineage_db["db_file"])
        try:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM code_changes WHERE task_id = ?",
                ("TASK-EXPLICIT",),
            ).fetchone()

            assert row is not None
            assert row["task_id"] == "TASK-EXPLICIT"
            assert row["file_path"] == "src/test.py"
            assert row["symbol_fqn"] == "src/test.py:TestClass.method"
            assert row["change_type"] == "modify"
            assert row["tool_used"] == "replace_symbol_body"
            assert row["old_content_hash"] is not None
            assert row["new_content_hash"] is not None
        finally:
            conn.close()

    def test_uses_active_task_when_no_task_id(self, lineage_db):
        """Test that helper uses active task from phase_state when task_id is None."""
        _record_to_lineage(
            task_id=None,  # Should use active task
            file_path="src/test.py",
            change_type="create",
            tool_used="insert_after_symbol",
            symbol_fqn="src/test.py:new_function",
            new_content="new function",
        )

        # Verify change was recorded with active task ID
        conn = sqlite3.connect(lineage_db["db_file"])
        try:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM code_changes WHERE task_id = ?",
                ("TASK-ACTIVE",),
            ).fetchone()

            assert row is not None
            assert row["task_id"] == "TASK-ACTIVE"
            assert row["change_type"] == "create"
            assert row["tool_used"] == "insert_after_symbol"
        finally:
            conn.close()

    def test_does_not_record_without_task_context(self, lineage_db):
        """Test graceful degradation when no task_id and no active task."""
        # Clear active task
        conn = sqlite3.connect(lineage_db["db_file"])
        try:
            conn.execute("UPDATE phase_state SET current_task_id = NULL WHERE id = 1")
            conn.commit()
        finally:
            conn.close()

        # Should not raise exception, just skip recording
        _record_to_lineage(
            task_id=None,
            file_path="src/test.py",
            change_type="modify",
            tool_used="replace_symbol_body",
        )

        # Verify no changes were recorded
        conn = sqlite3.connect(lineage_db["db_file"])
        try:
            count = conn.execute("SELECT COUNT(*) FROM code_changes").fetchone()[0]
            assert count == 0
        finally:
            conn.close()

    def test_does_not_fail_without_database(self, tmp_path, monkeypatch):
        """Test graceful degradation when no lineage database exists."""
        monkeypatch.chdir(tmp_path)

        # Should not raise exception
        _record_to_lineage(
            task_id="TASK-001",
            file_path="src/test.py",
            change_type="modify",
            tool_used="replace_symbol_body",
        )

        # No assertion needed - just verifying it doesn't crash

    def test_records_all_change_types(self, lineage_db):
        """Test that all change types are recorded correctly."""
        change_types = [
            ("modify", "replace_symbol_body"),
            ("create", "insert_after_symbol"),
            ("create", "insert_before_symbol"),
            ("rename", "rename_symbol"),
            ("delete", "delete_symbol"),
        ]

        for change_type, tool_used in change_types:
            _record_to_lineage(
                task_id=f"TASK-{change_type.upper()}",
                file_path="src/test.py",
                change_type=change_type,
                tool_used=tool_used,
                symbol_fqn=f"src/test.py:symbol_{change_type}",
            )

        # Verify all changes were recorded
        conn = sqlite3.connect(lineage_db["db_file"])
        try:
            rows = conn.execute(
                "SELECT change_type, tool_used FROM code_changes ORDER BY change_type"
            ).fetchall()

            assert len(rows) == 5
            recorded_types = [(row[0], row[1]) for row in rows]
            assert sorted(change_types) == sorted(recorded_types)
        finally:
            conn.close()

    def test_records_without_optional_content(self, lineage_db):
        """Test recording works without optional old/new content."""
        _record_to_lineage(
            task_id="TASK-NO-CONTENT",
            file_path="src/test.py",
            change_type="delete",
            tool_used="delete_symbol",
            symbol_fqn="src/test.py:deleted_symbol",
            # No old_content or new_content provided
        )

        # Verify change was recorded
        conn = sqlite3.connect(lineage_db["db_file"])
        try:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM code_changes WHERE task_id = ?",
                ("TASK-NO-CONTENT",),
            ).fetchone()

            assert row is not None
            assert row["old_content_hash"] is None
            assert row["new_content_hash"] is None
        finally:
            conn.close()
