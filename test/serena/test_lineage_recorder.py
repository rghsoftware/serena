"""Tests for lineage recording functionality."""

import hashlib
import sqlite3
from datetime import datetime

from serena.lineage.recorder import find_lineage_db, get_active_task, record_change


class TestFindLineageDb:
    """Tests for finding lineage database in project hierarchy."""

    def test_finds_sqlite_db_in_current_dir(self, tmp_path, monkeypatch):
        """Test finding SQLite database in current directory."""
        # Create .spectrena/lineage.db in temp directory
        spectrena_dir = tmp_path / ".spectrena"
        spectrena_dir.mkdir()
        db_file = spectrena_dir / "lineage.db"
        db_file.touch()

        # Change to temp directory
        monkeypatch.chdir(tmp_path)

        # Should find the database
        result = find_lineage_db()
        assert result == db_file
        assert result.exists()

    def test_finds_sqlite_db_in_parent_dir(self, tmp_path, monkeypatch):
        """Test finding SQLite database in parent directory."""
        # Create .spectrena/lineage.db in parent
        spectrena_dir = tmp_path / ".spectrena"
        spectrena_dir.mkdir()
        db_file = spectrena_dir / "lineage.db"
        db_file.touch()

        # Create subdirectory and change to it
        subdir = tmp_path / "subdir" / "nested"
        subdir.mkdir(parents=True)
        monkeypatch.chdir(subdir)

        # Should find the database in parent
        result = find_lineage_db()
        assert result == db_file

    def test_finds_surrealdb_dir_in_current_dir(self, tmp_path, monkeypatch):
        """Test finding SurrealDB embedded directory."""
        # Create .spectrena/lineage/ directory
        lineage_dir = tmp_path / ".spectrena" / "lineage"
        lineage_dir.mkdir(parents=True)

        # Change to temp directory
        monkeypatch.chdir(tmp_path)

        # Should find the database directory
        result = find_lineage_db()
        assert result == lineage_dir
        assert result.is_dir()

    def test_prefers_sqlite_over_surrealdb(self, tmp_path, monkeypatch):
        """Test that SQLite database is preferred when both exist."""
        # Create both SQLite and SurrealDB
        spectrena_dir = tmp_path / ".spectrena"
        spectrena_dir.mkdir()
        sqlite_file = spectrena_dir / "lineage.db"
        sqlite_file.touch()
        surreal_dir = spectrena_dir / "lineage"
        surreal_dir.mkdir()

        monkeypatch.chdir(tmp_path)

        # Should prefer SQLite
        result = find_lineage_db()
        assert result == sqlite_file

    def test_returns_none_when_no_db_found(self, tmp_path, monkeypatch):
        """Test graceful degradation when no database exists."""
        monkeypatch.chdir(tmp_path)
        result = find_lineage_db()
        assert result is None

    def test_searches_upward_through_hierarchy(self, tmp_path, monkeypatch):
        """Test that search correctly traverses parent directories."""
        # Create database at root level
        spectrena_dir = tmp_path / ".spectrena"
        spectrena_dir.mkdir()
        db_file = spectrena_dir / "lineage.db"
        db_file.touch()

        # Create deep nested structure
        deep_dir = tmp_path / "a" / "b" / "c" / "d"
        deep_dir.mkdir(parents=True)
        monkeypatch.chdir(deep_dir)

        # Should still find database at root
        result = find_lineage_db()
        assert result == db_file


class TestGetActiveTask:
    """Tests for retrieving active task from phase_state."""

    def test_returns_none_when_no_db_found(self, tmp_path, monkeypatch):
        """Test graceful degradation when no database exists."""
        monkeypatch.chdir(tmp_path)
        result = get_active_task()
        assert result is None

    def test_retrieves_active_task_with_all_fields(self, tmp_path, monkeypatch):
        """Test retrieving active task with complete information."""
        # Create and populate test database
        spectrena_dir = tmp_path / ".spectrena"
        spectrena_dir.mkdir()
        db_file = spectrena_dir / "lineage.db"

        conn = sqlite3.connect(db_file)
        try:
            # Create schema
            conn.execute(
                """
                CREATE TABLE specs (
                    spec_id TEXT PRIMARY KEY,
                    title TEXT NOT NULL
                )
            """
            )
            conn.execute(
                """
                CREATE TABLE plans (
                    plan_id TEXT PRIMARY KEY,
                    spec_id TEXT NOT NULL,
                    title TEXT NOT NULL,
                    FOREIGN KEY (spec_id) REFERENCES specs(spec_id)
                )
            """
            )
            conn.execute(
                """
                CREATE TABLE tasks (
                    task_id TEXT PRIMARY KEY,
                    plan_id TEXT NOT NULL,
                    title TEXT NOT NULL,
                    FOREIGN KEY (plan_id) REFERENCES plans(plan_id)
                )
            """
            )
            conn.execute(
                """
                CREATE TABLE phase_state (
                    id INTEGER PRIMARY KEY,
                    current_task_id TEXT,
                    FOREIGN KEY (current_task_id) REFERENCES tasks(task_id)
                )
            """
            )

            # Insert test data
            conn.execute("INSERT INTO specs VALUES ('SPEC-001', 'Test Spec')")
            conn.execute("INSERT INTO plans VALUES ('PLAN-001', 'SPEC-001', 'Test Plan')")
            conn.execute("INSERT INTO tasks VALUES ('TASK-001', 'PLAN-001', 'Test Task')")
            conn.execute("INSERT INTO phase_state VALUES (1, 'TASK-001')")
            conn.commit()
        finally:
            conn.close()

        monkeypatch.chdir(tmp_path)

        # Should retrieve complete task information
        result = get_active_task()
        assert result is not None
        assert result["current_task_id"] == "TASK-001"
        assert result["title"] == "Test Task"
        assert result["plan_id"] == "PLAN-001"
        assert result["spec_id"] == "SPEC-001"

    def test_returns_none_when_no_active_task(self, tmp_path, monkeypatch):
        """Test when phase_state has no current task."""
        # Create database with NULL current_task_id
        spectrena_dir = tmp_path / ".spectrena"
        spectrena_dir.mkdir()
        db_file = spectrena_dir / "lineage.db"

        conn = sqlite3.connect(db_file)
        try:
            conn.execute(
                """
                CREATE TABLE phase_state (
                    id INTEGER PRIMARY KEY,
                    current_task_id TEXT
                )
            """
            )
            conn.execute("INSERT INTO phase_state VALUES (1, NULL)")
            conn.commit()
        finally:
            conn.close()

        monkeypatch.chdir(tmp_path)

        result = get_active_task()
        assert result is None

    def test_handles_missing_phase_state_table(self, tmp_path, monkeypatch):
        """Test graceful handling when database schema is incomplete."""
        # Create database without phase_state table
        spectrena_dir = tmp_path / ".spectrena"
        spectrena_dir.mkdir()
        db_file = spectrena_dir / "lineage.db"
        db_file.touch()

        monkeypatch.chdir(tmp_path)

        # Should return None rather than raising exception
        result = get_active_task()
        assert result is None

    def test_returns_none_for_surrealdb(self, tmp_path, monkeypatch):
        """Test that SurrealDB returns None (not yet implemented)."""
        # Create SurrealDB directory
        lineage_dir = tmp_path / ".spectrena" / "lineage"
        lineage_dir.mkdir(parents=True)

        monkeypatch.chdir(tmp_path)

        # Should gracefully return None (not implemented)
        result = get_active_task()
        assert result is None


class TestRecordChange:
    """Tests for recording code changes to lineage database."""

    def test_returns_none_when_no_db_found(self, tmp_path, monkeypatch):
        """Test graceful degradation when no database exists."""
        monkeypatch.chdir(tmp_path)

        result = record_change(
            task_id="TASK-001",
            file_path="src/test.py",
            change_type="modify",
            tool_used="replace_symbol_body",
            symbol_fqn="src/test.py:TestClass.method",
            old_content="old code",
            new_content="new code",
        )

        assert result is None

    def test_records_change_with_all_fields(self, tmp_path, monkeypatch):
        """Test recording a complete code change."""
        # Create database with code_changes table
        spectrena_dir = tmp_path / ".spectrena"
        spectrena_dir.mkdir()
        db_file = spectrena_dir / "lineage.db"

        conn = sqlite3.connect(db_file)
        try:
            conn.execute(
                """
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
            """
            )
            conn.commit()
        finally:
            conn.close()

        monkeypatch.chdir(tmp_path)

        # Record a change
        old_content = "def old_method():\n    pass"
        new_content = "def new_method():\n    return True"

        change_id = record_change(
            task_id="TASK-001",
            file_path="src/test.py",
            change_type="modify",
            tool_used="replace_symbol_body",
            symbol_fqn="src/test.py:TestClass.method",
            old_content=old_content,
            new_content=new_content,
        )

        # Verify change was recorded
        assert change_id is not None
        assert isinstance(change_id, int)

        # Verify database content
        conn = sqlite3.connect(db_file)
        try:
            conn.row_factory = sqlite3.Row
            row = conn.execute("SELECT * FROM code_changes WHERE id = ?", (change_id,)).fetchone()

            assert row["task_id"] == "TASK-001"
            assert row["file_path"] == "src/test.py"
            assert row["symbol_fqn"] == "src/test.py:TestClass.method"
            assert row["change_type"] == "modify"
            assert row["tool_used"] == "replace_symbol_body"

            # Verify hashes
            expected_old_hash = hashlib.sha256(old_content.encode()).hexdigest()[:16]
            expected_new_hash = hashlib.sha256(new_content.encode()).hexdigest()[:16]
            assert row["old_content_hash"] == expected_old_hash
            assert row["new_content_hash"] == expected_new_hash

            # Verify timestamp is valid ISO-8601
            timestamp = datetime.fromisoformat(row["timestamp"])
            assert timestamp.tzinfo is not None  # Should have timezone
        finally:
            conn.close()

    def test_records_change_without_optional_fields(self, tmp_path, monkeypatch):
        """Test recording a change with minimal required fields."""
        # Create database
        spectrena_dir = tmp_path / ".spectrena"
        spectrena_dir.mkdir()
        db_file = spectrena_dir / "lineage.db"

        conn = sqlite3.connect(db_file)
        try:
            conn.execute(
                """
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
            """
            )
            conn.commit()
        finally:
            conn.close()

        monkeypatch.chdir(tmp_path)

        # Record change without symbol_fqn or content
        change_id = record_change(
            task_id="TASK-002",
            file_path="src/new_file.py",
            change_type="create",
            tool_used="insert_after_symbol",
        )

        assert change_id is not None

        # Verify optional fields are None
        conn = sqlite3.connect(db_file)
        try:
            conn.row_factory = sqlite3.Row
            row = conn.execute("SELECT * FROM code_changes WHERE id = ?", (change_id,)).fetchone()

            assert row["symbol_fqn"] is None
            assert row["old_content_hash"] is None
            assert row["new_content_hash"] is None
        finally:
            conn.close()

    def test_handles_database_errors_gracefully(self, tmp_path, monkeypatch):
        """Test graceful handling of database errors."""
        # Create database without code_changes table
        spectrena_dir = tmp_path / ".spectrena"
        spectrena_dir.mkdir()
        db_file = spectrena_dir / "lineage.db"
        db_file.touch()

        monkeypatch.chdir(tmp_path)

        # Should return None instead of raising exception
        result = record_change(
            task_id="TASK-001",
            file_path="src/test.py",
            change_type="modify",
            tool_used="replace_symbol_body",
        )

        assert result is None

    def test_records_multiple_changes_for_same_task(self, tmp_path, monkeypatch):
        """Test recording multiple changes for the same task."""
        # Create database
        spectrena_dir = tmp_path / ".spectrena"
        spectrena_dir.mkdir()
        db_file = spectrena_dir / "lineage.db"

        conn = sqlite3.connect(db_file)
        try:
            conn.execute(
                """
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
            """
            )
            conn.commit()
        finally:
            conn.close()

        monkeypatch.chdir(tmp_path)

        # Record multiple changes
        change_id_1 = record_change(
            task_id="TASK-001",
            file_path="src/file1.py",
            change_type="modify",
            tool_used="replace_symbol_body",
        )
        change_id_2 = record_change(
            task_id="TASK-001",
            file_path="src/file2.py",
            change_type="create",
            tool_used="insert_after_symbol",
        )
        change_id_3 = record_change(
            task_id="TASK-001",
            file_path="src/file1.py",
            change_type="rename",
            tool_used="rename_symbol",
        )

        assert change_id_1 is not None
        assert change_id_2 is not None
        assert change_id_3 is not None

        # Verify all changes recorded
        conn = sqlite3.connect(db_file)
        try:
            count = conn.execute("SELECT COUNT(*) FROM code_changes WHERE task_id = ?", ("TASK-001",)).fetchone()[0]
            assert count == 3
        finally:
            conn.close()

    def test_returns_none_for_surrealdb(self, tmp_path, monkeypatch):
        """Test that SurrealDB returns None (not yet implemented)."""
        # Create SurrealDB directory
        lineage_dir = tmp_path / ".spectrena" / "lineage"
        lineage_dir.mkdir(parents=True)

        monkeypatch.chdir(tmp_path)

        # Should gracefully return None (not implemented)
        result = record_change(
            task_id="TASK-001",
            file_path="src/test.py",
            change_type="modify",
            tool_used="replace_symbol_body",
        )

        assert result is None

    def test_hash_generation_consistency(self, tmp_path, monkeypatch):
        """Test that content hashes are generated consistently."""
        # Create database
        spectrena_dir = tmp_path / ".spectrena"
        spectrena_dir.mkdir()
        db_file = spectrena_dir / "lineage.db"

        conn = sqlite3.connect(db_file)
        try:
            conn.execute(
                """
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
            """
            )
            conn.commit()
        finally:
            conn.close()

        monkeypatch.chdir(tmp_path)

        content = "test content"
        expected_hash = hashlib.sha256(content.encode()).hexdigest()[:16]

        # Record change
        change_id = record_change(
            task_id="TASK-001",
            file_path="src/test.py",
            change_type="modify",
            tool_used="test_tool",
            old_content=content,
            new_content=content,
        )

        # Verify hashes match expected
        conn = sqlite3.connect(db_file)
        try:
            conn.row_factory = sqlite3.Row
            row = conn.execute("SELECT * FROM code_changes WHERE id = ?", (change_id,)).fetchone()

            assert row["old_content_hash"] == expected_hash
            assert row["new_content_hash"] == expected_hash
        finally:
            conn.close()
