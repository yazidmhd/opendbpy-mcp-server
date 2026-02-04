"""Tests for configuration module."""

import os
from pathlib import Path

import pytest
from opendb_mcp.config.loader import (
    substitute_env_vars,
    substitute_env_vars_in_object,
    create_config_from_dsn,
    validate_krb5_conf,
)
from opendb_mcp.config.types import Settings, parse_source_config


class TestEnvVarSubstitution:
    """Tests for environment variable substitution."""

    def test_simple_substitution(self):
        os.environ["TEST_VAR"] = "test_value"
        result = substitute_env_vars("${TEST_VAR}")
        assert result == "test_value"

    def test_default_value(self):
        result = substitute_env_vars("${NONEXISTENT_VAR:-default}")
        assert result == "default"

    def test_missing_no_default(self):
        result = substitute_env_vars("${COMPLETELY_MISSING}")
        # Should return original if no default and not set
        assert "${COMPLETELY_MISSING}" in result

    def test_object_substitution(self):
        os.environ["OBJ_TEST"] = "value"
        obj = {"key": "${OBJ_TEST}", "nested": {"inner": "${MISSING:-fallback}"}}
        result = substitute_env_vars_in_object(obj)
        assert result["key"] == "value"
        assert result["nested"]["inner"] == "fallback"


class TestSettings:
    """Tests for Settings model."""

    def test_defaults(self):
        settings = Settings()
        assert settings.readonly is False
        assert settings.max_rows == 1000
        assert settings.query_timeout == 30

    def test_custom_values(self):
        settings = Settings(readonly=True, max_rows=500, query_timeout=60)
        assert settings.readonly is True
        assert settings.max_rows == 500
        assert settings.query_timeout == 60

    def test_max_rows_validation(self):
        # Pydantic validates the range, values > 100000 should raise
        with pytest.raises(Exception):  # ValidationError
            Settings(max_rows=200000)

    def test_max_rows_boundary(self):
        # Test boundary value is accepted
        settings = Settings(max_rows=100000)
        assert settings.max_rows == 100000


class TestSourceConfigParsing:
    """Tests for source configuration parsing."""

    def test_postgres_dsn(self):
        config = parse_source_config({
            "id": "pg-test",
            "type": "postgres",
            "dsn": "postgres://localhost/test"
        })
        assert config.id == "pg-test"
        assert config.type == "postgres"
        assert config.dsn == "postgres://localhost/test"

    def test_postgres_host(self):
        config = parse_source_config({
            "id": "pg-host",
            "type": "postgres",
            "host": "localhost",
            "port": 5432,
            "database": "mydb"
        })
        assert config.id == "pg-host"
        assert config.host == "localhost"
        assert config.port == 5432

    def test_hive_kerberos(self):
        config = parse_source_config({
            "id": "hive-test",
            "type": "hive",
            "host": "hive.example.com",
            "auth_mechanism": "KERBEROS",
            "keytab": "/etc/keytab",
            "user_principal": "user@REALM"
        })
        assert config.id == "hive-test"
        assert config.type == "hive"
        assert config.auth_mechanism == "KERBEROS"


class TestDsnConfig:
    """Tests for DSN-based configuration creation."""

    def test_postgres_dsn(self):
        config = create_config_from_dsn("postgres://localhost/test")
        assert "default" in config.sources
        assert config.sources["default"].type == "postgres"

    def test_mysql_dsn(self):
        config = create_config_from_dsn("mysql://localhost/test")
        assert config.sources["default"].type == "mysql"

    def test_invalid_dsn(self):
        with pytest.raises(ValueError):
            create_config_from_dsn("invalid://localhost/test")


class TestValidateKrb5Conf:
    """Tests for validate_krb5_conf function."""

    def test_resolves_relative_path(self, tmp_path):
        """Test that relative paths are resolved relative to config_dir."""
        # Create krb5.conf file
        krb5_dir = tmp_path / "krb5"
        krb5_dir.mkdir()
        krb5_file = krb5_dir / "krb5.conf"
        krb5_file.write_text("[libdefaults]\ndefault_realm = EXAMPLE.COM\n")

        result = validate_krb5_conf("krb5/krb5.conf", tmp_path)

        assert result == str(krb5_file.resolve())

    def test_preserves_absolute_path(self, tmp_path):
        """Test that absolute paths are preserved."""
        # Create krb5.conf file
        krb5_file = tmp_path / "krb5.conf"
        krb5_file.write_text("[libdefaults]\ndefault_realm = EXAMPLE.COM\n")

        result = validate_krb5_conf(str(krb5_file), tmp_path)

        assert result == str(krb5_file.resolve())

    def test_raises_error_for_missing_file(self, tmp_path):
        """Test that FileNotFoundError is raised if krb5.conf doesn't exist."""
        with pytest.raises(FileNotFoundError) as exc_info:
            validate_krb5_conf("nonexistent/krb5.conf", tmp_path)

        assert "krb5.conf not found" in str(exc_info.value)

    def test_resolves_nested_relative_path(self, tmp_path):
        """Test that nested relative paths are resolved correctly."""
        # Create nested directory structure
        nested_dir = tmp_path / "config" / "kerberos" / "conf"
        nested_dir.mkdir(parents=True)
        krb5_file = nested_dir / "krb5.conf"
        krb5_file.write_text("[libdefaults]\ndefault_realm = EXAMPLE.COM\n")

        result = validate_krb5_conf("config/kerberos/conf/krb5.conf", tmp_path)

        assert result == str(krb5_file.resolve())
