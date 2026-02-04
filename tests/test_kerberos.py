"""Tests for Kerberos authentication service."""

import asyncio
import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from opendb_mcp.services.kerberos import KerberosAuth, KerberosConfig


class TestKerberosConfig:
    """Tests for KerberosConfig dataclass."""

    def test_default_krb5_conf_is_none(self):
        """Test that krb5_conf defaults to None."""
        config = KerberosConfig(keytab="/path/to/keytab", principal="user@REALM")

        assert config.krb5_conf is None

    def test_krb5_conf_can_be_set(self):
        """Test that krb5_conf can be set."""
        config = KerberosConfig(
            keytab="/path/to/keytab",
            principal="user@REALM",
            krb5_conf="/path/to/krb5.conf",
        )

        assert config.krb5_conf == "/path/to/krb5.conf"


class TestKerberosAuthEnv:
    """Tests for KerberosAuth environment variable handling."""

    def test_get_env_with_krb5_config_returns_none_when_not_set(self):
        """Test that _get_env_with_krb5_config returns None when krb5_conf is not set."""
        config = KerberosConfig(keytab="/path/to/keytab", principal="user@REALM")
        auth = KerberosAuth(config)

        result = auth._get_env_with_krb5_config()

        assert result is None

    def test_get_env_with_krb5_config_returns_env_with_krb5_config(self):
        """Test that _get_env_with_krb5_config returns env with KRB5_CONFIG set."""
        config = KerberosConfig(
            keytab="/path/to/keytab",
            principal="user@REALM",
            krb5_conf="/custom/krb5.conf",
        )
        auth = KerberosAuth(config)

        result = auth._get_env_with_krb5_config()

        assert result is not None
        assert result["KRB5_CONFIG"] == "/custom/krb5.conf"
        # Verify other env vars are preserved
        assert "PATH" in result

    def test_get_env_with_krb5_config_does_not_modify_os_environ(self):
        """Test that _get_env_with_krb5_config does not modify os.environ."""
        original_krb5_config = os.environ.get("KRB5_CONFIG")
        config = KerberosConfig(
            keytab="/path/to/keytab",
            principal="user@REALM",
            krb5_conf="/custom/krb5.conf",
        )
        auth = KerberosAuth(config)

        auth._get_env_with_krb5_config()

        # Verify os.environ was not modified
        assert os.environ.get("KRB5_CONFIG") == original_krb5_config


class TestKerberosAuthKinit:
    """Tests for KerberosAuth kinit with KRB5_CONFIG."""

    @pytest.mark.asyncio
    async def test_kinit_passes_env_when_krb5_conf_set(self, tmp_path):
        """Test that kinit receives env with KRB5_CONFIG when krb5_conf is set."""
        keytab_file = tmp_path / "test.keytab"
        keytab_file.write_bytes(b"test keytab content")

        config = KerberosConfig(
            keytab=str(keytab_file),
            principal="user@REALM",
            krb5_conf="/custom/krb5.conf",
        )
        auth = KerberosAuth(config)

        captured_env = None

        async def mock_subprocess(*args, **kwargs):
            nonlocal captured_env
            captured_env = kwargs.get("env")
            mock_process = MagicMock()
            mock_process.returncode = 0
            mock_process.communicate = AsyncMock(return_value=(b"", b""))
            return mock_process

        with patch("asyncio.create_subprocess_exec", side_effect=mock_subprocess):
            await auth._kinit(str(keytab_file), "user@REALM")

        assert captured_env is not None
        assert captured_env["KRB5_CONFIG"] == "/custom/krb5.conf"

    @pytest.mark.asyncio
    async def test_kinit_passes_none_env_when_krb5_conf_not_set(self, tmp_path):
        """Test that kinit receives env=None when krb5_conf is not set."""
        keytab_file = tmp_path / "test.keytab"
        keytab_file.write_bytes(b"test keytab content")

        config = KerberosConfig(
            keytab=str(keytab_file),
            principal="user@REALM",
        )
        auth = KerberosAuth(config)

        captured_env = "not_called"

        async def mock_subprocess(*args, **kwargs):
            nonlocal captured_env
            captured_env = kwargs.get("env")
            mock_process = MagicMock()
            mock_process.returncode = 0
            mock_process.communicate = AsyncMock(return_value=(b"", b""))
            return mock_process

        with patch("asyncio.create_subprocess_exec", side_effect=mock_subprocess):
            await auth._kinit(str(keytab_file), "user@REALM")

        assert captured_env is None


class TestKerberosAuthKdestroy:
    """Tests for KerberosAuth kdestroy with KRB5_CONFIG."""

    @pytest.mark.asyncio
    async def test_kdestroy_passes_env_when_krb5_conf_set(self):
        """Test that kdestroy receives env with KRB5_CONFIG when krb5_conf is set."""
        config = KerberosConfig(
            keytab="/path/to/keytab",
            principal="user@REALM",
            krb5_conf="/custom/krb5.conf",
        )
        auth = KerberosAuth(config)

        captured_env = None

        async def mock_subprocess(*args, **kwargs):
            nonlocal captured_env
            captured_env = kwargs.get("env")
            mock_process = MagicMock()
            mock_process.returncode = 0
            mock_process.communicate = AsyncMock(return_value=(b"", b""))
            return mock_process

        with patch("asyncio.create_subprocess_exec", side_effect=mock_subprocess):
            await auth._kdestroy()

        assert captured_env is not None
        assert captured_env["KRB5_CONFIG"] == "/custom/krb5.conf"

    @pytest.mark.asyncio
    async def test_kdestroy_passes_none_env_when_krb5_conf_not_set(self):
        """Test that kdestroy receives env=None when krb5_conf is not set."""
        config = KerberosConfig(
            keytab="/path/to/keytab",
            principal="user@REALM",
        )
        auth = KerberosAuth(config)

        captured_env = "not_called"

        async def mock_subprocess(*args, **kwargs):
            nonlocal captured_env
            captured_env = kwargs.get("env")
            mock_process = MagicMock()
            mock_process.returncode = 0
            mock_process.communicate = AsyncMock(return_value=(b"", b""))
            return mock_process

        with patch("asyncio.create_subprocess_exec", side_effect=mock_subprocess):
            await auth._kdestroy()

        assert captured_env is None


class TestKerberosAuthGetTicketExpiry:
    """Tests for KerberosAuth _get_ticket_expiry with KRB5_CONFIG."""

    @pytest.mark.asyncio
    async def test_get_ticket_expiry_passes_env_when_krb5_conf_set(self):
        """Test that klist receives env with KRB5_CONFIG when krb5_conf is set."""
        config = KerberosConfig(
            keytab="/path/to/keytab",
            principal="user@REALM",
            krb5_conf="/custom/krb5.conf",
        )
        auth = KerberosAuth(config)

        captured_env = None

        async def mock_subprocess(*args, **kwargs):
            nonlocal captured_env
            captured_env = kwargs.get("env")
            mock_process = MagicMock()
            mock_process.returncode = 0
            mock_process.communicate = AsyncMock(return_value=(b"", b""))
            return mock_process

        with patch("asyncio.create_subprocess_exec", side_effect=mock_subprocess):
            await auth._get_ticket_expiry()

        assert captured_env is not None
        assert captured_env["KRB5_CONFIG"] == "/custom/krb5.conf"
