"""Tests for Kerberos authentication service."""

import asyncio
import os
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from opendb_mcp.services.kerberos import KerberosAuth, KerberosConfig
from opendb_mcp.utils.errors import KerberosError


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


class TestIsTicketValid:
    """Tests for KerberosAuth._is_ticket_valid()."""

    def test_returns_false_when_not_initialized(self):
        config = KerberosConfig(keytab="/path/to/keytab", principal="user@REALM")
        auth = KerberosAuth(config)

        assert auth._is_ticket_valid() is False

    def test_returns_false_when_no_expiry(self):
        config = KerberosConfig(keytab="/path/to/keytab", principal="user@REALM")
        auth = KerberosAuth(config)
        auth._initialized = True
        auth._ticket_expiry = None

        assert auth._is_ticket_valid() is False

    def test_returns_false_when_expired(self):
        config = KerberosConfig(keytab="/path/to/keytab", principal="user@REALM")
        auth = KerberosAuth(config)
        auth._initialized = True
        auth._ticket_expiry = datetime.now() - timedelta(hours=1)

        assert auth._is_ticket_valid() is False

    def test_returns_false_when_within_buffer(self):
        config = KerberosConfig(keytab="/path/to/keytab", principal="user@REALM")
        auth = KerberosAuth(config)
        auth._initialized = True
        # Expires in 3 minutes — within the 5-minute buffer
        auth._ticket_expiry = datetime.now() + timedelta(minutes=3)

        assert auth._is_ticket_valid() is False

    def test_returns_true_when_well_before_expiry(self):
        config = KerberosConfig(keytab="/path/to/keytab", principal="user@REALM")
        auth = KerberosAuth(config)
        auth._initialized = True
        auth._ticket_expiry = datetime.now() + timedelta(hours=7)

        assert auth._is_ticket_valid() is True

    def test_is_valid_delegates_to_is_ticket_valid(self):
        config = KerberosConfig(keytab="/path/to/keytab", principal="user@REALM")
        auth = KerberosAuth(config)
        auth._initialized = True
        auth._ticket_expiry = datetime.now() + timedelta(hours=7)

        assert auth.is_valid() is True

        auth._ticket_expiry = datetime.now() - timedelta(hours=1)
        assert auth.is_valid() is False


class TestEnsureValid:
    """Tests for KerberosAuth.ensure_valid()."""

    @pytest.mark.asyncio
    async def test_skips_kinit_when_ticket_valid(self, tmp_path):
        """ensure_valid() should not run kinit when ticket is still valid."""
        keytab_file = tmp_path / "test.keytab"
        keytab_file.write_bytes(b"test keytab content")

        config = KerberosConfig(keytab=str(keytab_file), principal="user@REALM")
        auth = KerberosAuth(config)
        auth._initialized = True
        auth._ticket_expiry = datetime.now() + timedelta(hours=7)

        with patch.object(auth, "_kinit", new_callable=AsyncMock) as mock_kinit:
            await auth.ensure_valid()
            mock_kinit.assert_not_called()

    @pytest.mark.asyncio
    async def test_runs_kinit_when_expired(self, tmp_path):
        """ensure_valid() should run kinit when ticket is expired."""
        keytab_file = tmp_path / "test.keytab"
        keytab_file.write_bytes(b"test keytab content")

        config = KerberosConfig(keytab=str(keytab_file), principal="user@REALM")
        auth = KerberosAuth(config)
        auth._initialized = True
        auth._ticket_expiry = datetime.now() - timedelta(hours=1)

        with patch.object(auth, "_kinit", new_callable=AsyncMock) as mock_kinit:
            await auth.ensure_valid()
            mock_kinit.assert_called_once()

    @pytest.mark.asyncio
    async def test_runs_kinit_when_near_expiry(self, tmp_path):
        """ensure_valid() should run kinit when ticket is within buffer."""
        keytab_file = tmp_path / "test.keytab"
        keytab_file.write_bytes(b"test keytab content")

        config = KerberosConfig(keytab=str(keytab_file), principal="user@REALM")
        auth = KerberosAuth(config)
        auth._initialized = True
        auth._ticket_expiry = datetime.now() + timedelta(minutes=3)

        with patch.object(auth, "_kinit", new_callable=AsyncMock) as mock_kinit:
            await auth.ensure_valid()
            mock_kinit.assert_called_once()

    @pytest.mark.asyncio
    async def test_runs_kinit_when_not_initialized(self, tmp_path):
        """ensure_valid() should run kinit when not yet initialized."""
        keytab_file = tmp_path / "test.keytab"
        keytab_file.write_bytes(b"test keytab content")

        config = KerberosConfig(keytab=str(keytab_file), principal="user@REALM")
        auth = KerberosAuth(config)

        with patch.object(auth, "_kinit", new_callable=AsyncMock) as mock_kinit:
            await auth.ensure_valid()
            mock_kinit.assert_called_once()
            assert auth._initialized is True

    @pytest.mark.asyncio
    async def test_raises_on_kinit_failure(self, tmp_path):
        """ensure_valid() should raise KerberosError when kinit fails."""
        keytab_file = tmp_path / "test.keytab"
        keytab_file.write_bytes(b"test keytab content")

        config = KerberosConfig(keytab=str(keytab_file), principal="user@REALM")
        auth = KerberosAuth(config)

        with patch.object(
            auth, "_kinit", new_callable=AsyncMock, side_effect=KerberosError("kinit failed")
        ):
            with pytest.raises(KerberosError, match="Failed to refresh Kerberos ticket"):
                await auth.ensure_valid()

    @pytest.mark.asyncio
    async def test_self_heals_on_retry_after_failure(self, tmp_path):
        """After a kinit failure, the next ensure_valid() call should retry."""
        keytab_file = tmp_path / "test.keytab"
        keytab_file.write_bytes(b"test keytab content")

        config = KerberosConfig(keytab=str(keytab_file), principal="user@REALM")
        auth = KerberosAuth(config)

        # First call fails
        with patch.object(
            auth, "_kinit", new_callable=AsyncMock, side_effect=KerberosError("KDC unreachable")
        ):
            with pytest.raises(KerberosError):
                await auth.ensure_valid()

        # Second call succeeds
        with patch.object(auth, "_kinit", new_callable=AsyncMock) as mock_kinit:
            await auth.ensure_valid()
            mock_kinit.assert_called_once()
            assert auth._initialized is True


class TestInitializeWithoutGuard:
    """Tests that initialize() can be called multiple times."""

    @pytest.mark.asyncio
    async def test_initialize_can_be_called_twice(self, tmp_path):
        """initialize() should run kinit even if already initialized."""
        keytab_file = tmp_path / "test.keytab"
        keytab_file.write_bytes(b"test keytab content")

        config = KerberosConfig(keytab=str(keytab_file), principal="user@REALM")
        auth = KerberosAuth(config)

        async def mock_subprocess(*args, **kwargs):
            mock_process = MagicMock()
            mock_process.returncode = 0
            mock_process.communicate = AsyncMock(return_value=(b"", b""))
            return mock_process

        with patch("asyncio.create_subprocess_exec", side_effect=mock_subprocess):
            await auth.initialize()
            assert auth._initialized is True

            # Call again — should not raise or short-circuit
            await auth.initialize()
            assert auth._initialized is True
