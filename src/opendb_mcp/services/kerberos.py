"""
Kerberos authentication service for Hive/Impala.
"""

import asyncio
import os
import re
import stat
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from ..utils.errors import KerberosError
from ..utils.logger import logger


@dataclass
class KerberosConfig:
    """Configuration for Kerberos authentication."""

    keytab: str
    principal: str


class KerberosAuth:
    """Manages Kerberos authentication using keytab files."""

    def __init__(self, config: KerberosConfig):
        self.config = config
        self._initialized = False
        self._ticket_expiry: Optional[datetime] = None
        self._refresh_task: Optional[asyncio.Task] = None

    @property
    def is_initialized(self) -> bool:
        """Check if Kerberos is initialized."""
        return self._initialized

    async def initialize(self) -> None:
        """Initialize Kerberos authentication using keytab."""
        if self._initialized:
            return

        keytab_path = Path(self.config.keytab).resolve()

        # Validate keytab file exists
        if not keytab_path.exists():
            raise KerberosError(f"Keytab file not found: {keytab_path}")

        # Check file permissions
        mode = keytab_path.stat().st_mode
        if mode & stat.S_IROTH:  # World-readable
            logger.warning(
                f"Keytab file {keytab_path} is world-readable. This is a security risk."
            )

        try:
            await self._kinit(str(keytab_path), self.config.principal)
            self._initialized = True
            self._schedule_refresh()
            logger.info(f"Kerberos authentication initialized for {self.config.principal}")
        except Exception as e:
            raise KerberosError(f"Failed to initialize Kerberos: {e}", e) from e

    async def destroy(self) -> None:
        """Destroy Kerberos credentials."""
        if self._refresh_task:
            self._refresh_task.cancel()
            try:
                await self._refresh_task
            except asyncio.CancelledError:
                pass
            self._refresh_task = None

        try:
            await self._kdestroy()
            self._initialized = False
            self._ticket_expiry = None
            logger.info("Kerberos credentials destroyed")
        except Exception as e:
            logger.warning(f"Failed to destroy Kerberos credentials: {e}")

    def is_valid(self) -> bool:
        """Check if authentication is initialized and valid."""
        if not self._initialized:
            return False
        if self._ticket_expiry is None:
            return True
        return datetime.now() < self._ticket_expiry

    async def refresh(self) -> None:
        """Refresh Kerberos ticket if needed."""
        if not self._initialized:
            await self.initialize()
            return

        keytab_path = Path(self.config.keytab).resolve()
        await self._kinit(str(keytab_path), self.config.principal)
        self._schedule_refresh()

    async def _kinit(self, keytab_path: str, principal: str) -> None:
        """Run kinit to obtain Kerberos ticket."""
        process = await asyncio.create_subprocess_exec(
            "kinit",
            "-kt",
            keytab_path,
            principal,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        _, stderr = await process.communicate()

        if process.returncode != 0:
            raise KerberosError(f"kinit failed with code {process.returncode}: {stderr.decode()}")

        # Get ticket expiry time
        self._ticket_expiry = await self._get_ticket_expiry()

    async def _kdestroy(self) -> None:
        """Run kdestroy to destroy Kerberos credentials."""
        process = await asyncio.create_subprocess_exec(
            "kdestroy",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        await process.communicate()

        if process.returncode != 0:
            raise KerberosError(f"kdestroy failed with code {process.returncode}")

    async def _get_ticket_expiry(self) -> Optional[datetime]:
        """Get ticket expiry time from klist."""
        try:
            process = await asyncio.create_subprocess_exec(
                "klist",
                "-c",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            stdout, _ = await process.communicate()

            if process.returncode == 0:
                output = stdout.decode()
                # Try to parse expiry time from klist output
                # Format varies by OS, try common patterns
                patterns = [
                    r"(\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2})",  # MM/DD/YYYY HH:MM:SS
                    r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})",  # ISO format
                ]

                for pattern in patterns:
                    match = re.search(pattern, output)
                    if match:
                        try:
                            # Try to parse the date
                            date_str = match.group(1)
                            if "/" in date_str:
                                return datetime.strptime(date_str, "%m/%d/%Y %H:%M:%S")
                            else:
                                return datetime.fromisoformat(date_str)
                        except ValueError:
                            continue

            # Default to 8 hours from now if we can't parse
            return datetime.now() + timedelta(hours=8)

        except Exception:
            return datetime.now() + timedelta(hours=8)

    def _schedule_refresh(self) -> None:
        """Schedule ticket refresh before expiry."""
        if self._refresh_task:
            self._refresh_task.cancel()

        # Refresh 10 minutes before expiry, or in 4 hours if no expiry known
        if self._ticket_expiry:
            refresh_in = max(
                0,
                (self._ticket_expiry - datetime.now() - timedelta(minutes=10)).total_seconds(),
            )
        else:
            refresh_in = 4 * 60 * 60  # 4 hours

        async def refresh_loop() -> None:
            try:
                await asyncio.sleep(refresh_in)
                await self.refresh()
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.error(f"Failed to refresh Kerberos ticket: {e}")

        self._refresh_task = asyncio.create_task(refresh_loop())
