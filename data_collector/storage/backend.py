"""Pluggable storage backend interface and filesystem implementation.

``BaseStorageBackend`` defines the contract for storing and retrieving files.
``FilesystemBackend`` implements it for local directories, UNC paths
(``\\\\server\\share``), and mounted network shares -- all handled
transparently by ``pathlib.Path``.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from pathlib import Path


class BaseStorageBackend(ABC):
    """Interface for storage backends.

    Every backend stores and retrieves files using **relative paths** rooted
    at a backend-specific base directory.  The ``location_name`` property
    identifies the backend in ``StoredFile.location`` for multi-server
    tracking.
    """

    @abstractmethod
    def store(self, content: bytes, relative_path: Path) -> Path:
        """Write *content* to *relative_path* under the backend root.

        Creates parent directories as needed.

        Args:
            content: Raw file bytes to write.
            relative_path: Path relative to the backend root.

        Returns:
            Absolute path of the stored file.
        """

    @abstractmethod
    def retrieve(self, relative_path: Path) -> bytes:
        """Read and return file content from *relative_path*.

        Args:
            relative_path: Path relative to the backend root.

        Returns:
            Raw file bytes.

        Raises:
            FileNotFoundError: If the file does not exist.
        """

    @abstractmethod
    def delete(self, relative_path: Path) -> bool:
        """Delete the file at *relative_path*.

        Args:
            relative_path: Path relative to the backend root.

        Returns:
            ``True`` if the file existed and was deleted, ``False`` if it
            was already absent.
        """

    @abstractmethod
    def exists(self, relative_path: Path) -> bool:
        """Check whether a file exists at *relative_path*.

        Args:
            relative_path: Path relative to the backend root.

        Returns:
            ``True`` if the file exists, ``False`` otherwise.
        """

    @abstractmethod
    def get_size(self, relative_path: Path) -> int:
        """Return the size in bytes of the file at *relative_path*.

        Args:
            relative_path: Path relative to the backend root.

        Returns:
            File size in bytes.

        Raises:
            FileNotFoundError: If the file does not exist.
        """

    @property
    @abstractmethod
    def root(self) -> Path:
        """Base directory of this backend."""

    @property
    @abstractmethod
    def location_name(self) -> str:
        """Identifier for this backend (e.g. ``"local"``, ``"fs_market_hr"``)."""


class FilesystemBackend(BaseStorageBackend):
    """Filesystem-based storage backend for local and network paths.

    Handles local directories, UNC paths (``\\\\server\\share``), and
    mounted network shares transparently via ``pathlib.Path``.

    Args:
        root: Base directory (local path, UNC path, or mount point).
        location: Identifier stored in ``StoredFile.location``.
        logger: Optional logger for storage operations.

    Examples:
        Local storage::

            backend = FilesystemBackend(Path("C:/storage"), location="local")

        Network share::

            backend = FilesystemBackend(Path("//fileserver/share"), location="fs_market_hr")
    """

    def __init__(
        self,
        root: Path,
        location: str = "local",
        *,
        logger: logging.Logger | None = None,
    ) -> None:
        self._root = root.resolve()
        self._location = location
        self._logger = logger or logging.getLogger(__name__)

    def _resolve_path(self, relative_path: Path) -> Path:
        """Resolve a relative path to an absolute path under the backend root.

        Raises:
            ValueError: If the resolved path escapes the root directory.
        """
        absolute_path = (self._root / relative_path).resolve()
        try:
            absolute_path.relative_to(self._root)
        except ValueError:
            message = f"Path escapes backend root: {relative_path}"
            raise ValueError(message) from None
        return absolute_path

    def store(self, content: bytes, relative_path: Path) -> Path:
        """Write content to a file under the backend root.

        Creates parent directories as needed.  Overwrites the file if it
        already exists.

        Args:
            content: Raw file bytes to write.
            relative_path: Path relative to the backend root.

        Returns:
            Absolute path of the stored file.
        """
        absolute_path = self._resolve_path(relative_path)
        absolute_path.parent.mkdir(parents=True, exist_ok=True)
        absolute_path.write_bytes(content)
        self._logger.debug(f"Stored {len(content)} bytes to {absolute_path}")
        return absolute_path

    def retrieve(self, relative_path: Path) -> bytes:
        """Read and return file content.

        Args:
            relative_path: Path relative to the backend root.

        Returns:
            Raw file bytes.

        Raises:
            FileNotFoundError: If the file does not exist.
        """
        absolute_path = self._resolve_path(relative_path)
        return absolute_path.read_bytes()

    def delete(self, relative_path: Path) -> bool:
        """Delete a file from the backend.

        Args:
            relative_path: Path relative to the backend root.

        Returns:
            ``True`` if the file existed and was deleted, ``False`` if it
            was already absent.
        """
        absolute_path = self._resolve_path(relative_path)
        if absolute_path.is_file():
            absolute_path.unlink()
            self._logger.debug(f"Deleted {absolute_path}")
            return True
        return False

    def exists(self, relative_path: Path) -> bool:
        """Check whether a file exists.

        Args:
            relative_path: Path relative to the backend root.

        Returns:
            ``True`` if the file exists, ``False`` otherwise.
        """
        return self._resolve_path(relative_path).is_file()

    def get_size(self, relative_path: Path) -> int:
        """Return file size in bytes.

        Args:
            relative_path: Path relative to the backend root.

        Returns:
            File size in bytes.

        Raises:
            FileNotFoundError: If the file does not exist.
        """
        return self._resolve_path(relative_path).stat().st_size

    @property
    def root(self) -> Path:
        """Base directory of this backend."""
        return self._root

    @property
    def location_name(self) -> str:
        """Identifier for this backend."""
        return self._location
