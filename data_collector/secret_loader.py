"""
secret_loader.py

This script extracts an encrypted secrets.env.enc file from a zip archive,
decrypts it in memory using AES-256-CBC, and sets the contained environment
variables permanently on the system (User scope).
"""

from __future__ import annotations

import importlib
import io
import os
import platform
import zipfile
from pathlib import Path

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC


def _set_windows_env_var(name: str, value: str) -> None:
    """Set a user-scope environment variable in Windows registry."""
    winreg = importlib.import_module("winreg")

    with winreg.OpenKey(
        winreg.HKEY_CURRENT_USER,
        "Environment",
        0,
        winreg.KEY_SET_VALUE,
    ) as regkey:
        winreg.SetValueEx(regkey, name, 0, winreg.REG_EXPAND_SZ, value)


def _broadcast_windows_env_change() -> None:
    """Broadcast environment change notification on Windows."""
    ctypes = importlib.import_module("ctypes")

    hwnd_broadcast = 0xFFFF
    wm_settingchange = 0x1A
    ctypes.windll.user32.SendMessageW(hwnd_broadcast, wm_settingchange, 0, "Environment")


class SecretLoader:
    """Decrypt encrypted secret files and persist variables at user scope."""

    def __init__(self) -> None:
        self.secret_env: io.BytesIO | None = None

    @staticmethod
    def derive_key(password: str, salt: bytes, length: int = 32) -> bytes:
        """Derive an AES key from password and salt using PBKDF2-HMAC-SHA256."""
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=length,
            salt=salt,
            iterations=100000,
            backend=default_backend()
        )
        return kdf.derive(password.encode())

    def extract_and_decrypt_from_zip(self, zip_path: Path | str, filename: str = "secrets.env.enc") -> None:
        """Extract and decrypt encrypted env file from ZIP archive into memory."""
        password = os.getenv("DC_SECRET_PASSWORD")
        if password is None:
            password = input("Enter decryption password: ")

        with zipfile.ZipFile(zip_path, "r") as zipf, zipf.open(filename) as f:
            salt = f.read(16)
            iv = f.read(16)
            ciphertext = f.read()

        key = self.derive_key(password, salt)

        cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
        decryptor = cipher.decryptor()
        padded_plaintext = decryptor.update(ciphertext) + decryptor.finalize()

        unpadder = padding.PKCS7(128).unpadder()
        plaintext = unpadder.update(padded_plaintext) + unpadder.finalize()

        self.secret_env = io.BytesIO(plaintext)
        print("Decrypted and loaded into memory")

    def set_env_vars_permanently(self, env_file: Path | str = "secrets.env") -> None:
        """Set variables from decrypted/file env source to user-scope environment."""
        if self.secret_env is not None:
            self.secret_env.seek(0)
            lines = self.secret_env.read().decode().splitlines()
        elif Path(env_file).exists():
            with open(env_file, encoding="utf-8") as f:
                lines = f.readlines()
        else:
            raise FileNotFoundError("No in-memory or file-based secret env found.")

        os_type = platform.system()
        cnt = 0
        for line in lines:
            if '=' in line:
                cnt += 1
                name, value = line.strip().split('=', 1)
                name, value = name.strip(), value.strip()
                print(f"{cnt}: Setting {name}")

                if os_type == "Windows":
                    try:
                        _set_windows_env_var(name, value)
                        print(f"Set {name} = {value} (User scope)")
                    except Exception as e:
                        raise OSError(f"Failed to set {name}: {e}") from e
                else:
                    bashrc = Path.home() / ".bashrc"
                    try:
                        with open(bashrc, "a", encoding="utf-8") as bash_file:
                            bash_file.write(f'\nexport {name}="{value}"')
                        print(f"Set {name} = {value} in ~/.bashrc")
                    except Exception as e:
                        raise OSError(f"Failed to write to .bashrc: {e}") from e

        if os_type == "Windows":
            _broadcast_windows_env_change()
            print("Broadcasted environment change.")

if __name__ == "__main__":
    loader = SecretLoader()
    loader.extract_and_decrypt_from_zip(r"D:\Downloads\env-secrets-encrypted.zip")
    loader.set_env_vars_permanently()
