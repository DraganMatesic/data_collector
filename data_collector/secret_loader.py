"""
secret_loader.py

This script extracts an encrypted secrets.env.enc file from a zip archive,
decrypts it in memory using AES-256-CBC, and sets the contained environment
variables permanently on the system (User scope).
"""

import io
import os
import platform
import zipfile
from pathlib import Path
from typing import Optional, Union

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives import padding, hashes

if platform.system() == "Windows":
    import winreg
    import ctypes

class SecretLoader:
    def __init__(self):
        self.secret_env: Optional[io.BytesIO] = None

    @staticmethod
    def derive_key(password, salt, length=32):
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=length,
            salt=salt,
            iterations=100000,
            backend=default_backend()
        )
        return kdf.derive(password.encode())

    def extract_and_decrypt_from_zip(self, zip_path: Union[Path, str], filename: str = 'secrets.env.enc'):
        password = os.getenv("DC_SECRET_PASSWORD")
        if password is None:
            password = input("Enter decryption password: ")

        with zipfile.ZipFile(zip_path, 'r') as zipf:
            with zipf.open(filename) as f:
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
        print("✅ Decrypted and loaded into memory")

    def set_env_vars_permanently(self, env_file='secrets.env'):
        if self.secret_env is not None:
            self.secret_env.seek(0)
            lines = self.secret_env.read().decode().splitlines()
        elif Path(env_file).exists():
            with open(env_file, 'r') as f:
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
                        with winreg.OpenKey(
                            winreg.HKEY_CURRENT_USER,
                            "Environment",
                            0,
                            winreg.KEY_SET_VALUE,
                        ) as regkey:
                            winreg.SetValueEx(regkey, name, 0, winreg.REG_EXPAND_SZ, value)
                        print(f"✅ Set {name} = {value} (User scope)")
                    except Exception as e:
                        raise OSError(f"❌ Failed to set {name}: {e}")
                else:
                    bashrc = Path.home() / '.bashrc'
                    try:
                        with open(bashrc, 'a') as bash_file:
                            bash_file.write(f'\nexport {name}="{value}"')
                        print(f"✅ Set {name} = {value} in ~/.bashrc")
                    except Exception as e:
                        raise IOError(f"❌ Failed to write to .bashrc: {e}")

        if os_type == "Windows":
            hwnd_broadcast = 0xFFFF
            wm_settingchange = 0x1A
            ctypes.windll.user32.SendMessageW(hwnd_broadcast, wm_settingchange, 0, "Environment")
            print("🔄 Broadcasted environment change.")

if __name__ == "__main__":
    loader = SecretLoader()
    loader.extract_and_decrypt_from_zip(r"D:\Downloads\env-secrets-encrypted.zip")
    loader.set_env_vars_permanently()
