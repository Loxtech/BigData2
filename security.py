import os
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import padding

# En fast 256-bit nøgle til demonstration (I produktion gemmes denne i en Key Vault/Environment Variable)
SECRET_KEY = b'12345678901234567890123456789012' # 32 bytes = 256 bits

class SecurityModule:
    """
    Sikkerhedsmodul der implementerer både AES-CBC og AES-GCM.
    AES-GCM anvendes som primær metode jf. dokumentationen.

    Begrundelse for AES-GCM:
    - AES-GCM giver både kryptering og autentificering (integritet) i én operation.
    - Det er mere sikkert end AES-CBC, som kræver ekstra MAC for at sikre integriteten.
    - AES-GCM er modstandsdygtig over for padding oracle attacks, som AES-CBC kan være sårbar overfor.
    - AES-GCM er også mere effektiv på moderne hardware, da det kan parallelliseres.
    """

    @staticmethod
    def encrypt_aes_gcm(data: bytes, key: bytes = SECRET_KEY) -> bytes:
        """
        Krypterer data ved hjælp af AES-GCM (Authenticated Encryption).
        Returnerer: 12-byte Nonce + Ciphertext med Auth Tag.
        """
        aesgcm = AESGCM(key)
        nonce = os.urandom(12)  # Genererer unik 96-bit Nonce
        encrypted_data = aesgcm.encrypt(nonce, data, None)
        return nonce + encrypted_data  # Gemmer Nonce sammen med den krypterede data

    @staticmethod
    def decrypt_aes_gcm(encrypted_data: bytes, key: bytes = SECRET_KEY) -> bytes:
        """Dekrypterer AES-GCM data."""
        aesgcm = AESGCM(key)
        nonce = encrypted_data[:12]
        ciphertext = encrypted_data[12:]
        return aesgcm.decrypt(nonce, ciphertext, None)

    @staticmethod
    def encrypt_aes_cbc(data: bytes, key: bytes = SECRET_KEY) -> bytes:
        """
        Krypterer data ved hjælp af AES-CBC (Code-along metode).
        Kræver PKCS7 padding.
        """
        iv = os.urandom(16)  # 128-bit Initialization Vector
        padder = padding.PKCS7(128).padder()
        padded_data = padder.update(data) + padder.finalize()

        cipher = Cipher(algorithms.AES(key), modes.CBC(iv))
        encryptor = cipher.encryptor()
        ciphertext = encryptor.update(padded_data) + encryptor.finalize()
        return iv + ciphertext

    @staticmethod
    def decrypt_aes_cbc(encrypted_data: bytes, key: bytes = SECRET_KEY) -> bytes:
        """Dekrypterer AES-CBC data."""
        iv = encrypted_data[:16]
        ciphertext = encrypted_data[16:]

        cipher = Cipher(algorithms.AES(key), modes.CBC(iv))
        decryptor = cipher.decryptor()
        padded_data = decryptor.update(ciphertext) + decryptor.finalize()

        unpadder = padding.PKCS7(128).unpadder()
        return unpadder.update(padded_data) + unpadder.finalize()


# Helper-funktion til brug i Spark / Pipeline
def encrypt_dataset_gcm(csv_string: str) -> bytes:
    """Krypterer en hel CSV tekst-streng med den valgte metode (AES-GCM)."""
    return SecurityModule.encrypt_aes_gcm(csv_string.encode('utf-8'))

def decrypt_dataset_gcm(encrypted_bytes: bytes) -> str:
    """Dekrypterer AES-GCM bytes tilbage til en CSV-streng."""
    return SecurityModule.decrypt_aes_gcm(encrypted_bytes).decode('utf-8')