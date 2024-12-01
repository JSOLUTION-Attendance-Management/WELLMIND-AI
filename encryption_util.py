from Cryptodome.Cipher import AES
from Cryptodome.Util.Padding import unpad, pad
from base64 import b64decode, b64encode
import os
from dotenv import load_dotenv

load_dotenv()

class EncryptionUtil:
    def __init__(self):
        self.secret_key = os.getenv('SECRET_KEY').encode('utf-8')
        self.init_vector = os.getenv('INIT_VECTOR').encode('utf-8')
        assert len(self.secret_key) == 16, "Secret key must be 16 bytes long"
        assert len(self.init_vector) == 16, "Initialization vector must be 16 bytes long"

    def encrypt(self, data: str) -> str:
        """Encrypt the given data and return a Base64 encoded string."""
        try:
            # AES cipher 생성
            cipher = AES.new(self.secret_key, AES.MODE_CBC, self.init_vector)
            # PKCS5Padding 적용 후 암호화 수행
            padded_data = pad(data.encode('utf-8'), AES.block_size)
            encrypted_data = cipher.encrypt(padded_data)
            # Base64로 인코딩하여 반환
            return b64encode(encrypted_data).decode('utf-8')
        except Exception as e:
            raise ValueError(f"Error encrypting data: {e}")

    def decrypt(self, encrypted_data: str) -> str:
        if not encrypted_data:
            return ""
        try:
            # Base64로 인코딩된 데이터를 디코드
            decoded_data = b64decode(encrypted_data)
            # AES cipher 생성
            cipher = AES.new(self.secret_key, AES.MODE_CBC, self.init_vector)
            # 복호화 수행
            decrypted_data = cipher.decrypt(decoded_data)
            # PKCS5Padding 제거
            padding_length = decrypted_data[-1]
            return decrypted_data[:-padding_length].decode('utf-8')
        except Exception as e:
            raise ValueError(f"Error decrypting data: {e}")