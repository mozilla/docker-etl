import base64
import struct


def decode_base64url(data: str) -> bytes:
    padded = data + "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode(padded)


def encode_hpke_config(
    config_id: int, kem_id: int, kdf_id: int, aead_id: int, public_key_b64url: str
) -> str:
    # Decode the public key from base64url
    public_key = decode_base64url(public_key_b64url)

    if not (0 <= config_id <= 255):
        raise ValueError("config_id must be between 0 and 255")
    if len(public_key) > 65535:
        raise ValueError("public_key too long")

    # See https://ietf-wg-ppm.github.io/draft-ietf-ppm-dap/draft-ietf-ppm-dap.html#section-4.5.1-4
    # Pack big endian, byte, 2 bytes x 3
    encoded = struct.pack("!BHHH", config_id, kem_id, kdf_id, aead_id)
    # Pack big endian, 2 bytes
    encoded += struct.pack("!H", len(public_key)) + public_key

    # Return base64url-encoded HPKE config
    return base64.urlsafe_b64encode(encoded).decode("utf-8").rstrip("=")


b64_hpke = encode_hpke_config(
    config_id=1,
    kem_id=0x0020,  # DHKEM(X25519, HKDF-SHA256)
    kdf_id=0x0001,  # HKDF-SHA256
    aead_id=0x0001,  # AES-128-GCM
    public_key_b64url="your-collector-credentials-public-key-here",
)

print("Base64URL-encoded HPKE config:", b64_hpke)
