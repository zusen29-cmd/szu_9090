from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad
HELLX_KEY = bytes([89, 103, 38, 116, 99, 37, 68, 69, 117, 104, 54, 37, 90, 99, 94, 56])
HELLX_IV = bytes([54, 111, 121, 90, 68, 114, 50, 50, 69, 51, 121, 99, 104, 106, 77, 37])
def HELLXenc_aes(hex_data):
    cipher = AES.new(HELLX_KEY, AES.MODE_CBC, HELLX_IV)
    return cipher.encrypt(pad(bytes.fromhex(hex_data), AES.block_size)).hex()
def HELLXdec_aes(hex_data):
    cipher = AES.new(HELLX_KEY, AES.MODE_CBC, HELLX_IV)
    return unpad(cipher.decrypt(bytes.fromhex(hex_data)), AES.block_size).hex()
def HELLXenc_packet(hex_data, key, iv):
    cipher = AES.new(key, AES.MODE_CBC, iv)
    return cipher.encrypt(pad(bytes.fromhex(hex_data), 16)).hex()
def HELLXdec_packet(hex_data, key, iv):
    cipher = AES.new(key, AES.MODE_CBC, iv)
    return unpad(cipher.decrypt(bytes.fromhex(hex_data)), 16).hex()
