import struct
from encrypt import HELLXenc_packet, HELLXdec_packet
def HELLXenc_varint(n):
    if n < 0: return b''
    h = []
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            b |= 0x80
        h.append(b)
        if not n:
            break
    return bytes(h)
def HELLXdec_varint(data, pos):
    res, shift = 0, 0
    while pos < len(data):
        b = data[pos]
        res |= (b & 0x7f) << shift
        pos += 1
        if not (b & 0x80):
            return res, pos
        shift += 7
    return 0, pos
def HELLXenc_uid(uid):
    uid_int = int(uid)
    e = []
    while uid_int:
        e.append((uid_int & 0x7F) | (0x80 if uid_int > 0x7F else 0))
        uid_int >>= 7
    return bytes(e).hex()
def HELLXcreate_variant(field_number, value):
    field_header = (field_number << 3) | 0
    return HELLXenc_varint(field_header) + HELLXenc_varint(value)
def HELLXcreate_length(field_number, value):
    field_header = (field_number << 3) | 2
    encoded_value = value.encode('utf-8') if isinstance(value, str) else value
    return HELLXenc_varint(field_header) + HELLXenc_varint(len(encoded_value)) + encoded_value
def HELLXcreate_proto(fields):
    packet = bytearray()
    for field, value in fields.items():
        field = int(field)  # FIX: key có thể là string
        if isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    nested_packet = HELLXcreate_proto(item)
                    packet.extend(HELLXcreate_length(field, nested_packet))
                elif isinstance(item, int):
                    packet.extend(HELLXcreate_variant(field, item))
                elif isinstance(item, (str, bytes)):
                    packet.extend(HELLXcreate_length(field, item))
        elif isinstance(value, dict):
            nested_packet = HELLXcreate_proto(value)
            packet.extend(HELLXcreate_length(field, nested_packet))
        elif isinstance(value, int):
            packet.extend(HELLXcreate_variant(field, value))
        elif isinstance(value, (str, bytes)):
            packet.extend(HELLXcreate_length(field, value))
    return bytes(packet)
def HELLXdecode_proto(data):
    res_d = {}
    pos = 0
    while pos < len(data):
        try:
            tag, pos = HELLXdec_varint(data, pos)
            if tag == 0: break
            field_num = tag >> 3
            wire_type = tag & 0x07
            key = str(field_num)
            val = None
            if wire_type == 0:
                val, pos = HELLXdec_varint(data, pos)
            elif wire_type == 1:
                val = data[pos:pos+8].hex()
                pos += 8
            elif wire_type == 2:
                length, pos = HELLXdec_varint(data, pos)
                chunk = data[pos:pos+length]
                pos += length
                try:
                    s = chunk.decode('utf-8')
                    if all(32 <= ord(c) <= 126 or ord(c) > 160 for c in s):
                        val = s.strip()
                    else:
                        raise Exception
                except:
                    inner = HELLXdecode_proto(chunk)
                    val = inner if inner else chunk.hex()
            elif wire_type == 5:
                val = data[pos:pos+4].hex()
                pos += 4
            else:
                pos += 1
                continue
            if val is not None:
                if key in res_d:
                    if not isinstance(res_d[key], list):
                        res_d[key] = [res_d[key]]
                    res_d[key].append(val)
                else:
                    res_d[key] = val
        except Exception:
            break
    return res_d
def HELLXgenerate_packet_plain(proto_hex, opcode) -> bytes:
    payload = bytes.fromhex(proto_hex)
    length = len(payload)
    length_hex = hex(length)[2:].zfill(2)
    if len(length_hex) == 2: header = opcode + "000000" + length_hex
    elif len(length_hex) == 3: header = opcode + "00000" + length_hex
    elif len(length_hex) == 4: header = opcode + "0000" + length_hex
    elif len(length_hex) == 5: header = opcode + "000" + length_hex
    else: header = opcode + length_hex
    return bytes.fromhex(header) + payload
def HELLXgenerate_packet(proto_hex, opcode, key, iv):
    encrypted_payload = HELLXenc_packet(proto_hex, key, iv)
    length = len(encrypted_payload) // 2
    length_hex = hex(length)[2:].zfill(2)
    if len(length_hex) == 2: header = opcode + "000000" + length_hex
    elif len(length_hex) == 3: header = opcode + "00000" + length_hex
    elif len(length_hex) == 4: header = opcode + "0000" + length_hex
    elif len(length_hex) == 5: header = opcode + "000" + length_hex
    else: header = opcode + length_hex
    return bytes.fromhex(header + encrypted_payload)
