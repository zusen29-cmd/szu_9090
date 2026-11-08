from proto import HELLXdec_varint
def _read_varint(data, pos):
    return HELLXdec_varint(data, pos)
def _try_read_string_field(data, pos, expected_field_num):
    if pos >= len(data):
        return None, pos
    tag_byte, p = _read_varint(data, pos)
    fn = tag_byte >> 3
    wt = tag_byte & 0x7
    if fn != expected_field_num or wt != 2:
        return None, pos
    if p >= len(data):
        return None, pos
    length, p = _read_varint(data, p)
    if length <= 0 or p + length > len(data):
        return None, pos
    raw = data[p:p + length]
    try:
        return raw.decode('utf-8'), p + length
    except Exception:
        return None, pos
def _scan_idc_entries(data):
    entries = []
    seen_ipports = set()
    pos = 0
    while pos < len(data) - 6:
        if data[pos] != 0x0a:
            pos += 1
            continue
        region, p2 = _try_read_string_field(data, pos, 1)
        if region is None or not region.isalpha() or len(region) < 2:
            pos += 1
            continue
        idc_name, p3 = _try_read_string_field(data, p2, 2)
        if idc_name is None or len(idc_name) < 2:
            pos += 1
            continue
        ip_port, p4 = _try_read_string_field(data, p3, 3)
        if ip_port is None or ':' not in ip_port:
            pos += 1
            continue
        parts = ip_port.split(':')
        if len(parts) != 2 or not parts[1].isdigit():
            pos += 1
            continue
        if ip_port not in seen_ipports:
            seen_ipports.add(ip_port)
            entries.append({"region": region, "idc": idc_name, "ip_port": ip_port})
        pos = p4
    return entries
def _scan_field10_chunks(data, seen_offsets=None):
    if seen_offsets is None:
        seen_offsets = set()
    FIELD_10_TAG = 0x52
    results = []
    pos = 0
    while pos < len(data):
        if data[pos] != FIELD_10_TAG:
            pos += 1
            continue
        try:
            length, data_start = _read_varint(data, pos + 1)
        except Exception:
            pos += 1
            continue
        if length <= 0 or data_start + length > len(data):
            pos += 1
            continue
        chunk = data[data_start:data_start + length]
        end_pos = data_start + length
        abs_key = (pos, end_pos)
        if abs_key not in seen_offsets:
            seen_offsets.add(abs_key)
            results.append(chunk)
            nested = _scan_field10_chunks(chunk, seen_offsets)
            results.extend(nested)
        pos = end_pos
    return results
def _decode_idc_message(chunk):
    fields = {}
    pos = 0
    while pos < len(chunk):
        try:
            tag_byte, pos = _read_varint(chunk, pos)
            fn = tag_byte >> 3
            wt = tag_byte & 0x7
            if fn == 0 or wt not in (0, 1, 2, 5):
                break
            if wt == 0:
                val, pos = _read_varint(chunk, pos)
                fields[fn] = val
            elif wt == 2:
                length, pos = _read_varint(chunk, pos)
                raw_val = chunk[pos:pos + length]
                pos += length
                try:
                    fields[fn] = raw_val.decode('utf-8')
                except Exception:
                    fields[fn] = raw_val
            elif wt == 1:
                pos += 8
            elif wt == 5:
                pos += 4
        except Exception:
            break
    if 1 in fields and 2 in fields and 3 in fields:
        region  = fields[1] if isinstance(fields[1], str) else None
        idc_name = fields[2] if isinstance(fields[2], str) else None
        ip_port  = fields[3] if isinstance(fields[3], str) else None
        if region and idc_name and ip_port and ':' in ip_port:
            return {"region": region, "idc": idc_name, "ip_port": ip_port}
    return None
def _extract_webrtc_block(chunk):
    for marker in (b'ff.sdk.grtc', b'ff.rtc.grtc', b'ff.dr.grtc'):
        idx = chunk.find(marker)
        if idx != -1:
            break
    else:
        idx = chunk.find(b'garenanow')
        if idx == -1:
            return None
        while idx > 0 and chunk[idx - 1] in b'abcdefghijklmnopqrstuvwxyz0123456789.-':
            idx -= 1
    ALLOWED = set(b'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.:-;,_/')
    end = idx
    while end < len(chunk) and chunk[end] in ALLOWED:
        end += 1
    block_str = chunk[idx:end].decode('utf-8', errors='ignore')
    raw_parts = []
    for seg in block_str.split(';'):
        for sub in seg.split(','):
            p = sub.strip()
            if p:
                raw_parts.append(p)
    domains = [p for p in raw_parts if 'garenanow' in p or 'grtc' in p]
    def is_valid_ip(s):
        segs = s.split('.')
        if len(segs) != 4:
            return False
        return all(seg.isdigit() and 0 <= int(seg) <= 255 for seg in segs)
    ips = [p for p in raw_parts if p not in domains and is_valid_ip(p)]
    if not domains and not ips:
        return None
    return {"description": "WebRTC Fallback", "domains": domains, "ips": ips}
def HELLXextract_garena_voice_data(raw_bytes):
    result = {"idc_servers": [], "webrtc": None}
    seen_ipports = set()
    webrtc_added = False
    all_chunks = _scan_field10_chunks(raw_bytes)
    for chunk in all_chunks:
        idc = _decode_idc_message(chunk)
        if idc is not None:
            if idc['ip_port'] not in seen_ipports:
                seen_ipports.add(idc['ip_port'])
                result["idc_servers"].append(idc)
            continue
        if not webrtc_added and (b'ff.rtc.grtc' in chunk or b'garenanow' in chunk):
            webrtc = _extract_webrtc_block(chunk)
            if webrtc:
                result["webrtc"] = webrtc
                webrtc_added = True
    if not result["idc_servers"]:
        for entry in _scan_idc_entries(raw_bytes):
            if entry['ip_port'] not in seen_ipports:
                seen_ipports.add(entry['ip_port'])
                result["idc_servers"].append(entry)
    return result
def HELLXextract_voice_from_tcp(data):
    results = []
    seen = set()
    i = 0
    while i < len(data) - 7:
        if data[i:i+1].isdigit():
            j = i
            while j < len(data) and (data[j:j+1].isdigit() or data[j:j+1] == b'.'):
                j += 1
            candidate = data[i:j].decode('utf-8', errors='ignore')
            parts = candidate.split('.')
            if len(parts) == 4 and all(p.isdigit() and 0 <= int(p) <= 255 for p in parts):
                if parts[0] not in ('0', '1') and candidate not in seen:
                    seen.add(candidate)
                    results.append(candidate)
            i = j
        else:
            i += 1
    return results
