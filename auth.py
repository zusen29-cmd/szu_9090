import aiohttp
import ssl
import struct as _struct
import time as _time
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad
from proto import HELLXcreate_proto, HELLXgenerate_packet, HELLXenc_varint
from encrypt import HELLXenc_packet
from Pb2 import MajoRLoGinrEs_pb2, PorTs_pb2


# ---------------------------------------------------------------------------
# Version / IP fetch
# ---------------------------------------------------------------------------

async def HELLXfetch_version():
    # Dùng endpoint VN (ggwhitehawk) + region=VN để lấy đúng version OB55
    url = (
        "https://version.ggwhitehawk.com/live/ver.php"
        "?version=1.132.11&lang=hi&device=android&channel=android"
        "&appstore=googleplay&region=VN"
        "&whitelist_version=1.3.0&whitelist_sp_version=1.0.0"
    )
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json(content_type=None)
                    # VN endpoint trả latest_release_version (OB label) + remote_version (build)
                    remote_version = data.get("remote_version", "1.132.11")
                    server_url     = data.get("server_url", "")
                    client_ip      = data.get("client_ip", "")
                    return remote_version, server_url, client_ip
    except Exception as e:
        print(f"VERSION => {e}")
    # Fallback cứng OB55
    return "1.132.11", "", ""


async def HELLXfetch_city(ip):
    try:
        url = f"http://ip-api.com/json/{ip}?fields=status,city"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=5) as resp:
                if resp.status == 200:
                    data = await resp.json(content_type=None)
                    if data.get("status") == "success":
                        return data.get("city", "")
    except Exception as e:
        print(f"GEOIP => {e}")
    return ""


# ---------------------------------------------------------------------------
# Garena OAuth
# ---------------------------------------------------------------------------

async def HELLXgenerate_access(uid, password, user_agent):
    url = "https://100067.connect.garena.com/oauth/guest/token/grant"
    data = {
        "uid": uid,
        "password": password,
        "response_type": "token",
        "client_type": "2",
        "client_secret": "2ee44819e9b4598845141067b281621874d0d5d7af9d8f7e00c1e54715b7d1e3",
        "client_id": "100067",
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(
            url,
            headers={
                "Host": "100067.connect.garena.com",
                "User-Agent": user_agent,
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "close",
            },
            data=data,
        ) as response:
            if response.status != 200:
                return None, None
            res_data = await response.json()
            return res_data.get("open_id"), res_data.get("access_token")


# ---------------------------------------------------------------------------
# Major Login / GetLoginData
# ---------------------------------------------------------------------------

_AES_KEY = b'Yg&tc%DEuh6%Zc^8'
_AES_IV  = b'6oyZDr22E3ychjM%'

_DEVICE_POOL = [
    ("SM-G998B",   "Adreno (TM) 660",  "Android OS 12 / API-31 (SP1A.210812.016/G998BXXU5BULA)"),
    ("2201122G",   "Adreno (TM) 730",  "Android OS 13 / API-33 (TKQ1.220905.001/V14.0.6.0.TKMCNXM)"),
    ("RMX3700",    "Mali-G710 MC10",    "Android OS 14 / API-34 (UKQ1.230924.001/RMX3700.14.C.01)"),
    ("CPH2451",    "Adreno (TM) 740",  "Android OS 13 / API-33 (TP1A.220905.001/C.19)"),
    ("24094RAD4G", "Adreno (TM) 720",  "Android OS 14 / API-34 (UKQ1.230924.001/OS1.0.6.6.UNACNXM)"),
]

import random as _random, json as _json, os as _os, uuid as _uuid

_DEVICES_FILE = "devices_cache.json"

def _get_device(uid: str) -> dict:
    """Lấy hoặc tạo device fingerprint ổn định theo uid."""
    devices = {}
    if _os.path.exists(_DEVICES_FILE):
        try:
            with open(_DEVICES_FILE, "r", encoding="utf-8") as f:
                devices = _json.load(f)
        except Exception:
            pass
    if str(uid) in devices:
        return devices[str(uid)]
    model, gpu, os_ver = _random.choice(_DEVICE_POOL)
    dev = {
        "unique_device_id": f"Google|{_uuid.uuid4()}",
        "model":            model,
        "gpu_renderer":     gpu,
        "system_software":  os_ver,
        "screen_width":     _random.choice([1080, 1440, 2400]),
        "screen_height":    _random.choice([2400, 3200, 1080]),
        "screen_dpi":       str(_random.randint(300, 420)),
        "memory":           _random.randint(3000, 7500),
        "processor":        f"ARM64 FP ASIMD AES | {_random.randint(2200, 3200)} | {_random.randint(6, 12)}",
    }
    devices[str(uid)] = dev
    try:
        with open(_DEVICES_FILE, "w", encoding="utf-8") as f:
            _json.dump(devices, f, indent=2)
    except Exception:
        pass
    return dev


def HELLXbuild_login_payload(open_id, access_token, version, client_ip, city, uid=""):
    """Payload duy nhất dùng cho cả MajorLogin lẫn GetLoginData.
    Device fingerprint tạo ngẫu nhiên + cache theo uid để ổn định.
    """
    from datetime import datetime
    dev = _get_device(uid)
    fields = {
        3:  datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        4:  "free fire", 5: 4, 7: str(version),
        8:  dev["system_software"],
        9:  "Handheld", 10: "Viettel", 11: "WIFI",
        12: dev["screen_width"],
        13: dev["screen_height"],
        14: dev["screen_dpi"],
        15: dev["processor"],
        16: dev["memory"],
        17: dev["gpu_renderer"],
        18: "OpenGL ES 3.2 build 25.1@6715691",
        19: dev["unique_device_id"],
        20: client_ip,
        21: "vn", 22: str(open_id), 23: "4", 24: "Handheld",
        25: dev["model"], 26: "VN",
        29: str(access_token), 30: 1,
        41: "Viettel", 42: "WIFI",
        57: "7428b253defc164018c604a1ebbfebdf",
        60: 233141, 61: 172590, 62: 902,
        64: 173114, 65: 233141, 66: 173114, 67: 233141,
        73: 3,
        74: "/data/app/~~OOxSMPtiZSaEIgYJjPoHOg==/com.dts.freefireth-KkAAAdjU2FG5Ts4HwgDgCw==/lib/arm64",
        76: 1,
        77: "b8e0cd5e295eee42f5860d3c86e483dd|/data/app/~~OOxSMPtiZSaEIgYJjPoHOg==/com.dts.freefireth-KkAAAdjU2FG5Ts4HwgDgCw==/base.apk",
        78: 3, 79: 2, 81: "64", 83: "2019121229", 85: 3,
        86: "OpenGLES2", 87: 8191, 88: 4,
        90: city, 91: "SG", 92: 10968, 93: "android",
        94: "KqsHT0Q7h333TG5PRgHHMPeY52YAbiB2/CUjX28YaEK6kGm0ieehpzRhEeZ1N/GP2hWAoRRtOvXk8DqwEQgsJ2n+ohtc18X5bJZ6IS8WNFZf7sZI",
        95: 111107,
        96: "{\"cur_rate\":[60,90,120],\"support_etc2\":false}",
        97: 1, 99: "4", 100: "4",
        102: "4a501646550a090965",
        103: 1, 104: 1070, 105: 1,
        106: "https://dl.ak.freefiremobile.com/live/ABHotUpdates/|https://core-ak.freefiremobile.com/live/ABHotUpdates/|9a9a493d643462570dc50320b28533d0",
        107: "1.af7c82fd80a978e6",
    }
    proto_bytes = HELLXcreate_proto(fields)
    return AES.new(_AES_KEY, AES.MODE_CBC, _AES_IV).encrypt(pad(proto_bytes, AES.block_size))


# Alias tương thích với code cũ
HELLXbuild_major_login           = HELLXbuild_login_payload
HELLXbuild_get_login_data_payload = HELLXbuild_login_payload


async def HELLXmajor_login(payload, headers, server_url="https://loginbp.ggpolarbear.com/"):
    if not server_url.startswith("http"):
        server_url = "https://" + server_url
    url = server_url.rstrip("/") + "/MajorLogin"
    print(f"LOGIN[4/6] = > POST {url}")
    print(f"LOGIN[4/6] = > Request headers: {dict(headers)}")
    ssl_ctx = ssl.create_default_context()
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=payload, headers=headers, ssl=ssl_ctx, timeout=aiohttp.ClientTimeout(total=30)) as response:
                body = await response.read()
                print(f"LOGIN[4/6] = > Response status: {response.status}")
                print(f"LOGIN[4/6] = > Response headers: {dict(response.headers)}")
                print(f"LOGIN[4/6] = > Response body ({len(body)} bytes): {body[:64].hex()}")
                if response.status == 200:
                    return body
                print(f"LOGIN[4/6] = > Non-200: body text = {body[:256]!r}")
                return None
    except aiohttp.ClientConnectorError as e:
        print(f"LOGIN[4/6] = > Connection error: {e}")
    except aiohttp.ServerTimeoutError as e:
        print(f"LOGIN[4/6] = > Timeout: {e}")
    except Exception as e:
        print(f"LOGIN[4/6] = > Exception {type(e).__name__}: {e}")
    return None


async def HELLXget_login_data(base_url, open_id, access_token, token, headers, version="1.132.11", client_ip="129.45.8.219", city="Hanoi", uid=""):
    if not base_url:
        return None
    if not base_url.startswith("http"):
        base_url = "https://" + base_url
    url = f"{base_url}/GetLoginData"
    payload = HELLXbuild_login_payload(open_id, access_token, version, client_ip, city, uid=uid)
    ssl_ctx = ssl.create_default_context()
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE
    hdrs = headers.copy()
    hdrs["Authorization"] = f"Bearer {token}"
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(url, data=payload, headers=hdrs, ssl=ssl_ctx, timeout=30) as resp:
                if resp.status == 200:
                    return await resp.read()
        except Exception as e:
            print(f"AUTH => GetLoginData: {e}")
    return None


def HELLXdecode_major_login(response_bytes):
    proto = MajoRLoGinrEs_pb2.MajorLoginRes()
    proto.ParseFromString(response_bytes)
    return proto


def HELLXdecode_login_data(response_bytes):
    proto = PorTs_pb2.GetLoginData()
    proto.ParseFromString(response_bytes)
    return proto


# ---------------------------------------------------------------------------
# Online server auth packets (startup + ping)
# ---------------------------------------------------------------------------

_REGION_CODE_MAP = {
    "ME": 0x00, "IND": 0x01, "SG": 0x02, "TH": 0x03, "ID": 0x04,
    "VN": 0x05, "TW": 0x06, "US": 0x07, "SA": 0x08, "BR": 0x09,
    "RU": 0x0A, "BD": 0x0B, "PK": 0x0C,
}

def HELLXbuild_auth_startup(uid, token, timestamp, key, iv, region="VN", pkt_type="online"):
    """
    pkt_type="online" → pad2 = 13 zeros  (sock39699 — Online TCP)
    pkt_type="chat"   → pad2 = 5  zeros  (sock39801 — Chat TCP)
    Header: 01{region_code:02X}  e.g. "0105" cho VN
    """
    reg      = str(region).upper() if region else "VN"
    code     = _REGION_CODE_MAP.get(reg, 0x00)
    header   = f"{1:02d}{code:02X}"
    uid_hex  = hex(int(uid))[2:]
    uid_pad  = "0" * max(0, 16 - len(uid_hex))
    ts_hex   = f"{int(timestamp):X}"
    enc      = AES.new(key, AES.MODE_CBC, iv).encrypt(pad(token.encode(), AES.block_size)).hex()
    enc_len  = hex(len(enc) // 2)[2:]
    pad2     = "0000000000000" if pkt_type == "online" else "00000"
    return f"{header}{uid_pad}{uid_hex}{ts_hex}{pad2}{enc_len}{enc}"


def HELLXbuild_ping_packet(key, iv):
    fields = {1: 15, 2: {1: 804266360, 2: 1}}
    proto_hex = HELLXcreate_proto(fields).hex()
    return HELLXgenerate_packet(proto_hex, "0515", key, iv)


# ---------------------------------------------------------------------------
# Team join packet (needed to enter a room)
# ---------------------------------------------------------------------------

def HELLXbuild_join_sq(uid: int, code: str, key: bytes, iv: bytes) -> bytes:
    _F9_RAW = bytes.fromhex(
        "0a800130383846443242384141333045453934303230313031303030303030303030433030313530303031"
        "30303038303030313136423530454636304630303030303034363736323531343030303030303030303030"
        "30303030303030303030303030303030303030303030303030303066663030303030303030636163666131"
        "366410401a8f03725e5954140204480809515703510955030d5100085601010d54070a060d000000570204"
        "01070b001708004c715a45454e1b04180818130505491a45637e585e4174755953486270441f505e480155"
        "42426d5f6565041201481c005e701c66054b780557604263751c59514b444a435566010607091708024c7b"
        "474258704b587d6b7d445f454c62515d595272746a047c1e41540e170c455e486c0e7a470e710661685a56"
        "5c007058475a5e4e48794a615c460c1401487254764444534262666006404e090745001e49795c4e674d72"
        "41790a16084b54724e436542057f07610001616c5367774d5f7b706461710470700d17060d4d5f6202716e"
        "5b456c545f557142701f6d6352754f707645057e5e5a0c13074b5e78436462015503425a5f025376187542"
        "5548577b6d1e597e7247091a054c055d0367705d655c691e48000e6c624607737075607f6d010477540e17"
        "06094d7e470254430402621f4904057b78010100751b02687f784e1c5d680a1001484f5371476f7457795c"
        "0702775e5343550374647a6c7a08457672500822047e5b5950300b3a1a140544464205000607081514607f"
        "7c63736c785d5b725851545e4208312e3132332e3135480650025ad104036262535136393676524d527541"
        "6456324b796f566c63796661584f6548564d77696639476f78355758532f77785a4a426c55334d66336b59"
        "64307a5045483571785335596f385452325453585832496f5a4a565574444f6c4c74564f73526d676f5953"
        "6f33346a66574e2b724b4a2f345973424b41525a5a66324a464a4957506153635747594f684956464c5833"
        "4b7a693965324c50516d34366e49565965657950596b794b4d7263376f684d474b59574479443650317264"
        "3636706456704d6d5238663051524737506f45546577785a715a554a502f366961597562574a612f313853"
        "2b705239546b5839747073464f31666d564b595265764c347178367362544c435a316b4555767041365073"
        "54576a4e513161352f572b776a482f2b45554b4c7446597a4a515741757451786e6c653766533132475030"
        "6667712f70442b555a78477568663845646c556f4c434969706c6333795174664d474841696b7061575a75"
        "706371536f783764425072565179344d7276444353432f543876534c566a7976537958586c335948684c49"
        "322f6855627a6c39453871563554556965656947567946625768714d6f2b5472636d777a414e776c434c37"
        "5861514f6c74684c6b2f436d664f48594b4234366b35557449373142346f6f30545376674b456349704e51"
        "4575796d46506457703051477071755279426d7a4371463150724d59586336677947416e2f4d7043436669"
        "6d78364d304a4b554f417274454557506c4278724c4f2b6741357132624f64484b61782b784478376a2b61"
        "393432672f7541735a42354e5879653974513d3d"
    )
    fields = {
        1: 4,
        2: {
            4: bytes.fromhex("01030407090a0b1216191a201d27"),
            5: str(code),
            6: 6,
            8: 1,
            9: _F9_RAW,
            11: [
                {1: "IDC1", 2: 67, 3: "EUROPE"},
                {1: "IDC2", 2: 24, 3: "EUROPE"},
            ],
            13: "tr",
            16: bytes.fromhex("374f5219"),
            20: [
                {1: 3, 2: 357}, {1: 4, 2: 385}, {1: 5, 2: 156},
                {1: 29, 2: 204}, {1: 22, 2: 115}, {1: 14, 2: 139}, {1: 21},
            ],
        },
    }
    proto_hex = HELLXcreate_proto(fields).hex()
    return HELLXgenerate_packet(proto_hex, "0515", key, iv)


def HELLXbuild_leave_team(uid: int, key, iv) -> bytes:
    fields = {1: 7, 2: {1: uid}}
    proto_hex = HELLXcreate_proto(fields).hex()
    return HELLXgenerate_packet(proto_hex, "0515", key, iv)


# ---------------------------------------------------------------------------
# Voice packets
# ---------------------------------------------------------------------------

_VOICE_BASE_HEX = (
    "0a8e01"
    "0811100b180122"
    "04534f4e59"
    "2a013930cc80868c02"
    "3a0b61726d65616269"
    "2d763761"
    "4200"
    "4a2438323934386166322d313737392d346438372d613864352d356237363962376365623934"
    "522a464644353846423446373646363438433241354532314542434641334141453831423443394237443937"
    "5a05766f696365"
    "62075851"
    "2d42453432"
    "6802"
    "720471636f6d"
    "100b"
)

_STEP1_FIXED128_HEX = (
    "69839458c071310bbc7e2eede4101c55f0e8c682102c5b100c79"
    "291ae9800ad21d16f7a4e484d4c09f5960f58d00a576128a7bad"
    "036fe886b369e6e2e07ad339be2d93f40eb37e640790972809ea"
    "078b24540912ca6dd3f54183ac721db4c50cd3703941"
    "9f467a430d9429eeee04ead8b83073e54e45a51e0d33b9b273fd2942"
)

_VOICE_CUSTOM_MAGIC = bytes.fromhex("feffffff04b50001")


def HELLXbuild_open_micro(
    region_id: int, key: bytes, iv: bytes,
    region_prefix: str = "VN",
    app_id: str = "FFD58FB4F76F648C2A5E21EBCFA3AAE81B4C9B7D97",
    uuid: str = "f687ba460e-9b6b8bc39",
) -> bytes:
    """CMD 88 - open mic tren game server (online TCP).
    Voice MCU chi ACK mic_open UDP khi game server da authorize."""
    from proto import HELLXcreate_proto, HELLXgenerate_packet
    device = {
        1: 55, 2: 11, 3: 1, 4: "samsung", 5: "15",
        6: 562135116, 7: {12: 7005479156896394610}, 8: "",
        9: uuid, 10: app_id, 11: "voice",
        12: "SM-A145F", 13: 1, 14: "s5e3830",
    }
    voice_token = f"{app_id}{region_prefix}_{region_id}"
    fields = {1: 88, 2: device, 3: 4529, 4: voice_token, 5: "all"}
    proto_hex = HELLXcreate_proto(fields).hex()
    return HELLXgenerate_packet(proto_hex, "0515", key, iv)


def HELLXbuild_voice_step1(uid: str, region_id: int) -> bytes:
    base = bytearray(bytes.fromhex(_VOICE_BASE_HEX))
    base[4] = 0x00
    fixed128 = bytes.fromhex(_STEP1_FIXED128_HEX)
    payload = bytes(base) + b"\x12\x80\x01" + fixed128 + b"\x18\x01\x20\x06\x38\x01"
    return _struct.pack(">I", len(payload)) + payload


def HELLXbuild_voice_auth(uid: str, region_id: int, voice_port: int, region_prefix: str = "EUROPE") -> bytes:
    # uid phải là account_uid (game UID số nguyên dạng string), KHÔNG phải Garena UID
    base = bytearray(bytes.fromhex(_VOICE_BASE_HEX))
    base[4] = 0x07
    device_id = "FFD58FB4F76F648C2A5E21EBCFA3AAE81B4C9B7D97"
    # region_id có thể là uint64 lớn — luôn dùng str để tránh varint overflow
    region_str = f"{region_prefix}_{region_id}"
    field2_str = (device_id + region_str).encode()
    uid_b = uid.encode()
    fl = len(field2_str)
    fl_varint = bytes([fl]) if fl < 128 else bytes([0x80 | (fl & 0x7F), fl >> 7])
    payload = (
        bytes(base)
        + b"\x12" + fl_varint + field2_str
        + bytes.fromhex("20052801" "30d008" "380040005001")
        + b"\x5a" + bytes([len(uid_b)]) + uid_b
        + bytes.fromhex("60007000800101")
    )
    return _struct.pack(">I", len(payload)) + payload


def HELLXbuild_voice_ping(uid: str, region_id: int, voice_port: int, session_id: int = 0x0B) -> bytes:
    # uid phải là account_uid (game UID), KHÔNG phải Garena UID
    # session_id phải lấy từ field "5" trong Step3 auth response (self._voice_f5)
    base = bytearray(bytes.fromhex(_VOICE_BASE_HEX))
    base[4] = 0x09
    base_bytes = bytes(base[:-2])
    payload = base_bytes + b"\x10" + HELLXenc_varint(session_id)
    return _struct.pack(">I", len(payload)) + payload


def HELLXbuild_voice_stat_report(uid: str, region_id: int, session_id: int, region_prefix: str = "EUROPE") -> bytes:
    # uid phải là account_uid; session_id = self._voice_f5 từ Step3 response
    base = bytearray(bytes.fromhex(_VOICE_BASE_HEX))
    base[4] = 0x38
    base_bytes = bytes(base[:-2])
    region_str = f"{region_prefix}_{region_id}".encode()
    payload = (
        base_bytes
        + b"\x10" + HELLXenc_varint(session_id)
        + b"\x1a\x40" + b"FFD58FB4F76F648C2A5E21EBCFA3AAE81B4C9B7D97"
        + region_str
    )
    return _struct.pack(">I", len(payload)) + payload


def HELLXbuild_voice_broadcast_sub(uid: str, region_id: int, session_id: int, region_prefix: str = "EUROPE") -> bytes:
    # uid phải là account_uid; session_id = self._voice_f5 từ Step3 response
    base = bytearray(bytes.fromhex(_VOICE_BASE_HEX))
    base[4] = 0x37
    base_bytes = bytes(base[:-2])
    region_str = f"{region_prefix}_{region_id}".encode()
    payload = (
        base_bytes
        + b"\x10" + HELLXenc_varint(session_id)
        + b"\x1a\x40" + b"FFD58FB4F76F648C2A5E21EBCFA3AAE81B4C9B7D97"
        + region_str
        + b"\x22\x03all"
    )
    return _struct.pack(">I", len(payload)) + payload


def HELLXbuild_voice_auth_device(uid: str, region_id: int) -> bytes:
    payload = bytearray()
    payload.append(0x01)
    payload.append(0x0A)
    device_model = b"samsung"
    payload.append(len(device_model))
    payload.extend(device_model)
    payload.append(0x12)
    device_info = b"SM-S9210"
    payload.append(len(device_info))
    payload.extend(device_info)
    return _struct.pack(">I", len(payload)) + bytes(payload)


def HELLXbuild_voice_custom(
    uid: str,
    region_id: int,
    uuid: str = "f687ba460e-9b6b8bc39",
    device_id: str = "F76F648CCFA3AAE897",
    mcu_ip: str = "0.0.0.0",
    region_prefix: str = "EUROPE",
) -> bytes:
    # BUG FIX: uid phải là account_uid (game UID số nguyên dạng string),
    # KHÔNG phải Garena UID string (vd "8400545507").
    # uid_last3 dùng để nhận dạng session trên MCU — nếu sai thì MCU từ chối.
    # region_id là uint64 lớn (~2^61) — chỉ embed vào string, không encode varint.
    uid_str = uid.zfill(11)
    uid_last3 = uid_str[-3:].encode()
    region_str = f"{region_prefix}_{region_id}".encode()
    uuid_b = uuid.encode()
    device_b = device_id.encode()
    local_ip = b"0.0.0.0"
    mcu_ip_b = mcu_ip.encode()
    ts = int(_time.time()) & 0xFFFFFFFF
    pkt = bytearray()
    pkt.extend(_VOICE_CUSTOM_MAGIC)
    pkt.extend(b"\x00\x24"); pkt.extend(uuid_b);   pkt.append(0)
    pkt.extend(b"\x00\x2a"); pkt.extend(device_b); pkt.append(0)
    pkt.extend(b"\x00\x1a"); pkt.extend(region_str); pkt.append(0)
    pkt.extend(b"\x00\x0b"); pkt.extend(uid_last3); pkt.extend(b"\x00\x00\x00\x00")
    pkt.extend(b"\x28\x2e\x30\x00")
    pkt.append(0x07); pkt.extend(local_ip); pkt.extend(b"\x00")
    pkt.append(0x0D); pkt.extend(mcu_ip_b); pkt.extend(b"\x00")
    pkt.extend(b"\x17\x76")
    pkt.extend(_struct.pack(">I", ts))
    pkt.extend(b"\x00\x00\x00\x00")
    return _struct.pack(">I", len(pkt)) + bytes(pkt)


def HELLXparse_voice_custom_response(data: bytes):
    if len(data) > 4 and data[4:8] == b"\xfe\xff\xff\x7f":
        data = data[4:]
    if not data.startswith(b"\xfe\xff\xff\x7f"):
        return None
    import re
    for m in re.findall(rb"(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})", data):
        parts = m.split(b".")
        if all(0 <= int(p) <= 255 for p in parts) and parts[0] not in (b"0", b"1"):
            return m.decode()
    return None


def HELLXbuild_voice_redirect(uid: str, region_id: int, region_prefix: str = "EUROPE", ext_region: str = "de") -> bytes:
    # uid phải là account_uid; region_id embed vào string (không varint)
    device_id = "FFD58FB4F76F648C2A5E21EBCFA3AAE81B4C9B7D97"
    region_str = f"{region_prefix}_{region_id}"
    field1 = (device_id + region_str).encode()
    base = bytes.fromhex(_VOICE_BASE_HEX)
    inner = bytearray(base[3: 3 + 142])
    inner[1] = 0x11
    ext_b = ext_region.encode()
    payload = bytearray()
    payload.append(0x0A); payload.append(len(field1)); payload.extend(field1)
    payload.append(0x12); payload.append(0x8E); payload.append(0x01); payload.extend(inner)
    payload.append(0x18); payload.append(0x02)
    payload.append(0x22); payload.append(len(ext_b)); payload.extend(ext_b)
    return _struct.pack(">I", len(payload)) + bytes(payload)


# ---------------------------------------------------------------------------
# Chat auth + message (needed to receive !play commands from in-game chat)
# ---------------------------------------------------------------------------

def HELLXbuild_chat_auth(uid: int, region_id, key, iv) -> bytes:
    fields = {1: 3, 2: {1: int(uid), 3: "en", 4: str(region_id)}}
    proto_hex = HELLXcreate_proto(fields).hex()
    return HELLXgenerate_packet(proto_hex, "1215", key, iv)


def HELLXbuild_chat_close(uid: int, key, iv) -> bytes:
    fields = {1: 4, 2: {1: uid, 3: "en"}}
    proto_hex = HELLXcreate_proto(fields).hex()
    return HELLXgenerate_packet(proto_hex, "1215", key, iv)


import random as _random


async def _random_avatar():
    return _random.choice([902000068, 902000016, 902000011, 902000084, 902000085])


async def HELLXbuild_chat_message(uid: int, owner_uid: int, message: str, timestamp: int, key, iv) -> bytes:
    # await phải tách ra ngoài dict literal — Python không cho await trong dict value
    avatar_id = await _random_avatar()
    fields = {
        1: 1,
        2: {
            1: uid, 2: owner_uid, 4: message, 5: timestamp, 7: 6,
            9: {
                1: "HELLX", 2: avatar_id,
                3: 901000029, 4: 330, 5: 801043518, 7: 2, 10: 1, 12: 0,
                13: "", 14: {1: uid, 2: 8, 3: "\x10\x15\b\n\x0b\x13\f\x0f\x11\x04\x07\x02\x03\r\x0e\x12\x01\x05\x06"},
            },
            10: "en", 13: {3: 1}, 14: "",
        },
    }
    proto_hex = HELLXcreate_proto(fields).hex()
    return HELLXgenerate_packet(proto_hex, "1215", key, iv)
