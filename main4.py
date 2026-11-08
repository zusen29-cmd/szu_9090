import asyncio
import shutil
import sys
import struct
import json
import os
import tempfile as _tmp
import glob as _glob

from utils import HELLXload_accounts, HELLXdefault_headers, HELLXlog
from auth import (
    HELLXgenerate_access,
    HELLXbuild_login_payload,
    HELLXbuild_major_login,
    HELLXmajor_login,
    HELLXget_login_data,
    HELLXdecode_major_login,
    HELLXdecode_login_data,
    HELLXbuild_auth_startup,
    HELLXbuild_ping_packet,
    HELLXbuild_join_sq,
    HELLXbuild_leave_team,
    HELLXbuild_voice_step1,
    HELLXbuild_voice_auth,
    HELLXbuild_voice_ping,
    HELLXbuild_voice_stat_report,
    HELLXbuild_voice_broadcast_sub,
    HELLXbuild_open_micro,  # FIX: cần gửi sau custom để join voice channel
    HELLXbuild_voice_custom,
    HELLXparse_voice_custom_response,
    HELLXbuild_voice_redirect,
    HELLXbuild_chat_auth,
    HELLXbuild_chat_close,
    HELLXbuild_chat_message,
)
from network import HELLXconnect_online, HELLXping_loop, HELLXread_loop
from voice import HELLXextract_garena_voice_data, HELLXextract_voice_from_tcp
from proto import HELLXdecode_proto


# ---------------------------------------------------------------------------
# Protobuf encode helpers (mirror s3's _encode_varint / _pb_encode_sync)
# ---------------------------------------------------------------------------

def _venc(v: int) -> bytes:
    """Encode một unsigned varint."""
    result = []
    while True:
        byte = v & 0x7F
        v >>= 7
        if v:
            byte |= 0x80
        result.append(byte)
        if not v:
            break
    return bytes(result)


def _pb_enc(fields: dict) -> bytes:
    """Encode protobuf dict thành bytes (giống s3's _pb_encode_sync)."""
    buf = bytearray()
    for f, v in fields.items():
        if isinstance(v, list):
            for item in v:
                if isinstance(item, dict):
                    nested = _pb_enc(item)
                    buf += _venc((f << 3) | 2) + _venc(len(nested)) + nested
        elif isinstance(v, dict):
            nested = _pb_enc(v)
            buf += _venc((f << 3) | 2) + _venc(len(nested)) + nested
        elif isinstance(v, bool):
            buf += _venc((f << 3) | 0) + _venc(int(v))
        elif isinstance(v, int):
            buf += _venc((f << 3) | 0) + _venc(v)
        elif isinstance(v, str):
            d = v.encode()
            buf += _venc((f << 3) | 2) + _venc(len(d)) + d
        elif isinstance(v, (bytes, bytearray)):
            d = bytes(v)
            buf += _venc((f << 3) | 2) + _venc(len(d)) + d
    return bytes(buf)


# ---------------------------------------------------------------------------
# Ogg/Opus helpers
# ---------------------------------------------------------------------------

def _ensure_yt_dlp():
    """Cài yt-dlp nếu chưa có."""
    try:
        import yt_dlp  # noqa
        return True
    except ImportError:
        pass
    import subprocess as _sp
    HELLXlog("SETUP", "yt-dlp chưa được cài — đang cài tự động...")
    res = _sp.run(
        [sys.executable, "-m", "pip", "install", "yt-dlp", "--quiet", "--break-system-packages"],
        capture_output=True, text=True, timeout=120,
    )
    if res.returncode == 0:
        HELLXlog("SETUP", "yt-dlp đã cài xong.")
        return True
    HELLXlog("SETUP", f"Cài yt-dlp thất bại: {res.stderr.strip()[-200:]}")
    return False


def _yt_dlp_command():
    _ensure_yt_dlp()
    return [sys.executable, "-m", "yt_dlp"]


# ---------------------------------------------------------------------------
# NhacCuaTui API — search + stream (thay thế yt-dlp)
# ---------------------------------------------------------------------------

# JWT token mặc định (guest, không cần đăng nhập, hạn 1 năm)
_NCT_DEFAULT_TOKEN = (
    "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9"
    ".eyJkdCI6IjE3OTQwNTUyMTg5NzkiLCJuYmYiOjE3OTQwNTUyMTgsImxvZ2luTWV0aG9kIjoiNSIs"
    "ImV4cGlyZWREYXRlIjoiMCIsImV4cCI6MTgyNTU5MTIxOCwiZGV2aWNlaW5mbyI6IntcIkFkSURcIjpcIlwiLF"
    "wiQXBwTmFtZVwiOlwiV0VCXCIsXCJBcHBWZXJzaW9uXCI6XCIxXCIsXCJEZXZpY2VJRFwiOlwiODE1MDFlZDFm"
    "Y2MyMWVmOFwiLFwiRGV2aWNlTmFtZVwiOlwiXCIsXCJOZXR3b3JrXCI6XCJcIixcIk9zTmFtZVwiOlwiV0VCX"
    "CIsXCJPc1ZlcnNpb25cIjpcIldFQlwiLFwiUHJvdmlkZXJcIjpcIk5DVENvcnBcIixcIlVzZXJOYW1lXCI6XCJ"
    "cIixcImlzVk5cIjpmYWxzZX0iLCJidmVkIjoiMCIsImRldmljZUlkIjoiODE1MDFlZDFmY2MyMWVmOCIsImlhdC"
    "I6MTc5NDA1NTIxOCwidXQiOiIwIn0"
    ".YkDGzBSXVqNl-Ky_ECN-Ci-eT2Ofz0CwVMdDP9JKKeA"
)
_NCT_DEVICE_ID = "81501ed1fcc21ef8"


def _nct_headers(ts: int) -> dict:
    """Build request headers cho NhacCuaTui graph API."""
    return {
        "Content-Type": "application/json",
        "User-Agent": (
            "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/150.0.0.0 Mobile Safari/537.36"
        ),
        "Authorization": _NCT_DEFAULT_TOKEN,
        "x-nct-token": _NCT_DEFAULT_TOKEN,
        "X-nct-os": "wap",
        "X-nct-version": "1",
        "x-nct-appid": "6",
        "x-nct-language": "vi",
        "x-nct-deviceid": _NCT_DEVICE_ID,
        "x-nct-time": str(ts),
        "X-nct-uuid": _NCT_DEVICE_ID,
        "X-sign": f"0aad3ad01527f23a6d5eeb193513eab6{_NCT_DEVICE_ID}",
        "timestamp": str(ts),
        "x-partner": "",
        "X-nct-userid": "0",
        "Origin": "https://www.nhaccuatui.com",
        "Referer": "https://www.nhaccuatui.com/",
        "Accept": "*/*",
        "Accept-Language": "vi-VN,vi;q=0.9,en;q=0.8",
    }


def _nct_pick_stream(stream_list: list) -> str | None:
    """
    Chọn stream URL tốt nhất từ streamURL list của NCT.
    Ưu tiên: 320kbps (onlyVIP=False, status=1) → 128kbps → bất kỳ status=1
    """
    prefer = ["320", "128", "lossless"]
    # Lọc chỉ các URL free (onlyVIP=False) và playable (status=1)
    free_playable = [s for s in stream_list if not s.get("onlyVIP") and s.get("status") == 1]
    for quality in prefer:
        for s in free_playable:
            if s.get("type") == quality and s.get("stream"):
                return s["stream"]
    # Fallback: bất kỳ free URL nào
    for s in free_playable:
        if s.get("stream"):
            return s["stream"]
    return None


async def _nct_search(query: str, limit: int = 5) -> list:
    """
    Tìm kiếm bài hát qua NhacCuaTui graph API.
    Trả về list[dict]: title, artist, url (stream mp3), duration_s, key, image
    URL đây là stream mp3 trực tiếp — không cần yt-dlp.
    """
    import aiohttp as _aiohttp
    import time as _t
    import urllib.parse as _up

    ts = int(_t.time() * 1000)
    encoded_kw = _up.quote(query)
    endpoint = (
        f"https://graph.nhaccuatui.com/api/v3/search/all"
        f"?keyword={encoded_kw}&correct=true&timestamp={ts}"
    )
    body = {
        "keyword": encoded_kw,
        "pageindex": 1,
        "pagesize": max(limit * 2, 10),  # lấy dư để lọc bài không có stream
        "isShowLoading": True,
    }

    try:
        timeout = _aiohttp.ClientTimeout(total=15)
        async with _aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                endpoint, headers=_nct_headers(ts), json=body
            ) as resp:
                data = await resp.json(content_type=None)
    except Exception as e:
        HELLXlog("NCT", f"Search request lỗi: {e}")
        return []

    if data.get("code") != 0:
        HELLXlog("NCT", f"Search API error: code={data.get('code')} msg={data.get('msg')}")
        return []

    songs = data.get("data", {}).get("songs", [])
    results = []
    for song in songs:
        # Chỉ lấy bài có thể phát (statusPlay=1) và có stream URL
        if song.get("statusPlay") != 1:
            continue
        stream_url = _nct_pick_stream(song.get("streamURL", []))
        if not stream_url:
            continue
        dur_s = song.get("duration", 0)
        mm, ss = divmod(dur_s, 60)
        results.append({
            "title":    song.get("name", "Unknown"),
            "artist":   song.get("artistName", ""),
            "url":      stream_url,                     # mp3 stream trực tiếp
            "duration": f"{mm}:{ss:02d}",
            "duration_s": dur_s,
            "key":      song.get("key", ""),
            "image":    song.get("image", ""),
            "source":   "nct",
        })
        if len(results) >= limit:
            break

    HELLXlog("NCT", f"Search '{query}' → {len(results)} bài có stream")
    return results


async def _nct_download_bytes(stream_url: str) -> bytes:
    """
    Tải file mp3 từ NCT stream URL về bytes.
    Dùng aiohttp streaming để tránh timeout với file lớn.
    """
    import aiohttp as _aiohttp
    import time as _t

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/150.0.0.0 Mobile Safari/537.36"
        ),
        "Referer": "https://www.nhaccuatui.com/",
        "Accept": "*/*",
    }
    timeout = _aiohttp.ClientTimeout(total=120, connect=10)
    async with _aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(stream_url, headers=headers) as resp:
            if resp.status != 200:
                raise RuntimeError(f"NCT stream HTTP {resp.status}")
            return await resp.read()


async def _youtube_search_api(query: str, limit: int = 5) -> list:
    """
    Tìm kiếm YouTube qua MWEB Internal API (fallback khi NCT không có kết quả).
    Trả về list[dict]: title, channel, url, duration
    """
    import aiohttp as _aiohttp

    endpoint = "https://m.youtube.com/youtubei/v1/search?prettyPrint=false"
    headers = {
        "Content-Type": "application/json",
        "User-Agent": (
            "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/150.0.0.0 Mobile Safari/537.36"
        ),
        "X-Youtube-Client-Name": "2",
        "X-Youtube-Client-Version": "2.20261106.00.00",
        "X-Youtube-Bootstrap-Logged-In": "false",
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Origin": "https://m.youtube.com",
        "Referer": "https://m.youtube.com/",
    }
    body = {
        "context": {
            "client": {
                "hl": "en",
                "gl": "VN",
                "clientName": "MWEB",
                "clientVersion": "2.20261106.00.00",
                "platform": "MOBILE",
                "clientFormFactor": "SMALL_FORM_FACTOR",
                "userAgent": (
                    "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/150.0.0.0 Mobile Safari/537.36,gzip(gfe)"
                ),
                "timeZone": "Asia/Saigon",
            },
            "user": {"lockedSafetyMode": False},
            "request": {"useSsl": True},
        },
        "query": query,
        "params": "mAEA",
    }

    try:
        timeout = _aiohttp.ClientTimeout(total=15)
        async with _aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(endpoint, headers=headers, json=body) as resp:
                data = await resp.json(content_type=None)
    except Exception as e:
        HELLXlog("SEARCH", f"YouTube API request lỗi: {e}")
        return []

    results = []
    sections = (
        data.get("contents", {})
            .get("sectionListRenderer", {})
            .get("contents", [])
    )
    for section in sections:
        items = section.get("itemSectionRenderer", {}).get("contents", [])
        for item in items:
            v = item.get("videoWithContextRenderer")
            if not v:
                continue
            video_id = v.get("videoId", "")
            if not video_id:
                continue
            title_runs = v.get("headline", {}).get("runs", [])
            title = title_runs[0].get("text", "Unknown") if title_runs else "Unknown"
            ch_runs = v.get("shortBylineText", {}).get("runs", [])
            channel = ch_runs[0].get("text", "") if ch_runs else ""
            dur_runs = v.get("lengthText", {}).get("runs", [])
            duration = dur_runs[0].get("text", "?") if dur_runs else "?"
            results.append({
                "title":    title,
                "artist":   channel,
                "url":      f"https://www.youtube.com/watch?v={video_id}",
                "duration": duration,
                "source":   "youtube",
            })
            if len(results) >= limit:
                break
        if len(results) >= limit:
            break

    HELLXlog("SEARCH", f"YouTube tìm '{query}' → {len(results)} kết quả")
    return results


async def _search_music(query: str, limit: int = 5) -> list:
    """
    Tìm nhạc: ưu tiên NCT (có mp3 trực tiếp), fallback YouTube (cần yt-dlp).
    Trả về list[dict] thống nhất: title, artist, url, duration, source
    """
    # Nếu là URL trực tiếp → wrap thành 1 item
    if query.startswith("http"):
        source = "nct" if "nct.vn" in query or "nhaccuatui" in query else "youtube"
        return [{"title": query, "artist": "", "url": query,
                 "duration": "?", "source": source}]

    results = await _nct_search(query, limit)
    if results:
        return results

    # NCT không có → thử YouTube
    HELLXlog("SEARCH", f"NCT không có kết quả cho '{query}' — thử YouTube")
    yt = await _youtube_search_api(query, limit)
    return yt


def _ffmpeg_executable():
    exe = shutil.which("ffmpeg")
    if exe:
        return exe
    try:
        import imageio_ffmpeg
        return imageio_ffmpeg.get_ffmpeg_exe()
    except Exception as exc:
        raise FileNotFoundError("ffmpeg not found. Install imageio-ffmpeg or add ffmpeg to PATH.") from exc


def _ogg_opus_frames_bytes(data: bytes):
    _, frames = _parse_ogg_buf(data, drain_all=True)
    yield from frames


def _parse_ogg_buf(buf: bytes, drain_all=False):
    frames = []
    while True:
        pos = buf.find(b"OggS")
        if pos == -1:
            buf = buf[-3:] if len(buf) >= 3 else buf
            break
        if len(buf) < pos + 27:
            break
        n_segs = buf[pos + 26]
        header_end = pos + 27 + n_segs
        if len(buf) < header_end:
            break
        seg_table = buf[pos + 27: header_end]
        page_size = sum(seg_table)
        if len(buf) < header_end + page_size:
            break
        page_data = buf[header_end: header_end + page_size]
        buf = buf[header_end + page_size:]
        if page_data[:8] in (b"OpusHead", b"OpusTags"):
            continue
        off = 0
        for s in seg_table:
            frame = page_data[off: off + s]
            off += s
            if len(frame) < 4:
                continue
            if frame[0] == 0x78:
                # silence/comfort noise — bỏ qua, tránh pop noise
                continue
            if frame[0] == 0x4b and len(frame) > 2 and frame[1] == 0x01:
                # Opus TOC byte 0x4b = stereo multi-frame, FF GVoice server không nhận
                # patch → 0x48 = mono single-frame (HELLX fix)
                frame = bytes([0x48]) + frame[2:]
            frames.append(frame)
    return buf, frames


# ---------------------------------------------------------------------------
# Bot class
# ---------------------------------------------------------------------------

class HELLXBot:
    def __init__(self, uid, password):
        self.uid = uid
        self.password = password
        self.key = b"Yg&tc%DEuh6%Zc^8"
        self.iv  = b"6oyZDr22E3ychjM%"

        # Auth
        self.token = None
        self.login_url = None
        self.timestamp = None
        self.account_uid = None   # uint64 game account ID từ MajorLoginRes.account_uid

        # Online server
        self.online_ip   = "148.222.82.5"   # VN online server (override sau login)
        self.online_port = 39699
        self.writer      = None

        # Chat server
        self.chat_ip   = None
        self.chat_port = None
        self._chat_writer = None
        self._chat_reader = None
        self._chat_buffer = bytearray()
        self._chat_keepalive_task = None

        # Team / region
        self.voice_region_id      = None
        self.chat_region_id       = None
        self.team_owner_uid       = None
        self.voice_chat_key       = None
        self.region_prefix        = "VN"   # default VN, tự override sau login
        self.ext_region           = "vn"
        self._sdk_verify_domain   = "ff.rtc.grtc.garenanow.com"
        self._processed_invite_code = None

        # Voice connect
        self._voice_ip            = None
        self._voice_port          = None
        self._voice_writer        = None
        self._voice_reader        = None
        self._voice_f5            = 0
        self._voice_f3            = 0x13E6
        self._voice_ssrc_int      = 0  # sẽ lấy từ Step3 resp field[13] hoặc field[3]
        self._voice_ping_task     = None
        self._voice_connect_task  = None
        self.voice_candidates     = []
        self.voice_ssrc           = b"\x0c\x43\x03\x3e"
        self._voice_ssrc_refresh  = False
        self.voice_session_ts     = None
        self._voice_music_ok      = False  # True sau khi nhận Member Info + gửi CMD33
        self._voice_udp_sock      = None   # socket dung chung mic_open + stream
        self.opus_bitrate         = 18000
        self.opus_samplerate      = 16000

        # Playback
        self._udp_task            = None
        self._current_ogg         = None
        self._current_url         = None
        self._track_title         = ""
        self._track_channel       = ""
        self._track_total_frames  = 0
        self._track_current_frame = 0
        self._stopped_frame       = 0
        self._volume              = 100
        self._progress_task       = None
        self._emote_task          = None

        # Queue nhạc — asyncio.Queue để xử lý tuần tự, tránh race condition
        # khi nhiều người cùng /join bot với bài khác nhau
        self._track_queue         = asyncio.Queue()   # item: (query_or_url, title_hint)
        self._queue_task          = None              # task chạy _queue_worker
        self._play_lock           = asyncio.Lock()    # 1 lượt download+encode mỗi lúc
        self._download_sem        = None              # set từ main() — semaphore toàn cục

        # Telegram
        self._telegram_chat_id    = None
        self._joining             = False
        self._join_success        = False

        # Emote / squad
        self._squad_uids   = []          # UIDs của tất cả member trong squad (kể cả bot)
        self._region_code  = 0x11        # byte region VN — dùng trong header packet outgoing
        self._emote_ids    = [
            909054004, 909040010, 909000075, 909000063, 909035007,
            909000085, 909000090, 909000098, 909045001, 909000081,
            909039011, 909049010, 909000068, 909039011, 909038010,
            909041005, 909033002, 909038012,
        ]

        # Internal
        self._current_version     = "1.132.11"  # OB55

    # ------------------------------------------------------------------
    # Entry point
    # ------------------------------------------------------------------

    async def run(self):
        import time as _time
        HELLXlog("BOT", f"Starting uid={self.uid}")
        headers = HELLXdefault_headers()

        # ── STEP 1: Fetch version ────────────────────────────────────────
        HELLXlog("LOGIN[1/6]", "Fetching version from server...")
        from auth import HELLXfetch_version, HELLXfetch_city
        try:
            version, server_url, client_ip = await HELLXfetch_version()
        except Exception as e:
            HELLXlog("LOGIN[1/6]", f"EXCEPTION: {type(e).__name__}: {e}")
            return
        if not version:
            HELLXlog("LOGIN[1/6]", "FAILED — server returned empty version")
            HELLXlog("LOGIN[1/6]", f"server_url={server_url!r} client_ip={client_ip!r}")
            return
        HELLXlog("LOGIN[1/6]", f"OK — version={version} server_url={server_url} client_ip={client_ip}")

        # ── STEP 2: GeoIP city ───────────────────────────────────────────
        HELLXlog("LOGIN[2/6]", f"Fetching city for ip={client_ip}...")
        try:
            city = await HELLXfetch_city(client_ip)
        except Exception as e:
            HELLXlog("LOGIN[2/6]", f"EXCEPTION: {type(e).__name__}: {e} — dùng city=''")
            city = ""
        HELLXlog("LOGIN[2/6]", f"OK — city={city!r}")

        # ── STEP 3: OAuth (Garena token) ─────────────────────────────────
        HELLXlog("LOGIN[3/6]", f"OAuth uid={self.uid}...")
        try:
            open_id, access_token = await HELLXgenerate_access(self.uid, self.password, headers["User-Agent"])
        except Exception as e:
            HELLXlog("LOGIN[3/6]", f"EXCEPTION: {type(e).__name__}: {e}")
            return
        if not open_id or not access_token:
            HELLXlog("LOGIN[3/6]", f"FAILED — open_id={open_id!r} access_token={'*'*8 if access_token else None}")
            HELLXlog("LOGIN[3/6]", "Gợi ý: sai password / uid / account bị khoá / client_secret hết hạn")
            return
        HELLXlog("LOGIN[3/6]", f"OK — open_id={open_id} access_token={access_token[:8]}...")

        # ── STEP 4: MajorLogin ───────────────────────────────────────────
        HELLXlog("LOGIN[4/6]", f"MajorLogin → {server_url}...")
        try:
            major_payload = HELLXbuild_login_payload(open_id, access_token, version, client_ip, city, uid=str(self.uid))
            HELLXlog("LOGIN[4/6]", f"Payload built: {len(major_payload)} bytes")
            major_resp = await HELLXmajor_login(major_payload, headers, server_url)
        except Exception as e:
            HELLXlog("LOGIN[4/6]", f"EXCEPTION: {type(e).__name__}: {e}")
            return
        if not major_resp:
            HELLXlog("LOGIN[4/6]", "FAILED — server trả None/empty")
            HELLXlog("LOGIN[4/6]", "Gợi ý: sai server_url / version / APK md5 / AES key-iv không khớp")
            return
        HELLXlog("LOGIN[4/6]", f"OK — response {len(major_resp)} bytes (hex preview: {major_resp[:16].hex()})")

        decoded = None
        found_offset = -1
        for off in [0, 64] + list(range(128)):
            if off >= len(major_resp):
                break
            try:
                from Pb2 import MajoRLoGinrEs_pb2 as _mlr
                candidate = _mlr.MajorLoginRes()
                candidate.ParseFromString(major_resp[off:])
                if candidate.token and candidate.region:
                    decoded = candidate
                    found_offset = off
                    break
            except Exception:
                continue
        if not decoded:
            HELLXlog("LOGIN[4/6]", "Decode FAILED — không tìm được offset hợp lệ trong 128 byte đầu")
            HELLXlog("LOGIN[4/6]", f"Raw bytes hex: {major_resp.hex()}")
            return
        HELLXlog("LOGIN[4/6]", f"Decode OK tại offset={found_offset}")
        self.token     = decoded.token or None
        # FIX: lưu account_uid (game UID số nguyên) từ MajorLoginRes
        _acct_uid = getattr(decoded, "account_uid", None)
        if _acct_uid:
            self.account_uid = int(_acct_uid)
            HELLXlog("LOGIN[4/6]", f"account_uid từ server: {self.account_uid}")
        else:
            self.account_uid = int(self.uid)
            HELLXlog("LOGIN[4/6]", f"account_uid KHÔNG CÓ — fallback garena uid: {self.account_uid}")
        self.login_url = decoded.url or server_url
        self.timestamp = decoded.timestamp or int(_time.time())
        # Key/IV từ server — nếu có thì dùng, nếu không giữ hardcode mặc định
        _raw_key = getattr(decoded, "key", None) or getattr(decoded, "aes_ak", None)
        _raw_iv  = getattr(decoded, "iv",  None) or getattr(decoded, "iv_i",  None)
        if _raw_key:
            self.key = bytes(_raw_key)
            HELLXlog("LOGIN[4/6]", f"Key từ server: {bytes(_raw_key).hex()[:16]}...")
        else:
            HELLXlog("LOGIN[4/6]", f"Key từ server: KHÔNG CÓ — dùng hardcode")
        if _raw_iv:
            self.iv = bytes(_raw_iv)
            HELLXlog("LOGIN[4/6]", f"IV từ server:  {bytes(_raw_iv).hex()[:16]}...")
        else:
            HELLXlog("LOGIN[4/6]", f"IV từ server:  KHÔNG CÓ — dùng hardcode")
        HELLXlog("LOGIN[4/6]", f"Decoded — token={str(self.token)[:12]}... login_url={self.login_url} ts={self.timestamp}")
        if not self.token:
            HELLXlog("LOGIN[4/6]", "FAILED — token rỗng sau decode")
            HELLXlog("LOGIN[4/6]", f"Raw bytes hex: {major_resp.hex()}")
            return

        # ── STEP 5: GetLoginData ─────────────────────────────────────────
        HELLXlog("LOGIN[5/6]", f"GetLoginData → {self.login_url}...")
        try:
            login_data_resp = await HELLXget_login_data(
                self.login_url, open_id, access_token, self.token, headers,
                version, client_ip, city, uid=str(self.uid)
            )
        except Exception as e:
            HELLXlog("LOGIN[5/6]", f"EXCEPTION: {type(e).__name__}: {e}")
            return
        if not login_data_resp:
            HELLXlog("LOGIN[5/6]", "FAILED — server trả None/empty")
            HELLXlog("LOGIN[5/6]", "Gợi ý: token không hợp lệ / login_url sai / timeout")
            return
        HELLXlog("LOGIN[5/6]", f"OK — response {len(login_data_resp)} bytes (hex preview: {login_data_resp[:16].hex()})")

        # Parse voice candidates + region
        try:
            self.voice_candidates = []
            voice_data = HELLXextract_garena_voice_data(login_data_resp)
            for entry in voice_data["idc_servers"]:
                parts = entry["ip_port"].split(":")
                ip, port = parts[0], int(parts[1])
                if entry["idc"] == "IDC2":
                    self.voice_candidates.insert(0, (ip, port))
                else:
                    self.voice_candidates.append((ip, port))
            HELLXlog("LOGIN[5/6]", f"Voice candidates: {self.voice_candidates}")

            if voice_data["webrtc"]:
                sdk_domains = voice_data["webrtc"].get("domains", [])
                if sdk_domains:
                    self._sdk_verify_domain = sdk_domains[0]
                    HELLXlog("LOGIN[5/6]", f"SDK verify domain: {self._sdk_verify_domain}")
        except Exception as e:
            HELLXlog("LOGIN[5/6]", f"Voice data parse EXCEPTION: {type(e).__name__}: {e}")

        try:
            _raw_p = HELLXdecode_proto(login_data_resp)
            region_val = _raw_p.get("2") or _raw_p.get("3")
            HELLXlog("LOGIN[5/6]", f"Region raw field: {region_val!r}")
            if region_val and isinstance(region_val, str) and region_val.isalpha():
                self.region_prefix = region_val.upper()
                _r2e = {
                    "EUROPE": "eu", "VN": "vn", "SA": "sa", "BR": "br",
                    "ID": "id", "TW": "tw", "TH": "th", "SG": "sg",
                    "ME": "me", "IND": "in", "NA": "na", "LATAM": "la",
                }
                self.ext_region = _r2e.get(self.region_prefix, self.region_prefix.lower()[:2])
            HELLXlog("LOGIN[5/6]", f"Region: prefix={self.region_prefix} ext={self.ext_region}")
        except Exception as e:
            HELLXlog("LOGIN[5/6]", f"Region parse EXCEPTION: {type(e).__name__}: {e}")

        try:
            from Pb2 import PorTs_pb2 as _pts
            ld_proto = None
            # Thử parse thẳng trước
            try:
                _cand = _pts.GetLoginData()
                _cand.ParseFromString(login_data_resp)
                if _cand.Online_IP_Port or _cand.AccountIP_Port:
                    ld_proto = _cand
            except Exception:
                pass
            # Nếu không ra, scan offset 0→127
            if not ld_proto:
                for off in range(min(128, len(login_data_resp))):
                    try:
                        _cand = _pts.GetLoginData()
                        _cand.ParseFromString(login_data_resp[off:])
                        if _cand.Online_IP_Port or _cand.AccountIP_Port:
                            ld_proto = _cand
                            HELLXlog("LOGIN[5/6]", f"GetLoginData decode OK tại offset={off}")
                            break
                    except Exception:
                        continue
            if ld_proto:
                online_port_str  = getattr(ld_proto, "Online_IP_Port", None)
                account_port_str = getattr(ld_proto, "AccountIP_Port", None)
                HELLXlog("LOGIN[5/6]", f"Online_IP_Port={online_port_str!r} AccountIP_Port={account_port_str!r}")
                if online_port_str and ":" in online_port_str:
                    p = online_port_str.split(":")
                    self.online_ip, self.online_port = p[0], int(p[1])
                if account_port_str and ":" in account_port_str:
                    p = account_port_str.split(":")
                    self.chat_ip, self.chat_port = p[0], int(p[1])
            else:
                HELLXlog("LOGIN[5/6]", "GetLoginData decode FAILED — không tìm được offset hợp lệ")
                HELLXlog("LOGIN[5/6]", f"Raw hex: {login_data_resp.hex()}")
        except Exception as e:
            HELLXlog("LOGIN[5/6]", f"LoginData decode EXCEPTION: {type(e).__name__}: {e}")
            HELLXlog("LOGIN[5/6]", f"Raw hex: {login_data_resp.hex()}")

        self._current_version = version

        # ── ChooseEmote — đăng ký emote set trước khi vào game ──────────
        await self._choose_emote_http()

        # ── STEP 6: Online TCP connect ───────────────────────────────────
        HELLXlog("LOGIN[6/6]", f"Connecting online server {self.online_ip}:{self.online_port}...")
        try:
            auth_hex = HELLXbuild_auth_startup(self.account_uid, self.token, self.timestamp, self.key, self.iv, region=self.region_prefix)
            HELLXlog("LOGIN[6/6]", f"Auth packet: {len(auth_hex)//2} bytes (hex preview: {auth_hex[:32]})")
            reader, writer = await HELLXconnect_online(self.online_ip, self.online_port, auth_hex)
        except Exception as e:
            HELLXlog("LOGIN[6/6]", f"EXCEPTION: {type(e).__name__}: {e}")
            return
        if not writer:
            HELLXlog("LOGIN[6/6]", "FAILED — không kết nối được online server")
            HELLXlog("LOGIN[6/6]", f"Gợi ý: online_ip={self.online_ip} port={self.online_port} — kiểm tra firewall/IP")
            return
        HELLXlog("LOGIN[6/6]", f"OK — online TCP connected ✅")

        self.writer = writer
        asyncio.create_task(HELLXping_loop(writer, HELLXbuild_ping_packet, self.key, self.iv))
        await HELLXread_loop(reader, self._handle_tcp_data, self.key, self.iv)

    # ------------------------------------------------------------------
    # Incoming packet handler (online server)
    # ------------------------------------------------------------------

    async def _handle_tcp_data(self, data):
        buffer = bytearray(data)
        pos = 0
        while pos < len(buffer) - 5:
            opcode = buffer[pos]
            length = struct.unpack(">I", buffer[pos + 1: pos + 5])[0]
            if 0 < length < 200000 and pos + 5 + length <= len(buffer):
                payload = bytes(buffer[pos + 5: pos + 5 + length])
                try:
                    proto_data = HELLXdecode_proto(payload)
                    if not proto_data:
                        pos += 5 + length
                        continue

                    field5 = proto_data.get("5")

                    # Auto-accept invite
                    if isinstance(field5, dict):
                        code = field5.get("8")
                        owner_uid = field5.get("1", self.uid)
                        if code and isinstance(code, str) and "_" in code:
                            if code != self._processed_invite_code:
                                self._processed_invite_code = code
                                # Dùng join_sq flow — accept_invite không cần thiết

                    # Team joined → get region_id → start voice
                    if not self.voice_region_id and isinstance(field5, dict):
                        region_id = field5.get("17")
                        if region_id:
                            self.voice_region_id = int(region_id)
                            self.chat_region_id  = int(self.voice_region_id / 256 + 0.5) * 256
                            self.team_owner_uid  = int(field5.get("1", self.uid))
                            HELLXlog("TEAM", f"region_id={self.voice_region_id} owner={self.team_owner_uid}")
                            chat_key = field5.get("31")
                            if chat_key:
                                self.voice_chat_key = str(chat_key)
                            if self._joining:
                                self._join_success = True
                            asyncio.create_task(self._connect_chat())
                            if not self._voice_connect_task or self._voice_connect_task.done():
                                self._voice_connect_task = asyncio.create_task(self._voice_connect())

                    # Parse squad member UIDs từ field5["6"] (member list)
                    # FIX: luôn update kể cả khi member leave → squad chỉ còn owner
                    if isinstance(field5, dict):
                        msg_type = proto_data.get("4")
                        if isinstance(msg_type, (int, str)) and int(str(msg_type) or 0) in (3, 6, 8, 44, 56):
                            uids = []
                            owner = field5.get("1")
                            if owner and str(owner).isdigit():
                                uids.append(int(owner))
                            members = field5.get("6")
                            if isinstance(members, list):
                                for m in members:
                                    if isinstance(m, dict):
                                        mid = m.get("1")
                                        if mid and str(mid).isdigit():
                                            uids.append(int(mid))
                            # FIX: luôn đảm bảo team_owner_uid có trong list
                            # Tránh bug: người chơi leave → member list chỉ còn owner
                            # nhưng owner bị thiếu do parse sai field
                            if self.team_owner_uid and self.team_owner_uid not in uids:
                                uids.append(self.team_owner_uid)
                            # FIX: update bất kể list có 1 người hay nhiều người
                            # Code cũ: "if uids" đúng nhưng thiếu trường hợp
                            # owner leave → uids rỗng → squad_uids không reset
                            self._squad_uids = list(set(uids))
                            HELLXlog("TEAM", f"squad_uids updated: {self._squad_uids}")

                except Exception:
                    pass
                pos += 5 + length
            else:
                pos += 1

    # ------------------------------------------------------------------
    # Chat server
    # ------------------------------------------------------------------

    def _start_chat_connection(self, cr, cw):
        """Lưu reader/writer mới, cancel keepalive cũ, close writer cũ, start task mới."""
        # FIX: close writer cũ trước khi thay thế — tránh leak connection TCP cũ
        # còn treo và bị server kick -> _chat_read_loop cũ sẽ detect EOF và gọi
        # _reconnect_chat() không cần thiết ngay sau khi vừa connect team mới
        old_writer = self._chat_writer
        if old_writer and not old_writer.transport.is_closing():
            try:
                old_writer.close()
            except Exception:
                pass
        self._chat_reader = cr
        self._chat_writer = cw
        if self._chat_keepalive_task and not self._chat_keepalive_task.done():
            self._chat_keepalive_task.cancel()
        asyncio.create_task(self._chat_read_loop(cr))
        self._chat_keepalive_task = asyncio.create_task(self._chat_keepalive_loop(cw))

    async def _connect_chat(self):
        if not self.chat_ip or not self.voice_region_id:
            return
        try:
            auth_hex = HELLXbuild_auth_startup(self.account_uid, self.token, self.timestamp, self.key, self.iv, region=self.region_prefix, pkt_type="chat")
            cr, cw = await asyncio.wait_for(
                asyncio.open_connection(self.chat_ip, self.chat_port), timeout=5.0
            )
            cw.write(bytes.fromhex(auth_hex))
            await cw.drain()
            owner = self.team_owner_uid or int(self.uid)
            cw.write(HELLXbuild_chat_auth(owner, str(self.chat_region_id), self.key, self.iv))
            await cw.drain()
            HELLXlog("CHAT", f"Connected region={self.chat_region_id}")
            self._start_chat_connection(cr, cw)
        except Exception as e:
            HELLXlog("CHAT", f"Connect failed: {e}")

    async def _chat_read_loop(self, reader):
        try:
            while True:
                data = await reader.read(16384)
                if not data:
                    # FIX: chỉ reconnect nếu reader này vẫn là reader hiện tại
                    # (tránh reader cũ từ team trước gọi reconnect sau khi đã join team mới)
                    if reader is self._chat_reader:
                        HELLXlog("CHAT", "Disconnected — reconnecting...")
                        asyncio.create_task(self._reconnect_chat())
                    else:
                        HELLXlog("CHAT", "Old reader closed — ignoring.")
                    break
                await self._handle_chat_data(data)
        except Exception as e:
            if reader is self._chat_reader:
                HELLXlog("CHAT", f"Error: {e} — reconnecting...")
                asyncio.create_task(self._reconnect_chat())
            else:
                HELLXlog("CHAT", f"Old reader error (ignored): {e}")

    async def _reconnect_chat(self):
        await asyncio.sleep(2)
        if not self.chat_ip or not self.chat_port:
            return
        # FIX: không reconnect khi bot đã leave team (voice_region_id = None)
        # Tránh case: sau leave, _reconnect_chat() chạy với chat_region_id=None
        # -> gửi chat_auth thiếu region -> server reject -> TCP disconnect ngay khi join team mới
        if not self.voice_region_id:
            HELLXlog("CHAT", "Not in team — skipping reconnect.")
            return
        try:
            auth_hex = HELLXbuild_auth_startup(self.account_uid, self.token, self.timestamp, self.key, self.iv, region=self.region_prefix, pkt_type="chat")
            cr, cw = await asyncio.wait_for(
                asyncio.open_connection(self.chat_ip, self.chat_port), timeout=5.0
            )
            cw.write(bytes.fromhex(auth_hex))
            await cw.drain()
            owner = self.team_owner_uid or int(self.uid)
            cw.write(HELLXbuild_chat_auth(owner, str(self.chat_region_id), self.key, self.iv))
            await cw.drain()
            HELLXlog("CHAT", "Reconnected")
            self._start_chat_connection(cr, cw)
        except Exception as e:
            HELLXlog("CHAT", f"Reconnect failed: {e}")

    async def _chat_keepalive_loop(self, writer):
        try:
            while writer and not writer.transport.is_closing():
                await asyncio.sleep(15)   # 15s — tránh spam, chat server không cần quá dày
                try:
                    writer.write(bytes.fromhex("0201"))
                    await writer.drain()
                except Exception:
                    break
        except asyncio.CancelledError:
            pass

    async def _handle_chat_data(self, data: bytes):
        self._chat_buffer.extend(data)
        buffer = self._chat_buffer
        pos = 0
        while len(buffer) - pos >= 5:
            length = struct.unpack(">I", buffer[pos + 1: pos + 5])[0]
            if 0 < length < 200000 and pos + 5 + length <= len(buffer):
                payload = bytes(buffer[pos + 5: pos + 5 + length])
                try:
                    proto_data = HELLXdecode_proto(payload)
                    if proto_data:
                        field5 = proto_data.get("5")
                        if isinstance(field5, dict):
                            sender_uid = str(field5.get("1", ""))
                            message    = field5.get("4", "")
                            if isinstance(message, str) and message.strip() and sender_uid != str(self.uid):
                                HELLXlog("CHAT", f"[{sender_uid}]: {message}")
                                asyncio.create_task(self._handle_command(message.strip(), sender_uid))
                except Exception as e:
                    HELLXlog("CHAT", f"Parse error: {e}")
                pos += 5 + length
            else:
                break
        self._chat_buffer = buffer[pos:]

    # ------------------------------------------------------------------
    # In-game chat commands  (!play / !stop / !resume / !volume / !search / !help)
    # ------------------------------------------------------------------

    async def _handle_command(self, message: str, sender_uid: str):
        try:
            msg = message.strip()
            if msg.startswith("/") and not msg.startswith("//"):
                msg = "!" + msg[1:]
            parts = msg.split()
            cmd  = parts[0].lower() if parts else ""
            args = parts[1:] if len(parts) > 1 else []

            if cmd == "!help":
                await self._send_chat(
                    "[27EBF5]╔══ MUSIC BOT ══╗\n"
                    "[27EBF5]!search [tên bài]\n"
                    "[A627F5]  → Tìm nhạc (NCT, 10 bài)\n"
                    "[27EBF5]!play [số] hoặc [tên/URL]\n"
                    "[A627F5]  → Phát bài (số từ !search)\n"
                    "[27EBF5]!skip / !queue / !clear\n"
                    "[27EBF5]!stop / !resume\n"
                    "[27EBF5]!volume [0-200]\n"
                    "[27EBF5]!leave — bot rời team\n"
                    "[27EBF5]╚══════════════╝"
                )

            elif cmd == "!search":
                query = " ".join(args)
                if not query:
                    await self._send_chat("[F52727]Dùng: !search [tên bài]")
                    return
                await self._send_chat(f"[27EBF5]🔍 Đang tìm: {query[:40]}...")
                try:
                    results = await _nct_search(query, limit=10)
                    if not results:
                        await self._send_chat(f"[F52727]Không tìm thấy: {query[:30]}")
                        return
                    # Lưu cache theo uid của bot (dùng cho !play [số])
                    self._search_cache = results
                    lines = ["[27EBF5]🎵 Kết quả tìm kiếm:"]
                    for i, r in enumerate(results, 1):
                        artist = r.get("artist", "")
                        dur    = r.get("duration", "?")
                        title  = r["title"][:35]
                        lines.append(f"[A627F5]{i}. {title} [{dur}]")
                        if artist:
                            lines.append(f"[FFFFFF]   {artist[:30]}")
                    lines.append("[27EBF5]▶ !play [số] để phát")
                    await self._send_chat("\n".join(lines))
                except Exception as e:
                    await self._send_chat(f"[F52727]Lỗi tìm kiếm: {e}")

            elif cmd == "!play":
                query = " ".join(args)
                if not query:
                    await self._send_chat("[F52727]Dùng: !play [số] hoặc [tên/URL]")
                    return
                # Nếu là số → lấy từ search cache
                if query.isdigit():
                    cache = getattr(self, "_search_cache", [])
                    idx = int(query) - 1
                    if not cache:
                        await self._send_chat("[F52727]Chưa có kết quả tìm kiếm. Dùng !search trước.")
                        return
                    if idx < 0 or idx >= len(cache):
                        await self._send_chat(f"[F52727]Số không hợp lệ. Nhập 1-{len(cache)}.")
                        return
                    r = cache[idx]
                    await self._send_chat(f"[27EBF5]➕ Thêm: {r['title'][:45]}")
                    await self._play_from_query(r["url"], title_hint=r["title"])
                else:
                    await self._play_from_query(query)

            elif cmd in ("!stop", "!pause"):
                if self._udp_task and not self._udp_task.done():
                    self._stopped_frame = self._track_current_frame
                    self._udp_task.cancel()
                    if self._progress_task and not self._progress_task.done():
                        self._progress_task.cancel()
                    if self._emote_task and not self._emote_task.done():
                        self._emote_task.cancel()
                    await self._send_chat("[F52727]⏸ Stopped.")
                else:
                    await self._send_chat("[F52727]Nothing is playing.")

            elif cmd in ("!resume", "!continue"):
                if self._current_ogg and self._voice_ip:
                    sf = self._stopped_frame
                    if self._udp_task and not self._udp_task.done():
                        self._udp_task.cancel()
                    self._udp_task = asyncio.create_task(
                        self._voice_udp_loop(self._voice_ip, self._voice_port,
                                             ogg_data=self._current_ogg, start_frame=sf)
                    )
                    await self._send_chat(f"[27EBF5]▶ Resumed from {int(sf * 0.02 // 60)}:{int(sf * 0.02 % 60):02d}")
                else:
                    await self._send_chat("[F52727]No track to resume. Use !play first.")

            elif cmd == "!volume":
                if args:
                    try:
                        vol = max(0, min(200, int(args[0])))
                        self._volume = vol
                        await self._send_chat(f"[27EBF5]Volume set to {vol}%")
                    except ValueError:
                        await self._send_chat("[F52727]Usage: !volume [0-200]")
                else:
                    await self._send_chat(f"[27EBF5]Current volume: {self._volume}%")

            elif cmd in ("!skip", "!next"):
                if self._udp_task and not self._udp_task.done():
                    await self._skip_current()
                    nxt = self._track_queue.qsize()
                    if nxt > 0:
                        await self._send_chat(f"[27EBF5]⏭ Skipped. Còn {nxt} bài trong hàng chờ.")
                    else:
                        await self._send_chat("[27EBF5]⏭ Skipped. Hàng chờ trống.")
                else:
                    await self._send_chat("[F52727]Không có bài nào đang phát.")

            elif cmd in ("!queue", "!q"):
                is_playing = self._udp_task and not self._udp_task.done()
                items = list(self._track_queue._queue) if hasattr(self._track_queue, '_queue') else []
                if not is_playing and not items:
                    await self._send_chat("[27EBF5]📋 Hàng chờ trống.")
                else:
                    lines = ["[27EBF5]📋 Hàng chờ:"]
                    if is_playing and self._track_title:
                        lines.append(f"[27EBF5]▶ {self._track_title[:45]}")
                    for i, (q, t) in enumerate(items, 1):
                        label = t or q
                        lines.append(f"[A627F5]{i}. {label[:45]}")
                    await self._send_chat("\n".join(lines))

            elif cmd in ("!clear", "!stop"):
                await self._clear_queue()

            elif cmd in ("!leave", "!quit"):
                await self._leave_team()

        except Exception as e:
            HELLXlog("CMD", f"Error: {e}")

    async def _send_chat(self, message: str):
        if not self._chat_writer:
            return
        try:
            import time as _t
            ts    = int(_t.time())
            owner = self.team_owner_uid or int(self.uid)
            pkt   = await HELLXbuild_chat_message(int(self.uid), owner, message, ts, self.key, self.iv)
            self._chat_writer.write(pkt)
            await self._chat_writer.drain()
        except Exception as e:
            HELLXlog("CHAT", f"Send error: {e}")

    # ------------------------------------------------------------------
    # Leave team
    # ------------------------------------------------------------------

    async def _leave_team(self):
        HELLXlog("TEAM", "Leaving...")
        if self.writer:
            self.writer.write(HELLXbuild_leave_team(int(self.uid), self.key, self.iv))
            await self.writer.drain()
        if self._chat_writer:
            try:
                self._chat_writer.write(HELLXbuild_chat_close(int(self.uid), self.key, self.iv))
                await self._chat_writer.drain()
            except Exception:
                pass
            # FIX: đóng hẳn chat TCP connection sau khi leave
            # Nếu không close(), server sẽ tự đóng sau vài giây -> _chat_read_loop
            # sẽ nhận EOF -> gọi _reconnect_chat() -> nhưng lúc này voice_region_id
            # có thể đã là region của team MỚI -> reconnect nhầm context
            try:
                self._chat_writer.close()
            except Exception:
                pass
            self._chat_writer = None
            self._chat_reader = None
        # FIX: cancel keepalive task khi leave để không giữ connection zombie
        if self._chat_keepalive_task and not self._chat_keepalive_task.done():
            self._chat_keepalive_task.cancel()
            self._chat_keepalive_task = None
        # Clear queue và stop queue worker khi leave team
        while not self._track_queue.empty():
            try:
                self._track_queue.get_nowait()
                self._track_queue.task_done()
            except Exception:
                break
        if self._queue_task and not self._queue_task.done():
            self._queue_task.cancel()
            self._queue_task = None
        for task in (self._udp_task, self._progress_task, self._voice_ping_task, self._voice_connect_task):
            if task and not task.done():
                task.cancel()
        self.voice_region_id = None
        self.chat_region_id  = None
        self.team_owner_uid  = None
        self.voice_chat_key  = None
        self._processed_invite_code = None
        self._voice_ip   = None
        self._voice_port = None
        if self._voice_udp_sock:
            try: self._voice_udp_sock.close()
            except Exception: pass
            self._voice_udp_sock = None
        self._current_ogg = None
        self._current_url = None
        self._track_title = ""
        self._stopped_frame = 0
        HELLXlog("TEAM", "Left. Waiting for next invite...")

    # ------------------------------------------------------------------
    # Voice connect (Step 1 → Step 2 → Step 3)
    # ------------------------------------------------------------------

    async def _voice_connect(self):
        if not self.voice_region_id:
            HELLXlog("VOICE", "ERROR: No region_id")
            return
        self._voice_f5 = 0
        self._voice_f3 = 0x13E6
        self._voice_ssrc_int = 0
        self._voice_music_ok = False

        # [FIX] open_micro qua online TCP -> game server authorize mic
        # Thieu buoc nay: voice MCU khong biet bot duoc phep dung mic -> bo qua mic_open UDP
        # (s3 dung pgen.open_micro() - CMD type=88 - gui truoc khi pipeline chay)
        try:
            _om_pkt = HELLXbuild_open_micro(
                self.voice_region_id, self.key, self.iv,
                region_prefix=self.region_prefix
            )
            if self.writer and not self.writer.transport.is_closing():
                self.writer.write(_om_pkt)
                await self.writer.drain()
                HELLXlog("VOICE", "open_micro CMD88 sent -> online TCP")
                await asyncio.sleep(0.2)
        except Exception as _ome:
            HELLXlog("VOICE", f"open_micro failed: {_ome}")
        # BUG FIX: tất cả voice packet phải dùng account_uid (game UID),
        # không phải self.uid (Garena UID). uid_last3 trong custom packet
        # phải khớp với account_uid để MCU server nhận ra session.
        _voice_uid = str(self.account_uid) if self.account_uid else self.uid
        import re as _re
        import aiohttp as _aiohttp
        import struct as _st2

        async def _read_voice_frame(reader, timeout):
            header = await asyncio.wait_for(reader.readexactly(4), timeout)
            frame_length = _st2.unpack(">I", header)[0]
            if frame_length <= 0 or frame_length > 65536:
                raise ValueError(f"invalid frame length: {frame_length}")
            return await asyncio.wait_for(reader.readexactly(frame_length), timeout)

        _APP_ID   = "FFD58FB4F76F648C2A5E21EBCFA3AAE81B4C9B7D97"
        _DISP_IP  = "148.222.82.60"
        _DISP_PORT = 6674
        import uuid as _uuid_mod

        _uuid_str = str(_uuid_mod.uuid4())
        _clean_rid = str(self.voice_region_id).replace(f"{self.region_prefix}_", "")
        _disp_token = f"{_APP_ID}{self.region_prefix}_{_clean_rid}"
        _disp_dev = _pb_enc({
            1: 17, 2: 11, 3: 1, 4: "Redmi", 5: str(_voice_uid),
            6: 562135116, 7: "arm64-v8a", 8: "", 9: _uuid_str,
            10: _APP_ID, 11: "voiceb", 12: "24094RAD4G", 13: 2, 14: "mt6855",
        })
        _disp_pb = _pb_enc({1: _disp_token, 2: _disp_dev, 3: 2, 4: self.ext_region})

        voice_ip, voice_port = None, None
        HELLXlog("VOICE", f"Step2 (Dispatcher): {_DISP_IP}:{_DISP_PORT} region={self.region_prefix}_{_clean_rid}")
        try:
            _dr, _dw = await asyncio.wait_for(
                asyncio.open_connection(_DISP_IP, _DISP_PORT), timeout=5.0
            )
            _dw.write(_st2.pack(">I", len(_disp_pb)) + _disp_pb)
            await _dw.drain()
            _drl_b = await asyncio.wait_for(_dr.readexactly(4), timeout=5.0)
            _dresp  = await _dr.readexactly(_st2.unpack(">I", _drl_b)[0])
            _dw.close()
            try: await _dw.wait_closed()
            except Exception: pass
            HELLXlog("VOICE", f"Step2 resp: {_dresp.hex()[:60]}")
            # Parse: field 1 = IP (wire type 2), field 2 = port (wire type 0)
            _idx = 0
            while _idx < len(_dresp):
                _tag = _dresp[_idx]; _idx += 1
                _fn, _wt = _tag >> 3, _tag & 0x07
                if _wt == 2:
                    _ln, _idx2 = 0, _idx
                    _sh = 0
                    while _idx2 < len(_dresp):
                        _b = _dresp[_idx2]; _ln |= (_b & 0x7F) << _sh; _idx2 += 1; _sh += 7
                        if not (_b & 0x80): break
                    _idx = _idx2
                    _fv = _dresp[_idx: _idx + _ln]; _idx += _ln
                    if _fn == 1:
                        voice_ip = _fv.decode(errors="ignore")
                elif _wt == 0:
                    _pv, _sh = 0, 0
                    while _idx < len(_dresp):
                        _b = _dresp[_idx]; _pv |= (_b & 0x7F) << _sh; _idx += 1; _sh += 7
                        if not (_b & 0x80): break
                    if _fn == 2:
                        voice_port = _pv
                else:
                    break
        except Exception as e:
            HELLXlog("VOICE", f"Step2 Dispatcher failed: {e}")

        if not voice_ip or not voice_port:
            HELLXlog("VOICE", "ERROR: No voice IP/port from Dispatcher (Step2)")
            return

        # Step 3 — MCU voice server (s3-style _f9_packet format)
        HELLXlog("VOICE", f"Step3 (Handshake): {voice_ip}:{voice_port}")
        try:
            r3, w3 = await asyncio.wait_for(asyncio.open_connection(voice_ip, voice_port), timeout=5.0)
        except Exception as e:
            HELLXlog("VOICE", f"Step3 connect failed: {e}")
            return

        _f9_dev = _pb_enc({
            1: 7, 2: 11, 3: 1, 4: "Redmi", 5: str(_voice_uid),
            6: 562135116, 7: "arm64-v8a", 8: "", 9: _uuid_str,
            10: _APP_ID, 11: "voiceb", 12: "24094RAD4G", 13: 2, 14: "mt6855",
        })
        _f9_pb = _pb_enc({
            1: _f9_dev, 2: _disp_token,
            10: 1, 11: str(_voice_uid), 12: 0, 14: 0, 16: 1,
        })
        w3.write(_st2.pack(">I", len(_f9_pb)) + _f9_pb)
        await w3.drain()

        self._voice_udp_port = None  # sẽ lấy từ Step3 resp field[2]
        _resp3_raw = b""
        try:
            _resp3_raw = await _read_voice_frame(r3, 5.0)
            HELLXlog("VOICE", f"Step3 resp: {_resp3_raw.hex()[:60]}")

            # Parse raw varint fields — giống s3's execute_handshake
            _fields3 = {}; _idx3 = 0
            while _idx3 < len(_resp3_raw):
                _tag = _resp3_raw[_idx3]; _idx3 += 1
                _fn3, _wt3 = _tag >> 3, _tag & 0x07
                if _wt3 == 0:
                    _pv, _sh = 0, 0
                    while _idx3 < len(_resp3_raw):
                        _b = _resp3_raw[_idx3]; _pv |= (_b & 0x7F) << _sh; _idx3 += 1; _sh += 7
                        if not (_b & 0x80): break
                    _fields3[_fn3] = _pv
                elif _wt3 == 2:
                    _ln, _sh = 0, 0
                    while _idx3 < len(_resp3_raw):
                        _b = _resp3_raw[_idx3]; _ln |= (_b & 0x7F) << _sh; _idx3 += 1; _sh += 7
                        if not (_b & 0x80): break
                    _fields3[_fn3] = _resp3_raw[_idx3: _idx3 + _ln]; _idx3 += _ln
                else:
                    break

            HELLXlog("VOICE", f"Step3 raw fields: { {k: (v.hex()[:20] if isinstance(v, bytes) else v) for k, v in _fields3.items()} }")

            # field[2] = UDP port
            _udp_port_from_resp = _fields3.get(2) or _fields3.get(5)
            if isinstance(_udp_port_from_resp, int) and _udp_port_from_resp > 1024:
                self._voice_udp_port = _udp_port_from_resp
                HELLXlog("VOICE", f"UDP port from Step3: {self._voice_udp_port}")

            # field[5] = session_id
            _f5v = _fields3.get(5) or 0
            if isinstance(_f5v, bytes): _f5v = int.from_bytes(_f5v, "little") if _f5v else 0
            self._voice_f5 = int(_f5v)

            # field[3] = f3 (fixed_2 / session key)
            _f3v = _fields3.get(3) or 0
            if isinstance(_f3v, bytes): _f3v = int.from_bytes(_f3v, "little") if _f3v else 0
            if int(_f3v) > 0:
                self._voice_f3 = int(_f3v)

            # field[13] = SSRC 32-bit thực server gán (key fix)
            _f13v = _fields3.get(13, 0)
            if isinstance(_f13v, bytes): _f13v = int.from_bytes(_f13v, "little") if _f13v else 0
            _f13int = int(_f13v) & 0xFFFFFFFF
            self._voice_ssrc_int = _f13int if _f13int > 0 else (self._voice_f3 & 0xFFFFFFFF)

            HELLXlog("VOICE", f"Step3 parsed: f5={self._voice_f5} f3={self._voice_f3} "
                               f"ssrc=0x{self._voice_ssrc_int:08x} udp_port={self._voice_udp_port}")

            # Echo packet — gửi field[1] trở lại server (s3-style, bước s2 cũ bỏ sót)
            # Thiếu bước này: server không ACK → mic_open bị bỏ qua
            _f1_raw = _fields3.get(1, b"")
            if isinstance(_f1_raw, bytes) and _f1_raw:
                _echo_pb = bytes([0x0A, len(_f1_raw)]) + _f1_raw
            else:
                _echo_pb = _resp3_raw   # fallback: echo nguyên resp
            w3.write(_st2.pack(">I", len(_echo_pb)) + _echo_pb)
            await w3.drain()
            HELLXlog("VOICE", f"Step3 echo sent ({len(_echo_pb)} bytes)")

            # Đọc ACK (optional, timeout ok)
            try:
                _ack3 = await asyncio.wait_for(_dr.readexactly(4), timeout=5.0)
                await _dr.readexactly(_st2.unpack(">I", _ack3)[0])
            except Exception:
                pass

        except Exception as _e:
            HELLXlog("VOICE", f"Step3 parse error: {_e}")

        if self._voice_f5 == 0:
            HELLXlog("VOICE", "f5=0: wrong MCU session, retrying...")
            await self._send_chat("[F52727]Wrong voice IPs. Make bot leave and re-invite.")
            try: w3.close(); await w3.wait_closed()
            except Exception: pass
            await asyncio.sleep(3)
            self._voice_connect_task = asyncio.create_task(self._voice_connect())
            return

        custom = HELLXbuild_voice_custom(_voice_uid, self.voice_region_id, mcu_ip=voice_ip, region_prefix=self.region_prefix)
        w3.write(custom[:4]); await w3.drain()
        w3.write(custom[4:]); await w3.drain()

        try:
            resp4 = await _read_voice_frame(r3, 3.0)
            if resp4:
                raw = resp4[4:] if resp4[:4] != b"\xfe\xff\xff\x7f" else resp4
                idx = raw.find(b"\xfe\xff\xff\x7f")
                if idx != -1:
                    ts_idx = raw.find(b"\x17\x76", idx)
                    if ts_idx != -1 and ts_idx + 6 <= len(raw):
                        self.voice_session_ts = raw[ts_idx + 2: ts_idx + 6]
        except asyncio.TimeoutError:
            pass

        # FIX: gửi broadcast_sub để bot join voice channel — KHÔNG gửi cái này
        # thì server không đưa bot vào channel → không ai nghe thấy
        try:
            bsub = HELLXbuild_voice_broadcast_sub(
                _voice_uid, self.voice_region_id,
                session_id=self._voice_f5,
                region_prefix=self.region_prefix,
            )
            w3.write(bsub[:4]); await w3.drain()
            w3.write(bsub[4:]); await w3.drain()
            HELLXlog("VOICE", f"broadcast_sub sent (session={self._voice_f5})")
            await asyncio.sleep(0.1)
        except Exception as e:
            HELLXlog("VOICE", f"broadcast_sub failed: {e}")

        def _build_tcp_cmd_early(cmd_id):
            from proto import HELLXcreate_proto as _cproto
            _token = f"FFD58FB4F76F648C2A5E21EBCFA3AAE81B4C9B7D97VN_{self.voice_region_id}"
            _f1 = _cproto({1: cmd_id, 2: 0, 3: 0})
            _f3 = _cproto({1: _voice_uid, 2: 1})
            _pl = _cproto({1: _f1, 2: _token, 3: _f3})
            return struct.pack(">I", len(_pl)) + _pl

        try:
            w3.write(_build_tcp_cmd_early(9))
            await w3.drain()
            HELLXlog("VOICE", "CMD 9 sent (sync game server)")
            await asyncio.sleep(0.05)
        except Exception as e:
            HELLXlog("VOICE", f"CMD 9 failed: {e}")

        # Agora join — gửi feffff7f lên voice TCP (giữa CMD 9 và mic_open UDP — theo s3)
        try:
            _agora_pkt    = bytes.fromhex("feffff7f")
            _agora_framed = struct.pack(">I", len(_agora_pkt)) + _agora_pkt
            w3.write(_agora_framed)
            await w3.drain()
            HELLXlog("VOICE", "agora_join sent (feffff7f)")
            await asyncio.sleep(0.05)
        except Exception as e:
            HELLXlog("VOICE", f"agora_join failed: {e}")


        try:
            import socket as _esock, random as _ernd
            if self._voice_udp_sock:
                try: self._voice_udp_sock.close()
                except Exception: pass
            self._voice_udp_sock = _esock.socket(_esock.AF_INET, _esock.SOCK_DGRAM)
            self._voice_udp_sock.setblocking(False)
            _emic_seq = _ernd.randint(0, 0xFFFFFF)
            _emic_ts  = _ernd.randint(0, 0xFFFFFFFF)
            _essrc    = getattr(self, '_voice_ssrc_int', 0) or getattr(self, '_voice_f3', 0)
            _eudp_port = getattr(self, '_voice_udp_port', None) or (voice_port + 2)
            _emic_ba = bytearray()
            _emic_ba.append(0x71)
            _emic_ba += (_emic_seq & 0xFFFFFF).to_bytes(3, "big")
            _emic_ba += struct.pack(">I", _emic_ts & 0xFFFFFFFF)
            _emic_ba += struct.pack(">I", _essrc & 0xFFFFFFFF)
            _emic_ba += struct.pack(">I", 0x00000311)
            _emic_ba += bytes.fromhex("0000010720210000286e4672656520466972650000000000000000000000")
            _emic_ba += bytes.fromhex("e7877176")
            self._voice_udp_sock.sendto(bytes(_emic_ba), (voice_ip, _eudp_port))
            HELLXlog("VOICE", f"early mic_open sent -> {voice_ip}:{_eudp_port} (socket persistent)")
            await asyncio.sleep(0.1)
        except Exception as e:
            HELLXlog("VOICE", f"early mic_open failed: {e}")

        # CMD 24 + CMD 35 — báo server bot đã sẵn sàng (gửi SAU mic_open — theo s3)
        def _build_tcp_cmd(cmd_id):
            from proto import HELLXcreate_proto as _cproto
            _token = f"FFD58FB4F76F648C2A5E21EBCFA3AAE81B4C9B7D97VN_{self.voice_region_id}"
            _f1 = _cproto({1: cmd_id, 2: 0, 3: 0})
            _f3 = _cproto({1: _voice_uid, 2: 1})
            _pl = _cproto({1: _f1, 2: _token, 3: _f3})
            return struct.pack(">I", len(_pl)) + _pl

        for _cmd in (24, 35):
            try:
                w3.write(_build_tcp_cmd(_cmd))
                await w3.drain()
                HELLXlog("VOICE", f"CMD {_cmd} sent")
            except Exception as e:
                HELLXlog("VOICE", f"CMD {_cmd} failed: {e}")
        await asyncio.sleep(0.1)

        if self._voice_ping_task:
            self._voice_ping_task.cancel()
        self._voice_ping_task = asyncio.create_task(self._voice_ping_loop(w3, r3))
        self._voice_ip   = voice_ip
        self._voice_port = voice_port

        # TCP reader for re-auth packets
        async def _voice_tcp_reader():
            try:
                while True:
                    try:
                        data = await _read_voice_frame(r3, 30.0)
                    except asyncio.TimeoutError:
                        # Server không gửi gì trong 30s — bình thường, gửi ping và tiếp tục
                        continue
                    except (asyncio.IncompleteReadError, ConnectionError, EOFError, OSError):
                        HELLXlog("VOICE", "TCP: server closed connection — stopping UDP")
                        if self._udp_task and not self._udp_task.done():
                            self._udp_task.cancel()
                        break
                    try:
                        # Member Info: gói đầu tiên dài > 100 bytes server gửi về
                        # = xác nhận session OK → gửi agora_join lần 2 + CMD 33
                        # CMD 33 là trigger thực sự khiến server ACK mic_open
                        if len(data) > 100 and not self._voice_music_ok:
                            self._voice_music_ok = True
                            HELLXlog("VOICE", f"TCP: Member Info received ({len(data)}B) → agora_join#2 + CMD 33")
                            async def _do_member_info(_vu=_voice_uid):
                                try:
                                    # agora_join lần 2
                                    _ap = bytes.fromhex("feffff7f")
                                    w3.write(struct.pack(">I", len(_ap)) + _ap)
                                    await w3.drain()
                                    await asyncio.sleep(0.05)
                                    # CMD 33 — trigger server chấp nhận audio stream
                                    from proto import HELLXcreate_proto as _cproto
                                    _tok = f"FFD58FB4F76F648C2A5E21EBCFA3AAE81B4C9B7D97VN_{self.voice_region_id}"
                                    _f1  = _cproto({1: 33, 2: 0, 3: 0})
                                    _f3  = _cproto({1: _vu, 2: 1})
                                    _pl  = _cproto({1: _f1, 2: _tok, 3: _f3})
                                    w3.write(struct.pack(">I", len(_pl)) + _pl)
                                    await w3.drain()
                                    HELLXlog("VOICE", "CMD 33 sent → server sẽ ACK mic_open")
                                except Exception as _ex:
                                    HELLXlog("VOICE", f"member_info handler err: {_ex}")
                            asyncio.create_task(_do_member_info())

                        p = HELLXdecode_proto(data)
                        inner = p.get("1", {})
                        pkt_type = inner.get("1", 0) if isinstance(inner, dict) else 0
                        if pkt_type == 0x0B and p.get("2", 0):
                            async def _do_reauth(_vu=_voice_uid):
                                a = HELLXbuild_voice_auth(_vu, self.voice_region_id, voice_port, self.region_prefix)
                                w3.write(a[:4]); await w3.drain(); w3.write(a[4:]); await w3.drain()
                                c = HELLXbuild_voice_custom(_vu, self.voice_region_id, mcu_ip=voice_ip, region_prefix=self.region_prefix)
                                w3.write(c[:4]); await w3.drain(); w3.write(c[4:]); await w3.drain()
                                # Re-gửi broadcast_sub sau reauth
                                b = HELLXbuild_voice_broadcast_sub(_vu, self.voice_region_id, self._voice_f5, self.region_prefix)
                                w3.write(b[:4]); await w3.drain(); w3.write(b[4:]); await w3.drain()
                            asyncio.create_task(_do_reauth())
                    except Exception:
                        pass
            except asyncio.CancelledError:
                pass
            except Exception as e:
                HELLXlog("VOICE", f"TCP reader error: {e}")

        asyncio.create_task(_voice_tcp_reader())
        HELLXlog("VOICE", f"Step3 done. Streaming to {voice_ip}:{voice_port + 2}")

    async def _voice_ping_loop(self, writer, reader):
        # FIX: dùng account_uid và session_id thực (_voice_f5) từ Step3 response
        _voice_uid = str(self.account_uid) if self.account_uid else self.uid
        try:
            while writer and not writer.transport.is_closing():
                await asyncio.sleep(5)
                try:
                    # _voice_f5 là session_id thực từ Step3 auth response
                    # fallback về _voice_f3 nếu chưa có (trường hợp hiếm)
                    session_id = self._voice_f5 if self._voice_f5 else getattr(self, "_voice_f3", 0x0B)
                    writer.write(HELLXbuild_voice_ping(_voice_uid, self.voice_region_id, self._voice_port, session_id))
                    await writer.drain()
                except Exception as e:
                    HELLXlog("VOICE", f"Ping error: {e}")
                    break
        except asyncio.CancelledError:
            pass

    # ------------------------------------------------------------------
    # Emote helpers
    # ------------------------------------------------------------------

    async def _choose_emote_http(self):
        """Gọi HTTP /ChooseEmote để đăng ký emote set lên server — bắt buộc trước khi emote hiện trong game."""
        if not self.login_url or not self.token:
            return
        try:
            import aiohttp as _aio
            import time as _t
            url = self.login_url.rstrip("/") + "/ChooseEmote"
            headers = {
                "X-GA-SV": str(int(_t.time())),
                "ReleaseVersion": "OB55",
                "X-GA": "v1 1",
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/x-www-form-urlencoded",
            }
            # Hardcoded payload từ chodang — chỉ định emote set default
            data = bytes.fromhex("5D1645261 8C5DE3EE8F4C53603 7F84B7".replace(" ", ""))
            async with _aio.ClientSession() as sess:
                async with sess.post(url, data=data, headers=headers, timeout=_aio.ClientTimeout(total=8)) as resp:
                    HELLXlog("EMOTE", f"ChooseEmote HTTP → {resp.status}")
        except Exception as e:
            HELLXlog("EMOTE", f"ChooseEmote HTTP error: {e}")

    def _build_emote_pkt(self, emote_id: int, uid_list: list) -> bytes:
        from encrypt import HELLXenc_packet as _enc
        bot_uid = self.account_uid or int(self.uid)
        targets = [{1: int(uid), 3: emote_id} for uid in uid_list if uid]
        inner = _pb_enc({
            1: bot_uid,
            2: 0x362E3D41,
            5: targets,
        })
        proto = _pb_enc({1: 21, 2: inner})
        enc = bytes.fromhex(_enc(proto.hex(), self.key, self.iv))
        return bytes([0x05, self._region_code]) + struct.pack(">I", len(enc)) + enc

    async def _trigger_emote(self, emote_id: int = None):
        """Gửi 1 emote cho tất cả member trong squad + chính bot.
        FIX: luôn include team_owner_uid — tránh owner mất emote sau khi người khác leave."""
        try:
            if not self.writer or self.writer.transport.is_closing():
                return
            bot_uid = self.account_uid or int(self.uid)
            # Gom đủ: squad list hiện tại + bot + owner (tránh thiếu owner khi member leave)
            must_have = [bot_uid]
            if self.team_owner_uid:
                must_have.append(int(self.team_owner_uid))
            uids = list(set(self._squad_uids + must_have))
            if not uids:
                return
            eid = emote_id or (self._emote_ids[0] if self._emote_ids else 909054004)
            pkt = self._build_emote_pkt(eid, uids)
            self.writer.write(pkt)
            await self.writer.drain()
            HELLXlog("EMOTE", f"emote={eid} → uids={uids}")
        except Exception as e:
            HELLXlog("EMOTE", f"Error: {e}")

    async def _emote_loop(self):
        """Cycle qua tất cả emotes trong suốt bài hát (7s/emote), lặp lại từ đầu khi hết list.
        An toàn với concurrent: thoát ngay khi bị cancel hoặc udp_task đã done."""
        try:
            emotes = list(self._emote_ids) if self._emote_ids else [909054004]
            idx = 0
            # Delay nhỏ để tránh emote ngay lúc bài vừa bắt đầu (server chưa nhận session)
            await asyncio.sleep(2.0)
            while True:
                # Dừng khi bài hát kết thúc hoặc không còn trong team
                if not self.voice_region_id:
                    break
                if self._udp_task is None or self._udp_task.done():
                    break
                await self._trigger_emote(emotes[idx % len(emotes)])
                idx += 1
                # 7s giữa các emote — tương tự chodang dance loop
                # Chia nhỏ sleep để cancel nhanh hơn
                for _ in range(14):
                    await asyncio.sleep(0.5)
                    if self._udp_task is None or self._udp_task.done():
                        return
        except asyncio.CancelledError:
            pass
        except Exception as e:
            HELLXlog("EMOTE", f"loop error: {e}")


    async def _queue_worker(self):
        """Chạy liên tục, lấy từng bài từ queue và phát tuần tự.
        Khi queue hết bài và không còn bài mới sau QUEUE_IDLE_TIMEOUT giây → tự leave team."""
        QUEUE_IDLE_TIMEOUT = 30.0   # giây chờ sau khi hết queue trước khi leave
        try:
            while True:
                try:
                    # Chờ bài tiếp trong queue, timeout = QUEUE_IDLE_TIMEOUT
                    query, title_hint = await asyncio.wait_for(
                        self._track_queue.get(), timeout=QUEUE_IDLE_TIMEOUT
                    )
                except asyncio.TimeoutError:
                    # Hết queue, không có bài mới → leave team nếu đang trong team
                    if self.voice_region_id:
                        HELLXlog("QUEUE", "Queue trống sau timeout — tự leave team.")
                        await self._send_chat("[27EBF5]📋 Hết hàng chờ. Bot rời team...")
                        await asyncio.sleep(1.0)
                        await self._leave_team()
                    return   # kết thúc worker, sẽ được tạo lại khi có bài mới

                try:
                    await self._play_from_query_internal(query, title_hint)
                except Exception as e:
                    HELLXlog("QUEUE", f"Error playing {query[:40]}: {e}")
                finally:
                    self._track_queue.task_done()
        except asyncio.CancelledError:
            pass

    def _ensure_queue_worker(self):
        """Khởi động queue_worker nếu chưa chạy."""
        if self._queue_task is None or self._queue_task.done():
            self._queue_task = asyncio.create_task(self._queue_worker())

    async def _play_from_query(self, query: str, title_hint: str = ""):
        """Public entry point: đẩy bài vào queue, không block caller.
        Nếu queue rỗng và không đang phát -> phát ngay.
        Nếu đang phát -> add vào queue và báo số thứ tự."""
        self._ensure_queue_worker()
        qsize = self._track_queue.qsize()
        is_playing = self._udp_task and not self._udp_task.done()

        if qsize == 0 and not is_playing:
            # Phát ngay — đưa vào queue để worker xử lý
            await self._track_queue.put((query, title_hint))
        else:
            pos = qsize + (1 if is_playing else 0)
            await self._track_queue.put((query, title_hint))
            await self._send_chat(
                f"[A627F5]📋 Thêm vào hàng chờ #{pos + 1}\n"
                f"[5427F5]{(title_hint or query)[:50]}"
            )

    async def _skip_current(self):
        """Dừng bài hiện tại, chuyển sang bài tiếp theo trong queue."""
        if self._udp_task and not self._udp_task.done():
            self._udp_task.cancel()
            if self._progress_task and not self._progress_task.done():
                self._progress_task.cancel()
            if self._emote_task and not self._emote_task.done():
                self._emote_task.cancel()
            await self._send_chat("[27EBF5]⏭ Skipped.")

    async def _clear_queue(self):
        """Xóa toàn bộ queue."""
        while not self._track_queue.empty():
            try:
                self._track_queue.get_nowait()
                self._track_queue.task_done()
            except asyncio.QueueEmpty:
                break
        await self._skip_current()
        await self._send_chat("[F52727]🗑 Queue đã xóa.")

    async def _play_from_query_internal(self, query: str, title_hint: str = ""):
        """Thực thi tải + encode + phát một bài — được gọi bởi queue worker."""
        if not self._voice_ip or not self._voice_port:
            await self._send_chat("[27EBF5]Voice connecting, please wait...")
            deadline = asyncio.get_running_loop().time() + 15
            while (not self._voice_ip or not self._voice_port) and asyncio.get_running_loop().time() < deadline:
                await asyncio.sleep(0.5)
            if not self._voice_ip or not self._voice_port:
                await self._send_chat("[F52727]Voice connection failed.")
                return

        # Dừng bài đang phát (nếu có) trước khi phát bài mới
        if self._udp_task and not self._udp_task.done():
            self._udp_task.cancel()
            try:
                await asyncio.wait_for(asyncio.shield(self._udp_task), timeout=1.0)
            except Exception:
                pass
        for t in (self._progress_task, self._emote_task):
            if t and not t.done():
                t.cancel()

        import subprocess
        ogg_path = _tmp.mktemp(suffix=".opus")
        raw_path = _tmp.mktemp(suffix=".mp3")

        loop = asyncio.get_event_loop()

        # Semaphore toàn cục: giới hạn số download đồng thời trên tất cả bot
        # Tránh tình huống 3 bot cùng download -> băng thông cạn -> tất cả timeout
        sem = self._download_sem
        try:
            async with (sem if sem else asyncio.Lock()):
                await self._send_chat(f"[27EBF5]⏳ Downloading: {(title_hint or query)[:50]}")

                title, channel = title_hint or query, ""

                # ── NCT path: stream URL đã có sẵn mp3 ──────────────────────
                # Nếu query là NCT stream URL (đã search trước), download thẳng
                # Nếu query là từ khoá hoặc YouTube URL → search NCT trước
                stream_url = None
                is_nct_url = (
                    "nhaccuatui.com" in query
                    or "nct.vn" in query
                    or "nct-dl" in query
                    or query.startswith("http") and "nhaccuatui" in query
                )
                if is_nct_url:
                    stream_url = query
                    # title_hint đã được set từ /search cache, dùng luôn
                elif not query.startswith("http"):
                    # Từ khoá tìm kiếm → search NCT
                    nct_results = await _nct_search(query, limit=1)
                    if nct_results:
                        r = nct_results[0]
                        stream_url = r["url"]
                        title   = r["title"]
                        channel = r["artist"]
                        HELLXlog("PLAY", f"NCT stream: {title} — {stream_url[:60]}...")

                if stream_url:
                    # ── Download mp3 từ NCT ─────────────────────────────────
                    try:
                        mp3_bytes = await _nct_download_bytes(stream_url)
                        with open(raw_path, "wb") as f:
                            f.write(mp3_bytes)
                        HELLXlog("PLAY", f"NCT downloaded {len(mp3_bytes)//1024}KB → {raw_path}")
                        raw_file = raw_path
                    except Exception as e:
                        HELLXlog("PLAY", f"NCT download lỗi: {e} — fallback yt-dlp")
                        stream_url = None  # trigger yt-dlp fallback bên dưới

                if not stream_url:
                    # ── Fallback yt-dlp (YouTube URL hoặc NCT fail) ─────────
                    import json as _json
                    raw_path_yt = _tmp.mktemp(suffix=".raw")
                    def _run_ytdlp():
                        return subprocess.run(
                            _yt_dlp_command() + [
                                query if query.startswith("http") else f"ytsearch1:{query}",
                                "--extractor-args", "youtube:player_client=android",
                                "-x", "--audio-format", "best", "--audio-quality", "0",
                                "--print-json",
                                "-o", raw_path_yt, "--no-playlist",
                            ],
                            capture_output=True, text=True, timeout=60,
                        )
                    result = await loop.run_in_executor(None, _run_ytdlp)
                    if result.returncode != 0:
                        await self._send_chat("[F52727]Download failed.")
                        return

                    for line in result.stdout.strip().split("\n"):
                        try:
                            info = _json.loads(line)
                            if info.get("title"):
                                title   = info.get("title", query)
                                channel = info.get("channel", info.get("uploader", ""))
                                break
                        except Exception:
                            continue

                    matches = _glob.glob(raw_path_yt + ".*") or _glob.glob(raw_path_yt)
                    if not matches:
                        await self._send_chat("[F52727]Audio file not found after download.")
                        return
                    raw_file = matches[0]

                # ── ffmpeg: resample + normalize + encode Opus ───────────────
                # volume=5.0 để lớn hơn âm thanh game, dynaudnorm giữ đều
                def _run_encode():
                    import json as _json
                    import re as _re

                    # ── Pass 1: đo loudness thực tế của file ────────────────
                    # loudnorm EBU R128 — chuẩn broadcast, nhất quán mọi bài
                    p1 = subprocess.run([
                        "ffmpeg", "-y",
                        "-i", raw_file,
                        "-af", "loudnorm=I=-16:TP=-1.5:LRA=11:print_format=json",
                        "-f", "null", "-",
                    ], capture_output=True, text=True, timeout=120)

                    # Parse JSON từ stderr của pass 1
                    measured = {}
                    try:
                        m = _re.search(r'\{[^}]+\}', p1.stderr, _re.DOTALL)
                        if m:
                            measured = _json.loads(m.group())
                    except Exception:
                        pass

                    # ── Pass 2: encode với thông số đo được ─────────────────
                    if measured.get("input_i"):
                        # 2-pass loudnorm: chính xác, không artifact
                        af = (
                            f"loudnorm=I=-16:TP=-1.5:LRA=11"
                            f":measured_I={measured['input_i']}"
                            f":measured_TP={measured['input_tp']}"
                            f":measured_LRA={measured['input_lra']}"
                            f":measured_thresh={measured['input_thresh']}"
                            f":offset={measured['target_offset']}"
                            f":linear=true"
                        )
                    else:
                        # Fallback 1-pass nếu pass 1 fail
                        af = "loudnorm=I=-16:TP=-1.5:LRA=11:linear=true"

                    return subprocess.run([
                        "ffmpeg", "-loglevel", "quiet", "-y",
                        "-i", raw_file,
                        "-ar", "16000", "-ac", "1",
                        "-af", af,
                        "-c:a", "libopus",
                        "-b:a", f"{self.opus_bitrate // 1000}k",
                        "-vbr", "off",
                        "-frame_duration", "20",
                        "-application", "audio",
                        ogg_path,
                    ], capture_output=True, timeout=120)

                enc_result = await loop.run_in_executor(None, _run_encode)
                if enc_result.returncode != 0:
                    await self._send_chat("[F52727]Encode failed.")
                    return

                with open(ogg_path, "rb") as f:
                    ogg_data = f.read()
            # -- Semaphore released: download+encode xong, bắt đầu phát --

            # Kiểm tra lại còn trong team không (có thể đã leave trong lúc download)
            if not self.voice_region_id or not self._voice_ip:
                HELLXlog("PLAY", "Left team during download — aborting play.")
                return

            self._current_ogg   = ogg_data
            self._current_url   = query
            self._track_title   = title
            self._track_channel = channel
            frames = list(_ogg_opus_frames_bytes(ogg_data))
            self._track_total_frames  = len(frames)
            self._track_current_frame = 0
            self._stopped_frame       = 0

            # Hiển thị queue_size còn lại
            remaining = self._track_queue.qsize()
            queue_info = f"\n[A627F5]Hàng chờ: {remaining} bài" if remaining > 0 else ""
            await self._send_chat(
                f"[27EBF5]▶ Now Playing\n"
                f"[5427F5]{title[:50]}\n"
                f"[A627F5]{channel[:30]}"
                f"{queue_info}"
            )
            self._udp_task = asyncio.create_task(
                self._voice_udp_loop(self._voice_ip, self._voice_port, ogg_data=ogg_data)
            )
            self._progress_task = asyncio.create_task(self._progress_loop())
            # Emote loop — an toàn: cancel emote cũ trước khi tạo mới
            if self._emote_task and not self._emote_task.done():
                self._emote_task.cancel()
                try:
                    await asyncio.wait_for(asyncio.shield(self._emote_task), timeout=0.5)
                except Exception:
                    pass
            self._emote_task = asyncio.create_task(self._emote_loop())

            # Chờ bài phát xong trước khi lấy bài tiếp từ queue
            try:
                await asyncio.shield(self._udp_task)
            except (asyncio.CancelledError, Exception):
                pass

        except Exception as e:
            HELLXlog("PLAY", f"Error: {e}")
            await self._send_chat(f"[F52727]Play error: {e}")
        finally:
            for fp in [ogg_path, raw_path] + _glob.glob(raw_path + ".*"):
                try: os.remove(fp)
                except Exception: pass
            # Dọn file yt-dlp fallback nếu có
            try:
                for fp in _glob.glob(_tmp.gettempdir() + "/tmp*.raw*"):
                    try: os.remove(fp)
                    except Exception: pass
            except Exception: pass

    async def _play_from_url(self, url: str, title: str = ""):
        """Entry point từ Telegram — đợi voice kết nối rồi đẩy vào queue."""
        deadline = asyncio.get_running_loop().time() + 15
        while (not self._voice_ip or not self._voice_port) and asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0.5)
        await self._play_from_query(url, title_hint=title)

    async def _progress_loop(self):
        try:
            await asyncio.sleep(15)
            while self._udp_task and not self._udp_task.done():
                if self._track_total_frames > 0:
                    frame = self._track_current_frame
                    total = self._track_total_frames
                    pct   = min(frame / total, 1.0)
                    filled = int(16 * pct)
                    bar = "[27EBF5]" + "■" * filled + "[5427F5]" + "□" * (16 - filled)
                    e_s = int(frame * 0.02)
                    t_s = int(total * 0.02)
                    await self._send_chat(
                        f"[27EBF5]▶ [5427F5]{self._track_title[:38]}\n"
                        f"[A627F5]{self._track_channel[:30]}\n"
                        f"{bar}\n"
                        f"[27EBF5]{e_s//60}:{e_s%60:02d} [F227F5]/ [27EBF5]{t_s//60}:{t_s%60:02d}  "
                        f"[A627F5]Vol:{self._volume}%"
                    )
                await asyncio.sleep(10)
        except asyncio.CancelledError:
            pass

    # ------------------------------------------------------------------
    # UDP streaming loop
    # ------------------------------------------------------------------

    async def _voice_udp_loop(self, ip: str, port: int, ogg_data: bytes = None, start_frame: int = 0):
        import socket as _sock
        import struct as _st
        import time as _time

        # Ưu tiên dùng UDP port server trả về từ Step3 resp field[2]
        # Fallback: port+2 (convention cũ)
        udp_port = getattr(self, '_voice_udp_port', None) or (port + 2)
        HELLXlog("VOICE", f"UDP: using port {udp_port} (Step3 field[2]={getattr(self,'_voice_udp_port',None)})")
        if not ogg_data:
            HELLXlog("VOICE", "UDP: No audio data")
            return

        frames = list(_ogg_opus_frames_bytes(ogg_data))
        if not frames:
            HELLXlog("VOICE", "UDP: No opus frames decoded")
            return

        FRAME_SAMPLES   = 960          # 48kHz * 20ms — chuẩn Opus/RTP
        FRAME_DURATION  = 0.020
        import random as _rnd
        # seq và timestamp bắt đầu random để tránh bị server filter
        ts_val = _rnd.randint(0, 0xFFFFFFFF)
        seq    = _rnd.randint(0, 0xFFFF)
        # f3_val lấy từ Step3 resp field[3] — full 32-bit
        f3_val = getattr(self, '_voice_f3', 0x13e6) & 0xFFFFFFFF
        # SSRC thực từ Step3 field[13] hoặc fallback field[3] — KHÔNG hardcode
        ssrc_int = getattr(self, '_voice_ssrc_int', 0)
        if ssrc_int == 0:
            ssrc_int = f3_val  # fallback nếu Step3 chưa parse được field 13
        HELLXlog("VOICE", f"UDP: ssrc=0x{ssrc_int:08x} f3=0x{f3_val:08x}")
        # mic_open: format giống s3 — dynamic ssrc, seq, ts + "Free Fire" payload
        _mic_seq = _rnd.randint(0, 0xFFFFFF)
        _mic_ts  = _rnd.randint(0, 0xFFFFFFFF)
        _mic_open_ba = bytearray()
        _mic_open_ba.append(0x71)
        _mic_open_ba += (_mic_seq & 0xFFFFFF).to_bytes(3, "big")
        _mic_open_ba += struct.pack(">I", _mic_ts & 0xFFFFFFFF)
        _mic_open_ba += struct.pack(">I", ssrc_int & 0xFFFFFFFF)
        _mic_open_ba += struct.pack(">I", 0x00000311)
        _mic_open_ba += bytes.fromhex("0000010720210000286e4672656520466972650000000000000000000000")
        _mic_open_ba += bytes.fromhex("e7877176")
        mic_open = bytes(_mic_open_ba)
        HELLXlog("VOICE", f"UDP: mic_open={mic_open.hex()}")
        _udp_sock_owned = False
        udp = getattr(self, '_voice_udp_sock', None)
        if udp is None:
            udp = _sock.socket(_sock.AF_INET, _sock.SOCK_DGRAM)
            udp.setblocking(False)
            _udp_sock_owned = True
        loop = asyncio.get_event_loop()

        udp_rx_count = 0
        _udp_ack_received = asyncio.Event()

        async def _udp_receiver():
            nonlocal udp_rx_count
            try:
                while True:
                    packet, _ = await loop.sock_recvfrom(udp, 4096)
                    udp_rx_count += 1
                    # Server gửi bất kỳ packet nào về = ACK mic_open
                    _udp_ack_received.set()
            except Exception:
                pass

        udp_rx_task = asyncio.create_task(_udp_receiver())

        # Gửi mic_open, retry tối đa 3 lần nếu chưa nhận ACK
        _mic_open_ok = False
        for _retry in range(3):
            try:
                udp.sendto(mic_open, (ip, udp_port))
            except Exception as e:
                HELLXlog("VOICE", f"UDP: mic open failed: {e}")
                break
            try:
                await asyncio.wait_for(_udp_ack_received.wait(), timeout=1.0)
                _mic_open_ok = True
                break
            except asyncio.TimeoutError:
                HELLXlog("VOICE", f"UDP: mic open retry {_retry+1}/3")
        if not _mic_open_ok:
            HELLXlog("VOICE", "UDP: no ACK from server — streaming anyway")

        next_send = _time.monotonic()
        try:
            for i in range(start_frame, len(frames)):
                if i == start_frame:
                    HELLXlog("VOICE", f"UDP: Streaming {len(frames)} frames to {ip}:{udp_port}")

                opus_frame = frames[i]

                # Header: magic(2) + seq(2) + ts(4) + ssrc(4) + fixed_2(4)
                # FIXED_1 = ssrc thực từ server, FIXED_2 = f3 full 32-bit (giống s3)
                header = _st.pack('>2sHIII',
                                  b'\x41\x6f',
                                  seq & 0xFFFF,
                                  ts_val & 0xFFFFFFFF,
                                  ssrc_int & 0xFFFFFFFF,
                                  f3_val & 0xFFFFFFFF)
                try:
                    loop.call_soon_threadsafe(udp.sendto, header + opus_frame, (ip, udp_port))
                except Exception:
                    udp.sendto(header + opus_frame, (ip, udp_port))

                self._track_current_frame = i
                seq    = (seq + 1) & 0xFFFF
                ts_val = (ts_val + FRAME_SAMPLES) & 0xFFFFFFFF
                next_send += FRAME_DURATION
                sleep_dur = next_send - _time.monotonic()
                if sleep_dur > 0:
                    await asyncio.sleep(sleep_dur)

        except asyncio.CancelledError:
            self._stopped_frame = self._track_current_frame
            HELLXlog("VOICE", f"UDP: Stopped at frame {self._track_current_frame}")
        except Exception as e:
            HELLXlog("VOICE", f"UDP: Error: {e}")
        finally:
            udp_rx_task.cancel()
            if _udp_sock_owned:
                udp.close()
            HELLXlog("VOICE", "UDP: Loop ended")


# ---------------------------------------------------------------------------
# Telegram bot
# ---------------------------------------------------------------------------

async def run_telegram_bot(token: str, active_bots: list):
    import aiohttp
    import subprocess
    import json as _json

    offset       = 0
    url          = f"https://api.telegram.org/bot{token}/getUpdates"
    send_url     = f"https://api.telegram.org/bot{token}/sendMessage"
    search_cache = {}   # chat_id -> list of {"title", "channel", "url", "duration"}

    HELLXlog("TELEGRAM", "Polling started...")

    delete_url = f"https://api.telegram.org/bot{token}/deleteMessage"
    AUTO_DELETE_SECONDS = 120  # 2 phút

    async def _delete_after(chat_id, *message_ids, delay=AUTO_DELETE_SECONDS):
        """Xóa một hoặc nhiều message sau `delay` giây (silent, bỏ qua lỗi)."""
        await asyncio.sleep(delay)
        async with aiohttp.ClientSession() as s:
            for mid in message_ids:
                try:
                    await s.post(delete_url, json={"chat_id": chat_id, "message_id": mid})
                except Exception:
                    pass

    async def send(chat_id, text, user_msg_id=None):
        """Gửi tin nhắn; nếu truyền user_msg_id thì tự xóa cả 2 sau AUTO_DELETE_SECONDS."""
        async with aiohttp.ClientSession() as s:
            resp = await s.post(send_url, json={"chat_id": chat_id, "text": text})
            sent = await resp.json()
        bot_msg_id = sent.get("result", {}).get("message_id")
        if bot_msg_id:
            ids_to_delete = [bot_msg_id]
            if user_msg_id:
                ids_to_delete.append(user_msg_id)
            asyncio.create_task(_delete_after(chat_id, *ids_to_delete))

    # Bỏ qua toàn bộ update cũ khi khởi động (tránh reply tin nhắn tồn đọng)
    async with aiohttp.ClientSession() as _skip_sess:
        try:
            async with _skip_sess.get(
                f"{url}?offset=-1&timeout=0", timeout=aiohttp.ClientTimeout(total=10)
            ) as _r:
                _d = await _r.json()
                _results = _d.get("result", [])
                if _results:
                    offset = _results[-1]["update_id"] + 1
                    HELLXlog("TELEGRAM", f"Skipped pending updates, offset={offset}")
        except Exception as _e:
            HELLXlog("TELEGRAM", f"skip_pending failed (non-critical): {_e}")

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with session.get(
                    f"{url}?offset={offset}&timeout=30", timeout=aiohttp.ClientTimeout(total=35)
                ) as resp:
                    if resp.status != 200:
                        await asyncio.sleep(2)
                        continue
                    data = await resp.json()

                for result in data.get("result", []):
                    offset      = result["update_id"] + 1
                    msg         = result.get("message", {})
                    text        = msg.get("text", "").strip()
                    chat_id     = msg.get("chat", {}).get("id")
                    user_msg_id = msg.get("message_id")
                    if not text or not chat_id:
                        continue

                    # ── /start ──────────────────────────────────────────
                    if text == "/start":
                        await send(chat_id,
                            "🎵 Free Fire × Voice Bot\n\n"
                            "📌 Luồng sử dụng:\n"
                            "1️⃣ /search [tên bài] — tìm NCT\n"
                            "   /search ct [tên] — NhacCuaTui\n"
                            "   /search yt [tên] — YouTube\n"
                            "2️⃣ /join [team_code] [số] — vào team + phát bài",
                            user_msg_id=user_msg_id
                        )

                    # ── /search ─────────────────────────────────────────
                    # Prefix: "yt " → YouTube, "ct " → NhacCuaTui, không prefix → NCT
                    elif text.lower().startswith("/search"):
                        raw_query = text[7:].strip()
                        if not raw_query:
                            await send(chat_id,
                                "❌ Dùng:\n"
                                "/search [tên bài] — tìm NCT\n"
                                "/search ct [tên bài] — tìm NhacCuaTui\n"
                                "/search yt [tên bài] — tìm YouTube",
                                user_msg_id=user_msg_id)
                            continue

                        # Phân tích prefix
                        lower_q = raw_query.lower()
                        if lower_q.startswith("yt "):
                            force_source = "youtube"
                            query = raw_query[3:].strip()
                        elif lower_q.startswith("ct "):
                            force_source = "nct"
                            query = raw_query[3:].strip()
                        else:
                            force_source = "nct"   # mặc định NCT
                            query = raw_query

                        if not query:
                            await send(chat_id, "❌ Thiếu tên bài.", user_msg_id=user_msg_id)
                            continue

                        src_label = "NhacCuaTui" if force_source == "nct" else "YouTube"
                        await send(chat_id, f"🔍 Đang tìm trên {src_label}: {query}...", user_msg_id=user_msg_id)

                        try:
                            if force_source == "youtube":
                                results = await _youtube_search_api(query, limit=10)
                            else:
                                results = await _nct_search(query, limit=5)
                                if not results:
                                    # NCT không có → thông báo rõ, không tự fallback
                                    await send(chat_id,
                                        f"❌ NhacCuaTui không có kết quả cho: {query}\n"
                                        f"Thử: /search yt {query}",
                                        user_msg_id=user_msg_id)
                                    continue

                            if not results:
                                await send(chat_id, "❌ Không tìm thấy kết quả.", user_msg_id=user_msg_id)
                                continue

                            search_cache[chat_id] = results
                            source_tag = "🎵 NhacCuaTui" if results[0].get("source") == "nct" else "▶️ YouTube"
                            lines = [f"{source_tag} — Kết quả cho: {query}\n"]
                            for i, r in enumerate(results, 1):
                                artist = r.get("artist") or r.get("channel") or ""
                                lines.append(f"{i}. {r['title']} [{r['duration']}]\n   └ {artist}")
                            lines.append("\n▶ Dùng: /join [team_code] [số thứ tự]")
                            await send(chat_id, "\n".join(lines), user_msg_id=user_msg_id)

                        except Exception as e:
                            HELLXlog("SEARCH", f"Lỗi: {e}")
                            await send(chat_id, f"❌ Tìm kiếm thất bại: {e}", user_msg_id=user_msg_id)

                    # ── /join ────────────────────────────────────────────
                    elif text.lower().startswith("/join"):
                        parts = text.split()
                        # /join CODE           → join only (no play)
                        # /join CODE 2         → join + play bài số 2 từ search_cache
                        # /join CODE https://… → join + play URL trực tiếp
                        if len(parts) < 2:
                            await send(chat_id, "❌ Dùng: /join [team_code] [số thứ tự hoặc URL]", user_msg_id=user_msg_id)
                            continue

                        code        = parts[1]
                        play_url    = None
                        play_title  = ""

                        if len(parts) >= 3:
                            arg = parts[2]
                            if arg.startswith("http"):
                                play_url   = arg
                                play_title = arg
                            else:
                                try:
                                    idx    = int(arg) - 1
                                    cached = search_cache.get(chat_id, [])
                                    if idx < 0 or idx >= len(cached):
                                        await send(chat_id, "❌ Số thứ tự không hợp lệ. Dùng /search trước.", user_msg_id=user_msg_id)
                                        continue
                                    play_url   = cached[idx]["url"]
                                    play_title = cached[idx]["title"]
                                except ValueError:
                                    await send(chat_id, "❌ Tham số thứ hai phải là số thứ tự hoặc URL.", user_msg_id=user_msg_id)
                                    continue

                        # Tìm bot rảnh
                        count = 0
                        for bot in active_bots:
                            if bot.writer and not bot.writer.transport.is_closing():
                                if not bot.voice_region_id:
                                    try:
                                        join_pkt = HELLXbuild_join_sq(int(bot.uid), code, bot.key, bot.iv)
                                        bot.writer.write(join_pkt)
                                        await bot.writer.drain()
                                        count += 1
                                        bot._telegram_chat_id = chat_id
                                        bot._joining          = True

                                        async def _check(b=bot, c=chat_id, tc=code, url=play_url, title=play_title, umid=user_msg_id):
                                            b._join_success = False
                                            await asyncio.sleep(3)
                                            b._joining = False
                                            async with aiohttp.ClientSession() as s:
                                                if b._join_success:
                                                    if url:
                                                        r = await s.post(send_url, json={
                                                            "chat_id": c,
                                                            "text": f"✅ Đã vào team!\n🎵 Đang tải: {title[:60]}"
                                                        })
                                                        rd = await r.json()
                                                        bot_mid = rd.get("result", {}).get("message_id")
                                                        if bot_mid:
                                                            asyncio.create_task(_delete_after(c, bot_mid, *([umid] if umid else [])))
                                                        asyncio.create_task(b._play_from_url(url, title))
                                                    else:
                                                        r = await s.post(send_url, json={
                                                            "chat_id": c,
                                                            "text": f"✅ Đã vào team {tc}!\nGửi !play [bài hát] trong game để phát nhạc."
                                                        })
                                                        rd = await r.json()
                                                        bot_mid = rd.get("result", {}).get("message_id")
                                                        if bot_mid:
                                                            asyncio.create_task(_delete_after(c, bot_mid, *([umid] if umid else [])))
                                                else:
                                                    r = await s.post(send_url, json={
                                                        "chat_id": c,
                                                        "text": f"❌ Team code không hợp lệ: {tc}"
                                                    })
                                                    rd = await r.json()
                                                    bot_mid = rd.get("result", {}).get("message_id")
                                                    if bot_mid:
                                                        asyncio.create_task(_delete_after(c, bot_mid, *([umid] if umid else [])))

                                        asyncio.create_task(_check())
                                        break   # một bot mỗi lần join

                                    except Exception as ex:
                                        HELLXlog("TEAM", f"Join failed uid={bot.uid}: {ex}")

                        if count > 0:
                            await send(chat_id, f"⏳ Đang vào team {code}...", user_msg_id=user_msg_id)
                        else:
                            await send(chat_id, "❌ Không có bot rảnh.", user_msg_id=user_msg_id)

                    # ── /play (direct, dành cho bot đang trong team của user) ──
                    elif text.lower().startswith("/play"):
                        query = text[5:].strip()
                        if not query:
                            await send(chat_id, "❌ Dùng: /play [tên bài hoặc URL]", user_msg_id=user_msg_id)
                            continue
                        # Tìm bot đang trong team của chat_id này
                        target_bot = None
                        for b in active_bots:
                            if b._telegram_chat_id == chat_id and b.voice_region_id:
                                target_bot = b
                                break
                        if target_bot:
                            asyncio.create_task(target_bot._play_from_query(query))
                            await send(chat_id, f"🎵 Thêm vào hàng chờ: {query[:60]}", user_msg_id=user_msg_id)
                        else:
                            await send(chat_id, "❌ Bot chưa vào team. Dùng /join trước.", user_msg_id=user_msg_id)

                    # ── /skip ─────────────────────────────────────────────
                    elif text.lower() == "/skip":
                        target_bot = None
                        for b in active_bots:
                            if b._telegram_chat_id == chat_id and b.voice_region_id:
                                target_bot = b
                                break
                        if target_bot:
                            await target_bot._skip_current()
                            nxt = target_bot._track_queue.qsize()
                            msg = f"⏭ Skipped. Còn {nxt} bài trong hàng chờ." if nxt > 0 else "⏭ Skipped. Hàng chờ trống."
                            await send(chat_id, msg, user_msg_id=user_msg_id)
                        else:
                            await send(chat_id, "❌ Không có bot nào đang phát nhạc cho bạn.", user_msg_id=user_msg_id)

                    # ── /queue ────────────────────────────────────────────
                    elif text.lower() in ("/queue", "/q"):
                        target_bot = None
                        for b in active_bots:
                            if b._telegram_chat_id == chat_id and b.voice_region_id:
                                target_bot = b
                                break
                        if target_bot:
                            is_playing = target_bot._udp_task and not target_bot._udp_task.done()
                            items = list(target_bot._track_queue._queue) if hasattr(target_bot._track_queue, '_queue') else []
                            if not is_playing and not items:
                                await send(chat_id, "📋 Hàng chờ trống.", user_msg_id=user_msg_id)
                            else:
                                lines = ["📋 Hàng chờ nhạc:"]
                                if is_playing and target_bot._track_title:
                                    e_s = int(target_bot._track_current_frame * 0.02)
                                    t_s = int(target_bot._track_total_frames * 0.02)
                                    lines.append(f"▶ {target_bot._track_title[:45]} [{e_s//60}:{e_s%60:02d}/{t_s//60}:{t_s%60:02d}]")
                                for i, (q, t) in enumerate(items, 1):
                                    label = t or q
                                    lines.append(f"{i}. {label[:50]}")
                                await send(chat_id, "\n".join(lines), user_msg_id=user_msg_id)
                        else:
                            await send(chat_id, "❌ Không có bot nào đang hoạt động cho bạn.", user_msg_id=user_msg_id)

                    # ── /clear ────────────────────────────────────────────
                    elif text.lower() == "/clear":
                        target_bot = None
                        for b in active_bots:
                            if b._telegram_chat_id == chat_id and b.voice_region_id:
                                target_bot = b
                                break
                        if target_bot:
                            await target_bot._clear_queue()
                            await send(chat_id, "🗑 Đã xóa hàng chờ và dừng phát nhạc.", user_msg_id=user_msg_id)
                        else:
                            await send(chat_id, "❌ Không có bot nào đang hoạt động cho bạn.", user_msg_id=user_msg_id)

                    # ── /leave ───────────────────────────────────────────
                    elif text.lower() == "/leave":
                        left = 0
                        for bot in active_bots:
                            if bot._telegram_chat_id == chat_id and bot.voice_region_id:
                                asyncio.create_task(bot._leave_team())
                                left += 1
                        if left == 0:
                            # Fallback: leave tất cả bot đang trong team
                            for bot in active_bots:
                                if bot.voice_region_id:
                                    asyncio.create_task(bot._leave_team())
                                    left += 1
                        await send(chat_id, f"✅ Đã ra lệnh cho {left} bot rời team." if left else "ℹ️ Không có bot nào trong team.", user_msg_id=user_msg_id)

                    # ── /status ──────────────────────────────────────────
                    elif text.lower() == "/status":
                        lines = ["📊 Trạng thái bot:\n"]
                        for i, bot in enumerate(active_bots, 1):
                            in_team  = "✅ Trong team" if bot.voice_region_id else "⏳ Rảnh"
                            playing  = "🎵 Đang phát" if (bot._udp_task and not bot._udp_task.done()) else "⏸ Không phát"
                            q_size   = bot._track_queue.qsize()
                            lines.append(f"{i}. uid={bot.uid}  {in_team}  {playing}")
                            if bot._track_title:
                                e_s = int(bot._track_current_frame * 0.02)
                                t_s = int(bot._track_total_frames * 0.02)
                                lines.append(f"   └ {bot._track_title[:40]} [{e_s//60}:{e_s%60:02d}/{t_s//60}:{t_s%60:02d}]")
                            if q_size > 0:
                                lines.append(f"   📋 Hàng chờ: {q_size} bài")
                        await send(chat_id, "\n".join(lines), user_msg_id=user_msg_id)

            except Exception as e:
                HELLXlog("TELEGRAM", f"Error: {e}")
                await asyncio.sleep(5)


# ---------------------------------------------------------------------------
# Entry
# ---------------------------------------------------------------------------

async def main():
    config_path = "config.json"
    if not os.path.exists(config_path):
        with open(config_path, "w") as f:
            json.dump({"telegram_token": "YOUR_TOKEN_HERE"}, f, indent=4)
        HELLXlog("SYSTEM", "Created config.json — please fill in telegram_token.")
        return

    with open(config_path, "r") as f:
        config = json.load(f)

    token = config.get("telegram_token", "")
    HELLXlog("SYSTEM", "Starting HELLX Voice Bot")

    accounts = HELLXload_accounts("accs.json")
    if not accounts:
        HELLXlog("SYSTEM", "No accounts in accs.json")
        return

    # Semaphore toàn cục: tối đa 2 bot download cùng lúc
    # (tránh 3 bot đồng thời download -> hết băng thông -> tất cả timeout)
    _global_download_sem = asyncio.Semaphore(2)

    active_bots = []
    for uid, pwd in accounts:
        bot = HELLXBot(uid, pwd)
        bot._download_sem = _global_download_sem
        active_bots.append(bot)

    tasks = [bot.run() for bot in active_bots]

    if token and token != "YOUR_TOKEN_HERE":
        tasks.append(run_telegram_bot(token, active_bots))
    else:
        HELLXlog("SYSTEM", "Telegram token not set — Telegram bot disabled.")

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        HELLXlog("SYSTEM", "Stopped.")
