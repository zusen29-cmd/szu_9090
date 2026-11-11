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


# ---------------------------------------------------------------------------
# Opus encoding — dùng ffmpeg libopus trực tiếp, không cần opuslib/pyopus
# Chỉ cần ffmpeg build với --enable-libopus (mặc định trên hầu hết distro)
# ---------------------------------------------------------------------------
import subprocess as _subprocess
def _check_ffmpeg_opus() -> bool:
    try:
        r = _subprocess.run(
            ["ffmpeg", "-encoders"],
            capture_output=True, text=True, timeout=5
        )
        return "libopus" in r.stdout
    except Exception:
        return False

_OPUSLIB_OK = _check_ffmpeg_opus()
if not _OPUSLIB_OK:
    HELLXlog("SETUP", "[!] ffmpeg libopus không khả dụng — không phát được nhạc")
_opuslib = None   # giữ tương thích nếu code khác tham chiếu


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
        # Playback
        self._udp_task            = None
        self._current_url         = None
        self._track_title         = ""
        self._track_channel       = ""
        self._track_total_frames  = 0   # giữ để progress bar / Telegram dùng
        self._track_current_frame = 0
        self._stopped_frame       = 0
        self._volume              = 100
        self._progress_task       = None
        self._emote_task          = None
        # Producer/consumer audio (kiến trúc main-1: thread → queue → UDP)
        self._audio_q             = None   # queue.Queue chứa opus frames đã encode
        self._audio_stop          = False  # cờ dừng producer thread
        self._producer_th         = None   # threading.Thread chạy ffmpeg pipe

        # Queue nhạc — asyncio.Queue để xử lý tuần tự, tránh race condition
        # khi nhiều người cùng /join bot với bài khác nhau
        self._track_queue         = asyncio.Queue()   # item: (query_or_url, title_hint)
        self._queue_task          = None              # task chạy _queue_worker
        self._play_lock           = asyncio.Lock()    # 1 lượt download+encode mỗi lúc
        self._download_sem        = None              # set từ main() — semaphore toàn cục

        # Telegram
        self._telegram_chat_id    = None
        self._session_id          = None   # UUID mỗi lần join — isolation A/B
        self._joining             = False
        self._join_success        = False

        # PB mode (nhảy emote không cần nhạc)
        self._pb_mode             = False   # True khi join qua lệnh /pb
        self._pb_emote_task       = None    # task emote loop độc lập
        self._telegram_notify_cb  = None    # callback: async (chat_id, text) → gửi Telegram

        # Emote / squad
        self._squad_uids     = []        # UIDs của tất cả member trong squad (kể cả bot)
        self._squad_uids_raw = []        # UIDs từ packet TRƯỚC khi force-add owner → detect khi owner thoát nhóm
        self._region_code    = 0x11      # byte region VN — dùng trong header packet outgoing
        self._emote_ids      = [
            909054004, 909040010, 909000075, 909000063, 909035007,
            909000085, 909000090, 909000098, 909045001, 909000081,
            909039011, 909049010, 909000068, 909039011, 909038010,
            909041005, 909033002, 909038012,
        ]

        # Internal
        self._current_version     = "1.132.11"  # OB55
        self._online              = False   # True khi TCP online server đã connect thành công

    # ------------------------------------------------------------------
    # Entry point
    # ------------------------------------------------------------------

    async def run(self):
        import time as _time
        _retry_delay = 15  # giây chờ giữa các lần re-login
        while True:
            await self._run_once()
            HELLXlog("BOT", f"uid={self.uid} — offline, re-login sau {_retry_delay}s...")
            # Reset trạng thái online trước khi thử lại
            self.writer        = None
            self.token         = None
            self.voice_region_id = None
            self.chat_region_id  = None
            self.team_owner_uid  = None
            self._joining        = False
            self._join_success   = False
            if self._chat_writer:
                try:
                    self._chat_writer.close()
                except Exception:
                    pass
                self._chat_writer = None
                self._chat_reader = None
            if self._pb_emote_task and not self._pb_emote_task.done():
                self._pb_emote_task.cancel()
                self._pb_emote_task = None
            self._pb_mode = False
            await asyncio.sleep(_retry_delay)

    async def _run_once(self):
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

        self.writer   = writer
        self._online  = True
        asyncio.create_task(HELLXping_loop(writer, HELLXbuild_ping_packet, self.key, self.iv))
        await HELLXread_loop(reader, self._handle_tcp_data, self.key, self.iv)
        self._online = False

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
                    # field5["1"] = UID người gây sự kiện (người join/leave), KHÔNG phải owner
                    # field5["6"] = danh sách member thực của squad (list of dict, mỗi dict có key "1"=uid)
                    if isinstance(field5, dict):
                        msg_type = proto_data.get("4")
                        if isinstance(msg_type, (int, str)) and int(str(msg_type) or 0) in (3, 6, 8, 44, 56):
                            # Chỉ lấy UIDs từ field6 (member list thực) — KHÔNG dùng field1
                            members = field5.get("6")
                            member_uids = []
                            if isinstance(members, list):
                                for m in members:
                                    if isinstance(m, dict):
                                        mid = m.get("1")
                                        if mid and str(mid).isdigit():
                                            member_uids.append(int(mid))

                            # Lưu raw member list từ server (chưa force-add gì)
                            self._squad_uids_raw = list(set(member_uids))

                            # squad_uids dùng cho emote → luôn đảm bảo có owner và bot
                            uids = list(set(member_uids))
                            if self.team_owner_uid and self.team_owner_uid not in uids:
                                uids.append(self.team_owner_uid)
                            bot_uid = self.account_uid or int(self.uid)
                            if bot_uid not in uids:
                                uids.append(bot_uid)
                            self._squad_uids = uids
                            HELLXlog("TEAM", f"squad_uids updated: {self._squad_uids}")

                            # ── Detect khi chủ team thoát nhóm ──────────────────
                            # Điều kiện ĐÚNG:
                            #   1. Đang trong team và không đang joining
                            #   2. Biết owner là ai (team_owner_uid đã set)
                            #   3. field6 (member list) có ít nhất 1 người → packet hợp lệ, không phải rỗng
                            #   4. Owner KHÔNG có trong field6 → server xác nhận owner đã rời
                            #
                            # KHÔNG dùng field5["1"] vì đó là UID người gây sự kiện,
                            # khi member thường rời thì field5["1"] = UID họ → owner không có
                            # → false positive nếu dùng field1 thay cho field6
                            if (
                                self.voice_region_id
                                and not self._joining
                                and self.team_owner_uid
                                and len(self._squad_uids_raw) > 0          # packet có member → hợp lệ
                                and self.team_owner_uid not in self._squad_uids_raw  # owner không còn trong field6
                            ):
                                HELLXlog("TEAM", f"⚠ Owner {self.team_owner_uid} không còn trong squad — kích hoạt auto leave")
                                asyncio.create_task(self._on_owner_left())

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
                if self._current_url and self._voice_ip:
                    # Phát lại bài hiện tại từ đầu (producer/consumer không hỗ trợ seek)
                    await self._play_from_query(self._current_url, title_hint=self._track_title)
                    await self._send_chat(f"[27EBF5]▶ Restarting: {self._track_title[:40]}")
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
        for task in (self._udp_task, self._progress_task, self._voice_ping_task,
                      self._voice_connect_task, self._pb_emote_task):
            if task and not task.done():
                task.cancel()
        self._pb_mode        = False
        self._pb_emote_task  = None
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
        # Dừng producer thread
        self._audio_stop = True
        self._current_url   = None
        self._track_title   = ""
        self._stopped_frame = 0
        # Reset ownership — bot không còn thuộc về ai
        self._telegram_chat_id = None
        self._session_id       = None
        HELLXlog("TEAM", "Left. Waiting for next invite...")

    # ------------------------------------------------------------------
    # Auto-leave khi chủ team thoát nhóm
    # ------------------------------------------------------------------

    async def _on_owner_left(self):
        """Tự động rời team khi phát hiện chủ team đã thoát nhóm.
        Gửi chat game + thông báo Telegram + leave sạch."""
        if not self.voice_region_id:
            return  # đã leave rồi (có thể do race condition)
        HELLXlog("TEAM", "Chủ team thoát nhóm — bot tự rời...")
        try:
            await self._send_chat("[F52727]⚠ Chủ team đã thoát. Bot tự rời nhóm.")
        except Exception:
            pass
        # Thông báo về Telegram nếu có callback
        if self._telegram_chat_id and self._telegram_notify_cb:
            try:
                await self._telegram_notify_cb(
                    self._telegram_chat_id,
                    "⚠️ <b>Chủ team đã thoát nhóm!</b>\n└ Bot đã tự động rời team."
                )
            except Exception:
                pass
        await asyncio.sleep(1.0)
        await self._leave_team()

    # ------------------------------------------------------------------
    # PB emote loop (nhảy emote không cần nhạc)
    # ------------------------------------------------------------------

    async def _pb_send_welcome(self):
        """Chờ chat server kết nối rồi gửi tin chào nhiều màu vào tổ đội."""
        # Chờ _chat_writer sẵn sàng (tối đa 10s — connect_chat chạy song song)
        deadline = asyncio.get_event_loop().time() + 10
        while not self._chat_writer and asyncio.get_event_loop().time() < deadline:
            await asyncio.sleep(0.3)
        if not self._chat_writer:
            HELLXlog("EMOTE", "PB: chat chưa kết nối — bỏ welcome msg")
            return

        welcome = (
            "[FFFF00]Welcome [F52727]Bot [27EBF5]Emote [FF8C00]By [FFFF00]Senzu[F52727].!\n"
            "[A627F5]TG: [27EBF5]@S_ZU_01"
        )
        await self._send_chat(welcome)
        HELLXlog("EMOTE", "PB: welcome msg đã gửi")

    async def _pb_emote_loop(self):
        """Emote loop độc lập cho /pb mode.
        Không phụ thuộc vào _udp_task (không cần phát nhạc).
        Chạy liên tục cho đến khi: rời team | _pb_mode = False | task bị cancel."""
        try:
            HELLXlog("EMOTE", f"pb_emote_loop START uid={self.uid} region={self.voice_region_id} pb_mode={self._pb_mode}")
            # Đăng ký emote set với server trước khi nhảy
            await self._choose_emote_http()
            emotes = list(self._emote_ids) if self._emote_ids else [909054004]
            idx = 0
            # Chờ voice_region_id chắc chắn có (tối đa 5s)
            for _ in range(10):
                if self.voice_region_id:
                    break
                await asyncio.sleep(0.5)
            if not self.voice_region_id:
                HELLXlog("EMOTE", "pb_emote_loop: voice_region_id vẫn None sau 5s — abort")
                return
            # Delay nhỏ để voice connect ổn định + welcome msg kịp gửi trước
            await asyncio.sleep(2.0)
            HELLXlog("EMOTE", f"pb_emote_loop: bắt đầu nhảy emote, {len(emotes)} emotes")
            while self._pb_mode and self.voice_region_id and idx < len(emotes):
                await self._trigger_emote(emotes[idx])
                idx += 1
                HELLXlog("EMOTE", f"pb_emote_loop: emote {idx}/{len(emotes)} gửi")
                # 7s giữa các emote — chia nhỏ để cancel nhanh
                for _ in range(14):
                    await asyncio.sleep(0.5)
                    if not self._pb_mode or not self.voice_region_id:
                        HELLXlog("EMOTE", "pb_emote_loop: dừng do pb_mode=False hoặc rời team")
                        return
            # Nhảy hết toàn bộ emote → tự rời team
            if self._pb_mode and self.voice_region_id:
                HELLXlog("EMOTE", "pb_emote_loop: đã nhảy hết emote — tự rời team")
                # Thông báo Telegram trước khi rời
                if self._telegram_notify_cb and self._telegram_chat_id:
                    try:
                        await self._telegram_notify_cb(
                            self._telegram_chat_id,
                            "✅ <b>Bot đã nhảy xong tất cả emote!</b>\n"
                            "└ Tự động rời team."
                        )
                    except Exception:
                        pass
                await self._leave_team()
            HELLXlog("EMOTE", "pb_emote_loop END")
        except asyncio.CancelledError:
            HELLXlog("EMOTE", "pb_emote_loop CANCELLED")
            pass
        except Exception as e:
            HELLXlog("EMOTE", f"pb_emote_loop error: {e}")

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
        FIX: luôn include team_owner_uid — tránh owner mất emote sau khi người khác leave.
        FIX2: writer.drain() có timeout 0.1s để không block event loop lâu khi
              audio loop đang chạy đồng thời (_send_ev pattern cần event loop free)."""
        try:
            if not self.writer or self.writer.transport.is_closing():
                return
            bot_uid = self.account_uid or int(self.uid)
            must_have = [bot_uid]
            if self.team_owner_uid:
                must_have.append(int(self.team_owner_uid))
            uids = list(set(self._squad_uids + must_have))
            if not uids:
                return
            eid = emote_id or (self._emote_ids[0] if self._emote_ids else 909054004)
            pkt = self._build_emote_pkt(eid, uids)
            self.writer.write(pkt)
            # Timeout 0.1s: tránh drain block event loop khi TCP server chậm
            # Audio loop dùng asyncio.Event cần event loop free để nhận signal
            try:
                await asyncio.wait_for(self.writer.drain(), timeout=0.1)
            except asyncio.TimeoutError:
                pass   # buffer sẽ flush khi loop rảnh, không cần chờ
            HELLXlog("EMOTE", f"emote={eid} → uids={uids}")
        except Exception as e:
            HELLXlog("EMOTE", f"Error: {e}")

    async def _emote_loop(self):
        """Cycle qua tất cả emotes trong suốt bài hát (7s/emote), lặp lại từ đầu khi hết list.
        An toàn với concurrent: thoát ngay khi bị cancel hoặc udp_task đã done.
        Không chạy khi đang ở pb_mode (pb mode có loop riêng _pb_emote_loop)."""
        try:
            # Guard: nếu đang pb_mode thì không chạy emote loop này
            if self._pb_mode:
                HELLXlog("EMOTE", "_emote_loop skipped — pb_mode active")
                return
            emotes = list(self._emote_ids) if self._emote_ids else [909054004]
            idx = 0
            # Delay nhỏ để tránh emote ngay lúc bài vừa bắt đầu (server chưa nhận session)
            await asyncio.sleep(2.0)
            while True:
                # Dừng khi bài hát kết thúc hoặc không còn trong team
                if not self.voice_region_id:
                    break
                if self._pb_mode:   # pb mode chiếm quyền emote
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
        raw_path = _tmp.mktemp(suffix=".mp3")
        raw_file = raw_path   # sẽ gán lại nếu yt-dlp tạo file tên khác

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
                        stream_url = None   # trigger yt-dlp fallback bên dưới

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
                    raw_file = matches[0]   # có thể là .webm, .m4a, .mp3 …

                # ── raw_file đã sẵn sàng — producer thread sẽ đọc qua ffmpeg pipe ──
                # Semaphore release sau khi download xong, trước khi phát
                pass   # (kết thúc block "async with sem")

            # -- Semaphore released: download xong, bắt đầu phát --

            # Kiểm tra lại còn trong team không (có thể đã leave trong lúc download)
            if not self.voice_region_id or not self._voice_ip:
                HELLXlog("PLAY", "Left team during download — aborting play.")
                return

            if not _OPUSLIB_OK:
                await self._send_chat("[F52727]ffmpeg libopus không khả dụng — không phát được nhạc.")
                return

            self._current_url   = query
            self._track_title   = title
            self._track_channel = channel
            self._track_current_frame = 0
            self._track_total_frames  = 0   # không biết trước (stream thời gian thực)
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
                self._voice_udp_loop(self._voice_ip, self._voice_port, raw_file=raw_file)
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
            for fp in [raw_path] + _glob.glob(raw_path + ".*"):
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
                frame = self._track_current_frame
                total = self._track_total_frames
                e_s   = int(frame * 0.02)
                if total > 0:
                    # Biết độ dài → hiện thanh tiến trình
                    pct    = min(frame / total, 1.0)
                    filled = int(16 * pct)
                    bar    = "[27EBF5]" + "■" * filled + "[5427F5]" + "□" * (16 - filled)
                    t_s    = int(total * 0.02)
                    time_str = f"[27EBF5]{e_s//60}:{e_s%60:02d} [F227F5]/ [27EBF5]{t_s//60}:{t_s%60:02d}"
                else:
                    # Stream thời gian thực — chỉ hiện thời gian đã phát
                    bar      = "[27EBF5]" + "■" * 8 + "[5427F5]" + "□" * 8
                    time_str = f"[27EBF5]{e_s//60}:{e_s%60:02d} [5427F5]▶ live"
                await self._send_chat(
                    f"[27EBF5]▶ [5427F5]{self._track_title[:38]}\n"
                    f"[A627F5]{self._track_channel[:30]}\n"
                    f"{bar}\n"
                    f"{time_str}  [A627F5]Vol:{self._volume}%"
                )
                await asyncio.sleep(10)
        except asyncio.CancelledError:
            pass

    # ------------------------------------------------------------------
    # UDP streaming loop — kiến trúc producer/consumer
    #   _producer_thread : ffmpeg pipe → libopus raw frames → queue.Queue
    #   _voice_udp_loop  : timing_thread (OS) + async sender
    # Không cần opuslib/pyopus — ffmpeg tự encode libopus
    # ------------------------------------------------------------------

    def _producer_thread(self, raw_file: str, q: "queue.Queue"):
        """Thread riêng: ffmpeg encode thẳng libopus → raw opus frames → queue.
        Mỗi frame = 1 opus packet 20ms (960 samples @ 48kHz mono).
        Không cần opuslib — ffmpeg -f opus pipe xuất raw OGG-less opus packets
        qua segment_format obu; dùng segment_time 0.02 để ép đúng 20ms/frame.
        """
        import queue as _q
        # ffmpeg xuất raw opus frames qua pipe:
        #   -map_metadata -1  : bỏ metadata tránh header lạ đầu pipe
        #   -c:a libopus       : encode opus
        #   -b:a 96k           : bitrate 96kbps — đủ tốt cho voice/music
        #   -application audio : tối ưu cho nhạc (thay vì voip)
        #   -frame_duration 20 : đúng 20ms/frame = 960 samples
        #   -vbr on            : variable bitrate — chất lượng tốt hơn cbr
        #   -f data            : raw bytes pipe, không bọc container
        #
        # loudnorm EBU R128: normalize về -16 LUFS, true peak -1.5 dBFS
        # → bài to sẵn bị giảm, bài nhỏ được kéo lên — không bao giờ clip
        cmd = [
            "ffmpeg", "-i", raw_file,
            "-filter:a", (
                "loudnorm=I=-16:TP=-1.5:LRA=11,"    # EBU R128 normalize
                "dynaudnorm=f=200:g=15:p=0.90:m=30"  # smooth micro-variation
            ),
            "-map_metadata", "-1",
            "-c:a", "libopus",
            "-b:a", "96k",
            "-application", "audio",
            "-frame_duration", "20",
            "-vbr", "on",
            "-ac", "1", "-ar", "48000",
            "-f", "data",
            "-loglevel", "quiet", "pipe:1",
        ]
        import subprocess as _sp
        proc = _sp.Popen(cmd, stdout=_sp.PIPE, bufsize=0)
        # Mỗi opus frame raw ~100–400 bytes, nhưng độ dài biến đổi (VBR)
        # → đọc theo length-prefixed: 2 byte big-endian length + payload
        # ffmpeg -f data với libopus xuất từng packet kèm prefix tự động
        # Tuy nhiên với -f data thuần thì KHÔNG có prefix — ta cần đọc
        # frame boundary khác cách: dùng ogg muxer rồi parse OGG pages,
        # HOẶC đơn giản hơn: encode s16le rồi encode opus trong Python
        # qua subprocess ffmpeg từng chunk.
        #
        # Cách đơn giản & đáng tin nhất: pipe s16le ra, encode từng 1920-byte
        # chunk bằng ffmpeg sub-process nhỏ (1 lần gọi = 1 frame).
        # Nhưng spawn process mỗi 20ms quá tốn → dùng OGG page parser.
        #
        # GIẢI PHÁP THỰC TẾ NHẤT: dùng -f ogg và parse OGG pages để lấy
        # từng opus packet riêng lẻ mà không cần native binding.
        proc.terminate(); proc.wait()

        # Re-spawn với OGG muxer để lấy frame boundary rõ ràng
        cmd2 = [
            "ffmpeg", "-i", raw_file,
            "-filter:a", (
                "loudnorm=I=-16:TP=-1.5:LRA=11,"
                "dynaudnorm=f=200:g=15:p=0.90:m=30"
            ),
            "-c:a", "libopus",
            "-b:a", "96k",
            "-application", "audio",
            "-frame_duration", "20",
            "-vbr", "on",
            "-ac", "1", "-ar", "48000",
            "-f", "ogg",
            "-loglevel", "quiet", "pipe:1",
        ]
        proc2 = _sp.Popen(cmd2, stdout=_sp.PIPE, bufsize=0)

        def _read_exact(stream, n):
            buf = b""
            while len(buf) < n:
                chunk = stream.read(n - len(buf))
                if not chunk:
                    return None
                buf += chunk
            return buf

        def _parse_ogg_packets(stream):
            """Generator: yield từng opus packet từ OGG stream.
            OGG page format:
              4B  capture_pattern "OggS"
              1B  version
              1B  header_type
              8B  granule_position
              4B  serial_number
              4B  page_sequence
              4B  checksum
              1B  page_segments
              Nx1B segment_table
              data (concat segments)
            Packet boundary: segment size < 255 kết thúc packet.
            """
            import struct as _st
            while True:
                # Tìm OggS capture pattern
                hdr = _read_exact(stream, 27)
                if not hdr or len(hdr) < 27:
                    return
                if hdr[:4] != b"OggS":
                    # Lệch sync — tìm lại
                    continue
                n_segs = hdr[26]
                seg_table = _read_exact(stream, n_segs)
                if not seg_table:
                    return
                # Đọc toàn bộ data của page
                page_data_len = sum(seg_table)
                page_data = _read_exact(stream, page_data_len)
                if not page_data:
                    return
                # Parse packets từ segment table
                offset = 0
                pkt = b""
                for seg_sz in seg_table:
                    pkt += page_data[offset:offset + seg_sz]
                    offset += seg_sz
                    if seg_sz < 255:
                        # Kết thúc packet
                        yield bytes(pkt)
                        pkt = b""

        try:
            pkt_gen = _parse_ogg_packets(proc2.stdout)
            # 2 packet đầu là Opus ID header và Comment header — bỏ qua
            skip = 0
            for opus_pkt in pkt_gen:
                if self._audio_stop:
                    break
                if skip < 2:
                    skip += 1
                    continue
                if not opus_pkt:
                    continue
                q.put(opus_pkt, block=True, timeout=5.0)
            q.put(None, block=True, timeout=5.0)
        except Exception as e:
            HELLXlog("PRODUCER", f"{e}")
            try: q.put(None, block=False)
            except Exception: pass
        finally:
            proc2.terminate()
            proc2.wait()

    async def _voice_udp_loop(self, ip: str, port: int, raw_file: str = ""):
        """Async consumer: đọc từ queue, build packet, sendto UDP với timing chính xác.

        Kiến trúc 3 tầng:
          producer_thread  → audio_q (opus frames)
          timing_thread    → audio_q → send_q  (sleep 20ms chính xác ở OS thread)
          async loop       → send_q → udp.sendto  (gửi không block event loop)
        """
        import socket as _sock
        import struct as _st
        import time as _time
        import threading as _threading
        import queue as _q

        udp_port = getattr(self, '_voice_udp_port', None) or (port + 2)
        HELLXlog("VOICE", f"UDP: using port {udp_port}")

        if not raw_file or not _OPUSLIB_OK:
            HELLXlog("VOICE", "UDP: No raw_file or ffmpeg libopus missing")
            return

        import random as _rnd
        f3_val   = getattr(self, '_voice_f3', 0x13e6) & 0xFFFFFFFF
        ssrc_int = getattr(self, '_voice_ssrc_int', 0) or f3_val
        ts_val   = _rnd.randint(0, 0xFFFFFFFF)
        seq      = _rnd.randint(0, 0xFFFF)
        HELLXlog("VOICE", f"UDP: ssrc=0x{ssrc_int:08x} f3=0x{f3_val:08x}")

        # mic_open packet
        _mic_seq = _rnd.randint(0, 0xFFFFFF)
        _mic_ts  = _rnd.randint(0, 0xFFFFFFFF)
        mic_ba   = bytearray([0x71])
        mic_ba  += (_mic_seq & 0xFFFFFF).to_bytes(3, "big")
        mic_ba  += struct.pack(">I", _mic_ts & 0xFFFFFFFF)
        mic_ba  += struct.pack(">I", ssrc_int & 0xFFFFFFFF)
        mic_ba  += struct.pack(">I", 0x00000311)
        mic_ba  += bytes.fromhex("0000010720210000286e4672656520466972650000000000000000000000")
        mic_ba  += bytes.fromhex("e7877176")
        mic_open = bytes(mic_ba)

        _udp_sock_owned = False
        udp = getattr(self, '_voice_udp_sock', None)
        if udp is None:
            udp = _sock.socket(_sock.AF_INET, _sock.SOCK_DGRAM)
            udp.setblocking(False)
            _udp_sock_owned = True

        loop = asyncio.get_event_loop()
        _ack_ev = asyncio.Event()

        async def _udp_rx():
            try:
                while True:
                    pkt, _ = await loop.sock_recvfrom(udp, 4096)
                    _ack_ev.set()
            except Exception:
                pass

        udp_rx_task = asyncio.create_task(_udp_rx())
        for _retry in range(3):
            try: udp.sendto(mic_open, (ip, udp_port))
            except Exception as e:
                HELLXlog("VOICE", f"UDP: mic_open failed: {e}"); break
            try:
                await asyncio.wait_for(_ack_ev.wait(), timeout=1.0)
                break
            except asyncio.TimeoutError:
                HELLXlog("VOICE", f"UDP: mic_open retry {_retry+1}/3")

        # Khởi động producer thread
        # maxsize=800 (~16s buffer) để tránh đói khi ffmpeg chậm
        audio_q = _q.Queue(maxsize=800)
        self._audio_q    = audio_q
        self._audio_stop = False
        prod_th = _threading.Thread(target=self._producer_thread, args=(raw_file, audio_q), daemon=True)
        prod_th.start()
        self._producer_th = prod_th

        # Chờ 200 frames (~4s) trước khi bắt đầu — jitter buffer đủ lớn
        while audio_q.qsize() < 200 and not self._audio_stop:
            await asyncio.sleep(0.02)
        HELLXlog("VOICE", f"UDP: buffer {audio_q.qsize()} frames → start stream to {ip}:{udp_port}")

        FRAME_SAMPLES  = 960
        FRAME_DURATION = 0.020
        sent           = 0

        # seq/ts_val được dùng chung giữa timing_thread và async loop
        # → dùng list để có thể mutate từ bên trong closure
        _seq   = [seq]
        _ts    = [ts_val]

        def _build_pkt(opus_data: bytes) -> bytes:
            hdr = _st.pack('>2sHIII',
                           b'\x41\x6f',
                           _seq[0] & 0xFFFF,
                           _ts[0] & 0xFFFFFFFF,
                           ssrc_int & 0xFFFFFFFF,
                           f3_val & 0xFFFFFFFF)
            _seq[0] = (_seq[0] + 1) & 0xFFFF
            _ts[0]  = (_ts[0] + FRAME_SAMPLES) & 0xFFFFFFFF
            return hdr + opus_data

        # send_q: timing_thread đẩy packet đã build sẵn vào đây
        # maxsize=50 (~1s): đủ nhỏ để async loop luôn lấy được frame mới nhất
        send_q   = _q.Queue(maxsize=50)
        _stop_ev = _threading.Event()

        def _timing_thread():
            """OS thread: đảm bảo 20ms/frame chính xác.
            Dùng select() + busy-spin hybrid thay time.sleep() thuần:
            - time.sleep() trên Linux VPS bị scheduler jitter ±3–8ms → gây lắp nhắp
            - select([], [], [], timeout) đi qua pselect6 syscall, chính xác hơn ~3–5x
            - busy-spin 1ms cuối để đạt độ chính xác sub-ms
            """
            import select as _sel
            next_t = _time.perf_counter()
            while not _stop_ev.is_set() and not self._audio_stop:
                try:
                    opus = audio_q.get(timeout=0.1)
                except _q.Empty:
                    # Stall: reset timeline tránh burst catch-up
                    next_t = _time.perf_counter()
                    continue
                if opus is None:
                    try: send_q.put(None, timeout=1.0)
                    except Exception: pass
                    break
                pkt = _build_pkt(opus)
                try: send_q.put(pkt, timeout=0.5)
                except _q.Full:
                    pass   # consumer chưa kịp lấy → bỏ frame này, không stall producer
                next_t += FRAME_DURATION
                sl = next_t - _time.perf_counter()
                if sl > 0.002:
                    # select() chính xác hơn sleep() trên Linux kernel
                    _sel.select([], [], [], sl - 0.001)
                    # busy-spin 1ms cuối để đúng timing sub-ms
                    while _time.perf_counter() < next_t:
                        pass
                elif sl <= -0.050:
                    # Trễ tích lũy quá nhiều → reset, tránh burst gửi nhiều frame liên tiếp
                    next_t = _time.perf_counter() + FRAME_DURATION

        # _send_ev: timing_thread signal mỗi khi có packet sẵn trong send_q
        # → async loop dùng await thay run_in_executor, không tạo thread pool overhead
        _send_ev = asyncio.Event()

        def _timing_thread_signal():
            """Wrapper: sau khi put vào send_q, signal event loop từ OS thread."""
            loop.call_soon_threadsafe(_send_ev.set)

        # Patch timing_thread để gọi signal sau mỗi put
        _orig_timing = _timing_thread

        def _timing_thread():
            import select as _sel
            next_t = _time.perf_counter()
            while not _stop_ev.is_set() and not self._audio_stop:
                try:
                    opus = audio_q.get(timeout=0.1)
                except _q.Empty:
                    next_t = _time.perf_counter()
                    continue
                if opus is None:
                    try: send_q.put(None, timeout=1.0)
                    except Exception: pass
                    loop.call_soon_threadsafe(_send_ev.set)
                    break
                pkt = _build_pkt(opus)
                try:
                    send_q.put(pkt, timeout=0.5)
                    loop.call_soon_threadsafe(_send_ev.set)   # ← signal event loop
                except _q.Full:
                    pass
                next_t += FRAME_DURATION
                sl = next_t - _time.perf_counter()
                if sl > 0.002:
                    _sel.select([], [], [], sl - 0.001)
                    while _time.perf_counter() < next_t:
                        pass
                elif sl <= -0.050:
                    next_t = _time.perf_counter() + FRAME_DURATION

        timing_th = _threading.Thread(target=_timing_thread, daemon=True)
        timing_th.start()

        try:
            while not self._audio_stop:
                # Dùng Event thay run_in_executor:
                # - không tạo thread pool task mỗi 20ms
                # - không bị tranh chấp với emote writer.drain()
                # - event loop free để xử lý task khác giữa các frame
                try:
                    await asyncio.wait_for(_send_ev.wait(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                _send_ev.clear()
                # Drain toàn bộ send_q trong 1 lần wake (burst drain)
                while True:
                    try:
                        pkt = send_q.get_nowait()
                    except _q.Empty:
                        break
                    if pkt is None:
                        HELLXlog("VOICE", f"UDP: hết nhạc ({sent} frames)")
                        self._audio_stop = True
                        break
                    try:
                        udp.sendto(pkt, (ip, udp_port))
                    except OSError as e:
                        HELLXlog("VOICE", f"UDP: sock err: {e}")
                    sent += 1
                    self._track_current_frame = sent
                if self._audio_stop:
                    break

        except asyncio.CancelledError:
            self._stopped_frame = sent
            HELLXlog("VOICE", f"UDP: cancelled tại frame {sent}")
        except Exception as e:
            HELLXlog("VOICE", f"UDP: error: {e}")
        finally:
            _stop_ev.set()
            self._audio_stop = True
            udp_rx_task.cancel()
            if _udp_sock_owned:
                udp.close()
            HELLXlog("VOICE", "UDP: loop ended")


# ---------------------------------------------------------------------------
# Telegram bot
# ---------------------------------------------------------------------------

async def run_telegram_bot(token: str, active_bots: list):
    import aiohttp
    import subprocess
    import json as _json

    offset           = 0
    url              = f"https://api.telegram.org/bot{token}/getUpdates"
    send_url         = f"https://api.telegram.org/bot{token}/sendMessage"
    search_cache     = {}     # chat_id -> list of {"title", "channel", "url", "duration"}
    maintenance_mode = False  # Chế độ bảo trì — chỉ admin dùng được
    ADMIN_ID         = 1804489430

    HELLXlog("TELEGRAM", "Polling started...")

    delete_url = f"https://api.telegram.org/bot{token}/deleteMessage"
    edit_url   = f"https://api.telegram.org/bot{token}/editMessageText"
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

    async def send(chat_id, text, user_msg_id=None, edit_msg_id=None):
        """Gửi hoặc edit tin nhắn HTML.
        - edit_msg_id: edit tin nhắn đó thay vì gửi mới (dùng cho waiting→result).
        - user_msg_id: tự xóa cả 2 sau AUTO_DELETE_SECONDS.
        """
        async with aiohttp.ClientSession() as s:
            if edit_msg_id:
                resp = await s.post(edit_url, json={
                    "chat_id": chat_id,
                    "message_id": edit_msg_id,
                    "text": text,
                    "parse_mode": "HTML",
                })
                await resp.json()
                bot_msg_id = edit_msg_id
            else:
                resp = await s.post(send_url, json={
                    "chat_id": chat_id,
                    "text": text,
                    "parse_mode": "HTML",
                })
                sent = await resp.json()
                bot_msg_id = sent.get("result", {}).get("message_id")
        if bot_msg_id:
            ids_to_delete = [bot_msg_id]
            if user_msg_id:
                ids_to_delete.append(user_msg_id)
            asyncio.create_task(_delete_after(chat_id, *ids_to_delete))
        return bot_msg_id

    async def send_waiting(chat_id):
        """Gửi tin '⏳ Please Waiting...' và trả về message_id để edit sau."""
        async with aiohttp.ClientSession() as s:
            resp = await s.post(send_url, json={
                "chat_id": chat_id,
                "text": "⏳ <b>Please Waiting...</b>",
                "parse_mode": "HTML",
            })
            sent = await resp.json()
        return sent.get("result", {}).get("message_id")

    # Gán notify callback cho tất cả bot — dùng để gửi thông báo Telegram
    # khi có sự kiện đặc biệt (vd: chủ team thoát) mà không cần pass token vào class
    for _b in active_bots:
        _b._telegram_notify_cb = send

    async def _skip_pending_updates():
        """Bỏ qua toàn bộ update tồn đọng, trả về offset mới nhất.
        Gọi mỗi lần (re)connect — kể cả sau khi bot offline rồi online lại —
        để tránh xử lý lại lệnh cũ.
        """
        nonlocal offset
        try:
            async with aiohttp.ClientSession() as _s:
                async with _s.get(
                    f"{url}?offset=-1&timeout=0",
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as _r:
                    _d = await _r.json(content_type=None)
                    _results = _d.get("result", [])
                    if _results:
                        offset = _results[-1]["update_id"] + 1
                        HELLXlog("TELEGRAM", f"Skipped pending updates → offset={offset}")
                    else:
                        HELLXlog("TELEGRAM", "No pending updates to skip")
        except Exception as _e:
            HELLXlog("TELEGRAM", f"skip_pending failed (non-critical): {_e}")

    # Bỏ qua toàn bộ update cũ khi khởi động lần đầu
    await _skip_pending_updates()

    while True:
        try:
          # Mỗi lần reconnect (session mới) → skip lại để tránh replay tin nhắn cũ
          await _skip_pending_updates()
          async with aiohttp.ClientSession() as session:
            while True:
                try:
                    async with session.get(
                        f"{url}?offset={offset}&timeout=30", timeout=aiohttp.ClientTimeout(total=35)
                    ) as resp:
                        if resp.status != 200:
                            HELLXlog("TELEGRAM", f"Poll HTTP {resp.status} — retry in 3s")
                            await asyncio.sleep(3)
                            continue
                        try:
                            data = await resp.json(content_type=None)
                        except Exception as je:
                            HELLXlog("TELEGRAM", f"JSON parse error: {je} — skip")
                            await asyncio.sleep(2)
                            continue

                    for result in data.get("result", []):
                        offset      = result["update_id"] + 1
                        msg         = result.get("message", {})
                        text        = msg.get("text", "").strip()
                        chat_id     = msg.get("chat", {}).get("id")
                        user_msg_id = msg.get("message_id")
                        if not text or not chat_id:
                            continue
                        HELLXlog("TELEGRAM", f"CMD: {text!r} from chat_id={chat_id}")

                        is_admin = (chat_id == ADMIN_ID)

                        # ── /maintenance (chỉ admin) ─────────────────────────
                        if text.lower() in ("/maintenance on", "/maintenance off"):
                            if not is_admin:
                                await send(chat_id, "🚫 <b>Bạn không có quyền dùng lệnh này.</b>", user_msg_id=user_msg_id)
                                continue
                            maintenance_mode = text.lower().endswith("on")
                            if maintenance_mode:
                                await send(chat_id,
                                    "🔧 <b>Chế độ bảo trì đã BẬT.</b>\n"
                                    "└ Tất cả người dùng không thể dùng bot cho đến khi tắt.\n"
                                    "└ Tắt: <code>/maintenance off</code>",
                                    user_msg_id=user_msg_id)
                            else:
                                await send(chat_id,
                                    "✅ <b>Chế độ bảo trì đã TẮT.</b>\n"
                                    "└ Người dùng có thể sử dụng bot bình thường.",
                                    user_msg_id=user_msg_id)
                            continue

                        # ── Chặn toàn bộ lệnh nếu đang bảo trì (trừ admin) ──
                        if maintenance_mode and not is_admin:
                            await send(chat_id,
                                "🔧 <b>Bot đang bảo trì!</b>\n"
                                "└ Vui lòng thử lại sau ít phút.",
                                user_msg_id=user_msg_id)
                            continue

                        # ── /start ──────────────────────────────────────────
                        if text == "/start":
                            await send(chat_id,
                                "┌─────────────────────────┐\n"
                                "│  🤖  <b>TCP BOT — Free Fire</b>  │\n"
                                "└─────────────────────────┘\n\n"
                                "🎵 <b>PHÁT NHẠC</b>\n"
                                "├ <code>/search [tên bài]</code> — tìm NhacCuaTui\n"
                                "├ <code>/search yt [tên]</code> — tìm YouTube\n"
                                "├ <code>/join [code]</code> — vào team\n"
                                "├ <code>/join [code] [số]</code> — vào team + phát bài\n"
                                "├ <code>/play [tên/URL]</code> — thêm hàng chờ\n"
                                "├ <code>/skip</code> — bỏ bài đang phát\n"
                                "├ <code>/queue</code> — xem hàng chờ\n"
                                "└ <code>/clear</code> — xóa toàn bộ hàng chờ\n\n"
                                "💃 <b>NHẢY EMOTE</b>\n"
                                "└ <code>/pb [team_code]</code> — bot vào team nhảy liên tục\n\n"
                                "🚀 <b>START MATCH</b>\n"
                                "├ <code>/sp [team_code]</code> — Spam Tìm Trận (ST, TC Rank Huyền Thoại 5* trở lại)\n"
                                "└ <code>/5 [uid_game]</code> — tạo đội 5 người\n\n"
                                "👻 <b>GHOST</b>\n"
                                "└ <code>/js [team_code] [tên]</code> — ghost teamcodei\n\n"
                                "🔧 <b>QUẢN LÝ</b>\n"
                                "├ <code>/leave</code> — bot rời team\n"
                                "└ <code>/status</code> — xem trạng thái tất cả bot\n\n"
                                "🎮 <b>LỆNH TRONG GAME</b> <i>(chat tổ đội)</i>\n"
                                "<code>!play</code>  <code>!skip</code>  <code>!queue</code>  <code>!clear</code>  <code>!volume [0-200]</code>  <code>!leave</code>",
                                user_msg_id=user_msg_id
                            )

                        # ── /search ─────────────────────────────────────────
                        # Prefix: "yt " → YouTube, "ct " → NhacCuaTui, không prefix → NCT
                        elif text.lower().startswith("/search"):
                            raw_query = text[7:].strip()
                            if not raw_query:
                                await send(chat_id,
                                    "❌ <b>Thiếu từ khoá tìm kiếm!</b>\n\n"
                                    "📖 <b>Cú pháp:</b>\n"
                                    "├ <code>/search [tên bài]</code> — tìm NhacCuaTui\n"
                                    "├ <code>/search ct [tên bài]</code> — tìm NhacCuaTui\n"
                                    "└ <code>/search yt [tên bài]</code> — tìm YouTube",
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
                                await send(chat_id, "❌ <b>Thiếu tên bài!</b>\n└ Nhập tên bài sau prefix.", user_msg_id=user_msg_id)
                                continue

                            wait_mid = await send_waiting(chat_id)

                            try:
                                if force_source == "youtube":
                                    results = await _youtube_search_api(query, limit=10)
                                else:
                                    results = await _nct_search(query, limit=5)
                                    if not results:
                                        await send(chat_id,
                                            f"😔 <b>NhacCuaTui</b> không có kết quả cho:\n"
                                            f"└ <i>{query}</i>\n\n"
                                            f"💡 <b>Thử tìm trên YouTube:</b>\n"
                                            f"└ <code>/search yt {query}</code>",
                                            user_msg_id=user_msg_id, edit_msg_id=wait_mid)
                                        continue

                                if not results:
                                    await send(chat_id, "😔 <b>Không tìm thấy kết quả nào.</b>\n└ Hãy thử từ khoá khác.", user_msg_id=user_msg_id, edit_msg_id=wait_mid)
                                    continue

                                search_cache[chat_id] = results
                                source_tag = "🎵 <b>NhacCuaTui</b>" if results[0].get("source") == "nct" else "▶️ <b>YouTube</b>"
                                lines = [
                                    f"┌── {source_tag} ──",
                                    f"│ 🔎 Kết quả: <i>{query}</i>",
                                    f"└──────────────────\n",
                                ]
                                for i, r in enumerate(results, 1):
                                    artist = r.get("artist") or r.get("channel") or "Unknown"
                                    lines.append(f"<b>{i}.</b> {r['title']}\n    <i>🎤 {artist}</i>  <code>⏱ {r['duration']}</code>")

                                # Kiểm tra user có bot đang active không
                                # → nếu có, thêm shortcut /play [số]; nếu không, hướng dẫn /join
                                user_active_bot = next(
                                    (b for b in active_bots
                                     if b._telegram_chat_id == chat_id
                                     and b.voice_region_id
                                     and b._session_id is not None),
                                    None
                                )
                                if user_active_bot:
                                    lines.append(f"\n▶️ <b>Bot của bạn đang trong team!</b>\n└ Phát ngay: <code>/play [số thứ tự]</code>  •  VD: <code>/play 1</code>")
                                else:
                                    lines.append(f"\n▶️ <b>Phát nhạc:</b> <code>/join [team_code] [số thứ tự]</code>\n└ VD: <code>/join ABCD123 1</code>")
                                await send(chat_id, "\n".join(lines), user_msg_id=user_msg_id, edit_msg_id=wait_mid)

                            except Exception as e:
                                HELLXlog("SEARCH", f"Lỗi: {e}")
                                await send(chat_id, f"❌ <b>Tìm kiếm thất bại!</b>\n└ <code>{e}</code>", user_msg_id=user_msg_id, edit_msg_id=wait_mid)

                        # ── /join ────────────────────────────────────────────
                        elif text.lower().startswith("/join"):
                            if not any(b._online for b in active_bots):
                                await send(chat_id, "❌ <b>Không có bot nào Online!</b>\n└ Hệ thống đang khởi động lại, thử lại sau ít phút.", user_msg_id=user_msg_id)
                                continue
                            parts = text.split()
                            # /join CODE           → join only (no play)
                            # /join CODE 2         → join + play bài số 2 từ search_cache
                            # /join CODE https://… → join + play URL trực tiếp
                            if len(parts) < 2:
                                await send(chat_id,
                                    "❌ <b>Thiếu team code!</b>\n\n"
                                    "📖 <b>Cú pháp:</b>\n"
                                    "├ <code>/join [team_code]</code>\n"
                                    "└ <code>/join [team_code] [số thứ tự]</code>",
                                    user_msg_id=user_msg_id)
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
                                            await send(chat_id,
                                                "❌ <b>Số thứ tự không hợp lệ!</b>\n"
                                                "└ Hãy dùng <code>/search</code> để tìm bài trước.",
                                                user_msg_id=user_msg_id)
                                            continue
                                        play_url   = cached[idx]["url"]
                                        play_title = cached[idx]["title"]
                                    except ValueError:
                                        await send(chat_id,
                                            "❌ <b>Tham số không hợp lệ!</b>\n"
                                            "└ Phải là <b>số thứ tự</b> (1, 2, 3…) hoặc <b>URL</b>.",
                                            user_msg_id=user_msg_id)
                                        continue

                            # Tìm bot rảnh — check cả _joining tránh spam /join
                            # Bot "rảnh" = không trong team, không đang join, và _telegram_chat_id=None
                            # (đảm bảo bot không bị chia sẻ giữa 2 người dùng)
                            wait_mid = await send_waiting(chat_id)
                            count = 0
                            import uuid as _uuid_tg
                            for bot in active_bots:
                                if bot.writer and not bot.writer.transport.is_closing():
                                    if (not bot.voice_region_id
                                            and not bot._joining
                                            and bot._telegram_chat_id is None):
                                        try:
                                            # Gán session trước khi gửi packet
                                            # → tránh race condition nếu server confirm cực nhanh
                                            _sid = str(_uuid_tg.uuid4())
                                            bot._telegram_chat_id = chat_id
                                            bot._session_id       = _sid
                                            bot._joining          = True
                                            join_pkt = HELLXbuild_join_sq(int(bot.uid), code, bot.key, bot.iv)
                                            bot.writer.write(join_pkt)
                                            await bot.writer.drain()
                                            count += 1

                                            async def _check(b=bot, c=chat_id, tc=code, url=play_url, title=play_title, umid=user_msg_id, wmid=wait_mid, sid=_sid):
                                                b._join_success = False
                                                # Poll 6s thay vì sleep cứng 3s
                                                for _i in range(12):
                                                    await asyncio.sleep(0.5)
                                                    if b._join_success:
                                                        break
                                                b._joining = False
                                                # Nếu join thất bại, giải phóng bot
                                                if not b._join_success:
                                                    if b._session_id == sid:
                                                        b._telegram_chat_id = None
                                                        b._session_id       = None
                                                if b._join_success:
                                                    if url:
                                                        await send(c,
                                                            f"✅ <b>Đã vào team thành công!</b>\n"
                                                            f"├ Mã: <code>{tc}</code>\n"
                                                            f"└ 🎵 Đang tải: <i>{title[:60]}</i>",
                                                            user_msg_id=umid, edit_msg_id=wmid)
                                                        asyncio.create_task(b._play_from_url(url, title))
                                                    else:
                                                        await send(c,
                                                            f"✅ <b>Đã vào team thành công!</b>\n"
                                                            f"├ Mã: <code>{tc}</code>\n"
                                                            f"└ 💬 Chat <code>!play [bài hát]</code> trong game để phát nhạc.\n"
                                                            f"└ ⏱ Bot tự rời sau <b>10 giây</b> nếu không dùng.",
                                                            user_msg_id=umid, edit_msg_id=wmid)

                                                        # ── Auto-leave sau 10s nếu không phát gì ──
                                                        async def _auto_leave_idle(b=b, c=c, tc=tc, sid=sid):
                                                            await asyncio.sleep(10)
                                                            # Chỉ leave nếu bot vẫn thuộc session này VÀ không có bài nào đang phát/chờ
                                                            if b._session_id != sid:
                                                                return   # session đã thay đổi (bot đã được tái dùng)
                                                            is_playing = b._udp_task and not b._udp_task.done()
                                                            has_queue  = not b._track_queue.empty()
                                                            if b.voice_region_id and not is_playing and not has_queue:
                                                                HELLXlog("TEAM", f"Auto-leave idle uid={b.uid} tc={tc}")
                                                                asyncio.create_task(b._leave_team())
                                                                await send(c,
                                                                    f"🚪 <b>Bot tự rời team do không hoạt động!</b>\n"
                                                                    f"└ Mã: <code>{tc}</code>")

                                                        asyncio.create_task(_auto_leave_idle())
                                                else:
                                                    await send(c,
                                                        f"❌ <b>Vào team thất bại!</b>\n"
                                                        f"└ Team code không hợp lệ: <code>{tc}</code>",
                                                        user_msg_id=umid, edit_msg_id=wmid)

                                            asyncio.create_task(_check())
                                            break   # một bot mỗi lần join

                                        except Exception as ex:
                                            HELLXlog("TEAM", f"Join failed uid={bot.uid}: {ex}")
                                            # Giải phóng bot nếu exception khi gửi packet
                                            if bot._telegram_chat_id == chat_id and bot._session_id == _sid:
                                                bot._telegram_chat_id = None
                                                bot._session_id       = None
                                                bot._joining          = False

                            if count > 0:
                                pass  # waiting msg đã gửi, _check() sẽ edit thành kết quả
                            else:
                                my_joining = [b for b in active_bots if b._joining and b._telegram_chat_id == chat_id]
                                my_busy    = [b for b in active_bots if b.voice_region_id and b._telegram_chat_id == chat_id]
                                others_busy = [b for b in active_bots if b._telegram_chat_id is not None and b._telegram_chat_id != chat_id]
                                all_busy   = [b for b in active_bots if b.voice_region_id or b._joining or b._telegram_chat_id is not None]
                                if my_joining:
                                    await send(chat_id, "⏳ <b>Bot của bạn đang xử lý lệnh trước.</b>\n└ Chờ vài giây rồi thử lại.", user_msg_id=user_msg_id, edit_msg_id=wait_mid)
                                elif my_busy:
                                    await send(chat_id, "❌ <b>Bot của bạn đang trong team!</b>\n└ Dùng <code>/leave</code> trước.", user_msg_id=user_msg_id, edit_msg_id=wait_mid)
                                elif len(others_busy) == len(active_bots):
                                    await send(chat_id, "❌ <b>Không có bot rảnh!</b>\n└ Tất cả bot đang được người khác sử dụng.\n└ Thử lại sau ít phút.", user_msg_id=user_msg_id, edit_msg_id=wait_mid)
                                elif all_busy:
                                    await send(chat_id, "❌ <b>Không có bot rảnh!</b>\n└ Tất cả bot đang được người khác sử dụng.", user_msg_id=user_msg_id, edit_msg_id=wait_mid)
                                else:
                                    await send(chat_id, "❌ <b>Không có bot rảnh!</b>\n└ Dùng <code>/status</code> để kiểm tra.", user_msg_id=user_msg_id, edit_msg_id=wait_mid)

                        # ── /pb TeamCode (join team + nhảy emote, không cần nhạc) ──
                        elif text.lower().startswith("/pb"):
                            if not any(b._online for b in active_bots):
                                await send(chat_id, "❌ <b>Không có bot nào Online!</b>\n└ Hệ thống đang khởi động lại, thử lại sau ít phút.", user_msg_id=user_msg_id)
                                continue
                            parts = text.split()
                            if len(parts) < 2:
                                await send(chat_id,
                                    "❌ <b>Thiếu team code!</b>\n\n"
                                    "📖 <b>Cú pháp:</b> <code>/pb [team_code]</code>\n"
                                    "└ 💃 Bot vào team và nhảy emote liên tục, không cần nhạc.",
                                    user_msg_id=user_msg_id)
                                continue

                            pb_code = parts[1]

                            # Tìm bot rảnh — phải check cả _joining và _telegram_chat_id
                            # để đảm bảo không chia sẻ bot giữa 2 người dùng khác nhau
                            wait_mid = await send_waiting(chat_id)
                            pb_count = 0
                            for bot in active_bots:
                                if bot.writer and not bot.writer.transport.is_closing():
                                    if (not bot.voice_region_id
                                            and not bot._joining
                                            and bot._telegram_chat_id is None):
                                        try:
                                            # Gán session trước khi gửi packet (tránh race condition)
                                            _sid_pb = str(_uuid_tg.uuid4())
                                            bot._telegram_chat_id  = chat_id
                                            bot._session_id        = _sid_pb
                                            bot._telegram_notify_cb = send  # đảm bảo luôn mới nhất
                                            bot._joining           = True
                                            join_pkt = HELLXbuild_join_sq(int(bot.uid), pb_code, bot.key, bot.iv)
                                            bot.writer.write(join_pkt)
                                            await bot.writer.drain()
                                            pb_count += 1

                                            async def _check_pb(b=bot, c=chat_id, tc=pb_code, umid=user_msg_id, wmid=wait_mid, sid=_sid_pb):
                                                b._join_success = False
                                                # Chờ tối đa 6s để nhận join confirm từ server
                                                # (trước là 3s — dễ miss nếu server lag)
                                                for _i in range(12):
                                                    await asyncio.sleep(0.5)
                                                    if b._join_success:
                                                        break
                                                b._joining = False
                                                # Nếu join thất bại, giải phóng bot
                                                if not b._join_success:
                                                    if b._session_id == sid:
                                                        b._telegram_chat_id = None
                                                        b._session_id       = None
                                                HELLXlog("PB", f"_check_pb: uid={b.uid} join_success={b._join_success} voice_region={b.voice_region_id}")
                                                if b._join_success:
                                                    b._pb_mode = True
                                                    if b._pb_emote_task and not b._pb_emote_task.done():
                                                        b._pb_emote_task.cancel()
                                                    # Gửi welcome msg vào chat team (chạy song song với emote loop)
                                                    asyncio.create_task(b._pb_send_welcome())
                                                    b._pb_emote_task = asyncio.create_task(b._pb_emote_loop())
                                                    await send(c,
                                                        f"💃 <b>Đã vào team — đang nhảy emote!</b>\n"
                                                        f"├ Mã: <code>{tc}</code>\n"
                                                        f"├ 📨 Đã gửi tin chào vào chat tổ đội\n"
                                                        f"├ ⚠️ Bot tự rời nếu chủ team thoát\n"
                                                        f"└ 🛑 Dùng <code>/leave</code> để dừng",
                                                        user_msg_id=umid, edit_msg_id=wmid)
                                                else:
                                                    await send(c,
                                                        f"❌ <b>Vào team thất bại!</b>\n"
                                                        f"└ Team code không hợp lệ: <code>{tc}</code>",
                                                        user_msg_id=umid, edit_msg_id=wmid)

                                            asyncio.create_task(_check_pb())
                                            break  # một bot mỗi lần join

                                        except Exception as ex:
                                            HELLXlog("TEAM", f"PB join failed uid={bot.uid}: {ex}")

                            if pb_count > 0:
                                pass  # waiting msg đã gửi, _check_pb() sẽ edit thành kết quả
                            else:
                                my_joining = [b for b in active_bots if b._joining and b._telegram_chat_id == chat_id]
                                my_busy    = [b for b in active_bots if b.voice_region_id and b._telegram_chat_id == chat_id]
                                all_busy   = [b for b in active_bots if b.voice_region_id or b._joining]
                                if my_joining:
                                    await send(chat_id, "⏳ <b>Bot của bạn đang xử lý lệnh trước.</b>\n└ Chờ vài giây rồi thử lại.", user_msg_id=user_msg_id, edit_msg_id=wait_mid)
                                elif my_busy:
                                    await send(chat_id, "❌ <b>Bot của bạn đang trong team!</b>\n└ Dùng <code>/leave</code> trước.", user_msg_id=user_msg_id, edit_msg_id=wait_mid)
                                elif all_busy:
                                    await send(chat_id, "❌ <b>Không có bot rảnh!</b>\n└ Tất cả bot đang được người khác sử dụng.", user_msg_id=user_msg_id, edit_msg_id=wait_mid)
                                else:
                                    await send(chat_id, "❌ <b>Không có bot rảnh!</b>\n└ Dùng <code>/status</code> để kiểm tra.", user_msg_id=user_msg_id, edit_msg_id=wait_mid)

                        # ── /sp TeamCode (gọi API start match) ──────────────────
                        elif text.lower().startswith("/sp"):
                            tc = text[3:].strip()
                            if not tc:
                                await send(chat_id,
                                    "❌ <b>Thiếu team code!</b>\n"
                                    "└ <b>Cú pháp:</b> <code>/sp [team_code]</code>",
                                    user_msg_id=user_msg_id)
                                continue
                            # Phản hồi ngay, không chờ API
                            await send(chat_id,
                                f"✅ <b>Success StartMatch</b>\n"
                                f"└ Target : <code>{tc}</code>",
                                user_msg_id=user_msg_id)
                            # Gọi API trong background — chỉ log lỗi nếu fail
                            async def _call_start_api(team_code=tc, cid=chat_id):
                                try:
                                    async with aiohttp.ClientSession() as _s:
                                        async with _s.get(
                                            f"http://de28.spaceify.eu:26110/joins?tc={team_code}",
                                            timeout=aiohttp.ClientTimeout(total=10)
                                        ) as _r:
                                            if _r.status != 200:
                                                body = await _r.text()
                                                await send(cid,
                                                    f"⚠️ <b>API lỗi!</b>\n"
                                                    f"├ Status: <code>{_r.status}</code>\n"
                                                    f"└ <code>{body[:200]}</code>"
                                                )
                                except asyncio.TimeoutError:
                                    await send(cid, "⚠️ <b>API timeout!</b>\n└ Server không phản hồi sau 10s.")
                                except Exception as _ex:
                                    await send(cid, f"⚠️ <b>API lỗi!</b>\n└ <code>{_ex}</code>")
                            asyncio.create_task(_call_start_api())

                        # ── /5 UID (gọi API tạo đội 5 người) ────────────────────
                        elif text.lower().startswith("/5"):
                            uid_arg = text[2:].strip()
                            if not uid_arg:
                                await send(chat_id,
                                    "❌ <b>Thiếu UID game!</b>\n"
                                    "└ <b>Cú pháp:</b> <code>/5 [uid_game]</code>",
                                    user_msg_id=user_msg_id)
                                continue
                            # Gọi API và chờ kết quả để phản hồi đẹp
                            wait_msg = await asyncio.get_event_loop().run_in_executor(None, lambda: None)
                            try:
                                async with aiohttp.ClientSession() as _s:
                                    async with _s.get(
                                        f"http://de28.spaceify.eu:26110/inv?uid={uid_arg}",
                                        timeout=aiohttp.ClientTimeout(total=15)
                                    ) as _r:
                                        status_code = _r.status
                                        body = await _r.text()
                                        if status_code == 200:
                                            await send(chat_id,
                                                f"✅ <b>Tạo đội 5 thành công!</b>\n"
                                                f"├ UID : <code>{uid_arg}</code>\n"
                                                f"└ Phản hồi : <code>{body[:200]}</code>",
                                                user_msg_id=user_msg_id)
                                        else:
                                            await send(chat_id,
                                                f"❌ <b>Tạo đội thất bại!</b>\n"
                                                f"├ UID : <code>{uid_arg}</code>\n"
                                                f"├ Status : <code>{status_code}</code>\n"
                                                f"└ <code>{body[:200]}</code>",
                                                user_msg_id=user_msg_id)
                            except asyncio.TimeoutError:
                                await send(chat_id,
                                    f"⚠️ <b>API timeout!</b>\n"
                                    f"└ Server không phản hồi sau 15s.",
                                    user_msg_id=user_msg_id)
                            except Exception as _ex:
                                await send(chat_id,
                                    f"⚠️ <b>API lỗi!</b>\n└ <code>{_ex}</code>",
                                    user_msg_id=user_msg_id)

                        # ── /js Teamcode [name] (ghost join + đổi tên) ───────────
                        elif text.lower().startswith("/js"):
                            js_args = text[3:].strip()
                            js_parts = js_args.split(None, 1)
                            if not js_parts:
                                await send(chat_id,
                                    "❌ <b>Thiếu tham số!</b>\n"
                                    "└ <b>Cú pháp:</b> <code>/js [team_code] [tên]</code>",
                                    user_msg_id=user_msg_id)
                                continue
                            js_tc = js_parts[0].strip()

                            # Tên mặc định khi người dùng không nhập
                            _DEFAULT_GHOST_NAME = "[b][c][1E90FF]Tiktok: [00FFFF]@jocker_90a"

                            # ── Thêm mã màu ngẫu nhiên cho từng ký tự tên ghost ──
                            # Nếu tên đã có mã màu ([…]) thì giữ nguyên, không đè thêm.
                            def _ghost_colorize(name: str) -> str:
                                import random as _r
                                if "[" in name:
                                    return name
                                _PALETTE = [
                                    "FF0000", "FF4400", "FF8800", "FFCC00", "FFFF00",
                                    "CCFF00", "88FF00", "00FF44", "00FF88", "00FFFF",
                                    "00CCFF", "0088FF", "4400FF", "8800FF", "CC00FF",
                                    "FF00FF", "FF0088", "FF0044", "FF4466", "FF8844",
                                    "FFD700", "FF69B4", "00FA9A", "1E90FF", "7FFF00",
                                    "FF6347", "FF1493", "9370DB", "40E0D0", "ADFF2F",
                                ]
                                color = _r.choice(_PALETTE)
                                return f"[{color}]{name}"

                            if len(js_parts) >= 2:
                                # Người dùng nhập tên → áp màu ngẫu nhiên (nếu chưa có màu)
                                js_name_raw = js_parts[1].strip()
                                js_name     = _ghost_colorize(js_name_raw)
                            else:
                                # Không nhập tên → dùng tên mặc định (đã có màu sẵn)
                                js_name_raw = "Tiktok: @jocker_90a"
                                js_name     = _DEFAULT_GHOST_NAME

                            # Tìm 2 bot rảnh (dùng 2 bot / lần ghost)
                            js_bots = []
                            for _b in active_bots:
                                if _b.writer and not _b.writer.transport.is_closing() \
                                        and not _b.voice_region_id and not _b._joining:
                                    js_bots.append(_b)
                                    if len(js_bots) >= 2:
                                        break
                            if not js_bots:
                                await send(chat_id,
                                    "❌ <b>Không có bot rảnh!</b>\n"
                                    "└ Dùng <code>/status</code> để kiểm tra.",
                                    user_msg_id=user_msg_id)
                                continue

                            wait_mid = await send_waiting(chat_id)

                            async def _do_ghost(bots=js_bots, tc=js_tc, ghost_name=js_name, cid=chat_id, umid=user_msg_id, wmid=wait_mid):
                                try:
                                    # ── Helpers build packet theo đúng Chu_Giang.py ─
                                    def _enc_vr(n):
                                        """Encode varint."""
                                        if n < 0:
                                            return b""
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

                                    def _create_variant(fn, val):
                                        return _enc_vr((fn << 3) | 0) + _enc_vr(val)

                                    def _create_length(fn, val):
                                        ev = val.encode() if isinstance(val, str) else val
                                        return _enc_vr((fn << 3) | 2) + _enc_vr(len(ev)) + ev

                                    def _create_proto(fields):
                                        pkt = bytearray()
                                        for f, v in fields.items():
                                            if isinstance(v, dict):
                                                nested = _create_proto(v)
                                                pkt += _create_length(f, nested)
                                            elif isinstance(v, int):
                                                pkt += _create_variant(f, v)
                                            elif isinstance(v, (str, bytes)):
                                                pkt += _create_length(f, v)
                                        return bytes(pkt)

                                    def _make_ghost_pkt(uid, name, sq, key, iv):
                                        from Crypto.Cipher import AES as _AES
                                        from Crypto.Util.Padding import pad as _pad
                                        fields = {
                                            1: 61,
                                            2: {
                                                1: int(uid),
                                                2: {
                                                    1: int(uid),
                                                    2: 1159,
                                                    3: str(name),
                                                    5: 12,
                                                    6: 9999999,
                                                    7: 1,
                                                    8: {2: 1, 3: 1},
                                                    9: 3,
                                                },
                                                3: str(sq),
                                            }
                                        }
                                        proto_hex = _create_proto(fields).hex()
                                        enc_bytes = _AES.new(key, _AES.MODE_CBC, iv).encrypt(
                                            _pad(bytes.fromhex(proto_hex), 16)
                                        )
                                        enc_hex = enc_bytes.hex()
                                        pkt_len = len(enc_hex) // 2
                                        return bytes.fromhex("0515") + pkt_len.to_bytes(4, "big") + enc_bytes

                                    def _make_exit_pkt(key, iv):
                                        from Crypto.Cipher import AES as _AES
                                        from Crypto.Util.Padding import pad as _pad
                                        fields = {1: 7, 2: {1: "000000"}}
                                        proto_hex = _create_proto(fields).hex()
                                        enc_bytes = _AES.new(key, _AES.MODE_CBC, iv).encrypt(
                                            _pad(bytes.fromhex(proto_hex), 16)
                                        )
                                        pkt_len = len(enc_bytes)
                                        return bytes.fromhex("0515") + pkt_len.to_bytes(4, "big") + enc_bytes

                                    # FIX: Reset trạng thái team sau ghost để /status hiển thị đúng.
                                    # _make_exit_pkt chỉ gửi packet thô, không gọi _leave_team()
                                    # → voice_region_id không được clear → status vẫn báo "Trong team".
                                    def _reset_ghost_state(b):
                                        b.voice_region_id    = None
                                        b.chat_region_id     = None
                                        b.team_owner_uid     = None
                                        b.voice_chat_key     = None
                                        b._joining           = False
                                        b._telegram_chat_id  = None   # isolation: release ownership
                                        b._session_id        = None
                                        for _gt in (b._voice_connect_task, b._udp_task,
                                                    b._voice_ping_task, b._progress_task):
                                            if _gt and not _gt.done():
                                                _gt.cancel()
                                        b._voice_connect_task = None
                                        b._voice_ip    = None
                                        b._voice_port  = None
                                        if b._chat_writer:
                                            try:
                                                b._chat_writer.close()
                                            except Exception:
                                                pass
                                            b._chat_writer = None
                                            b._chat_reader = None
                                        if b._chat_keepalive_task and not b._chat_keepalive_task.done():
                                            b._chat_keepalive_task.cancel()
                                            b._chat_keepalive_task = None

                                    # ── Bước 1: Bot đầu join team để lấy owner + squad ──
                                    bot = bots[0]
                                    bot.team_owner_uid = None
                                    bot.voice_chat_key = None
                                    join_pkt = HELLXbuild_join_sq(int(bot.uid), tc, bot.key, bot.iv)
                                    bot.writer.write(join_pkt)
                                    await bot.writer.drain()

                                    owner_uid = None
                                    squad_key = None
                                    deadline = asyncio.get_event_loop().time() + 8.0
                                    while asyncio.get_event_loop().time() < deadline:
                                        await asyncio.sleep(0.3)
                                        if bot.team_owner_uid and bot.voice_chat_key:
                                            owner_uid = int(bot.team_owner_uid)
                                            squad_key = str(bot.voice_chat_key)
                                            break

                                    if not owner_uid or not squad_key:
                                        await send(cid,
                                            f"❌ <b>Ghost thất bại!</b>\n"
                                            f"└ Không lấy được owner/squad từ <code>{tc}</code>",
                                            user_msg_id=umid, edit_msg_id=wmid)
                                        try:
                                            bot.writer.write(_make_exit_pkt(bot.key, bot.iv))
                                            await bot.writer.drain()
                                        except Exception:
                                            pass
                                        # FIX: Reset state khi ghost thất bại
                                        _reset_ghost_state(bot)
                                        return

                                    # ── Bước 2: Bot đầu exit team ───────────────────
                                    try:
                                        exit_pkt = _make_exit_pkt(bot.key, bot.iv)
                                        bot.writer.write(exit_pkt)
                                        await bot.writer.drain()
                                    except Exception:
                                        pass
                                    await asyncio.sleep(0.5)

                                    # ── Bước 3+4: Tất cả bots gửi ghost (x1 + x2) ──
                                    success_count = 0
                                    for _gb in bots:
                                        try:
                                            # Ghost lần 1
                                            _gp1 = _make_ghost_pkt(owner_uid, ghost_name, squad_key, _gb.key, _gb.iv)
                                            _gb.writer.write(_gp1)
                                            await _gb.writer.drain()
                                            HELLXlog("GHOST", f"ghost x1 bot={_gb.uid} owner={owner_uid} name={ghost_name} tc={tc}")
                                            await asyncio.sleep(0.3)
                                            # Ghost lần 2 (đảm bảo)
                                            _gp2 = _make_ghost_pkt(owner_uid, ghost_name, squad_key, _gb.key, _gb.iv)
                                            _gb.writer.write(_gp2)
                                            await _gb.writer.drain()
                                            HELLXlog("GHOST", f"ghost x2 bot={_gb.uid} owner={owner_uid}")
                                            success_count += 1
                                        except Exception as _ge:
                                            HELLXlog("GHOST", f"bot={_gb.uid} ghost lỗi: {_ge}")
                                        finally:
                                            _reset_ghost_state(_gb)

                                    import re as _re2
                                    _display_name = _re2.sub(r"\[[0-9A-Fa-f]{6}\]", "", ghost_name)
                                    await send(cid,
                                        f"✅ <b>Ghost thành công!</b>\n"
                                        f"├ Team  : <code>{tc}</code>\n"
                                        f"├ Owner : <code>{owner_uid}</code>\n"
                                        f"├ Bot   : <b>{success_count}/{len(bots)}</b> bot đã ghost\n"
                                        f"└ Tên   : <code>{_display_name}</code>",
                                        user_msg_id=umid, edit_msg_id=wmid)

                                except Exception as _ex:
                                    HELLXlog("GHOST", f"Error: {_ex}")
                                    await send(cid,
                                        f"❌ <b>Ghost lỗi!</b>\n└ <code>{_ex}</code>",
                                        user_msg_id=umid, edit_msg_id=wmid)
                                    # FIX: Reset state khi ghost exception (bot có thể đã join)
                                    for _gb in bots:
                                        _reset_ghost_state(_gb)

                            asyncio.create_task(_do_ghost())

                        # ── /play (direct, dành cho bot đang trong team của user) ──
                        # Hỗ trợ:
                        #   /play [tên bài hoặc URL]  → tìm kiếm hoặc phát trực tiếp
                        #   /play [số]                → phát bài số [số] từ search_cache
                        elif text.lower().startswith("/play"):
                            query = text[5:].strip()
                            if not query:
                                await send(chat_id,
                                    "❌ <b>Thiếu tên bài hoặc URL!</b>\n\n"
                                    "📖 <b>Cú pháp:</b>\n"
                                    "├ <code>/play [tên bài]</code>\n"
                                    "├ <code>/play [URL]</code>\n"
                                    "└ <code>/play [số thứ tự]</code> — từ kết quả <code>/search</code>",
                                    user_msg_id=user_msg_id)
                                continue

                            # Tìm bot đang trong team của chat_id này, đúng session
                            target_bot = None
                            for b in active_bots:
                                if (b._telegram_chat_id == chat_id
                                        and b.voice_region_id
                                        and b._session_id is not None):
                                    target_bot = b
                                    break

                            if not target_bot:
                                await send(chat_id,
                                    "❌ <b>Bot chưa vào team!</b>\n"
                                    "└ Dùng <code>/join [team_code]</code> trước.",
                                    user_msg_id=user_msg_id)
                                continue

                            # Phân tích: nếu là số thứ tự → lấy từ search_cache của chat_id này
                            play_url   = None
                            play_title = ""
                            try:
                                idx = int(query) - 1
                                cached = search_cache.get(chat_id, [])
                                if idx < 0 or idx >= len(cached):
                                    await send(chat_id,
                                        f"❌ <b>Số thứ tự không hợp lệ!</b>\n"
                                        f"└ Kết quả <code>/search</code> của bạn có <b>{len(cached)}</b> bài.",
                                        user_msg_id=user_msg_id)
                                    continue
                                play_url   = cached[idx]["url"]
                                play_title = cached[idx]["title"]
                            except ValueError:
                                # Không phải số → query thông thường
                                play_url   = None
                                play_title = query

                            if play_url:
                                asyncio.create_task(target_bot._play_from_url(play_url, play_title))
                                await send(chat_id,
                                    f"🎵 <b>Đã thêm vào hàng chờ!</b>\n└ <i>{play_title[:60]}</i>",
                                    user_msg_id=user_msg_id)
                            else:
                                asyncio.create_task(target_bot._play_from_query(query))
                                await send(chat_id,
                                    f"🎵 <b>Đã thêm vào hàng chờ!</b>\n└ <i>{query[:60]}</i>",
                                    user_msg_id=user_msg_id)

                        # ── /skip ─────────────────────────────────────────────
                        elif text.lower() == "/skip":
                            target_bot = None
                            for b in active_bots:
                                if (b._telegram_chat_id == chat_id
                                        and b.voice_region_id
                                        and b._session_id is not None):
                                    target_bot = b
                                    break
                            if target_bot:
                                await target_bot._skip_current()
                                nxt = target_bot._track_queue.qsize()
                                msg = (
                                    f"⏭ <b>Đã bỏ bài!</b>\n└ 📋 Còn <b>{nxt}</b> bài trong hàng chờ."
                                    if nxt > 0 else
                                    "⏭ <b>Đã bỏ bài!</b>\n└ 📋 Hàng chờ trống."
                                )
                                await send(chat_id, msg, user_msg_id=user_msg_id)
                            else:
                                await send(chat_id, "❌ <b>Không có bot nào đang phát nhạc cho bạn.</b>", user_msg_id=user_msg_id)

                        # ── /queue ────────────────────────────────────────────
                        elif text.lower() in ("/queue", "/q"):
                            target_bot = None
                            for b in active_bots:
                                if (b._telegram_chat_id == chat_id
                                        and b.voice_region_id
                                        and b._session_id is not None):
                                    target_bot = b
                                    break
                            if target_bot:
                                is_playing = target_bot._udp_task and not target_bot._udp_task.done()
                                items = list(target_bot._track_queue._queue) if hasattr(target_bot._track_queue, '_queue') else []
                                if not is_playing and not items:
                                    await send(chat_id, "📋 <b>Hàng chờ trống.</b>", user_msg_id=user_msg_id)
                                else:
                                    lines = ["📋 <b>Hàng chờ nhạc:</b>\n"]
                                    if is_playing and target_bot._track_title:
                                        e_s = int(target_bot._track_current_frame * 0.02)
                                        t_s = int(target_bot._track_total_frames * 0.02)
                                        lines.append(f"▶️ <b>{target_bot._track_title[:45]}</b>\n    <code>{e_s//60}:{e_s%60:02d} / {t_s//60}:{t_s%60:02d}</code>")
                                    for i, (q, t) in enumerate(items, 1):
                                        label = t or q
                                        lines.append(f"<b>{i}.</b> {label[:50]}")
                                    await send(chat_id, "\n".join(lines), user_msg_id=user_msg_id)
                            else:
                                await send(chat_id, "❌ <b>Không có bot nào đang hoạt động cho bạn.</b>", user_msg_id=user_msg_id)

                        # ── /clear ────────────────────────────────────────────
                        elif text.lower() == "/clear":
                            target_bot = None
                            for b in active_bots:
                                if (b._telegram_chat_id == chat_id
                                        and b.voice_region_id
                                        and b._session_id is not None):
                                    target_bot = b
                                    break
                            if target_bot:
                                await target_bot._clear_queue()
                                await send(chat_id, "🗑 <b>Đã xóa hàng chờ và dừng phát nhạc.</b>", user_msg_id=user_msg_id)
                            else:
                                await send(chat_id, "❌ <b>Không có bot nào đang hoạt động cho bạn.</b>", user_msg_id=user_msg_id)

                        # ── /leave ───────────────────────────────────────────
                        elif text.lower() == "/leave":
                            left = 0
                            for bot in active_bots:
                                if (bot._telegram_chat_id == chat_id
                                        and bot.voice_region_id
                                        and bot._session_id is not None):
                                    asyncio.create_task(bot._leave_team())
                                    left += 1
                            await send(chat_id,
                                f"✅ <b>Đã ra lệnh rời team!</b>\n└ <b>{left}</b> bot đang rời…"
                                if left else
                                "ℹ️ <b>Không có bot nào của bạn trong team.</b>",
                                user_msg_id=user_msg_id)

                        # ── /status ──────────────────────────────────────────
                        elif text.lower() == "/status":
                            lines = ["📊 <b>Trạng thái bot:</b>\n"]
                            for i, bot in enumerate(active_bots, 1):
                                in_team  = "✅ <b>Trong team</b>" if bot.voice_region_id else "💤 Rảnh"
                                playing  = "🎵 Đang phát" if (bot._udp_task and not bot._udp_task.done()) else "⏸ Không phát"
                                q_size   = bot._track_queue.qsize()
                                pb_flag  = "  💃 <i>PB mode</i>" if bot._pb_mode else ""
                                lines.append(f"<b>{i}.</b> <code>{bot.uid}</code>  {in_team}  {playing}{pb_flag}")
                                if bot._track_title:
                                    e_s = int(bot._track_current_frame * 0.02)
                                    t_s = int(bot._track_total_frames * 0.02)
                                    lines.append(f"    └ 🎵 <i>{bot._track_title[:40]}</i>  <code>{e_s//60}:{e_s%60:02d}/{t_s//60}:{t_s%60:02d}</code>")
                                if q_size > 0:
                                    lines.append(f"    └ 📋 Hàng chờ: <b>{q_size}</b> bài")
                            await send(chat_id, "\n".join(lines), user_msg_id=user_msg_id)

                except Exception as e:
                    import traceback as _tb
                    HELLXlog("TELEGRAM", f"Inner loop error: {type(e).__name__}: {e}")
                    HELLXlog("TELEGRAM", _tb.format_exc()[-400:])
                    await asyncio.sleep(5)

        except Exception as e:
            import traceback as _tb
            HELLXlog("TELEGRAM", f"Session error (sẽ tái tạo session): {type(e).__name__}: {e}")
            HELLXlog("TELEGRAM", _tb.format_exc()[-300:])
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
