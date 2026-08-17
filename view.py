"""
Telegram account manager / SMM bot.

Accounts live in the sessions/ folder (that folder is the single source of truth
for logins). MongoDB holds only client subscriptions, approved users and the
activity log — no session material.
"""

import asyncio
import contextlib
import logging
import re
import sys
import datetime
import random
import os
import json
import zipfile
import shutil
import sqlite3
import tempfile
import time
from html import escape as esc
from math import ceil
from typing import Dict, List, Optional, Tuple

from telethon import TelegramClient, events, Button, utils
from telethon.sessions import StringSession
from telethon.errors import (
    SessionPasswordNeededError,
    FloodWaitError,
    UserAlreadyParticipantError,
    InviteRequestSentError,
    MessageNotModifiedError,
)
from telethon.tl.functions.messages import (
    ImportChatInviteRequest,
    SendReactionRequest,
    GetMessagesViewsRequest,
)
from telethon.tl.functions.channels import (
    JoinChannelRequest, LeaveChannelRequest, GetFullChannelRequest,
)
from telethon.tl.functions.account import (
    UpdateStatusRequest,
    UpdateProfileRequest,
    GetPasswordRequest,
    UpdatePasswordSettingsRequest,
)
from telethon.tl.functions.photos import (
    UploadProfilePhotoRequest,
    DeletePhotosRequest,
)
from telethon.tl.types import (
    PeerChannel, ReactionEmoji,
    ChatReactionsSome,
    UpdateGroupCall, GroupCall,
    InputCheckPasswordSRP, PasswordKdfAlgoSHA256SHA256PBKDF2HMACSHA512iter100000SHA256ModPow,
)
from motor.motor_asyncio import AsyncIOMotorClient
from bson.objectid import ObjectId

# Optional so the bot still boots on a host without ffmpeg/ntgcalls; every
# live-audio path checks TGCALLS_OK first and reports the missing dependency
# instead of crashing at import time.
try:
    from pytgcalls import PyTgCalls, filters as pytgf
    from pytgcalls.types import MediaStream, AudioQuality, GroupCallConfig
    TGCALLS_OK = True
    TGCALLS_ERR = ""
except Exception as _e:
    PyTgCalls = None
    TGCALLS_OK = False
    TGCALLS_ERR = f"{type(_e).__name__}: {_e}"

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger("viewbot")

# Telethon logs "Got difference for channel ... updates" at INFO for every
# single update on every monitoring account. With a few dozen accounts sitting
# in hundreds of channels this is thousands of useless lines a minute — it
# buries the lines that actually matter (joins, flood waits, dead accounts) and
# makes bot.log grow by hundreds of MB a day. Keep Telethon at WARNING; our own
# "viewbot" logger stays at INFO.
for _noisy in ("telethon", "telethon.client.updates", "telethon.network",
               "telethon.network.mtprotosender", "pytgcalls", "ntgcalls",
               "pymongo", "asyncio"):
    logging.getLogger(_noisy).setLevel(logging.WARNING)

# ═══════════════════════ CONFIGURATION ═══════════════════════
API_ID = 21538384
API_HASH = "9b8e9b10a5c34b67054aceca02bf423e"
BOT_TOKEN = "8912703088:AAG1YBb91E3l0h6Uqdk0azztRpRSnwpYva0"
MONGO_URI = "mongodb+srv://avinash:avinash12@cluster0.wnwd1fv.mongodb.net/?appName=Cluster0"

OWNER_IDS = [8015937475]

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SESSIONS_DIR = os.path.join(BASE_DIR, "sessions")
TRASH_DIR = os.path.join(BASE_DIR, "sessions_trash")
os.makedirs(SESSIONS_DIR, exist_ok=True)
os.makedirs(os.path.join(BASE_DIR, "audio"), exist_ok=True)

# Every live audio stream costs an ffmpeg process, a WebRTC connection and a
# second MTProto socket. The usual 1024-descriptor default runs out partway
# through a large fleet, and the first casualty is Telethon's session sqlite —
# so accounts start dropping for a reason that looks nothing like the cause.
try:
    import resource
    _soft, _hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    if _soft < 65535:
        resource.setrlimit(resource.RLIMIT_NOFILE, (min(65535, _hard), _hard))
except Exception:
    pass

# A .session that is locked is almost always transient (journal recovery or a
# second process opening it). Never read a lock as "this account is dead".
SESSION_LOCK_RETRIES = 4
SESSION_LOCK_BACKOFF = 1.5

# Bounded fan-out. Joins are the flood-sensitive operation, so they stay slow.
JOIN_CONCURRENCY = 3
REACT_CONCURRENCY = 20
VIEW_CONCURRENCY = 50
# GetMessagesViewsRequest takes a list of ids — one call can cover many posts.
VIEW_BATCH_SIZE = 100
# Editing a progress message on every single operation is what earns the *bot*
# a FloodWait. Never edit more often than this.
PROGRESS_EDIT_INTERVAL = 3.0
# Only this many accounts carry the new-post listener; the rest would just
# deliver duplicates of the same update.
MONITOR_CLIENTS = 5
# How often to check monitored channels for a running live stream.
LIVE_POLL_SECONDS = 20
# Telegram drops a silent participant after ~60s, so re-assert well inside that.
LIVE_KEEPALIVE_SECONDS = 30
# The MP3 every account streams into a live call.
AUDIO_DIR = os.path.join(BASE_DIR, "audio")
LIVE_AUDIO_PATH = os.path.join(AUDIO_DIR, "live.mp3")
# ffmpeg restarts the file this many times. NOT -1: py-tgcalls filters argv
# against `ffmpeg -h full` and drops any token starting with "-", which would
# silently strip the "-1" and leave a broken bare "-stream_loop".
AUDIO_LOOP_COUNT = 999999
# Each PyTgCalls spawns its own thread pool; the default of 16 across 22
# accounts would be ~350 threads for work that is almost entirely I/O wait.
TGCALLS_WORKERS = 2
# Accounts to put into a call at once. One-at-a-time took ~3 minutes for 60
# accounts; the whole batch at once trips Telegram's join flood limit.
LIVE_JOIN_BATCH = 6
# Ceiling on simultaneous audio streams, owner-adjustable from the Audio menu
# (0 = no limit). This is only a comfort setting. The real protection is
# FD_HEADROOM below: each stream costs an ffmpeg process, a WebRTC connection and
# a second MTProto socket, and running the process out of file descriptors kills
# the *session* sqlite files too — taking every account offline, not just the
# live ones. That is what a raw "join with all 60" did before.
DEFAULT_LIVE_CAP = 25
LIVE_CAP = DEFAULT_LIVE_CAP
# Swap part of the fleet out of a call every N seconds (0 = never). A two-hour
# live otherwise parks the same 25 accounts in the same call for two hours,
# which is both the least natural pattern and the most load a single account can
# carry. Owner-adjustable from Audio > Rotation.
DEFAULT_LIVE_ROTATE = 1200
LIVE_ROTATE = DEFAULT_LIVE_ROTATE
# Share of the streaming accounts replaced on each rotation. Half keeps the
# participant count visibly steady while still retiring everyone over time.
LIVE_ROTATE_FRACTION = 0.5
# Accounts to pull into the channel per streaming slot when rotation is on;
# the extras sit outside the call as the pool to swap in from.
LIVE_POOL_MULT = 2
# Refuse to start a stream when fewer than this many descriptors are spare.
FD_HEADROOM = 200
# Telegram refuses a join once an account is in 500 channels/supergroups. Stop
# short of it so there is room for the odd manual join and for Go Live.
MAX_CHANNELS_PER_ACCOUNT = 450
KEEP_ALIVE_INTERVAL = 300
PER_PAGE = 8
# When a new account is added (phone login, string session or ZIP import) walk
# it into every active client channel in the background, and add it to those
# subscriptions' joined_accounts. Without this a new account is a member of
# nothing, so it never gets picked for a reaction, view or live and the client's
# joined-accounts count never moves.
AUTO_ONBOARD = True

# Global semaphore shared across ALL auto-react/view tasks.
# This prevents concurrent flood when many new posts arrive at the same time.
# A per-call semaphore multiplied by N simultaneous posts = N×REACT_CONCURRENCY
# concurrent requests, which is what causes the flood-wait storm.
_AUTO_REACT_SEM: Optional[asyncio.Semaphore] = None


def _get_auto_react_sem() -> asyncio.Semaphore:
    """Lazily create the shared semaphore on first use (inside a running loop)."""
    global _AUTO_REACT_SEM
    if _AUTO_REACT_SEM is None:
        _AUTO_REACT_SEM = asyncio.Semaphore(REACT_CONCURRENCY)
    return _AUTO_REACT_SEM


def utcnow() -> datetime.datetime:
    """Return the current UTC time as a naive datetime (compatible with MongoDB)."""
    return datetime.datetime.now(datetime.UTC).replace(tzinfo=None)


START_TIME = datetime.datetime.now()

task_states: Dict[int, dict] = {}
monitored_channels: set = set()
clients_with_monitor: set = set()
recent_messages: set = set()
active_calls: dict = {}
# account_key -> PyTgCalls instance (created lazily, one per account)
TGCALLS: Dict[str, object] = {}
# channel_id -> set of account keys currently streaming audio there
LIVE_AUDIO: Dict[int, set] = {}
# channel_id -> {"pool": [keys usable for this call], "target": how many should
# be streaming, "label": display name, "since": {key: joined at}, "rotated": ts}
LIVE_SESSIONS: Dict[int, dict] = {}
# account_key -> monotonic time of its last reaction/view, and how many requests
# it has in flight right now. Together these are what stops the fleet from
# leaning on the same sessions when many channels post at the same moment.
ACC_LAST_USED: Dict[str, float] = {}
ACC_INFLIGHT: Dict[str, int] = {}
# account_key -> when Telegram last said this account is frozen. Frozen accounts
# stay connected and stay in the folder, but they cannot join anything, so every
# picker skips them until the cooldown lapses and we retry once.
FROZEN_ACCOUNTS: Dict[str, float] = {}
FROZEN_RETRY_AFTER = 6 * 3600
# Last (channels, accounts) pair reported by setup_channel_monitors().
_LAST_MONITOR_STATE: Optional[Tuple[int, int]] = None
# str(peer) -> list of allowed emojis, or None when the channel allows any
ALLOWED_REACTIONS_CACHE: Dict[str, Optional[list]] = {}
# (account_key, peer_spec) -> resolved input entity
PEER_CACHE: Dict[Tuple[str, str], object] = {}


# ═══════════════════════ TEXT STYLING ═══════════════════════
_SMALL_CAPS = {
    "A": "ᴀ", "B": "ʙ", "C": "ᴄ", "D": "ᴅ", "E": "ᴇ", "F": "ғ", "G": "ɢ",
    "H": "ʜ", "I": "ɪ", "J": "ᴊ", "K": "ᴋ", "L": "ʟ", "M": "ᴍ", "N": "ɴ",
    "O": "ᴏ", "P": "ᴘ", "Q": "ǫ", "R": "ʀ", "S": "ꜱ", "T": "ᴛ", "U": "ᴜ",
    "V": "ᴠ", "W": "ᴡ", "X": "x", "Y": "ʏ", "Z": "ᴢ",
}


def S(text: str) -> str:
    """Style plain text into the bot's display font.

    Only ever call this on plain text. It rewrites every ASCII letter, so
    passing HTML through it would destroy the tags — build messages as
    ``f"{E_CROWN} {S('TITLE')}"`` instead of styling the whole string.
    """
    def word(m):
        w = m.group(0)
        return "".join(_SMALL_CAPS.get(c, c.lower()) for c in w.upper())

    return re.sub(r"[A-Za-z]+", word, text)


# Kept so older call sites keep working.
style_text = S


# ═══════════════════════ PREMIUM EMOJI ═══════════════════════
def ce(emoji_id: str, fallback: str) -> str:
    """A premium (custom) emoji. Non-premium clients see the fallback."""
    return f'<tg-emoji emoji-id="{emoji_id}">{fallback}</tg-emoji>'


E_CROWN   = ce("5039727497143387500", "👑"); E_STAR    = ce("5042176294222037888", "⭐")
E_FIRE    = ce("5389038097860144794", "🔥"); E_CHECK   = ce("5039844895779455925", "✅")
E_CROSS   = ce("5040042498634810056", "❌"); E_PHONE   = ce("5407025283456835913", "📱")
E_CHANNEL = ce("5041888071851705019", "📣"); E_CAL     = ce("5413879192267805083", "🗓")
E_CLOCK   = ce("6285240160120477644", "⏰"); E_WARN    = ce("5039665997506675838", "⚠️")
E_GREEN   = ce("5039928501612839813", "🟢"); E_RED     = ce("5042042652019655612", "🔴")
E_YELLOW  = ce("5339082633160703625", "🟡"); E_LOCK    = ce("5305609152704297298", "🔒")
E_ROCKET  = ce("5389057356493511934", "🚀"); E_BELL    = ce("5042111805288089118", "🔔")
E_GIFT    = ce("5039778134807806727", "🎁"); E_CHART   = ce("5042290883949495533", "📊")
E_PERSON  = ce("6165860934242798778", "👤"); E_REFRESH = ce("5041837837914211014", "🔄")
E_TRASH   = ce("5039614900280754969", "🗑"); E_SHIELD  = ce("5042328396193864923", "🛡")
E_PLUS    = ce("5039844895779455925", "➕"); E_PAGE    = ce("5042290883949495533", "📄")
E_SPARKLE = ce("5389038097860144794", "✨"); E_HEART   = ce("5040042498634810056", "💖")
E_TARGET  = ce("5041888071851705019", "🎯"); E_DIAMOND = ce("6285240160120477644", "💎")
E_EYE     = ce("6165860934242798778", "👁"); E_THUMB   = ce("5039844895779455925", "👍")

DIV = "━" * 21
DOT = "•"


def card(icon: str, title: str, lines: List[str], footer: Optional[str] = None) -> str:
    """One consistent message layout: icon + styled title, rule, body, rule."""
    out = [f"{icon} <b>{S(title)}</b>", f"<code>{DIV}</code>"]
    out += [l for l in lines if l is not None]
    if footer:
        out += [f"<code>{DIV}</code>", footer]
    return "\n".join(out)


def field(icon: str, label: str, value: str) -> str:
    return f"{icon} {S(label)}: <b>{value}</b>"


def bar(pct: int, width: int = 12) -> str:
    pct = max(0, min(100, int(pct)))
    filled = int(pct / 100 * width)
    return "▰" * filled + "▱" * (width - filled)


def uptime_str() -> str:
    mins = int((datetime.datetime.now() - START_TIME).total_seconds() / 60)
    if mins < 60:
        return f"{mins}m"
    if mins < 1440:
        return f"{mins // 60}h {mins % 60}m"
    return f"{mins // 1440}d {(mins % 1440) // 60}h"


# ═══════════════════════ BUTTONS ═══════════════════════
# Telethon's TL layer has no `style` / `icon_custom_emoji_id` on
# KeyboardButtonCallback (those are Bot-API-only fields), so a button cannot be
# coloured or carry a premium emoji here. Buttons therefore get a plain unicode
# icon plus the styled font, and `style=` is accepted and ignored so the call
# sites read the same as stream.py's — and so one edit here is enough if a
# future Telethon exposes the field.
_STYLE_ICON = {
    "primary": "",
    "success": "✅ ",
    "danger": "🚫 ",
    "warning": "⚠️ ",
}


def btn(text: str, data: str, style: Optional[str] = None, icon: str = "") -> Button:
    prefix = icon + " " if icon else _STYLE_ICON.get(style or "", "")
    return Button.inline(f"{prefix}{S(text)}", data.encode())


def kb_nav(target: str = "home") -> list:
    return [[btn("Back", target, icon="⬅️"), btn("Home", "home", icon="🏠")]]


def add_another_kb(same: str) -> list:
    """Buttons after a successful login: repeat the same method, or switch.

    `same` is the callback that started this login ("add_phone" or
    "add_string"), so the shortcut lands straight on the step just used
    instead of the method-picker.
    """
    other = "add_string" if same == "add_phone" else "add_phone"
    other_label = "String Session" if other == "add_string" else "Phone Login"
    other_icon = "🔑" if other == "add_string" else "📱"
    return [[btn("Add Another", same, icon="➕"),
             btn(other_label, other, icon=other_icon)],
            [btn("Accounts", "menu_accounts", icon="⬅️"),
             btn("Home", "home", icon="🏠")]]


def pager(page: int, total_pages: int, prefix: str, back: str = "home") -> list:
    rows = []
    nav = []
    if page > 1:
        nav.append(btn("Prev", f"{prefix}_page_{page - 1}", icon="◀️"))
    nav.append(Button.inline(f"📄 {page}/{total_pages}", b"noop"))
    if page < total_pages:
        nav.append(btn("Next", f"{prefix}_page_{page + 1}", icon="▶️"))
    if total_pages > 1:
        rows.append(nav)
    rows.append([btn("Back", back, icon="⬅️"), btn("Home", "home", icon="🏠")])
    return rows


def paginate(items: list, page: int, per_page: int = PER_PAGE) -> Tuple[list, int, int]:
    total_pages = max(1, ceil(len(items) / per_page))
    page = max(1, min(page, total_pages))
    start = (page - 1) * per_page
    return items[start:start + per_page], page, total_pages


# ═══════════════════════ DEVICE FINGERPRINTING ═══════════════════════
# A Telegram auth key is bound to the api_id that created it, and Telegram
# cross-checks the announced device signature against that api_id. Opening a
# session born in Telegram Desktop (api_id 4 / 2040) while announcing Telethon's
# default device string reads as a hijacked session and gets revoked within a
# day or two. So keep whatever the bundle's JSON says, and otherwise fall back
# to a profile that matches the api_id rather than to Telethon's defaults.
DESKTOP_API_IDS = {4, 2040, 2496}

DESKTOP_PROFILE = {
    "device_model": "Desktop",
    "system_version": "Windows 10",
    "app_version": "4.8.1 x64",
    "lang_code": "en",
    "system_lang_code": "en-US",
}
ANDROID_PROFILE = {
    "device_model": "Samsung SM-G973F",
    "system_version": "SDK 30",
    "app_version": "9.4.9 (3383)",
    "lang_code": "en",
    "system_lang_code": "en-US",
}

_META_ALIASES = {
    "api_id": ("api_id", "app_id", "apiId", "appId"),
    "api_hash": ("api_hash", "app_hash", "apiHash", "appHash"),
    "device_model": ("device_model", "device", "deviceModel"),
    "system_version": ("system_version", "sdk", "systemVersion", "system"),
    "app_version": ("app_version", "appVersion", "app"),
    "lang_code": ("lang_code", "lang", "langCode", "lang_pack"),
    "system_lang_code": ("system_lang_code", "system_lang_pack", "systemLangCode"),
    "user_id": ("user_id", "id", "userId"),
    "first_name": ("first_name", "firstName", "name"),
    "phone": ("phone", "phone_number", "phoneNumber"),
}


def meta_get(meta: dict, key: str):
    for alias in _META_ALIASES.get(key, (key,)):
        val = meta.get(alias)
        if val not in (None, ""):
            return val
    return None


def device_profile_for(api_id, meta: Optional[dict] = None) -> dict:
    """Device params for a session: the bundle's own values win, else match api_id."""
    try:
        api_id = int(api_id) if api_id else None
    except (TypeError, ValueError):
        api_id = None
    base = dict(DESKTOP_PROFILE if api_id in DESKTOP_API_IDS else ANDROID_PROFILE)
    if meta:
        for f in ("device_model", "system_version", "app_version",
                  "lang_code", "system_lang_code"):
            val = meta_get(meta, f)
            if val:
                base[f] = str(val)
    return base


def is_db_locked(err: Exception) -> bool:
    if isinstance(err, sqlite3.OperationalError):
        return True
    msg = str(err).lower()
    return "database is locked" in msg or "database table is locked" in msg


DEAD_ACCOUNT_MARKERS = (
    "USER_DEACTIVATED", "AUTH_KEY_UNREGISTERED", "SESSION_REVOKED",
    "USER_DEACTIVATED_BAN", "AUTH_KEY_DUPLICATED", "AUTH_KEY_INVALID",
    "SESSION_EXPIRED", "PHONE_NUMBER_BANNED",
)


FROZEN_ACCOUNT_MARKERS = (
    "FROZEN_METHOD_INVALID", "FROZENMETHODINVALID",
    "NOT AVAILABLE FOR FROZEN ACCOUNTS",
)


def is_frozen_account_error(err: Exception) -> bool:
    """True when Telegram refused the call because the account is frozen.

    A frozen account is still authorised — get_me() works, it stays connected —
    but every join/invite method returns FrozenMethodInvalidError. Left in the
    pool it is picked over and over and every Go Live / client join wastes a
    slot on it, which is exactly what the log showed. Treat it as unusable for
    membership work instead, without ever deleting the session.
    """
    msg = str(err).upper()
    return any(m in msg for m in FROZEN_ACCOUNT_MARKERS)


def is_dead_account_error(err: Exception) -> bool:
    """True only when Telegram itself says the account or key is gone.

    A lock, a timeout or a network blip is not evidence that an account is dead,
    and must never be treated as grounds for deleting anything.
    """
    if is_db_locked(err):
        return False
    msg = str(err).upper()
    return any(m in msg for m in DEAD_ACCOUNT_MARKERS)


# ═══════════════════════ MONGODB (clients only) ═══════════════════════
try:
    mongo_client = AsyncIOMotorClient(MONGO_URI, serverSelectionTimeoutMS=20000)
    mdb = mongo_client["tg_manager_bot"]
    col_history = mdb["history"]
    col_approved = mdb["approved_users"]
    col_clients = mdb["clients"]
    col_stats = mdb["bot_stats"]
    col_settings = mdb["settings"]
    logger.info("MongoDB client created")
except Exception as e:
    logger.error(f"MongoDB error: {e}")
    sys.exit(1)


# ═══════════════════════ ACCOUNT STORE (sessions folder) ═══════════════════════
class Account:
    """One logged-in account, backed by a .session file in sessions/."""

    __slots__ = ("key", "stem", "path", "phone", "user_id", "name",
                 "api_id", "api_hash", "device", "client", "state")

    def __init__(self, stem, path, api_id, api_hash, device):
        self.stem = stem
        self.path = path
        self.api_id = api_id
        self.api_hash = api_hash
        self.device = device
        self.key = stem
        self.phone = ""
        self.user_id = 0
        self.name = ""
        self.client: Optional[TelegramClient] = None
        self.state = "unknown"   # alive | dead | unknown

    @property
    def label(self) -> str:
        return self.phone or self.name or self.stem


# key -> Account (key is the phone when known, else id_<user_id>, else the stem)
ACCOUNTS: Dict[str, Account] = {}
# Sessions the folder holds that would not authorise. Never auto-deleted.
PROBLEM_SESSIONS: Dict[str, str] = {}


def acc_keys() -> List[str]:
    return [k for k, a in ACCOUNTS.items() if a.client is not None]


def acc_count() -> int:
    return len(acc_keys())


def acc_client(key: str) -> Optional[TelegramClient]:
    a = ACCOUNTS.get(key)
    return a.client if a else None


def mark_frozen(key: str):
    """Remember that Telegram refused a membership call for this account."""
    if key and key not in FROZEN_ACCOUNTS:
        logger.warning(f"{key}: account is frozen — skipping it for joins "
                       f"for the next {FROZEN_RETRY_AFTER // 3600}h")
    if key:
        FROZEN_ACCOUNTS[key] = time.monotonic()


def is_frozen(key: str) -> bool:
    """Is this account currently marked frozen (and still inside cooldown)?"""
    ts = FROZEN_ACCOUNTS.get(key)
    if ts is None:
        return False
    if time.monotonic() - ts > FROZEN_RETRY_AFTER:
        # Freezes do get lifted. Let it back into the pool and find out.
        FROZEN_ACCOUNTS.pop(key, None)
        return False
    return True


def joinable_keys(keys: Optional[List[str]] = None) -> List[str]:
    """Online accounts that can actually join something right now."""
    pool = acc_keys() if keys is None else keys
    return [k for k in pool if acc_client(k) and not is_frozen(k)]


def joinable_count() -> int:
    return len(joinable_keys())


def meta_path(session_path: str) -> str:
    return os.path.splitext(session_path)[0] + ".json"


def read_meta(session_path: str) -> dict:
    p = meta_path(session_path)
    if os.path.exists(p):
        try:
            with open(p, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                return data
        except Exception as e:
            logger.debug(f"meta unreadable {p}: {e}")
    return {}


def write_meta(session_path: str, **fields):
    p = meta_path(session_path)
    data = read_meta(session_path)
    data.update({k: v for k, v in fields.items() if v not in (None, "")})
    try:
        with open(p, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        logger.warning(f"Could not write {p}: {e}")


def creds_from_meta(meta: dict) -> Tuple[int, str]:
    api_id = meta_get(meta, "api_id")
    api_hash = meta_get(meta, "api_hash")
    if api_id and api_hash:
        try:
            return int(api_id), str(api_hash)
        except (TypeError, ValueError):
            pass
    return API_ID, API_HASH


def to_trash(session_path: str) -> int:
    """Move a session (and its sidecar) to sessions_trash instead of deleting it.

    Deleting is irreversible and a wrong "this is dead" call then costs a real
    account, so nothing here ever unlinks a file.
    """
    os.makedirs(TRASH_DIR, exist_ok=True)
    stamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    stem = os.path.splitext(os.path.basename(session_path))[0]
    moved = 0
    base = os.path.splitext(session_path)[0]
    for ext in (".session", ".session-journal", ".session-wal", ".session-shm", ".json"):
        src = base + ext
        if os.path.exists(src):
            try:
                os.replace(src, os.path.join(TRASH_DIR, f"{stem}_{stamp}{ext}"))
                moved += 1
            except Exception as e:
                logger.warning(f"trash {src}: {e}")
    return moved


def trash_entries() -> List[str]:
    if not os.path.isdir(TRASH_DIR):
        return []
    return sorted(f for f in os.listdir(TRASH_DIR) if f.endswith(".session"))


def restore_from_trash() -> int:
    """Move every trashed session back into sessions/."""
    restored = 0
    for f in trash_entries():
        stem = f[: -len(".session")]
        # strip the _YYYYmmdd_HHMMSS stamp to recover the original name
        orig = re.sub(r"_\d{8}_\d{6}$", "", stem)
        for ext in (".session", ".json"):
            src = os.path.join(TRASH_DIR, stem + ext)
            if not os.path.exists(src):
                continue
            dest = os.path.join(SESSIONS_DIR, orig + ext)
            n = 1
            while os.path.exists(dest):
                dest = os.path.join(SESSIONS_DIR, f"{orig}_{n}{ext}")
                n += 1
            try:
                os.replace(src, dest)
                if ext == ".session":
                    restored += 1
            except Exception as e:
                logger.warning(f"restore {src}: {e}")
    return restored


async def probe_and_register(session_path: str) -> Tuple[str, Optional[Account]]:
    """Open one session file and register it if it authorises.

    Returns ("alive"|"dead"|"unknown", account). A "dead" or "unknown" result
    never removes anything from disk — that is always an explicit admin action.
    """
    stem = os.path.splitext(os.path.basename(session_path))[0]
    meta = read_meta(session_path)
    api_id, api_hash = creds_from_meta(meta)
    device = device_profile_for(api_id, meta)
    acc = Account(stem, session_path, api_id, api_hash, device)

    for attempt in range(SESSION_LOCK_RETRIES):
        client = None
        try:
            client = TelegramClient(
                os.path.splitext(session_path)[0], api_id, api_hash, **device
            )
            await client.connect()
            if not await client.is_user_authorized():
                await client.disconnect()
                acc.state = "dead"
                PROBLEM_SESSIONS[stem] = "not authorised"
                return "dead", acc

            me = await client.get_me()
            acc.client = client
            acc.user_id = getattr(me, "id", 0) or 0
            acc.phone = f"+{me.phone}" if getattr(me, "phone", None) else ""
            acc.name = (getattr(me, "first_name", "") or "").strip() or "Account"
            acc.key = acc.phone or (f"id_{acc.user_id}" if acc.user_id else stem)
            acc.state = "alive"

            existing = ACCOUNTS.get(acc.key)
            if existing is not None and existing.client is not None:
                if existing.stem == stem:
                    # Same file re-probed during a reload — the live connection
                    # is already registered; just discard the new one quietly.
                    await client.disconnect()
                    PROBLEM_SESSIONS.pop(stem, None)
                    return "alive", existing
                else:
                    # Genuinely two different .session files for the same account.
                    # Keep the first one connected; flag the second as duplicate.
                    await client.disconnect()
                    PROBLEM_SESSIONS[stem] = f"duplicate of {existing.stem}"
                    return "unknown", acc

            ACCOUNTS[acc.key] = acc
            PROBLEM_SESSIONS.pop(stem, None)
            write_meta(session_path, api_id=api_id, api_hash=api_hash,
                       user_id=acc.user_id, phone=acc.phone,
                       first_name=acc.name, **device)
            return "alive", acc

        except Exception as e:
            if client is not None:
                try:
                    await client.disconnect()
                except Exception:
                    pass
            if is_db_locked(e) and attempt < SESSION_LOCK_RETRIES - 1:
                await asyncio.sleep(SESSION_LOCK_BACKOFF * (attempt + 1)
                                    + random.uniform(0, 0.5))
                continue
            if is_db_locked(e):
                logger.error(
                    f"{stem}: session file locked after {SESSION_LOCK_RETRIES} tries — "
                    f"another copy of this bot is probably running "
                    f"(check: pgrep -f view.py)"
                )
                PROBLEM_SESSIONS[stem] = "file locked"
                return "unknown", acc
            if is_dead_account_error(e):
                logger.warning(f"{stem}: {e}")
                PROBLEM_SESSIONS[stem] = str(e)[:60]
                return "dead", acc
            logger.error(f"{stem}: {type(e).__name__}: {e}")
            PROBLEM_SESSIONS[stem] = f"{type(e).__name__}"
            return "unknown", acc

    PROBLEM_SESSIONS[stem] = "unknown"
    return "unknown", acc


async def import_string_session(session_string: str, hint: str = "") -> Optional[Account]:
    """Turn a Telethon string session into a .session file in the folder.

    The folder is the only place logins are read from, so a pasted string is
    written out as a real session file rather than kept in memory or in the db.
    """
    client = None
    try:
        client = TelegramClient(StringSession(session_string), API_ID, API_HASH,
                                **ANDROID_PROFILE)
        await client.connect()
        if not await client.is_user_authorized():
            await client.disconnect()
            return None
        me = await client.get_me()
        phone = f"+{me.phone}" if getattr(me, "phone", None) else f"id_{me.id}"
        # copy the auth key out while still connected — one connection only
        auth_key = client.session.auth_key
        dc_id = client.session.dc_id
        server = client.session.server_address
        port = client.session.port
        await client.disconnect()
    except Exception as e:
        logger.error(f"string session rejected: {e}")
        if client is not None:
            try:
                await client.disconnect()
            except Exception:
                pass
        return None

    stem = re.sub(r"[^0-9A-Za-z_+-]", "", phone) or (hint or "account")
    dest = os.path.join(SESSIONS_DIR, stem + ".session")
    n = 1
    while os.path.exists(dest):
        dest = os.path.join(SESSIONS_DIR, f"{stem}_{n}.session")
        n += 1

    try:
        dst = TelegramClient(os.path.splitext(dest)[0], API_ID, API_HASH,
                             **ANDROID_PROFILE)
        dst.session.set_dc(dc_id, server, port)
        dst.session.auth_key = auth_key
        dst.session.save()
        await dst.disconnect()
    except Exception as e:
        logger.error(f"could not persist string session: {e}")
        return None

    write_meta(dest, api_id=API_ID, api_hash=API_HASH, phone=phone,
               **ANDROID_PROFILE)
    state, acc = await probe_and_register(dest)
    return acc if state == "alive" else None


async def load_all_sessions(progress=None) -> dict:
    """Load every account from the sessions folder. The folder is the truth."""
    logger.info("Loading accounts from sessions/ ...")
    files = sorted(
        os.path.join(SESSIONS_DIR, f)
        for f in os.listdir(SESSIONS_DIR)
        if f.endswith(".session")
    )
    logger.info(f"{len(files)} .session file(s) found")

    tally = {"alive": 0, "dead": 0, "unknown": 0}
    sem = asyncio.Semaphore(10)

    async def one(path):
        async with sem:
            state, _ = await probe_and_register(path)
            tally[state] += 1
            await asyncio.sleep(random.uniform(0.05, 0.25))

    for i in range(0, len(files), 25):
        await asyncio.gather(*(one(p) for p in files[i:i + 25]))
        if progress:
            await progress(min(i + 25, len(files)), len(files), tally)

    # string.txt is converted into real session files so nothing lives outside
    # the folder's own format.
    string_file = os.path.join(SESSIONS_DIR, "string.txt")
    if os.path.exists(string_file):
        with open(string_file, "r", encoding="utf-8") as f:
            lines = [l.strip() for l in f if l.strip()]
        logger.info(f"{len(lines)} string session(s) in string.txt")
        for line in lines:
            if await import_string_session(line):
                tally["alive"] += 1
            await asyncio.sleep(0.3)
        try:
            os.replace(string_file, string_file + ".imported")
        except Exception:
            pass

    logger.info(f"Accounts online: {acc_count()} "
                f"(dead {tally['dead']}, unclear {tally['unknown']})")
    return tally


async def disconnect_account(key: str):
    acc = ACCOUNTS.get(key)
    if not acc or not acc.client:
        return
    # Drop the audio bridge first — it holds a reference to this client, and a
    # stale PyTgCalls would keep being reused after a reload replaced the
    # underlying TelegramClient.
    for chat_id in list(LIVE_AUDIO):
        await stop_live_audio(key, chat_id)
    TGCALLS.pop(key, None)
    try:
        await acc.client.disconnect()
    except Exception:
        pass
    acc.client = None
    clients_with_monitor.discard(key)


async def keep_alive_task():
    """One sweeper for every account, instead of a task per account."""
    while True:
        await asyncio.sleep(KEEP_ALIVE_INTERVAL)
        keys = acc_keys()
        random.shuffle(keys)
        for key in keys:
            client = acc_client(key)
            if not client:
                continue
            try:
                await client(UpdateStatusRequest(offline=False))
            except Exception as e:
                if is_dead_account_error(e):
                    logger.warning(f"{key} reported dead by Telegram: {e}")
                    ACCOUNTS[key].state = "dead"
            await asyncio.sleep(random.uniform(0.2, 0.8))


# ═══════════════════════ HISTORY / APPROVAL ═══════════════════════
async def log_activity(phone, action, target, status):
    try:
        await col_history.insert_one({
            "phone": phone, "action": action, "target": target,
            "status": str(status)[:120],
            "timestamp": utcnow(),
        })
    except Exception:
        pass


async def get_today_joins() -> int:
    try:
        today = datetime.datetime.now().date()
        return await col_history.count_documents({
            "action": {"$in": ["JOIN", "CLIENT_JOIN"]},
            "timestamp": {"$gte": datetime.datetime.combine(today, datetime.time.min)},
        })
    except Exception:
        return 0


async def increment_stats(reactions: int = 0, views: int = 0):
    """Increment reaction/view counters — global total and today's bucket."""
    today = datetime.datetime.now().date().isoformat()
    try:
        if reactions:
            await col_stats.update_one(
                {"_id": "global"},
                {"$inc": {"total_reactions": reactions}},
                upsert=True,
            )
            await col_stats.update_one(
                {"_id": f"day_{today}"},
                {"$inc": {"reactions": reactions}},
                upsert=True,
            )
        if views:
            await col_stats.update_one(
                {"_id": "global"},
                {"$inc": {"total_views": views}},
                upsert=True,
            )
            await col_stats.update_one(
                {"_id": f"day_{today}"},
                {"$inc": {"views": views}},
                upsert=True,
            )
    except Exception:
        pass


async def get_reaction_view_stats() -> Tuple[int, int, int, int]:
    """Return (total_reactions, total_views, today_reactions, today_views)."""
    today = datetime.datetime.now().date().isoformat()
    try:
        g = await col_stats.find_one({"_id": "global"}) or {}
        d = await col_stats.find_one({"_id": f"day_{today}"}) or {}
        return (
            int(g.get("total_reactions", 0)),
            int(g.get("total_views", 0)),
            int(d.get("reactions", 0)),
            int(d.get("views", 0)),
        )
    except Exception:
        return 0, 0, 0, 0


async def approve_user(user_id: int, by: int) -> bool:
    try:
        await col_approved.update_one(
            {"user_id": user_id},
            {"$set": {"user_id": user_id, "approved_by": by,
                      "approved_at": utcnow(),
                      "is_approved": True}},
            upsert=True,
        )
        return True
    except Exception:
        return False


async def unapprove_user(user_id: int) -> bool:
    try:
        r = await col_approved.delete_one({"user_id": user_id})
        return r.deleted_count > 0
    except Exception:
        return False


async def is_user_approved(user_id: int) -> bool:
    if user_id in OWNER_IDS:
        return True
    try:
        return await col_approved.find_one(
            {"user_id": user_id, "is_approved": True}) is not None
    except Exception:
        return False


async def get_approved_users() -> List[dict]:
    out = []
    try:
        async for d in col_approved.find({"is_approved": True}):
            out.append(d)
    except Exception:
        pass
    return out


# ═══════════════════════ CLIENT SUBSCRIPTIONS (MongoDB) ═══════════════════════
def norm_channel_id(cid) -> Optional[int]:
    """Normalise a stored channel id to Telegram's marked (-100…) form.

    Older rows saved ``entity.id`` (unmarked), while ``event.chat_id`` is marked;
    comparing the two directly is why monitoring silently matched nothing.
    """
    if cid in (None, ""):
        return None
    try:
        cid = int(cid)
    except (TypeError, ValueError):
        return None
    if cid < 0:
        return cid
    return int(f"-100{cid}")


def channel_peer(cid: int) -> PeerChannel:
    raw, _ = utils.resolve_id(norm_channel_id(cid))
    return PeerChannel(raw)


async def create_client_subscription(data: dict) -> dict:
    now = utcnow()
    doc = {
        "client_user_id": data["client_user_id"],
        "client_name": data.get("client_name", "Unknown"),
        "channel_link": data["channel_link"],
        "channel_id": norm_channel_id(data.get("channel_id")),
        "channel_username": data.get("channel_username"),
        "channel_type": data.get("channel_type", "public"),
        "accounts_count": data["accounts_count"],
        "reactions_per_post": data["reactions_per_post"],
        "views_per_post": data["views_per_post"],
        "livestream_accounts": data.get("livestream_accounts", 0),
        "subscription_days": data["subscription_days"],
        "created_at": now,
        "expires_at": now + datetime.timedelta(days=data["subscription_days"]),
        "status": "active",
        "joined_accounts": data.get("joined_accounts", []),
        "last_reminder_sent": None,
        "total_posts_processed": 0,
        "updated_at": now,
    }
    res = await col_clients.insert_one(doc)
    doc["_id"] = res.inserted_id
    return doc


async def get_client_subscriptions(status=None) -> List[dict]:
    q = {"status": status} if status else {}
    out = []
    async for d in col_clients.find(q).sort("created_at", -1):
        out.append(d)
    return out


async def get_subs_for_user(user_id: int) -> List[dict]:
    """Every subscription belonging to one client, newest first.

    A client can hold several: one per channel, each with its own package,
    length and expiry. They are separate documents, so pausing, upgrading or
    deleting one never touches the others.
    """
    out = []
    async for d in col_clients.find({"client_user_id": int(user_id)}) \
            .sort("created_at", -1):
        out.append(d)
    return out


async def client_index() -> List[dict]:
    """One row per client: user id, name, channel count, soonest expiry."""
    by_user: Dict[int, dict] = {}
    async for d in col_clients.find().sort("created_at", -1):
        uid = int(d.get("client_user_id", 0) or 0)
        row = by_user.setdefault(uid, {"user_id": uid,
                                       "name": d.get("client_name", "Unknown"),
                                       "channels": 0, "active": 0,
                                       "soonest": None})
        row["channels"] += 1
        if d.get("status") == "active":
            row["active"] += 1
            exp = d.get("expires_at")
            if exp and (row["soonest"] is None or exp < row["soonest"]):
                row["soonest"] = exp
    return sorted(by_user.values(), key=lambda r: -r["channels"])


async def sub_exists_for_channel(user_id: int, channel_link: str,
                                 channel_username=None) -> bool:
    """Has this client already got a live subscription on this channel?"""
    q = {"client_user_id": int(user_id),
         "status": {"$ne": "expired"},
         "$or": [{"channel_link": channel_link}]}
    if channel_username:
        q["$or"].append({"channel_username": channel_username})
    return await col_clients.count_documents(q) > 0


async def account_load() -> Dict[str, int]:
    """How many live subscriptions each account is already serving.

    Every account starts at 0; the ones that never got picked stay there, which
    is exactly what the balancer needs to see.
    """
    load = {k: 0 for k in acc_keys()}
    async for d in col_clients.find({"status": {"$ne": "expired"}},
                                    {"joined_accounts": 1}):
        for k in d.get("joined_accounts", []) or []:
            if k in load:
                load[k] += 1
    return load


async def pick_accounts(n: int) -> Tuple[List[str], Dict[str, int]]:
    """The `n` least-busy accounts, so channels spread across the whole fleet.

    acc_keys() is insertion-ordered, so slicing it handed every new channel the
    same accounts from the front: with 200 channels the first accounts would be
    in all of them and the last ones in none. That concentrates every flood
    wait, every reaction and every live join onto a handful of sessions, and
    leaves the rest of the fleet doing nothing.

    Accounts at MAX_CHANNELS_PER_ACCOUNT are skipped entirely — Telegram caps a
    user at 500 channels and refuses the join past it.
    """
    load = await account_load()
    # Frozen accounts are skipped: they answer every join with
    # FrozenMethodInvalidError, so handing them a slot means the client silently
    # gets fewer members than the package promised.
    usable = [k for k in joinable_keys()
              if load.get(k, 0) < MAX_CHANNELS_PER_ACCOUNT]
    # Shuffle first so equally-loaded accounts do not always tie in the same
    # order, which would just recreate the front-of-the-list bias.
    random.shuffle(usable)
    usable.sort(key=lambda k: load.get(k, 0))
    return usable[:n], load


def pick_workers(pool: List[str], n: int) -> List[str]:
    """`n` accounts out of `pool`, the least-recently-worked ones first.

    random.sample() draws uniformly, and uniform is not the same as even: across
    a burst of posts the same account keeps coming up by chance. With one account
    sitting in ~50 channels, several of those channels posting in the same minute
    is normal, and that is exactly how one session collects a flood wait while
    its neighbours idle. Ordering by last-use spreads the work by construction
    instead of hoping the dice do it.

    Two things push an account to the back of the queue:
      * requests already in flight — it is mid-job for another channel;
      * streaming audio right now — it is already carrying a WebRTC connection
        and a second socket, so let a quieter account take the reaction.
    Neither is a hard exclusion: with a small pool those accounts are still
    better than serving nobody.
    """
    if n <= 0:
        return []
    cand = [k for k in pool if acc_client(k)]
    random.shuffle(cand)  # ties break randomly, not by list order
    cand.sort(key=lambda k: (live_busy_in(k) is not None,
                             ACC_INFLIGHT.get(k, 0),
                             ACC_LAST_USED.get(k, 0.0)))
    return cand[:n]


@contextlib.contextmanager
def working(key: str):
    """Count one in-flight job on `key` and stamp it as just used."""
    ACC_INFLIGHT[key] = ACC_INFLIGHT.get(key, 0) + 1
    try:
        yield
    finally:
        ACC_INFLIGHT[key] = max(0, ACC_INFLIGHT.get(key, 1) - 1)
        ACC_LAST_USED[key] = time.monotonic()


async def join_extra_accounts(doc: dict, keys: List[str]) -> List[str]:
    """Put `keys` into the subscription's channel and record them on the doc.

    The subscription's joined_accounts list is a snapshot of who was free when
    the client was created; it is not a reservation. When those accounts turn
    out to be busy elsewhere this is how the fleet floats to where it is needed:
    borrow idle accounts, join them, and from then on they are members too, so
    the next post or live already has them.

    Returns the keys that are actually in the channel now.
    """
    if not keys:
        return []
    spec = doc.get("channel_username")
    ok: List[str] = []
    sem = asyncio.Semaphore(JOIN_CONCURRENCY)

    async def join_one(key):
        async with sem:
            client = acc_client(key)
            if not client or is_frozen(key):
                return
            try:
                if spec:
                    await client(JoinChannelRequest(spec))
                elif doc.get("channel_type") == "private":
                    _, invite = parse_target(doc["channel_link"])
                    await client(ImportChatInviteRequest(invite))
                else:
                    await client(JoinChannelRequest(channel_peer(doc["channel_id"])))
                ok.append(key)
                await log_activity(key, "CLIENT_JOIN", doc["channel_link"], "top-up")
            except UserAlreadyParticipantError:
                ok.append(key)
            except Exception as e:
                if is_frozen_account_error(e):
                    mark_frozen(key)
                await log_activity(key, "CLIENT_JOIN", doc["channel_link"],
                                   str(e)[:40])
            await asyncio.sleep(0.4)

    await asyncio.gather(*(join_one(k) for k in keys))
    if ok:
        try:
            await col_clients.update_one({"_id": doc["_id"]},
                                         {"$addToSet": {"joined_accounts":
                                                        {"$each": ok}}})
        except Exception as e:
            logger.warning(f"top-up save failed for {doc.get('_id')}: {e}")
        invalidate_peer_cache()
    return ok


async def onboard_new_accounts(keys: List[str],
                               notify: Optional[int] = None) -> dict:
    """Walk brand-new accounts into every active client channel.

    Until now a freshly imported account sat idle: subscriptions carry a fixed
    ``joined_accounts`` snapshot taken when the client was created, so an account
    added afterwards was never a member of anything and never got picked for a
    reaction, a view or a live. The fleet grew but the work stayed on the old
    sessions.

    This joins each new account to every active subscription's channel and adds
    it to that subscription's joined_accounts, so the client's "joined accounts"
    count goes up and the account starts being used from the next post onwards.

    Slow on purpose: joins are the flood-sensitive call, and a new account that
    joins 50 channels in a minute is the classic way to get one frozen.
    """
    keys = [k for k in keys if acc_client(k) and not is_frozen(k)]
    stats = {"accounts": len(keys), "channels": 0, "joins": 0,
             "already": 0, "failed": 0, "frozen": 0, "skipped": 0}
    if not keys:
        return stats

    subs = await get_client_subscriptions(status="active")
    now = utcnow()
    subs = [s for s in subs if s.get("expires_at") and s["expires_at"] > now]
    stats["channels"] = len(subs)
    if not subs:
        return stats

    load = await account_load()

    for sub in subs:
        spec = sub.get("channel_username")
        already_in = set(sub.get("joined_accounts", []) or [])
        fresh: List[str] = []

        for key in keys:
            if key in already_in:
                continue
            if is_frozen(key):
                stats["skipped"] += 1
                continue
            if load.get(key, 0) >= MAX_CHANNELS_PER_ACCOUNT:
                stats["skipped"] += 1
                continue
            client = acc_client(key)
            if not client:
                continue
            try:
                if spec:
                    await client(JoinChannelRequest(spec))
                elif sub.get("channel_type") == "private":
                    _, invite = parse_target(sub["channel_link"])
                    await client(ImportChatInviteRequest(invite))
                elif sub.get("channel_id"):
                    await client(JoinChannelRequest(
                        channel_peer(sub["channel_id"])))
                else:
                    stats["skipped"] += 1
                    continue
                fresh.append(key)
                load[key] = load.get(key, 0) + 1
                stats["joins"] += 1
                await log_activity(key, "AUTO_JOIN", sub["channel_link"],
                                   "new account")
            except UserAlreadyParticipantError:
                fresh.append(key)
                load[key] = load.get(key, 0) + 1
                stats["already"] += 1
            except FloodWaitError as e:
                stats["failed"] += 1
                await log_activity(key, "AUTO_JOIN", sub["channel_link"],
                                   f"flood {e.seconds}s")
                # A flood wait on joins applies to this account, not the
                # channel — give it a rest and carry on with the others.
                if e.seconds <= 60:
                    await asyncio.sleep(e.seconds)
            except Exception as e:
                if is_frozen_account_error(e):
                    mark_frozen(key)
                    stats["frozen"] += 1
                else:
                    stats["failed"] += 1
                await log_activity(key, "AUTO_JOIN", sub["channel_link"],
                                   str(e)[:40])
            await asyncio.sleep(random.uniform(1.2, 2.2))

        if fresh:
            try:
                await col_clients.update_one(
                    {"_id": sub["_id"]},
                    {"$addToSet": {"joined_accounts": {"$each": fresh}},
                     "$set": {"updated_at": utcnow()}})
            except Exception as e:
                logger.warning(f"onboard save failed for {sub.get('_id')}: {e}")

    invalidate_peer_cache()
    await setup_channel_monitors()
    logger.info(
        f"onboarding: {stats['accounts']} new account(s) -> "
        f"{stats['channels']} channel(s): {stats['joins']} joined, "
        f"{stats['already']} already in, {stats['failed']} failed, "
        f"{stats['frozen']} frozen")

    if notify:
        try:
            await bot.send_message(notify, card(E_CHECK, "New Accounts Onboarded", [
                field(E_PERSON, "New accounts", str(stats["accounts"])),
                field(E_CHANNEL, "Client channels", str(stats["channels"])),
                "",
                field(E_CHECK, "Joined", str(stats["joins"])),
                field(E_YELLOW, "Already member", str(stats["already"])),
                field(E_CROSS, "Failed", str(stats["failed"])),
                field(E_WARN, "Frozen", str(stats["frozen"])),
            ], footer=f"{E_SHIELD} {S('These accounts now count towards every client and will be used from the next post.')}"))
        except Exception:
            pass
    return stats


async def accounts_missing_from_subs() -> List[str]:
    """Online, non-frozen accounts that are not on every active subscription.

    This is the safety net for the onboarding above: a ZIP imported while the
    bot was busy, an account that hit a flood wait halfway through, or sessions
    dropped into the folder by hand and picked up by Reload all end up here, and
    the periodic sweep walks them in.
    """
    now = utcnow()
    subs = [s for s in await get_client_subscriptions(status="active")
            if s.get("expires_at") and s["expires_at"] > now]
    if not subs:
        return []
    out = []
    for key in joinable_keys():
        for s in subs:
            if key not in (s.get("joined_accounts") or []):
                out.append(key)
                break
    return out


async def onboard_sweep_task():
    """Every so often, walk any account that is missing from a client channel in."""
    while True:
        await asyncio.sleep(900)
        if not AUTO_ONBOARD:
            continue
        try:
            missing = await accounts_missing_from_subs()
            if missing:
                logger.info(f"onboarding sweep: {len(missing)} account(s) "
                            f"not yet in every client channel")
                await onboard_new_accounts(missing)
        except Exception as e:
            logger.error(f"onboarding sweep: {e}")


def schedule_onboarding(keys: List[str], notify: Optional[int] = None):
    """Run onboarding in the background so the import UI stays responsive."""
    keys = [k for k in dict.fromkeys(keys) if k]
    if not keys or not AUTO_ONBOARD:
        return
    asyncio.create_task(onboard_new_accounts(keys, notify))


def chats_from_join_result(res) -> list:
    """Pull the chat list out of whatever a join/import call returned.

    Older layers answered ImportChatInviteRequest with Updates (which has
    .chats). Newer ones wrap it in ChatInviteJoinResultOk, whose .updates holds
    the Updates object — reading res.chats there raised
    "'ChatInviteJoinResultOk' object has no attribute 'chats'" and lost the join
    even though the account was already inside the channel.
    """
    seen = []
    node = res
    for _ in range(4):
        if node is None:
            break
        chats = getattr(node, "chats", None)
        if chats:
            seen = list(chats)
            break
        node = getattr(node, "updates", None)
    return seen


async def get_client_doc(client_id) -> Optional[dict]:
    try:
        return await col_clients.find_one({"_id": ObjectId(client_id)})
    except Exception:
        return None


async def update_client_status(client_id, status):
    await col_clients.update_one(
        {"_id": ObjectId(client_id)},
        {"$set": {"status": status, "updated_at": utcnow()}},
    )


async def extend_client_subscription(client_id, days) -> Optional[dict]:
    doc = await get_client_doc(client_id)
    if not doc:
        return None
    base = max(doc["expires_at"], utcnow())
    await col_clients.update_one(
        {"_id": doc["_id"]},
        {"$set": {"expires_at": base + datetime.timedelta(days=days),
                  "status": "active",
                  "updated_at": utcnow()}},
    )
    return await get_client_doc(client_id)


async def delete_client_subscription(client_id) -> bool:
    doc = await get_client_doc(client_id)
    if not doc:
        return False
    for key in doc.get("joined_accounts", []):
        client = acc_client(key)
        if not client:
            continue
        try:
            if doc.get("channel_username"):
                await client(LeaveChannelRequest(doc["channel_username"]))
            elif doc.get("channel_id"):
                await client(LeaveChannelRequest(channel_peer(doc["channel_id"])))
        except Exception:
            pass
        await asyncio.sleep(0.15)
    await col_clients.delete_one({"_id": doc["_id"]})
    await setup_channel_monitors()
    return True


async def get_active_subscriptions_for_channel(channel_id) -> List[dict]:
    marked = norm_channel_id(channel_id)
    if marked is None:
        return []
    raw, _ = utils.resolve_id(marked)
    now = utcnow()
    out = []
    # Match both id forms so rows written by the old code still fire.
    async for d in col_clients.find({
        "channel_id": {"$in": [marked, raw]},
        "status": "active",
        "expires_at": {"$gt": now},
    }):
        out.append(d)
    return out


async def get_expiring_subscriptions(days=3) -> List[dict]:
    now = utcnow()
    out = []
    async for d in col_clients.find({
        "status": "active",
        "expires_at": {"$lte": now + datetime.timedelta(days=days), "$gt": now},
        "$or": [
            {"last_reminder_sent": None},
            {"last_reminder_sent": {"$exists": False}},
            {"last_reminder_sent": {"$lt": now - datetime.timedelta(days=1)}},
        ],
    }):
        out.append(d)
    return out


# ═══════════════════════ LINK PARSING ═══════════════════════
def parse_target(link: str) -> Tuple[Optional[str], Optional[str]]:
    """Parse a channel link -> ("public", username) | ("private", invite hash)."""
    link = link.strip()
    m = re.search(r"t\.me/(?:\+|joinchat/)([A-Za-z0-9_-]+)", link)
    if m:
        return "private", m.group(1)
    m = re.search(r"(?:t\.me/|@)([A-Za-z0-9_]{4,32})", link)
    if m and m.group(1) != "c":
        return "public", m.group(1)
    if link.lstrip("-").isdigit():
        return "id", link
    return None, None


def parse_post_link(link: str):
    """Parse a post link -> (peer_spec, msg_id).

    peer_spec is a username for public posts, or the marked -100… id for
    private ones. ``t.me/c/…`` links used to be rejected outright, which is why
    private channels could not be reacted to at all.
    """
    link = link.strip()
    m = re.search(r"t\.me/c/(\d+)/(?:\d+/)?(\d+)", link)
    if m:
        return int(f"-100{m.group(1)}"), int(m.group(2))
    m = re.search(r"t\.me/([A-Za-z0-9_]{4,32})/(?:\d+/)?(\d+)", link)
    if m and m.group(1) != "c":
        return m.group(1), int(m.group(2))
    return None, None


def is_post_link(link: str) -> bool:
    return parse_post_link(link)[0] is not None


async def resolve_peer(key: str, client: TelegramClient, spec):
    """Resolve a peer for one account, refreshing dialogs once if needed.

    Telethon can only build an input peer for a channel it holds an access hash
    for; a fresh session has none, which surfaces as PEER_ID_INVALID even though
    the account is a member. One dialog sweep fixes that.
    """
    cache_key = (key, str(spec))
    if cache_key in PEER_CACHE:
        return PEER_CACHE[cache_key]

    target = channel_peer(spec) if isinstance(spec, int) else spec
    try:
        peer = await client.get_input_entity(target)
    except Exception:
        try:
            async for _ in client.iter_dialogs(limit=200):
                pass
            peer = await client.get_input_entity(target)
        except Exception:
            return None
    PEER_CACHE[cache_key] = peer
    return peer


def invalidate_peer_cache(spec=None):
    if spec is None:
        PEER_CACHE.clear()
        return
    for k in [k for k in PEER_CACHE if k[1] == str(spec)]:
        PEER_CACHE.pop(k, None)


# ═══════════════════════ REACTION / VIEW PRIMITIVES ═══════════════════════
async def send_reaction(client, peer, msg_id, emoji) -> Tuple[bool, Optional[str]]:
    try:
        await client(SendReactionRequest(
            peer=peer, msg_id=msg_id,
            reaction=[ReactionEmoji(emoticon=emoji)],
        ))
        return True, None
    except FloodWaitError as e:
        return False, f"flood {e.seconds}s"
    except Exception as e:
        return False, str(e)[:60]


async def send_views(client, peer, msg_ids: List[int]) -> Tuple[bool, Optional[str]]:
    """Increment the real view counter.

    ``get_messages`` only *reads* a post; the counter only moves for
    GetMessagesViewsRequest with increment=True. The old code used the former,
    which is why view counts never changed.
    """
    try:
        await client(GetMessagesViewsRequest(
            peer=peer, id=list(msg_ids)[:VIEW_BATCH_SIZE], increment=True,
        ))
        return True, None
    except FloodWaitError as e:
        return False, f"flood {e.seconds}s"
    except Exception as e:
        return False, str(e)[:60]


async def ensure_member(client, spec) -> bool:
    if not isinstance(spec, str):
        return True
    try:
        await client(JoinChannelRequest(spec))
        return True
    except UserAlreadyParticipantError:
        return True
    except Exception as e:
        if is_frozen_account_error(e):
            # The key is not passed in here, so the caller's own handler records
            # it; log it once so the reason is visible in bot.log.
            logger.debug(f"ensure_member: frozen account refused {spec}")
        return False


# ═══════════════════════ PROGRESS REPORTER ═══════════════════════
class Progress:
    """Throttled progress editor — at most one edit per PROGRESS_EDIT_INTERVAL."""

    def __init__(self, msg):
        self.msg = msg
        self._last = 0.0
        self._loop = asyncio.get_running_loop()

    async def show(self, text: str, force: bool = False):
        now = self._loop.time()
        if not force and now - self._last < PROGRESS_EDIT_INTERVAL:
            return
        self._last = now
        try:
            await self.msg.edit(text)
        except MessageNotModifiedError:
            pass
        except Exception as e:
            logger.debug(f"progress edit: {e}")


def progress_card(title: str, done: int, total: int, stats: List[str],
                  detail: str = "") -> str:
    pct = int(done / total * 100) if total else 100
    lines = [
        f"<code>{bar(pct)}</code>  <b>{pct}%</b>",
        f"{E_CHART} {S('Done')}: <b>{done}/{total}</b>",
    ]
    if detail:
        lines.append(f"{E_TARGET} {detail}")
    lines.append("")
    lines += stats
    return card(E_ROCKET, title, lines)


# ═══════════════════════ EXECUTORS ═══════════════════════
async def execute_join(event, state, limit):
    links = state["links_data"]
    delay = state.get("delay", 1)
    # Frozen accounts cannot join anything, so they never take a slot here.
    keys = joinable_keys()[:limit]
    if not keys:
        return await event.respond(card(E_CROSS, "No Accounts", [S("Add accounts first.")]),
                                   buttons=kb_nav())

    msg = await event.respond(card(E_ROCKET, "Joining",
                                   [field(E_CHANNEL, "Links", str(len(links))),
                                    field(E_PERSON, "Accounts", str(len(keys)))]))
    prog = Progress(msg)
    total = len(links) * len(keys)
    counters = {"ok": 0, "already": 0, "failed": 0, "done": 0}
    sem = asyncio.Semaphore(JOIN_CONCURRENCY)

    async def join_one(key, info):
        async with sem:
            client = acc_client(key)
            if not client:
                counters["failed"] += 1
                counters["done"] += 1
                return
            try:
                if info["type"] == "public":
                    await client(JoinChannelRequest(info["target"]))
                else:
                    await client(ImportChatInviteRequest(info["target"]))
                counters["ok"] += 1
                await log_activity(key, "JOIN", info["link"], "Success")
            except UserAlreadyParticipantError:
                counters["already"] += 1
                counters["ok"] += 1
            except InviteRequestSentError:
                counters["ok"] += 1
                await log_activity(key, "JOIN", info["link"], "Request sent")
            except FloodWaitError as e:
                if e.seconds <= 30:
                    await asyncio.sleep(e.seconds)
                    try:
                        if info["type"] == "public":
                            await client(JoinChannelRequest(info["target"]))
                        else:
                            await client(ImportChatInviteRequest(info["target"]))
                        counters["ok"] += 1
                    except Exception:
                        counters["failed"] += 1
                else:
                    counters["failed"] += 1
                    await log_activity(key, "JOIN", info["link"], f"flood {e.seconds}s")
            except Exception as e:
                if is_frozen_account_error(e):
                    mark_frozen(key)
                counters["failed"] += 1
                await log_activity(key, "JOIN", info["link"], str(e)[:40])
            counters["done"] += 1
            await asyncio.sleep(delay)
            await prog.show(progress_card(
                "Join Progress", counters["done"], total,
                [field(E_CHECK, "Joined", str(counters["ok"] - counters["already"])),
                 field(E_YELLOW, "Already", str(counters["already"])),
                 field(E_CROSS, "Failed", str(counters["failed"]))],
                detail=esc(info["link"][:40])))

    for info in links:
        await asyncio.gather(*(join_one(k, info) for k in keys))
        invalidate_peer_cache()

    rate = int(counters["ok"] / total * 100) if total else 0
    await prog.show(card(E_CHECK, "Join Complete", [
        field(E_CHECK, "Joined", str(counters["ok"] - counters["already"])),
        field(E_YELLOW, "Already member", str(counters["already"])),
        field(E_CROSS, "Failed", str(counters["failed"])),
        "",
        field(E_CHANNEL, "Links", str(len(links))),
        field(E_PERSON, "Accounts", str(len(keys))),
        field(E_CHART, "Success rate", f"{rate}%"),
    ]), force=True)
    try:
        await msg.edit(buttons=kb_nav())
    except Exception:
        pass


async def execute_react_view(event, state, limit, multi=False):
    links = state["links_data"]
    emojis = state["emojis"] if multi else [state["emoji"]]
    speed = state.get("speed", 0.2)
    do_views = state.get("do_views", True)
    keys = acc_keys()[:limit]
    if not keys:
        return await event.respond(card(E_CROSS, "No Accounts", [S("Add accounts first.")]),
                                   buttons=kb_nav())

    title = "Multi React" if multi else "React & View"
    msg = await event.respond(card(E_FIRE, title, [
        field(E_CHANNEL, "Posts", str(len(links))),
        field(E_PERSON, "Accounts", str(len(keys))),
        field(E_HEART, "Emojis", esc(" ".join(emojis))),
    ]))
    prog = Progress(msg)
    total = len(links) * len(keys)
    c = {"done": 0, "react": 0, "views": 0, "failed": 0, "noaccess": 0}
    sem = asyncio.Semaphore(REACT_CONCURRENCY)

    async def work(key, spec, msg_id, shown):
        async with sem:
            client = acc_client(key)
            if not client:
                c["failed"] += 1
                c["done"] += 1
                return
            peer = await resolve_peer(key, client, spec)
            if peer is None and isinstance(spec, str):
                await ensure_member(client, spec)
                peer = await resolve_peer(key, client, spec)
            if peer is None:
                c["noaccess"] += 1
                c["done"] += 1
                return

            for emoji in emojis:
                ok, err = await send_reaction(client, peer, msg_id, emoji)
                if ok:
                    c["react"] += 1
                else:
                    c["failed"] += 1
                    if err and "INVALID" in err.upper():
                        invalidate_peer_cache(spec)
                if len(emojis) > 1:
                    await asyncio.sleep(0.1)

            if do_views:
                ok, _ = await send_views(client, peer, [msg_id])
                if ok:
                    c["views"] += 1

            c["done"] += 1
            await asyncio.sleep(speed)
            await prog.show(progress_card(title, c["done"], total, [
                field(E_THUMB, "Reactions", str(c["react"])),
                field(E_EYE, "Views", str(c["views"])),
                field(E_CROSS, "Failed", str(c["failed"])),
                field(E_LOCK, "No access", str(c["noaccess"])),
            ], detail=esc(shown[:40])))

    for info in links:
        spec, msg_id = parse_post_link(info["link"])
        if spec is None:
            c["failed"] += len(keys)
            c["done"] += len(keys)
            continue
        await asyncio.gather(*(work(k, spec, msg_id, info["link"]) for k in keys))

    await increment_stats(reactions=c["react"], views=c["views"])
    await prog.show(card(E_CHECK, f"{title} Complete", [
        field(E_THUMB, "Reactions sent", str(c["react"])),
        field(E_EYE, "Views sent", str(c["views"])),
        field(E_CROSS, "Failed", str(c["failed"])),
        field(E_LOCK, "No access", str(c["noaccess"])),
        "",
        field(E_CHANNEL, "Posts", str(len(links))),
        field(E_PERSON, "Accounts", str(len(keys))),
    ], footer=f"{E_SPARKLE} {S('Telegram updates counters within a minute.')}"),
        force=True)
    try:
        await msg.edit(buttons=kb_nav())
    except Exception:
        pass


async def execute_views(event, links: List[str], limit: int):
    """Send real views for one or many posts, batching ids per channel."""
    keys = acc_keys()[:limit] if limit > 0 else acc_keys()
    if not keys:
        return await event.respond(card(E_CROSS, "No Accounts", [S("Add accounts first.")]),
                                   buttons=kb_nav())

    # group post ids per channel so one request covers many posts
    grouped: Dict[object, List[int]] = {}
    invalid = 0
    for link in links:
        spec, mid = parse_post_link(link)
        if spec is None:
            invalid += 1
            continue
        grouped.setdefault(spec, []).append(mid)

    if not grouped:
        return await event.respond(card(E_CROSS, "Invalid Link", [
            S("Valid formats:"),
            f"{DOT} <code>https://t.me/username/123</code>",
            f"{DOT} <code>https://t.me/c/123456789/123</code>",
        ]), buttons=kb_nav())

    msg = await event.respond(card(E_EYE, "Sending Real Views", [
        field(E_CHANNEL, "Channels", str(len(grouped))),
        field(E_PAGE, "Posts", str(sum(len(v) for v in grouped.values()))),
        field(E_PERSON, "Accounts", str(len(keys))),
    ]))
    prog = Progress(msg)
    total = len(grouped) * len(keys)
    c = {"done": 0, "ok": 0, "views": 0, "failed": 0, "noaccess": 0}
    sem = asyncio.Semaphore(VIEW_CONCURRENCY)

    async def work(key, spec, ids):
        async with sem:
            client = acc_client(key)
            if not client:
                c["failed"] += 1
                c["done"] += 1
                return
            peer = await resolve_peer(key, client, spec)
            if peer is None and isinstance(spec, str):
                await ensure_member(client, spec)
                peer = await resolve_peer(key, client, spec)
            if peer is None:
                c["noaccess"] += 1
                c["done"] += 1
                await log_activity(key, "VIEW", str(spec), "no access")
                return
            for i in range(0, len(ids), VIEW_BATCH_SIZE):
                chunk = ids[i:i + VIEW_BATCH_SIZE]
                ok, err = await send_views(client, peer, chunk)
                if ok:
                    c["ok"] += 1
                    c["views"] += len(chunk)
                else:
                    c["failed"] += 1
                    if err and "INVALID" in err.upper():
                        invalidate_peer_cache(spec)
            c["done"] += 1
            await asyncio.sleep(0.05)
            await prog.show(progress_card("Real Views", c["done"], total, [
                field(E_EYE, "Views sent", str(c["views"])),
                field(E_CROSS, "Failed", str(c["failed"])),
                field(E_LOCK, "No access", str(c["noaccess"])),
            ]))

    for spec, ids in grouped.items():
        await asyncio.gather(*(work(k, spec, ids) for k in keys))

    await increment_stats(views=c["views"])
    attempted = c["ok"] + c["failed"] + c["noaccess"]
    rate = int(c["ok"] / attempted * 100) if attempted else 0
    lines = [
        field(E_EYE, "Real views sent", str(c["views"])),
        field(E_CROSS, "Failed", str(c["failed"])),
        field(E_LOCK, "Not a member", str(c["noaccess"])),
        "",
        field(E_PAGE, "Posts", str(sum(len(v) for v in grouped.values()))),
        field(E_PERSON, "Accounts used", str(len(keys))),
        field(E_CHART, "Success rate", f"{rate}%"),
    ]
    if invalid:
        lines.append(field(E_WARN, "Skipped bad links", str(invalid)))
    await prog.show(card(E_CHECK, "Views Complete", lines,
                         footer=f"{E_SPARKLE} {S('Counter updates in 5-30 seconds.')}"),
                    force=True)
    try:
        await msg.edit(buttons=kb_nav())
    except Exception:
        pass


async def execute_leave_specific(event, link: str):
    ltype, target = parse_target(link)
    if not ltype:
        return await event.respond(card(E_CROSS, "Invalid Link",
                                        [S("Send a channel link or @username.")]),
                                   buttons=kb_nav())
    keys = acc_keys()
    msg = await event.respond(card(E_TRASH, "Leaving Channel",
                                    [field(E_CHANNEL, "Target", esc(link[:50]))]))
    prog = Progress(msg)
    left = failed = 0
    for i, key in enumerate(keys, 1):
        client = acc_client(key)
        if not client:
            continue
        try:
            if ltype == "public":
                await client(LeaveChannelRequest(target))
            else:
                ent = await client.get_entity(link)
                await client.delete_dialog(ent)
            left += 1
        except Exception:
            failed += 1
        await asyncio.sleep(0.3)
        await prog.show(progress_card("Leaving", i, len(keys), [
            field(E_CHECK, "Left", str(left)),
            field(E_CROSS, "Failed", str(failed)),
        ]))
    invalidate_peer_cache()
    await prog.show(card(E_CHECK, "Leave Complete", [
        field(E_CHECK, "Left from", f"{left} {S('accounts')}"),
        field(E_CROSS, "Failed", str(failed)),
    ]), force=True)
    try:
        await msg.edit(buttons=kb_nav())
    except Exception:
        pass


async def execute_leave_all(event):
    keys = acc_keys()
    msg = await event.respond(card(E_WARN, "Leaving All Chats",
                                    [S("This cannot be undone.")]))
    prog = Progress(msg)
    total = 0
    for i, key in enumerate(keys, 1):
        client = acc_client(key)
        if not client:
            continue
        try:
            async for dialog in client.iter_dialogs():
                if dialog.is_channel or dialog.is_group:
                    try:
                        await client.delete_dialog(dialog.entity)
                        total += 1
                        await asyncio.sleep(0.3)
                    except Exception:
                        pass
        except Exception:
            pass
        await prog.show(progress_card("Leaving All", i, len(keys),
                                      [field(E_CHECK, "Chats left", str(total))]))
    invalidate_peer_cache()
    await prog.show(card(E_CHECK, "Done", [
        field(E_CHECK, "Chats left", str(total)),
        field(E_PERSON, "Accounts", str(len(keys))),
    ]), force=True)
    try:
        await msg.edit(buttons=kb_nav())
    except Exception:
        pass


REACTION_EMOJIS_WEIGHTED = {
    "❤": 25, "👍": 22, "🔥": 20, "🎉": 15, "😍": 18, "👏": 14,
    "🥰": 16, "💯": 12, "👀": 10, "💪": 11, "🏆": 8,  "🎯": 9,
    "🤩": 13, "😎": 17, "🙏": 10, "🌟": 14, "💥": 9,  "✨": 12,
    "🎊": 8,  "🥳": 10,
}
REACTION_EMOJIS = list(REACTION_EMOJIS_WEIGHTED.keys())


def distribute_reactions(total: int, allowed=None) -> dict:
    """Spread `total` reactions over several emojis in uneven amounts.

    Giving every account the same random emoji makes a post look machine-made:
    real posts have one or two popular reactions and a long tail of single
    hits. Ported from stream.py's ReactionDistributor.
    """
    if total <= 0:
        return {}
    pool = [e for e in (allowed or REACTION_EMOJIS) if e] or REACTION_EMOJIS
    weights = [REACTION_EMOJIS_WEIGHTED.get(e, 10) for e in pool]

    cap = min(len(pool), total)
    lo = max(1, min(8, cap))
    hi = max(lo, min(11, cap))
    num_emojis = random.randint(lo, hi)

    selected, available, avail_w = [], list(pool), list(weights)
    for _ in range(num_emojis):
        if not available:
            break
        total_w = sum(avail_w)
        r = random.uniform(0, total_w) if total_w > 0 else 0
        cumsum, pick = 0, available[0]
        for emoji, w in zip(available, avail_w):
            cumsum += w
            if r <= cumsum:
                pick = emoji
                break
        idx = available.index(pick)
        selected.append(pick)
        available.pop(idx)
        avail_w.pop(idx)
    if not selected:
        return {}

    selected.sort(key=lambda e: REACTION_EMOJIS_WEIGHTED.get(e, 10), reverse=True)
    if random.random() < 0.3 and len(selected) >= 3:
        top3 = selected[:3]
        random.shuffle(top3)
        selected = top3 + selected[3:]

    counts, remaining = {}, total
    shares = [0.30, 0.22, 0.15, 0.11, 0.08] + [0.05] * max(0, len(selected) - 5)
    for i, emoji in enumerate(selected):
        if remaining <= 0:
            break
        share = shares[i] if i < len(shares) else 0.05
        count = max(1, min(int(total * share * random.uniform(0.85, 1.15)), remaining))
        counts[emoji] = count
        remaining -= count
    # Whatever rounding left over goes onto the most popular few.
    for emoji in list(counts.keys())[:3]:
        if remaining <= 0:
            break
        add = min(remaining, random.randint(1, max(1, remaining // 2)))
        counts[emoji] += add
        remaining -= add
    if remaining > 0 and counts:
        first = next(iter(counts))
        counts[first] += remaining
    return counts


async def get_allowed_reactions(key: str, client, peer) -> Optional[list]:
    """Emojis this channel actually permits, or None when unrestricted."""
    cache_key = str(peer)
    if cache_key in ALLOWED_REACTIONS_CACHE:
        return ALLOWED_REACTIONS_CACHE[cache_key]
    allowed = None
    try:
        full = await client(GetFullChannelRequest(peer))
        ar = getattr(full.full_chat, "available_reactions", None)
        if isinstance(ar, ChatReactionsSome):
            allowed = [e for e in
                       (getattr(r, "emoticon", None) for r in (ar.reactions or []))
                       if e]
    except Exception:
        allowed = None
    ALLOWED_REACTIONS_CACHE[cache_key] = allowed
    return allowed


# ═══════════════════════ AUTO REACT / VIEW ON NEW POSTS ═══════════════════════
async def process_new_post(channel_id: int, message_id: int):
    subs = await get_active_subscriptions_for_channel(channel_id)
    if not subs:
        return

    for sub in subs:
        if sub["expires_at"] < utcnow():
            await update_client_status(str(sub["_id"]), "expired")
            continue

        joined = [k for k in sub.get("joined_accounts", []) if acc_client(k)]
        if not joined:
            logger.info(f"sub {sub['_id']}: no joined account is online")
            continue

        spec = sub.get("channel_username") or norm_channel_id(sub.get("channel_id")) \
            or channel_id
        n_react = min(int(sub.get("reactions_per_post", 0) or 0), len(joined))
        n_views = min(int(sub.get("views_per_post", 0) or 0), len(joined))

        # Least-recently-worked accounts first, so the load walks around the
        # whole joined list instead of landing on whoever the dice picked. The
        # view slice is taken after the react slice is marked busy, which makes
        # the two lists prefer different accounts without forcing them apart:
        # if the sub only has a handful joined, overlap is still allowed.
        react_keys = pick_workers(joined, n_react)
        for k in react_keys:
            ACC_INFLIGHT[k] = ACC_INFLIGHT.get(k, 0) + 1
        try:
            view_keys = pick_workers(joined, n_views)
        finally:
            for k in react_keys:
                ACC_INFLIGHT[k] = max(0, ACC_INFLIGHT.get(k, 1) - 1)

        # Use the shared global semaphore so all concurrent process_new_post
        # calls stay within REACT_CONCURRENCY total, not REACT_CONCURRENCY each.
        sem = _get_auto_react_sem()

        emoji_plan = []
        if react_keys:
            probe = react_keys[0]
            allowed = None
            probe_peer = await resolve_peer(probe, acc_client(probe), spec)
            if probe_peer is not None:
                allowed = await get_allowed_reactions(probe, acc_client(probe),
                                                      probe_peer)
            for emoji, count in distribute_reactions(len(react_keys), allowed).items():
                emoji_plan.extend([emoji] * count)
            random.shuffle(emoji_plan)

        async def react(key, emoji):
            async with sem:
                with working(key):
                    client = acc_client(key)
                    peer = await resolve_peer(key, client, spec)
                    if peer is None:
                        return
                    await send_reaction(client, peer, message_id, emoji)
                    await asyncio.sleep(random.uniform(0.1, 0.5))

        async def view(key):
            async with sem:  # same global sem covers views too
                with working(key):
                    client = acc_client(key)
                    peer = await resolve_peer(key, client, spec)
                    if peer is None:
                        return
                    await send_views(client, peer, [message_id])
                    await asyncio.sleep(random.uniform(0.05, 0.3))

        # Small random stagger per subscription so multiple subs on the same
        # post don't all start in the same millisecond.
        await asyncio.sleep(random.uniform(0.1, 1.0))
        await asyncio.gather(*[react(k, e) for k, e in zip(react_keys, emoji_plan)],
                             *[view(k) for k in view_keys],
                             return_exceptions=True)

        await col_clients.update_one({"_id": sub["_id"]},
                                     {"$inc": {"total_posts_processed": 1}})
        await increment_stats(reactions=len(react_keys), views=len(view_keys))
        logger.info(f"post {message_id}: {len(react_keys)} reactions, "
                    f"{len(view_keys)} views for {sub.get('client_name')}")


async def load_settings():
    """Restore owner-set limits from Mongo so a restart keeps them."""
    global LIVE_CAP, LIVE_ROTATE
    try:
        doc = await col_settings.find_one({"_id": "live"}) or {}
        LIVE_CAP = max(0, int(doc.get("cap", DEFAULT_LIVE_CAP)))
        LIVE_ROTATE = max(0, int(doc.get("rotate", DEFAULT_LIVE_ROTATE)))
    except Exception as e:
        logger.warning(f"settings load failed, using defaults: {e}")


async def save_setting(field_name: str, value: int):
    try:
        await col_settings.update_one({"_id": "live"},
                                      {"$set": {field_name: value}}, upsert=True)
    except Exception as e:
        logger.warning(f"settings save failed: {e}")


async def save_live_cap(value: int):
    global LIVE_CAP
    LIVE_CAP = max(0, int(value))
    await save_setting("cap", LIVE_CAP)


async def save_live_rotate(value: int):
    global LIVE_ROTATE
    LIVE_ROTATE = max(0, int(value))
    await save_setting("rotate", LIVE_ROTATE)


def cap_text() -> str:
    return S("unlimited") if not LIVE_CAP else str(LIVE_CAP)


def rotate_text() -> str:
    if not LIVE_ROTATE:
        return S("off")
    if LIVE_ROTATE % 3600 == 0:
        return f"{LIVE_ROTATE // 3600}h"
    return f"{LIVE_ROTATE // 60}m"


def live_limit(n: int) -> int:
    """Clamp a requested account count to the cap (0 = no cap)."""
    return min(n, LIVE_CAP) if LIVE_CAP else n


def audio_ready() -> bool:
    return TGCALLS_OK and os.path.exists(LIVE_AUDIO_PATH)


def live_stream_source():
    """The looping MP3 every account feeds into a call."""
    return MediaStream(
        LIVE_AUDIO_PATH,
        audio_parameters=AudioQuality.HIGH,
        video_flags=MediaStream.Flags.IGNORE,
        ffmpeg_parameters=f"-stream_loop {AUDIO_LOOP_COUNT}",
    )


def fd_headroom() -> int:
    """Spare file descriptors, or a large number when it cannot be measured.

    Running out mid-stream does not just fail the join: Telethon can no longer
    open its session sqlite files, so every account drops. Better to refuse a
    stream than to take the whole fleet down.
    """
    try:
        import resource
        soft, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
        if soft in (resource.RLIM_INFINITY, -1):
            return 10 ** 6
        used = len(os.listdir("/proc/self/fd"))
        return soft - used
    except Exception:
        return 10 ** 6


def live_stream_count() -> int:
    return sum(len(v) for v in LIVE_AUDIO.values())


def live_busy_in(key: str) -> Optional[int]:
    """The call this account is already streaming into, if any.

    The same fleet serves every client, so two clients going live at the same
    time will both pick from the same accounts. One account cannot be in two
    group calls: Telegram drops it from the first, so the earlier client
    silently loses a participant. Every selection point checks this first.
    """
    for cid, keys in LIVE_AUDIO.items():
        if key in keys:
            return cid
    return None


def free_live_keys(keys: List[str], chat_id: Optional[int] = None) -> List[str]:
    """Those of `keys` that are online and not tied up in another call."""
    out = []
    for k in keys:
        if not acc_client(k) or is_frozen(k):
            continue
        busy = live_busy_in(k)
        if busy is None or busy == chat_id:
            out.append(k)
    return out


async def get_tgcalls(key: str):
    """One PyTgCalls per account, created on first use and then reused."""
    if key in TGCALLS:
        return TGCALLS[key]
    client = acc_client(key)
    if not client:
        return None
    call = PyTgCalls(client, workers=TGCALLS_WORKERS)

    @call.on_update(pytgf.stream_end())
    async def _restart(_c, update):
        # -stream_loop covers ~everything, but if the count ever runs out the
        # account would go silent and get dropped, so start the file again.
        try:
            await call.play(update.chat_id, live_stream_source())
        except Exception as e:
            logger.debug(f"stream restart {key}: {e}")

    await call.start()
    TGCALLS[key] = call
    return call


async def play_live_audio(key: str, chat_id: int) -> Tuple[bool, str]:
    """Put one account into `chat_id`'s call and start streaming the MP3.

    play() performs the real WebRTC join itself, so no JoinGroupCallRequest is
    sent here — issuing one would conflict, since Telegram permits a single
    join per peer per call.
    """
    if not TGCALLS_OK:
        return False, "py-tgcalls not installed"
    if not os.path.exists(LIVE_AUDIO_PATH):
        return False, "no audio file set"
    if key not in LIVE_AUDIO.get(chat_id, set()):
        busy = live_busy_in(key)
        if busy is not None:
            return False, f"already live in {busy}"
        if LIVE_CAP and live_stream_count() >= LIVE_CAP:
            return False, f"stream limit {LIVE_CAP} reached"
        spare = fd_headroom()
        if spare < FD_HEADROOM:
            return False, f"only {spare} file descriptors left"
    try:
        call = await get_tgcalls(key)
        if call is None:
            return False, "account offline"
        await call.play(chat_id, live_stream_source(),
                        GroupCallConfig(auto_start=False))
        LIVE_AUDIO.setdefault(chat_id, set()).add(key)
        return True, ""
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"[:80]


async def stop_live_audio(key: str, chat_id: int):
    call = TGCALLS.get(key)
    if call is not None:
        try:
            await call.leave_call(chat_id)
        except Exception as e:
            logger.debug(f"leave call {key}: {e}")
    LIVE_AUDIO.get(chat_id, set()).discard(key)
    LIVE_SESSIONS.get(chat_id, {}).get("since", {}).pop(key, None)


def live_session(chat_id: int) -> dict:
    return LIVE_SESSIONS.setdefault(chat_id, {"pool": [], "target": 0,
                                              "label": "", "since": {},
                                              "rotated": time.monotonic()})


def register_live_session(chat_id: int, pool: List[str], target: int,
                          label: str = "") -> dict:
    """Record who *may* stream into this call, and how many should at a time.

    Rotation swaps between the pool and the streaming set, so the pool has to be
    larger than the target for it to have anything to swap in.

    Additive, because two clients can buy the same channel: the second one to
    fire must not wipe the first one's pool, it should widen it.
    """
    sess = live_session(chat_id)
    sess["pool"] = list(dict.fromkeys(list(sess.get("pool") or []) + list(pool)))
    sess["target"] = sess.get("target", 0) + target
    if label and label not in sess.get("label", ""):
        sess["label"] = f"{sess['label']} + {label}" if sess.get("label") else label
    return sess


def end_live_session(chat_id: int):
    LIVE_AUDIO.pop(chat_id, None)
    LIVE_SESSIONS.pop(chat_id, None)


async def join_live_with_audio(keys: List[str], chat_id: int,
                               label: str = "") -> Tuple[int, dict]:
    """Stream the MP3 from every key into chat_id. Returns (ok_count, errors)."""
    ok = 0
    errors: Dict[str, str] = {}
    sess = live_session(chat_id)
    if label:
        sess["label"] = label

    async def one(key):
        nonlocal ok
        good, err = await play_live_audio(key, chat_id)
        if good:
            ok += 1
            sess["since"][key] = time.monotonic()
            await log_activity(key, "LIVE_JOIN", str(chat_id), "Success")
            logger.info(f"live audio ok: {key} -> {chat_id}")
        else:
            errors[key] = err
            await log_activity(key, "LIVE_JOIN", str(chat_id), err[:40])
            logger.warning(f"live audio {key}: {err}")

    # Joining one at a time meant the last of 60 accounts arrived three minutes
    # after the stream started. Go in parallel batches instead: fast enough to
    # look simultaneous, small enough that Telegram does not flood-wait.
    for i in range(0, len(keys), LIVE_JOIN_BATCH):
        batch = keys[i:i + LIVE_JOIN_BATCH]
        await asyncio.gather(*(one(k) for k in batch), return_exceptions=True)
        if i + LIVE_JOIN_BATCH < len(keys):
            await asyncio.sleep(random.uniform(0.8, 1.5))

    logger.info(f"live stream: {ok}/{len(keys)} accounts streaming audio"
                + (f" for {label}" if label else ""))
    return ok, errors


async def rotate_live_accounts(chat_id: int) -> Tuple[int, int]:
    """Swap the longest-serving accounts out of a call for rested ones.

    Without this the same accounts sit in one call for the whole broadcast —
    hours, on a long live. Retiring them in shifts spreads the load over the
    fleet and looks like an audience rather than a fixed block of listeners.
    Returns (left, joined).
    """
    sess = LIVE_SESSIONS.get(chat_id)
    if not sess:
        return 0, 0
    current = [k for k in LIVE_AUDIO.get(chat_id, set())]
    if not current:
        return 0, 0

    def rested():
        return [k for k in free_live_keys(sess["pool"], chat_id)
                if k not in LIVE_AUDIO.get(chat_id, set())]

    going, joined = [], 0
    resting = rested()
    if resting:
        n = min(max(1, int(len(current) * LIVE_ROTATE_FRACTION)),
                len(resting), len(current))
        since = sess.setdefault("since", {})
        current.sort(key=lambda k: since.get(k, 0.0))
        going, coming = current[:n], random.sample(resting, n)

        # Leave before joining: play_live_audio refuses once the stream limit is
        # reached, so swapping the other way round would fail at the ceiling.
        for key in going:
            await stop_live_audio(key, chat_id)
        joined, _ = await join_live_with_audio(coming, chat_id)
    else:
        logger.debug(f"rotation {chat_id}: pool has no rested free account")

    # Backfill whatever the fleet lost since the stream began — an account that
    # went offline is dropped by the keepalive and never replaced otherwise, so
    # a long live slowly bleeds away the count the client paid for.
    short = int(sess.get("target", 0)) - len(LIVE_AUDIO.get(chat_id, set()))
    if short > 0:
        spare = rested()
        if spare:
            extra, _ = await join_live_with_audio(
                random.sample(spare, min(short, len(spare))), chat_id)
            joined += extra
            logger.info(f"live backfill {chat_id}: {extra} added "
                        f"(was {short} short of {sess.get('target')})")

    if going or joined:
        logger.info(f"live rotation {chat_id}: {len(going)} out, {joined} in "
                    f"({len(LIVE_AUDIO.get(chat_id, set()))} streaming)")
    return len(going), joined


async def process_live_stream_start(channel_id: int, call):
    """Join a live stream with the configured number of accounts."""
    subs = await get_active_subscriptions_for_channel(channel_id)
    if not subs:
        return

    if not audio_ready():
        why = TGCALLS_ERR if not TGCALLS_OK else "no audio file set"
        logger.warning(f"live stream in {channel_id} ignored — {why}")
        for owner in OWNER_IDS:
            try:
                await bot.send_message(owner, card(E_WARN, "Live Stream Skipped", [
                    field(E_CHANNEL, "Channel", str(channel_id)),
                    "",
                    S("Accounts cannot join without audio to play."),
                    f"<code>{esc(why)}</code>",
                ]), buttons=[[btn("Set Audio", "audio_menu", icon="🎵")]])
            except Exception:
                pass
        return

    for sub in subs:
        if sub["expires_at"] < utcnow():
            await update_client_status(str(sub["_id"]), "expired")
            continue

        n_live = int(sub.get("livestream_accounts", 0) or 0)
        if n_live == 0:
            continue
        if live_limit(n_live) != n_live:
            logger.warning(f"sub {sub['_id']}: live accounts capped "
                           f"{n_live} -> {LIVE_CAP} (Audio > Stream Limit)")
            n_live = live_limit(n_live)

        joined = [k for k in sub.get("joined_accounts", []) if acc_client(k)]

        # Another client may already be live and holding some of these. An
        # account can only sit in one group call, so those are gone for now.
        free = free_live_keys(joined, channel_id)

        # Top up from the rest of the fleet. This is the point of a shared
        # fleet: 200 accounts serving 200 channels means this channel's own
        # joined list is only ~n_live wide, and if another live borrowed half of
        # it the client would silently get half the accounts they paid for —
        # while dozens of accounts sat idle in other channels. Take the
        # least-busy free ones, join them, and they are members from now on.
        if len(free) < n_live:
            need = n_live - len(free)
            load = await account_load()
            spare = [k for k in free_live_keys(acc_keys(), channel_id)
                     if k not in joined
                     and load.get(k, 0) < MAX_CHANNELS_PER_ACCOUNT]
            random.shuffle(spare)
            spare.sort(key=lambda k: load.get(k, 0))
            if spare:
                added = await join_extra_accounts(sub, spare[:need])
                logger.info(f"sub {sub['_id']}: borrowed {len(added)}/{need} "
                            f"idle accounts for the live")
                joined = list(dict.fromkeys(joined + added))
                free = free_live_keys(joined, channel_id)

        if not free:
            logger.warning(f"sub {sub['_id']}: every account is already in "
                           f"another live call — nothing to send")
            continue
        if len(free) < n_live:
            logger.warning(f"sub {sub['_id']}: only {len(free)} of {n_live} "
                           f"accounts free (rest are live elsewhere)")

        live_keys = random.sample(free, min(n_live, len(free)))
        # Everyone the client subscription already put in the channel is a valid
        # stand-in, so the whole joined list becomes the rotation pool.
        register_live_session(channel_id, joined, len(live_keys),
                              str(sub.get("client_name", "")))
        await join_live_with_audio(live_keys, channel_id,
                                   str(sub.get("client_name", "")))


async def live_keepalive_task():
    """Restart any account whose audio stopped while the call is still running.

    A participant that sends nothing is dropped after ~60s. Streaming audio is
    what normally prevents that; this only catches a stream that died (ffmpeg
    exited, network blip) and would otherwise leave the account silently gone.

    Rotation is driven from here too rather than from its own loop: this task
    has already confirmed the call is alive, so a separate timer would only
    repeat that same GetFullChannelRequest poll against the accounts.
    """
    while True:
        await asyncio.sleep(LIVE_KEEPALIVE_SECONDS)
        if not audio_ready():
            continue
        for chat_id, keys in list(LIVE_AUDIO.items()):
            call_live = await get_active_call(chat_id, list(keys))
            if call_live is None:
                for key in list(keys):
                    await stop_live_audio(key, chat_id)
                end_live_session(chat_id)
                logger.info(f"live stream ended in {chat_id} — accounts left")
                continue

            sess = LIVE_SESSIONS.get(chat_id)
            if LIVE_ROTATE and sess is not None:
                due = time.monotonic() - sess.get("rotated", 0) >= LIVE_ROTATE
                if due:
                    sess["rotated"] = time.monotonic()
                    try:
                        await rotate_live_accounts(chat_id)
                    except Exception as e:
                        logger.warning(f"rotation {chat_id}: "
                                       f"{type(e).__name__}: {e}")
                    keys = LIVE_AUDIO.get(chat_id, set())

            for key in list(keys):
                call = TGCALLS.get(key)
                if call is None:
                    keys.discard(key)
                    continue
                try:
                    calls = await call.calls
                    if chat_id not in calls:
                        good, err = await play_live_audio(key, chat_id)
                        logger.info(f"live re-stream {key} -> {chat_id}: "
                                    f"{'ok' if good else err}")
                except Exception as e:
                    logger.debug(f"keepalive {key}: {type(e).__name__}: {e}")
                await asyncio.sleep(random.uniform(0.1, 0.4))


async def get_active_call(channel_id: int, keys=None):
    """Return the channel's running group call, or None.

    Uses GetFullChannelRequest rather than update events: the "voice chat
    started" notice is a MessageService, which events.NewMessage drops before
    any handler runs, and raw UpdateGroupCall is not delivered reliably to
    accounts that are plain channel members.

    The poll runs every LIVE_POLL_SECONDS forever, so the account order is
    shuffled: always asking the same one would put every one of those requests
    on a single account and eventually earn it a flood wait.
    """
    pool = list(keys or acc_keys())
    random.shuffle(pool)
    for key in pool:
        client = acc_client(key)
        if not client:
            continue
        try:
            peer = await resolve_peer(key, client, channel_id)
            if peer is None:
                continue
            full = await client(GetFullChannelRequest(peer))
            return full.full_chat.call
        except Exception:
            continue
    return None


async def livestream_watch_task():
    """Poll monitored channels and fire joins the moment a stream goes live."""
    while True:
        try:
            subs = await get_client_subscriptions(status="active")
            targets = {}
            for s in subs:
                cid = norm_channel_id(s.get("channel_id"))
                if cid is None or int(s.get("livestream_accounts", 0) or 0) == 0:
                    continue
                # Poll with accounts already in the channel — they can read
                # the full channel without an extra resolve round-trip.
                pool = targets.setdefault(cid, [])
                for k in s.get("joined_accounts", []):
                    if k not in pool and acc_client(k):
                        pool.append(k)

            for chat_id, keys in targets.items():
                call = await get_active_call(chat_id, keys)
                if call is None:
                    active_calls.pop(chat_id, None)
                    continue
                if active_calls.get(chat_id) == call.id:
                    continue          # already handled this stream
                active_calls[chat_id] = call.id
                logger.info(f"live stream detected in {chat_id} (call {call.id})")
                asyncio.create_task(process_live_stream_start(chat_id, call))
        except Exception as e:
            logger.error(f"livestream watch: {e}")
        await asyncio.sleep(LIVE_POLL_SECONDS)


async def channel_monitor_handler(event):
    if event.is_private or event.is_group:
        return
    chat_id = event.chat_id           # already the marked -100… form

    msg_id = event.message.id
    cache_key = f"{chat_id}:{msg_id}"
    if cache_key in recent_messages:
        return
    recent_messages.add(cache_key)
    if len(recent_messages) > 2000:
        recent_messages.clear()
    if chat_id in monitored_channels:
        asyncio.create_task(process_new_post(chat_id, msg_id))


async def group_call_handler(update):
    """Catch live streams via the raw UpdateGroupCall.

    events.NewMessage cannot be used here: Telethon drops MessageService
    updates before building the event, and the "voice chat started" notice is
    exactly that, so the service-message route never fired at all.

    This only supplements livestream_watch_task — the update is not delivered
    to every member account, so the poller stays the source of truth.
    """
    if not isinstance(update, UpdateGroupCall):
        return
    call = update.call
    # GroupCallDiscarded arrives on the same update when the stream ends.
    if not isinstance(call, GroupCall):
        return
    chat_id = norm_channel_id(getattr(update, "chat_id", None))
    if chat_id is None or chat_id not in monitored_channels:
        return
    # Every monitoring account receives this same update, so join once only.
    if active_calls.get(chat_id) == call.id:
        return
    active_calls[chat_id] = call.id
    logger.info(f"live stream detected in {chat_id} (call {call.id})")
    asyncio.create_task(process_live_stream_start(chat_id, call))


async def setup_channel_monitors():
    """Attach the new-post listener to a few accounts that are actually members."""
    global monitored_channels
    subs = await get_client_subscriptions(status="active")
    monitored_channels = {
        cid for cid in (norm_channel_id(s.get("channel_id")) for s in subs)
        if cid is not None
    }

    # Prefer accounts that joined a monitored channel; they are the ones that
    # will actually receive the update.
    candidates: List[str] = []
    for s in subs:
        for k in s.get("joined_accounts", []):
            if acc_client(k) and k not in candidates:
                candidates.append(k)
    if not candidates:
        candidates = acc_keys()
    wanted = set(candidates[:MONITOR_CLIENTS])

    for key in wanted - clients_with_monitor:
        client = acc_client(key)
        if not client:
            continue
        client.add_event_handler(channel_monitor_handler, events.NewMessage)
        client.add_event_handler(group_call_handler, events.Raw)
        clients_with_monitor.add(key)

    for key in list(clients_with_monitor - wanted):
        client = acc_client(key)
        if client:
            try:
                client.remove_event_handler(channel_monitor_handler)
                client.remove_event_handler(group_call_handler)
            except Exception:
                pass
        clients_with_monitor.discard(key)

    # This runs every 5 minutes; only say something when it actually changed,
    # otherwise the line just repeats forever in the log.
    global _LAST_MONITOR_STATE
    stateline = (len(monitored_channels), len(clients_with_monitor))
    if stateline != _LAST_MONITOR_STATE:
        _LAST_MONITOR_STATE = stateline
        logger.info(f"Monitoring {len(monitored_channels)} channel(s) via "
                    f"{len(clients_with_monitor)} account(s)")


async def monitor_task():
    while True:
        try:
            await setup_channel_monitors()
        except Exception as e:
            logger.error(f"monitor setup: {e}")
        await asyncio.sleep(300)


async def reminder_task():
    while True:
        await asyncio.sleep(3600)
        try:
            for sub in await get_expiring_subscriptions(3):
                left = max(0, (sub["expires_at"] - utcnow()).days)
                text = card(E_BELL, "Subscription Reminder", [
                    field(E_PERSON, "Client", esc(str(sub.get("client_name", "Unknown")))),
                    field(E_CHANNEL, "Channel", esc(str(sub["channel_link"]))),
                    field(E_CLOCK, "Expires in", f"{left} {S('days')}"),
                    field(E_CAL, "Expiry", sub["expires_at"].strftime("%d %b %Y %H:%M")),
                ], footer=f"{E_GIFT} {S('Renew now to avoid interruption.')}")
                try:
                    await bot.send_message(int(sub["client_user_id"]), text)
                except Exception:
                    pass
                for owner in OWNER_IDS:
                    try:
                        await bot.send_message(owner, text)
                    except Exception:
                        pass
                await col_clients.update_one(
                    {"_id": sub["_id"]},
                    {"$set": {"last_reminder_sent": utcnow()}})
        except Exception as e:
            logger.error(f"reminder task: {e}")


async def expiry_check_task():
    while True:
        await asyncio.sleep(1800)
        try:
            now = utcnow()
            async for doc in col_clients.find({"status": "active",
                                               "expires_at": {"$lt": now}}):
                await update_client_status(str(doc["_id"]), "expired")
                try:
                    await bot.send_message(int(doc["client_user_id"]), card(
                        E_RED, "Subscription Expired", [
                            field(E_CHANNEL, "Channel", esc(str(doc["channel_link"]))),
                            field(E_CAL, "Expired",
                                  doc["expires_at"].strftime("%d %b %Y %H:%M")),
                        ], footer=f"{E_BELL} {S('Contact admin to renew.')}"))
                except Exception:
                    pass
            await setup_channel_monitors()
        except Exception as e:
            logger.error(f"expiry task: {e}")


# ═══════════════════════ SCREENS ═══════════════════════
async def safe_edit(event, text, buttons=None):
    try:
        await event.edit(text, buttons=buttons)
    except MessageNotModifiedError:
        pass
    except Exception as e:
        logger.debug(f"edit failed: {e}")
        try:
            await event.respond(text, buttons=buttons)
        except Exception:
            pass


async def show_menu(event, user_id, edit=True):
    online = acc_count()
    problems = len(PROBLEM_SESSIONS)
    joins = await get_today_joins()
    try:
        total_clients = await col_clients.count_documents({})
        active_subs = await col_clients.count_documents({"status": "active"})
    except Exception:
        total_clients = active_subs = 0

    is_owner = user_id in OWNER_IDS
    text = card(E_ROCKET, "Reaction & Views Panel", [
        f"{E_PERSON} {S('Accounts online')}: <b>{online}</b>"
        + (f"  <i>({problems} {S('need review')})</i>" if problems else ""),
        field(E_CROWN, "Clients", f"{active_subs} {S('active')} / {total_clients}"),
        field(E_CHART, "Joins today", str(joins)),
        field(E_CLOCK, "Uptime", uptime_str()),
        field(E_GREEN, "Status", S("Online 24/7")),
        "",
        f"{E_TARGET} {S('Choose an action below')}",
    ], footer=f"{E_DIAMOND} {S('Sessions load from the sessions folder')}")

    if is_owner:
        buttons = [
            [btn("Create Client", "create_client", icon="👑"),
             btn("Manage Clients", "manage_clients", icon="📋")],
            [btn("React + View", "react_view", icon="🔥"),
             btn("Multi React", "multi_react", icon="💥")],
            [btn("Views", "views_menu", icon="👁"),
             btn("Join", "join", icon="📥")],
            [btn("Leave", "leave", icon="📤"),
             btn("Accounts", "menu_accounts", icon="👤")],
            [btn("Go Live", "go_live", icon="🎤"),
             btn("Audio", "audio_menu", icon="🎵")],
            [btn("Statistics", "stats", icon="📊"),
             btn("Access", "approve", icon="🛡")],
            [btn("Help", "help", icon="💡")],
        ]
    else:
        buttons = [
            [btn("React + View", "react_view", icon="🔥"),
             btn("Multi React", "multi_react", icon="💥")],
            [btn("Views", "views_menu", icon="👁"),
             btn("Join", "join", icon="📥")],
            [btn("Leave", "leave", icon="📤"),
             btn("Accounts List", "list_accounts", icon="👤")],
            [btn("Help", "help", icon="💡")],
        ]

    if edit and hasattr(event, "edit"):
        await safe_edit(event, text, buttons)
    else:
        await event.respond(text, buttons=buttons)


async def scan_dead_accounts() -> List[str]:
    """Ping every connected account. Return keys of definitively dead ones."""
    dead_keys: List[str] = []
    for key in list(acc_keys()):
        client = acc_client(key)
        if not client:
            continue
        try:
            await client(UpdateStatusRequest(offline=False))
        except Exception as e:
            if is_dead_account_error(e):
                ACCOUNTS[key].state = "dead"
                dead_keys.append(key)
            elif is_frozen_account_error(e):
                # Frozen is not dead: the session is fine and must not be
                # trashed, it just cannot join or invite anything.
                mark_frozen(key)
            else:
                logger.debug(f"scan {key}: {e}")
        await asyncio.sleep(0.2)
    return dead_keys


async def show_accounts_menu(event):
    online = acc_count()
    files = len([f for f in os.listdir(SESSIONS_DIR) if f.endswith(".session")])
    dead = sum(1 for a in ACCOUNTS.values() if a.state == "dead")
    text = card(E_PERSON, "Accounts", [
        field(E_GREEN, "Online", str(online)),
        field(E_PAGE, "Session files", str(files)),
        field(E_WARN, "Need review", str(len(PROBLEM_SESSIONS))),
        field(E_RED, "Detected dead", str(dead)),
        field(E_LOCK, "Frozen (cannot join)", str(sum(1 for k in ACCOUNTS
                                                      if is_frozen(k)))),
        field(E_TRASH, "In trash", str(len(trash_entries()))),
    ], footer=f"{E_SHIELD} {S('Removing an account moves it to trash, never deletes it')}")
    buttons = [
        [btn("Add Account", "add", icon="➕"),
         btn("Import ZIP", "import_zip", icon="📦")],
        [btn("Accounts List", "list_accounts", icon="📋"),
         btn("Reload Folder", "reload_sessions", icon="🔄")],
        [btn("Needs Review", "problem_sessions", icon="⚠️"),
         btn("Remove Account", "remove_account", icon="🚫")],
        [btn("Scan Dead Accounts", "scan_dead", icon="🔍"),
         btn("Trash", "trash_menu", icon="🗑")],
        [btn("Stop All Accounts", "acc_stop_all_ask", icon="⏸"),
         btn("Remove All Dead", "acc_rm_dead_ask", icon="🚫")],
        [btn("Sync To Client Channels", "sync_onboard", icon="🔗")],
        [btn("Home", "home", icon="🏠")],
    ]
    await safe_edit(event, text, buttons)


async def show_accounts_list(event, page=1):
    keys = sorted(acc_keys())
    if not keys:
        return await safe_edit(event, card(E_CROSS, "No Accounts", [
            S("The sessions folder has no working account yet."),
            "",
            f"{DOT} {S('Import a ZIP of .session files')}",
            f"{DOT} {S('Or log in with a phone number')}",
        ]), [[btn("Import ZIP", "import_zip", icon="📦"),
              btn("Add Account", "add", icon="➕")],
             [btn("Back", "menu_accounts", icon="⬅️")]])

    rows, page, total_pages = paginate(keys, page)
    lines = []
    start = (page - 1) * PER_PAGE
    for i, key in enumerate(rows, start + 1):
        acc = ACCOUNTS[key]
        dot = E_GREEN if acc.client else E_RED
        lines.append(f"{dot} <b>{i}.</b> <code>{esc(acc.label)}</code> "
                     f"<i>{esc(acc.name[:14])}</i>")
    text = card(E_PERSON, "Accounts List", lines,
                footer=field(E_CHART, "Total online", str(len(keys))))
    # Add a ⚙ Manage button for each account (up to 2 per row)
    mgmt_buttons = []
    for i in range(0, len(rows), 2):
        pair = rows[i:i+2]
        mgmt_buttons.append([
            Button.inline(f"⚙ {ACCOUNTS[k].label[:15]}", f"acc_mgmt_{k}".encode())
            for k in pair
        ])
    mgmt_buttons += pager(page, total_pages, "acc", "menu_accounts")
    await safe_edit(event, text, mgmt_buttons)


async def show_problem_sessions(event, page=1):
    items = sorted(PROBLEM_SESSIONS.items())
    if not items:
        return await safe_edit(event, card(E_CHECK, "All Clear", [
            S("Every session file in the folder is online."),
        ]), kb_nav("menu_accounts"))
    rows, page, total_pages = paginate(items, page)
    lines = [f"{E_WARN} <code>{esc(stem)}</code> — <i>{esc(reason)}</i>"
             for stem, reason in rows]
    text = card(E_WARN, "Needs Review", lines, footer=(
        f"{E_LOCK} {S('Re-login sends a fresh OTP and replaces the session file.')} "
        f"{E_TRASH} {S('Trash moves it to the trash folder instead.')} "
        f"{S('A locked file usually means a second bot process is running.')}"))
    # Re-login + Trash per session so the owner can fix or clean each one
    trash_buttons = []
    for stem, _ in rows:
        trash_buttons.append([
            Button.inline(f"🔑 {esc(stem[:12])}", f"prob_login_{stem}".encode()),
            Button.inline("🗑", f"prob_rm_{stem}".encode()),
        ])
    trash_buttons += pager(page, total_pages, "prob", "menu_accounts")
    await safe_edit(event, text, trash_buttons)


async def show_remove_list(event, page=1):
    keys = sorted(acc_keys())
    if not keys:
        return await safe_edit(event, card(E_CROSS, "No Accounts", [S("Nothing to remove.")]),
                               kb_nav("menu_accounts"))
    rows, page, total_pages = paginate(keys, page, 6)
    buttons = []
    for i in range(0, len(rows), 2):
        buttons.append([
            Button.inline(f"🚫 {ACCOUNTS[k].label[:16]}", f"remask_{k}".encode())
            for k in rows[i:i + 2]
        ])
    buttons += pager(page, total_pages, "rem", "menu_accounts")
    await safe_edit(event, card(E_TRASH, "Remove Account", [
        S("Pick an account to take offline."),
        "",
        f"{E_SHIELD} {S('Its session file moves to trash and can be restored.')}",
    ]), buttons)


async def show_audio_menu(event):
    have = os.path.exists(LIVE_AUDIO_PATH)
    streaming = sum(len(v) for v in LIVE_AUDIO.values())
    lines = []
    if not TGCALLS_OK:
        lines += [f"{E_CROSS} {S('py-tgcalls is not installed')}",
                  f"<code>{esc(TGCALLS_ERR[:120])}</code>",
                  "",
                  f"{S('On the server run')}:",
                  "<code>pip install py-tgcalls</code>",
                  "<code>apt install -y ffmpeg</code>",
                  ""]
    if have:
        size = os.path.getsize(LIVE_AUDIO_PATH) / (1024 * 1024)
        lines += [field(E_CHECK, "Audio file", f"{size:.1f} MB"),
                  field(E_REFRESH, "Repeat", S("loops forever"))]
    else:
        lines += [f"{E_WARN} {S('No audio file set yet.')}",
                  f"{S('Accounts cannot hold a live stream without it.')}"]
    lines += [field(E_ROCKET, "Streaming now", f"{streaming} / {cap_text()}"),
              field(E_REFRESH, "Rotation", rotate_text())]
    spare = fd_headroom()
    if spare < 10 ** 6:
        lines.append(field(E_PAGE, "Free descriptors", str(spare)))

    buttons = [[btn("Set Audio" if not have else "Replace Audio",
                    "audio_set", icon="🎵")]]
    if have:
        buttons.append([btn("Remove Audio", "audio_del", icon="🗑")])
    buttons.append([btn("Stream Limit", "live_cap", icon="🛡"),
                    btn("Rotation", "live_rot", icon="🔄")])
    if streaming:
        buttons.append([btn("Live Now", "live_now", icon="🎤"),
                        btn("Stop All", "live_stop_all", icon="⏹")])
    buttons.append([btn("Home", "home", icon="🏠")])
    await safe_edit(event, card(E_ROCKET, "Live Audio", lines, footer=(
        f"{E_SHIELD} {S('Accounts play this file so Telegram keeps them in the call.')}"
    )), buttons)


async def show_live_now(event):
    """Every call the fleet is streaming into, with a stop button each."""
    active = {c: k for c, k in LIVE_AUDIO.items() if k}
    lines, buttons = [], []
    if not active:
        lines.append(S("No account is in a live stream right now."))
    else:
        for chat_id, keys in active.items():
            sess = LIVE_SESSIONS.get(chat_id, {})
            label = sess.get("label") or str(chat_id)
            pool = len(sess.get("pool", []))
            # Not field(): the label is a link or a client name, and S() would
            # rewrite its letters into the display font.
            lines.append(f"{E_CHANNEL} <code>{esc(label[:34])}</code> — "
                         f"<b>{len(keys)}</b> {S('streaming')}"
                         + (f", {pool - len(keys)} {S('resting')}"
                            if pool > len(keys) else ""))
            buttons.append([btn(f"Stop {label[:14]}",
                                f"live_stop_{chat_id}", icon="⏹"),
                            btn("Rotate", f"live_rotnow_{chat_id}", icon="🔄")])
        lines += ["", field(E_ROCKET, "Total", f"{live_stream_count()} / {cap_text()}"),
                  field(E_REFRESH, "Rotation", rotate_text())]
        buttons.append([btn("Stop All", "live_stop_all", icon="⏹")])
    buttons.append([btn("Refresh", "live_now", icon="🔄"),
                    btn("Back", "audio_menu", icon="⬅️")])
    await safe_edit(event, card(E_ROCKET, "Live Now", lines, footer=(
        f"{E_SHIELD} {S('Stopping pulls those accounts out of the call.')}"
    )), buttons)


async def show_live_rotation(event):
    """How often streaming accounts are swapped for rested ones."""
    lines = [field(E_REFRESH, "Rotate every", rotate_text()),
             field(E_PERSON, "Swap each time",
                   f"{int(LIVE_ROTATE_FRACTION * 100)}% {S('of the streamers')}"),
             field(E_ROCKET, "Streaming now", str(live_stream_count())),
             "",
             S("On a two hour live the same accounts would otherwise sit in the "
               "same call for two hours."),
             S("Rotation retires the longest-serving ones and brings rested "
               "accounts in, so the load spreads across the fleet."),
             "",
             f"{E_WARN} {S('Needs spare accounts in the channel. Go Live pulls in')} "
             f"{LIVE_POOL_MULT}× {S('the requested count for this.')}"]

    def row(vals):
        return [btn(v[0], f"live_rot_{v[1]}",
                    icon="✅" if v[1] == LIVE_ROTATE else "") for v in vals]

    buttons = [row([("10m", 600), ("20m", 1200), ("30m", 1800)]),
               row([("45m", 2700), ("1h", 3600), ("2h", 7200)]),
               [btn("Off", "live_rot_0", icon="✅" if not LIVE_ROTATE else "")],
               [btn("Back", "audio_menu", icon="⬅️")]]
    await safe_edit(event, card(E_REFRESH, "Rotation", lines, footer=(
        f"{E_SHIELD} {S('Accounts leave and rejoin in shifts, never all at once.')}"
    )), buttons)


async def show_live_cap(event):
    """Owner-set ceiling on simultaneous streams."""
    spare = fd_headroom()
    lines = [field(E_SHIELD, "Current limit", cap_text()),
             field(E_ROCKET, "Streaming now", str(live_stream_count())),
             field(E_PERSON, "Accounts online", str(acc_count()))]
    if spare < 10 ** 6:
        lines.append(field(E_PAGE, "Free descriptors", str(spare)))
    lines += [
        "",
        S("Each streaming account costs an ffmpeg process, a webrtc "
          "connection and a second socket."),
        S("Raise this in steps and watch free descriptors — if they run out "
          "every account drops, not just the live ones."),
    ]
    def row(vals):
        return [btn(str(v), f"live_cap_{v}",
                    icon="✅" if v == LIVE_CAP else "") for v in vals]

    buttons = [row([10, 25, 50]), row([75, 100, 150]),
               [btn("Unlimited", "live_cap_0",
                    icon="✅" if not LIVE_CAP else "⚠️")],
               [btn("Back", "audio_menu", icon="⬅️")]]
    await safe_edit(event, card(E_SHIELD, "Stream Limit", lines, footer=(
        f"{E_WARN} {S('Unlimited is allowed — the descriptor guard still applies.')}"
    )), buttons)


async def show_trash(event):
    entries = trash_entries()
    lines = ([f"{DOT} <code>{esc(e)}</code>" for e in entries[:10]]
             or [S("Trash is empty.")])
    if len(entries) > 10:
        lines.append(f"<i>+ {len(entries) - 10} {S('more')}</i>")
    text = card(E_TRASH, "Session Trash", lines,
                footer=field(E_PAGE, "Total", str(len(entries))))
    buttons = []
    if entries:
        buttons.append([btn("Restore All", "trash_restore", icon="♻️"),
                        btn("Clear All", "trash_clear_ask", icon="🗑")])
    buttons.append([btn("Back", "menu_accounts", icon="⬅️"),
                    btn("Home", "home", icon="🏠")])
    await safe_edit(event, text, buttons)


async def show_clients(event, page=1):
    """One row per client, not per channel — a client may hold several."""
    index = await client_index()
    if not index:
        return await safe_edit(event, card(E_CROWN, "No Clients", [
            S("No subscription has been created yet."),
        ]), [[btn("Create Client", "create_client", icon="👑")],
             [btn("Home", "home", icon="🏠")]])

    rows, page, total_pages = paginate(index, page, 6)
    lines, buttons = [], []
    now = utcnow()
    for i, r in enumerate(rows, (page - 1) * 6 + 1):
        dot = E_GREEN if r["active"] else E_RED
        left = (f"{max(0, (r['soonest'] - now).days)}{S('d')}"
                if r["soonest"] else S("expired"))
        lines.append(
            f"{dot} <b>{i}.</b> <i>{esc(str(r['name'])[:18])}</i> {DOT} "
            f"<b>{r['channels']}</b> {S('ch')} {DOT} {left}"
        )
        buttons.append([Button.inline(
            f"{'🟢' if r['active'] else '🔴'} {str(r['name'])[:16]} "
            f"({r['channels']})",
            f"cuser_{r['user_id']}".encode())])
    buttons += pager(page, total_pages, "mc", "home")
    total_subs = sum(r["channels"] for r in index)
    text = card(E_CROWN, "Manage Clients", lines, footer=(
        f"{E_CHART} {S('Clients')}: <b>{len(index)}</b> {DOT} "
        f"{S('Subscriptions')}: <b>{total_subs}</b>"))
    await safe_edit(event, text, buttons)


async def show_client_channels(event, user_id, page=1):
    """Every channel one client has bought, each its own subscription."""
    subs = await get_subs_for_user(int(user_id))
    if not subs:
        return await safe_edit(event, card(E_CROSS, "Not Found", [
            S("This client has no subscription left."),
        ]), kb_nav("manage_clients"))

    name = subs[0].get("client_name", "Unknown")
    rows, page, total_pages = paginate(subs, page, 6)
    now = utcnow()
    lines = [field(E_PERSON, "Client", esc(str(name))),
             f"{E_STAR} {S('User ID')}: <code>{user_id}</code>",
             ""]
    buttons = []
    for i, c in enumerate(rows, (page - 1) * 6 + 1):
        st = c.get("status", "active")
        dot = E_GREEN if st == "active" else (E_RED if st == "expired" else E_YELLOW)
        left = max(0, (c["expires_at"] - now).days)
        lines.append(
            f"{dot} <b>{i}.</b> <code>{esc(str(c['channel_link'])[:26])}</code>"
            f"\n     {c['accounts_count']} {S('acc')} {DOT} "
            f"{c['reactions_per_post']} {S('react')} {DOT} "
            f"{c['views_per_post']} {S('views')} {DOT} "
            f"{c.get('livestream_accounts', 0)} {S('live')} {DOT} {left}{S('d')}"
        )
        buttons.append([Button.inline(
            f"{'🟢' if st == 'active' else '🔴' if st == 'expired' else '🟡'} "
            f"{str(c['channel_link'])[-20:]}",
            f"client_{c['_id']}".encode())])
    buttons.append([btn("Add Channel", f"cadd_{user_id}", icon="➕")])
    buttons += pager(page, total_pages, f"cu{user_id}", "manage_clients")
    await safe_edit(event, card(E_CROWN, "Client Channels", lines, footer=(
        f"{E_SHIELD} {S('Each channel has its own package and expiry.')}"
    )), buttons)


async def show_client_detail(event, client_id):
    doc = await get_client_doc(client_id)
    if not doc:
        return await safe_edit(event, card(E_CROSS, "Not Found",
                                            [S("This client no longer exists.")]),
                               kb_nav("manage_clients"))
    st = doc.get("status", "active")
    dot = E_GREEN if st == "active" else (E_RED if st == "expired" else E_YELLOW)
    left = max(0, (doc["expires_at"] - utcnow()).days)
    online = len([k for k in doc.get("joined_accounts", []) if acc_client(k)])
    uid = int(doc.get("client_user_id", 0) or 0)
    siblings = len(await get_subs_for_user(uid)) - 1

    text = card(E_PAGE, "Client Details", [
        field(E_PERSON, "Name", esc(str(doc.get("client_name", "Unknown")))),
        f"{E_STAR} {S('User ID')}: <code>{doc['client_user_id']}</code>",
        field(E_CHANNEL, "Channel", esc(str(doc["channel_link"]))),
        (field(E_CROWN, "Other channels", str(siblings)) if siblings > 0 else None),
        "",
        field(E_PERSON, "Accounts", f"{doc['accounts_count']} "
                                    f"({online} {S('online')})"),
        # Members actually recorded on the channel — this is what grows when new
        # accounts are imported and auto-joined, so it can exceed the package
        # size the client originally bought.
        field(E_CHECK, "Joined accounts",
              str(len(doc.get("joined_accounts", []) or []))),
        field(E_THUMB, "Reactions / post", str(doc["reactions_per_post"])),
        field(E_EYE, "Views / post", str(doc["views_per_post"])),
        field(E_ROCKET, "Live stream accounts", str(doc.get("livestream_accounts", 0))),
        field(E_CAL, "Package days", str(doc["subscription_days"])),
        "",
        field(E_CLOCK, "Days left", str(left)),
        field(E_CAL, "Expires", doc["expires_at"].strftime("%d %b %Y %H:%M")),
        field(E_CHART, "Posts processed", str(doc.get("total_posts_processed", 0))),
        f"{dot} {S('Status')}: <b>{S(st.upper())}</b>",
    ])

    cid = str(doc["_id"])
    buttons = []
    if st == "active":
        buttons.append([btn("Upgrade", f"upg_{cid}", icon="⬆️"),
                        btn("Extend", f"ext_{cid}", icon="🗓")])
        buttons.append([btn("Pause", f"stop_{cid}", icon="⏸"),
                        btn("Delete", f"delask_{cid}", icon="🚫")])
    elif st == "stopped":
        buttons.append([btn("Resume", f"start_{cid}", icon="▶️"),
                        btn("Extend", f"ext_{cid}", icon="🗓")])
        buttons.append([btn("Delete", f"delask_{cid}", icon="🚫")])
    else:
        buttons.append([btn("Renew", f"ext_{cid}", icon="♻️"),
                        btn("Upgrade", f"upg_{cid}", icon="⬆️")])
        buttons.append([btn("Delete", f"delask_{cid}", icon="🚫")])
    buttons.append([btn("Add Channel", f"cadd_{uid}", icon="➕"),
                    btn("All Channels", f"cuser_{uid}", icon="📋")])
    buttons.append([btn("Back", "manage_clients", icon="⬅️"),
                    btn("Home", "home", icon="🏠")])
    await safe_edit(event, text, buttons)


async def show_stats(event):
    users = await get_approved_users()
    joins = await get_today_joins()
    total_react, total_views, today_react, today_views = await get_reaction_view_stats()
    try:
        total = await col_clients.count_documents({})
        active = await col_clients.count_documents({"status": "active"})
        expired = await col_clients.count_documents({"status": "expired"})
        posts = 0
        async for d in col_clients.find({}, {"total_posts_processed": 1}):
            posts += int(d.get("total_posts_processed", 0) or 0)
    except Exception:
        total = active = expired = posts = 0

    # Channel load spread. If min and max drift far apart the fleet is lopsided:
    # a few sessions carry every channel and eat every flood wait. pick_accounts()
    # keeps them level, so this line is how you check it is actually working.
    try:
        load = await account_load()
        vals = sorted(load.values())
        spread = (f"{vals[0]}-{vals[-1]} ({S('idle')}: "
                  f"{sum(1 for v in vals if v == 0)})") if vals else "-"
    except Exception:
        spread = "-"

    text = card(E_CHART, "Statistics", [
        field(E_PERSON, "Accounts online", str(acc_count())),
        field(E_CHANNEL, "Channels per account", spread),
        field(E_WARN, "Sessions to review", str(len(PROBLEM_SESSIONS))),
        field(E_TRASH, "In trash", str(len(trash_entries()))),
        "",
        f"{E_THUMB} <b>{S('Reactions')}</b>",
        field(E_FIRE, "  Today", str(today_react)),
        field(E_CHART, "  Total", str(total_react)),
        "",
        f"{E_EYE} <b>{S('Views')}</b>",
        field(E_FIRE, "  Today", str(today_views)),
        field(E_CHART, "  Total", str(total_views)),
        "",
        field(E_CROWN, "Clients total", str(total)),
        field(E_GREEN, "Active", str(active)),
        field(E_RED, "Expired", str(expired)),
        field(E_FIRE, "Posts processed", str(posts)),
        "",
        field(E_SHIELD, "Approved users", str(len(users))),
        field(E_CHART, "Joins today", str(joins)),
        field(E_CLOCK, "Uptime", uptime_str()),
        field(E_CHANNEL, "Channels watched", str(len(monitored_channels))),
    ])
    await safe_edit(event, text, [[btn("Refresh", "stats", icon="🔄")],
                                  [btn("Home", "home", icon="🏠")]])


async def show_help(event):
    text = card(E_SPARKLE, "Help", [
        f"{E_CROWN} <b>{S('Create Client')}</b>",
        f"  {S('Set channel, accounts, reactions, views and days. New posts then get reactions and views automatically.')}",
        "",
        f"{E_FIRE} <b>{S('React + View')}</b>",
        f"  {S('One emoji on one or more posts, plus a real view from each account.')}",
        "",
        f"{E_EYE} <b>{S('Views')}</b>",
        f"  {S('Real view counter increment. Works on public and private posts.')}",
        "",
        f"{E_PERSON} <b>{S('Accounts')}</b>",
        f"  {S('Sessions load from the sessions folder. Import a ZIP of .session files or log in by phone.')}",
        "",
        f"{E_PAGE} <b>{S('Link formats')}</b>",
        f"  <code>https://t.me/channel/123</code>",
        f"  <code>https://t.me/c/1234567890/123</code>",
    ], footer=f"{E_SHIELD} {S('Commands')}: /start /add /remove /list /reload /id")
    await safe_edit(event, text, [[btn("Home", "home", icon="🏠")]])


# ═══════════════════════ BOT ═══════════════════════
bot = TelegramClient(os.path.join(BASE_DIR, "bot_session"), API_ID, API_HASH)
bot.parse_mode = "html"


@bot.on(events.NewMessage(pattern=r"^/start"))
async def cmd_start(event):
    task_states.pop(event.chat_id, None)
    if not await is_user_approved(event.sender_id):
        return await event.respond(card(E_LOCK, "Access Denied", [
            S("You are not approved to use this bot."),
            "",
            f"{E_PERSON} {S('Your ID')}: <code>{event.sender_id}</code>",
            f"{E_BELL} {S('Send this ID to the owner.')}",
        ]))
    await show_menu(event, event.sender_id, edit=False)


@bot.on(events.NewMessage(pattern=r"^/id"))
async def cmd_id(event):
    await event.respond(card(E_PERSON, "Your ID", [
        f"<code>{event.sender_id}</code>",
    ]))


@bot.on(events.NewMessage(pattern=r"^/add\s+(\d+)"))
async def cmd_add(event):
    if event.sender_id not in OWNER_IDS:
        return await event.respond(card(E_LOCK, "Owner Only", [S("Not allowed.")]))
    uid = int(event.pattern_match.group(1))
    ok = await approve_user(uid, event.sender_id)
    await event.respond(card(E_CHECK if ok else E_CROSS,
                             "Approved" if ok else "Failed",
                             [f"<code>{uid}</code>"]))


@bot.on(events.NewMessage(pattern=r"^/remove\s+(\d+)"))
async def cmd_remove(event):
    if event.sender_id not in OWNER_IDS:
        return await event.respond(card(E_LOCK, "Owner Only", [S("Not allowed.")]))
    uid = int(event.pattern_match.group(1))
    ok = await unapprove_user(uid)
    await event.respond(card(E_CHECK if ok else E_WARN,
                             "Removed" if ok else "Not Found",
                             [f"<code>{uid}</code>"]))


@bot.on(events.NewMessage(pattern=r"^/list"))
async def cmd_list(event):
    if not await is_user_approved(event.sender_id):
        return
    users = await get_approved_users()
    lines = [f"{E_PERSON} <code>{u['user_id']}</code>" for u in users] or \
            [S("No approved users.")]
    await event.respond(card(E_SHIELD, "Approved Users", lines))


@bot.on(events.NewMessage(pattern=r"^/reload"))
async def cmd_reload(event):
    if event.sender_id not in OWNER_IDS:
        return
    msg = await event.respond(card(E_REFRESH, "Reloading Sessions",
                                    [S("Reading the sessions folder...")]))
    tally = await load_all_sessions()
    await setup_channel_monitors()
    # Sessions dropped into the folder by hand are new accounts too — put them
    # into the client channels instead of leaving them idle.
    schedule_onboarding(await accounts_missing_from_subs(), notify=event.chat_id)
    await msg.edit(card(E_CHECK, "Reload Complete", [
        field(E_GREEN, "Online", str(acc_count())),
        field(E_RED, "Not authorised", str(tally["dead"])),
        field(E_WARN, "Unclear", str(tally["unknown"])),
    ], footer=f"{E_SHIELD} {S('No file was deleted.')}"), buttons=kb_nav())


# ═══════════════════════ CALLBACK ROUTER ═══════════════════════
@bot.on(events.CallbackQuery)
async def on_callback(event):
    uid = event.sender_id
    if not await is_user_approved(uid):
        return await event.answer("Access denied", alert=True)

    data = event.data.decode()
    owner = uid in OWNER_IDS
    logger.info(f"cb {uid}: {data}")

    try:
        await route_callback(event, uid, owner, data)
    except MessageNotModifiedError:
        pass
    except Exception as e:
        logger.error(f"callback {data}: {e}", exc_info=True)
        try:
            await event.answer(f"Error: {str(e)[:150]}", alert=True)
        except Exception:
            pass


OWNER_ONLY = {
    "create_client", "manage_clients", "menu_accounts", "add", "add_phone",
    "add_string", "import_zip", "remove_account", "reload_sessions",
    "problem_sessions", "trash_menu", "trash_restore", "trash_clear_ask",
    "trash_clear_do", "scan_dead", "acc_stop_all_ask", "acc_stop_all_do",
    "acc_rm_dead_ask", "acc_rm_dead_do", "approve", "stats", "sync_onboard",
    "leave_all", "exec_leave_all",
    "go_live", "audio_menu", "audio_set", "audio_del", "live_stop_all",
    "live_now", "live_cap", "live_rot",
}
OWNER_ONLY_PREFIX = ("client_", "stop_", "start_", "delask_", "del_", "ext_",
                     "upg_", "remask_", "remdo_", "mc_page_", "prob_page_",
                     "rem_page_", "prob_rm_", "prob_login_", "acc_mgmt_", "acc_otp_",
                     "acc_2fa_", "acc_name_", "acc_photo_", "acc_2faset_",
                     "live_stop_", "live_cap_", "live_rot_", "live_rotnow_",
                     "cu", "cadd_")


async def route_callback(event, uid, owner, data):
    if data == "noop":
        return await event.answer()

    if not owner and (data in OWNER_ONLY
                      or data.startswith(OWNER_ONLY_PREFIX)):
        return await event.answer("Owner only", alert=True)

    # ── navigation ────────────────────────────────────────────
    if data in ("home", "back", "cancel"):
        task_states.pop(event.chat_id, None)
        return await show_menu(event, uid)

    if data == "help":
        return await show_help(event)
    if data == "stats":
        return await show_stats(event)

    # ── accounts ──────────────────────────────────────────────
    if data == "menu_accounts":
        return await show_accounts_menu(event)

    if data == "list_accounts" or data.startswith("acc_page_"):
        page = int(data.rsplit("_", 1)[1]) if data.startswith("acc_page_") else 1
        return await show_accounts_list(event, page)

    # ── Re-login a problem session with a fresh OTP ────────────
    if data.startswith("prob_login_"):
        return await start_relogin(event, data[len("prob_login_"):])

    # ── Remove a single problem session ───────────────────────
    if data.startswith("prob_rm_"):
        stem = data[len("prob_rm_"):]
        reason = PROBLEM_SESSIONS.get(stem, "unknown")
        sess_path = os.path.join(SESSIONS_DIR, stem + ".session")
        if os.path.exists(sess_path):
            moved = to_trash(sess_path)
            PROBLEM_SESSIONS.pop(stem, None)
            await event.answer(f"Moved to trash ({moved} file(s))")
        else:
            PROBLEM_SESSIONS.pop(stem, None)
            await event.answer("Entry cleared (file not found)")
        return await show_problem_sessions(event, 1)

    # ── Per-account management ────────────────────────────────
    if data.startswith("acc_mgmt_"):
        key = data[len("acc_mgmt_"):]
        acc = ACCOUNTS.get(key)
        if not acc or not acc.client:
            return await event.answer("Account not online", alert=True)
        text = card(E_PERSON, "Manage Account", [
            field(E_PHONE, "Account", esc(acc.label)),
            field(E_PERSON, "Name", esc(acc.name)),
        ])
        buttons = [
            [btn("Latest OTP", f"acc_otp_{key}", icon="🔑"),
             btn("Change Name", f"acc_name_{key}", icon="✏️")],
            [btn("Change Photo", f"acc_photo_{key}", icon="📷"),
             btn("Change 2FA", f"acc_2fa_{key}", icon="🔒")],
            [btn("Back", "list_accounts", icon="⬅️")],
        ]
        return await safe_edit(event, text, buttons)

    if data.startswith("acc_otp_"):
        key = data[len("acc_otp_"):]
        acc = ACCOUNTS.get(key)
        if not acc or not acc.client:
            return await event.answer("Account not online", alert=True)
        try:
            msgs = await acc.client.get_messages(777000, limit=5)
            codes = []
            for m in msgs:
                if not m or not m.message:
                    continue
                match = re.search(r"\b(\d{5,6})\b", m.message)
                if match:
                    when = m.date.strftime("%d %b %H:%M") if m.date else "?"
                    codes.append(f"<code>{match.group(1)}</code>  <i>({when})</i>")
            lines = codes[:5] or [S("No recent OTP found in Telegram service messages.")]
        except Exception as e:
            lines = [f"<code>{esc(str(e)[:120])}</code>"]
        text = card(E_LOCK, "Recent OTPs", [
            field(E_PHONE, "Account", esc(acc.label)),
            "",
        ] + lines)
        return await safe_edit(event, text, [[btn("Back", f"acc_mgmt_{key}", icon="⬅️")]])

    if data.startswith("acc_2fa_"):
        key = data[len("acc_2fa_"):]
        acc = ACCOUNTS.get(key)
        if not acc or not acc.client:
            return await event.answer("Account not online", alert=True)
        try:
            pwd_info = await acc.client(GetPasswordRequest())
            has_pwd = getattr(pwd_info, "has_password", False)
            hint = getattr(pwd_info, "hint", "") or ""
            status_line = (f"{E_CHECK} {S('2FA is ON')}" + (f" — <i>{esc(hint)}</i>" if hint else "")) if has_pwd                 else f"{E_CROSS} {S('2FA is OFF — no password set')}"
        except Exception as e:
            status_line = f"<code>{esc(str(e)[:80])}</code>"
        text = card(E_LOCK, "2FA Password", [
            field(E_PHONE, "Account", esc(acc.label)),
            "",
            status_line,
            "",
            S("Send the new 2FA password in the next message."),
            S("Send a dash (-) to remove the password."),
        ])
        task_states[event.chat_id] = {"type": "acc_2fa_set", "key": key, "step": "new_pwd"}
        return await safe_edit(event, text, [[btn("Cancel", f"acc_mgmt_{key}", icon="↩️")]])

    if data.startswith("acc_name_"):
        key = data[len("acc_name_"):]
        acc = ACCOUNTS.get(key)
        if not acc or not acc.client:
            return await event.answer("Account not online", alert=True)
        task_states[event.chat_id] = {"type": "acc_name_set", "key": key}
        text = card(E_PERSON, "Change Name", [
            field(E_PHONE, "Account", esc(acc.label)),
            field(E_PERSON, "Current name", esc(acc.name)),
            "",
            S("Send the new first name (or first+last separated by space)."),
        ])
        return await safe_edit(event, text, [[btn("Cancel", f"acc_mgmt_{key}", icon="↩️")]])

    if data.startswith("acc_photo_"):
        key = data[len("acc_photo_"):]
        acc = ACCOUNTS.get(key)
        if not acc or not acc.client:
            return await event.answer("Account not online", alert=True)
        task_states[event.chat_id] = {"type": "acc_photo_set", "key": key}
        text = card(E_PERSON, "Change Photo", [
            field(E_PHONE, "Account", esc(acc.label)),
            "",
            S("Send the new profile photo as an image."),
            S("The current photo will be replaced."),
        ])
        return await safe_edit(event, text, [[btn("Cancel", f"acc_mgmt_{key}", icon="↩️")]])

    if data == "problem_sessions" or data.startswith("prob_page_"):
        page = int(data.rsplit("_", 1)[1]) if data.startswith("prob_page_") else 1
        return await show_problem_sessions(event, page)

    if data == "remove_account" or data.startswith("rem_page_"):
        page = int(data.rsplit("_", 1)[1]) if data.startswith("rem_page_") else 1
        return await show_remove_list(event, page)

    if data.startswith("remask_"):
        key = data[len("remask_"):]
        acc = ACCOUNTS.get(key)
        if not acc:
            return await event.answer("Account not found", alert=True)
        return await safe_edit(event, card(E_WARN, "Confirm Remove", [
            field(E_PHONE, "Account", esc(acc.label)),
            field(E_PERSON, "Name", esc(acc.name)),
            "",
            f"{E_SHIELD} {S('The session file moves to trash. You can restore it.')}",
        ]), [[btn("Yes, remove", f"remdo_{key}", icon="🚫"),
              btn("Cancel", "remove_account", icon="↩️")]])

    if data.startswith("remdo_"):
        key = data[len("remdo_"):]
        acc = ACCOUNTS.get(key)
        if not acc:
            return await event.answer("Account not found", alert=True)
        await disconnect_account(key)
        moved = to_trash(acc.path)
        ACCOUNTS.pop(key, None)
        invalidate_peer_cache()
        logger.info(f"removed {key}: {moved} file(s) -> trash")
        await event.answer("Moved to trash")
        return await show_remove_list(event, 1)

    if data == "trash_menu":
        return await show_trash(event)

    if data == "trash_restore":
        n = restore_from_trash()
        await event.answer(f"{n} restored")
        if n:
            await load_all_sessions()
            await setup_channel_monitors()
        return await show_trash(event)

    if data == "trash_clear_ask":
        entries = trash_entries()
        if not entries:
            await event.answer("Trash is already empty")
            return await show_trash(event)
        return await safe_edit(event, card(E_WARN, "Clear Trash Permanently", [
            field(E_TRASH, "Files to delete", str(len(entries))),
            "",
            f"{E_CROSS} {S('This permanently deletes ALL trashed session files.')}",
            f"{E_SHIELD} {S('This cannot be undone. Deleted sessions are gone forever.')}",
        ]), [[btn("Yes, delete forever", "trash_clear_do", style="danger"),
              btn("Cancel", "trash_menu", icon="↩️")]])

    if data == "trash_clear_do":
        entries = trash_entries()
        deleted = 0
        for fname in entries:
            stem = fname[: -len(".session")]
            for ext in (".session", ".session-journal", ".session-wal",
                        ".session-shm", ".json"):
                p = os.path.join(TRASH_DIR, stem + ext)
                if os.path.exists(p):
                    try:
                        os.unlink(p)
                        deleted += 1
                    except Exception as e:
                        logger.warning(f"clear trash {p}: {e}")
        await event.answer(f"Deleted {deleted} file(s)")
        return await show_trash(event)

    if data == "scan_dead":
        await safe_edit(event, card(E_REFRESH, "Scanning Accounts",
                                    [S("Pinging all connected accounts — please wait...")]))
        dead_keys = await scan_dead_accounts()
        if not dead_keys:
            return await safe_edit(event, card(E_CHECK, "All Accounts Alive", [
                field(E_GREEN, "Accounts scanned", str(acc_count())),
                "",
                S("No frozen or dead account found."),
            ]), kb_nav("menu_accounts"))
        lines = [f"{E_RED} <code>{esc(k)}</code>" for k in dead_keys[:15]]
        if len(dead_keys) > 15:
            lines.append(f"<i>+ {len(dead_keys) - 15} more</i>")
        return await safe_edit(event, card(E_WARN, "Dead Accounts Found", lines + [
            "",
            field(E_RED, "Dead / frozen", str(len(dead_keys))),
            field(E_GREEN, "Still alive", str(acc_count() - len(dead_keys))),
            "",
            f"{E_SHIELD} {S('Use Remove All Dead to trash them.')}",
        ]), [[btn("Remove All Dead", "acc_rm_dead_ask", style="danger"),
              btn("Back", "menu_accounts", icon="⬅️")]])

    if data == "acc_stop_all_ask":
        return await safe_edit(event, card(E_WARN, "Stop All Accounts", [
            field(E_PERSON, "Accounts online", str(acc_count())),
            "",
            f"{S('Disconnect every account from Telegram.')}",
            f"{E_SHIELD} {S('Session files are NOT touched. Reload to reconnect.')}",
        ]), [[btn("Yes, stop all", "acc_stop_all_do", style="danger"),
              btn("Cancel", "menu_accounts", icon="↩️")]])

    if data == "acc_stop_all_do":
        keys = list(acc_keys())
        for key in keys:
            await disconnect_account(key)
        ACCOUNTS.clear()
        PROBLEM_SESSIONS.clear()
        await setup_channel_monitors()
        await event.answer(f"Stopped {len(keys)} account(s)")
        return await show_accounts_menu(event)

    if data == "acc_rm_dead_ask":
        dead_keys = [k for k, a in ACCOUNTS.items() if a.state == "dead"]
        if not dead_keys:
            await event.answer("No dead accounts found. Run Scan Dead first.", alert=True)
            return await show_accounts_menu(event)
        return await safe_edit(event, card(E_WARN, "Remove All Dead Accounts", [
            field(E_RED, "Dead accounts to remove", str(len(dead_keys))),
            "",
            f"{E_SHIELD} {S('Session files move to trash — not permanently deleted.')}",
        ]), [[btn("Yes, trash them all", "acc_rm_dead_do", style="danger"),
              btn("Cancel", "menu_accounts", icon="↩️")]])

    if data == "acc_rm_dead_do":
        dead_keys = [k for k, a in ACCOUNTS.items() if a.state == "dead"]
        removed = 0
        for key in dead_keys:
            acc = ACCOUNTS.get(key)
            if not acc:
                continue
            await disconnect_account(key)
            to_trash(acc.path)
            ACCOUNTS.pop(key, None)
            removed += 1
        invalidate_peer_cache()
        await setup_channel_monitors()
        await event.answer(f"Trashed {removed} dead account(s)")
        return await show_accounts_menu(event)

    if data == "sync_onboard":
        # Manual version of the automatic onboarding: join every online account
        # to every active client channel it is not already in.
        await safe_edit(event, card(E_REFRESH, "Syncing Accounts", [
            S("Checking which accounts are missing from client channels..."),
        ]))
        missing = await accounts_missing_from_subs()
        if not missing:
            return await safe_edit(event, card(E_CHECK, "Already Synced", [
                field(E_PERSON, "Accounts online", str(acc_count())),
                "",
                S("Every account is already in every active client channel."),
            ]), kb_nav("menu_accounts"))
        await safe_edit(event, card(E_REFRESH, "Syncing Accounts", [
            field(E_PERSON, "Accounts to join", str(len(missing))),
            "",
            S("Joining slowly to avoid flood limits. You will get a summary "
              "when it finishes."),
        ]), kb_nav("menu_accounts"))
        asyncio.create_task(onboard_new_accounts(missing, notify=event.chat_id))
        return

    if data == "reload_sessions":
        await safe_edit(event, card(E_REFRESH, "Reloading",
                                    [S("Reading the sessions folder...")]))
        tally = await load_all_sessions()
        await setup_channel_monitors()
        schedule_onboarding(await accounts_missing_from_subs(),
                            notify=event.chat_id)
        return await safe_edit(event, card(E_CHECK, "Reload Complete", [
            field(E_GREEN, "Online", str(acc_count())),
            field(E_RED, "Not authorised", str(tally["dead"])),
            field(E_WARN, "Unclear", str(tally["unknown"])),
        ], footer=f"{E_SHIELD} {S('No file was deleted.')}"),
            kb_nav("menu_accounts"))

    if data == "add":
        return await safe_edit(event, card(E_PLUS, "Add Account", [
            S("Log in with a phone number, or paste a Telethon string session."),
            "",
            f"{E_SHIELD} {S('Either way the login is saved into the sessions folder.')}",
        ]), [[btn("Phone Login", "add_phone", icon="📱"),
              btn("String Session", "add_string", icon="🔑")],
             [btn("Back", "menu_accounts", icon="⬅️")]])

    if data == "add_phone":
        task_states[event.chat_id] = {"type": "login", "step": "phone"}
        return await safe_edit(event, card(E_PHONE, "Phone Login", [
            S("Send the phone number with country code."),
            "",
            f"{S('Example')}: <code>+919876543210</code>",
        ]), [[btn("Cancel", "menu_accounts", icon="↩️")]])

    if data == "add_string":
        task_states[event.chat_id] = {"type": "login", "step": "string"}
        return await safe_edit(event, card(E_LOCK, "String Session", [
            S("Send a Telethon string session."),
        ]), [[btn("Cancel", "menu_accounts", icon="↩️")]])

    if data == "import_zip":
        task_states[event.chat_id] = {"type": "import_zip", "step": "wait_file"}
        return await safe_edit(event, card(E_GIFT, "Import ZIP", [
            S("Send a ZIP containing any of:"),
            f"{DOT} <code>*.session</code> {S('files')}",
            f"{DOT} <code>*.json</code> {S('sidecars (api_id, device)')}",
            f"{DOT} <code>string.txt</code> {S('with one session per line')}",
            "",
            f"{E_SHIELD} <b>{S('Keep the .json files')}</b> — "
            f"{S('they carry the api_id and device the session was made with. Without them Telegram can revoke the session in a day or two.')}",
        ]), [[btn("Cancel", "menu_accounts", icon="↩️")]])

    # ── clients ───────────────────────────────────────────────
    if data == "create_client":
        if acc_count() == 0:
            return await event.answer("No accounts online. Add accounts first.",
                                      alert=True)
        task_states[event.chat_id] = {"type": "create_client", "step": "user_id"}
        return await safe_edit(event, card(E_CROWN, "Create Client", [
            f"<b>{S('Step 1 of 6')}</b>",
            "",
            S("Send the client's Telegram user ID (numbers only)."),
            "",
            f"{E_BELL} {S('The client can get it from')} /id",
            E_CROWN + " " + S("Same ID again adds another channel for that "
                              "client, kept as a separate subscription."),
        ]), [[btn("Cancel", "home", icon="↩️")]])

    if data == "manage_clients" or data.startswith("mc_page_"):
        page = int(data.rsplit("_", 1)[1]) if data.startswith("mc_page_") else 1
        return await show_clients(event, page)

    # One client's channel list, and its pager (prefix is "cu<user id>").
    if data.startswith("cuser_"):
        return await show_client_channels(event, int(data[len("cuser_"):]))

    if data.startswith("cu") and "_page_" in data:
        uid, _, pg = data[2:].partition("_page_")
        return await show_client_channels(event, int(uid), int(pg))

    # Sell the same client another channel: same user id, fresh package.
    if data.startswith("cadd_"):
        if acc_count() == 0:
            return await event.answer("No accounts online. Add accounts first.",
                                      alert=True)
        uid = int(data[len("cadd_"):])
        task_states[event.chat_id] = {"type": "create_client",
                                      "step": "channel_link",
                                      "client_user_id": uid}
        subs = await get_subs_for_user(uid)
        name = subs[0].get("client_name", "Unknown") if subs else "Unknown"
        return await safe_edit(event, card(E_CHANNEL, "Add Channel", [
            field(E_PERSON, "Client", esc(str(name))),
            f"{E_STAR} {S('User ID')}: <code>{uid}</code>",
            field(E_CROWN, "Channels already", str(len(subs))),
            "",
            S("Send the link of the new channel."),
            "",
            f"{DOT} <code>https://t.me/channel</code>",
            f"{DOT} <code>https://t.me/+inviteHash</code>",
            "",
            E_SHIELD + " " + S("This becomes a separate subscription with its "
                               "own package and expiry."),
        ]), [[btn("Cancel", f"cuser_{uid}", icon="↩️")]])

    if data.startswith("client_"):
        return await show_client_detail(event, data[len("client_"):])

    if data.startswith("stop_"):
        cid = data[len("stop_"):]
        await update_client_status(cid, "stopped")
        await setup_channel_monitors()
        await event.answer("Paused")
        return await show_client_detail(event, cid)

    if data.startswith("start_"):
        cid = data[len("start_"):]
        await update_client_status(cid, "active")
        await setup_channel_monitors()
        await event.answer("Resumed")
        return await show_client_detail(event, cid)

    if data.startswith("delask_"):
        cid = data[len("delask_"):]
        doc = await get_client_doc(cid)
        if not doc:
            return await event.answer("Not found", alert=True)
        return await safe_edit(event, card(E_WARN, "Confirm Delete", [
            field(E_PERSON, "Client", esc(str(doc.get("client_name", "Unknown")))),
            field(E_CHANNEL, "Channel", esc(str(doc["channel_link"]))),
            "",
            f"{E_TRASH} {S('The subscription is deleted and every joined account leaves the channel.')}",
        ]), [[btn("Yes, delete", f"del_{cid}", icon="🚫"),
              btn("Cancel", f"client_{cid}", icon="↩️")]])

    if data.startswith("del_"):
        cid = data[len("del_"):]
        await safe_edit(event, card(E_REFRESH, "Deleting",
                                    [S("Leaving the channel from every account...")]))
        await delete_client_subscription(cid)
        await event.answer("Deleted")
        return await show_clients(event, 1)

    if data.startswith("ext_"):
        cid = data[len("ext_"):]
        doc = await get_client_doc(cid)
        if not doc:
            return await event.answer("Not found", alert=True)
        task_states[event.chat_id] = {"type": "extend_client",
                                      "client_id": cid, "step": "days"}
        return await safe_edit(event, card(E_CAL, "Extend Subscription", [
            field(E_PERSON, "Client", esc(str(doc.get("client_name", "Unknown")))),
            field(E_CAL, "Current expiry",
                  doc["expires_at"].strftime("%d %b %Y %H:%M")),
            "",
            S("Send how many days to add."),
        ]), [[btn("Cancel", f"client_{cid}", icon="↩️")]])

    # The two fixed upg_* actions must be matched before the upg_<id> prefix,
    # or "keep"/"apply" get read as an ObjectId and the wizard dead-ends.
    if data == "upg_keep":
        st = task_states.get(event.chat_id)
        if not st or st.get("type") != "upgrade_client":
            return await event.answer("This upgrade expired — open the client again",
                                      alert=True)
        return await upgrade_advance(event, st, keep=True)

    if data == "upg_apply":
        st = task_states.get(event.chat_id)
        if not st or st.get("type") != "upgrade_client":
            return await event.answer("This upgrade expired — open the client again",
                                      alert=True)
        return await apply_upgrade(event, st)

    if data.startswith("upg_"):
        cid = data[len("upg_"):]
        doc = await get_client_doc(cid)
        if not doc:
            return await event.answer("Not found", alert=True)
        task_states[event.chat_id] = {"type": "upgrade_client", "client_id": cid,
                                      "step": "accounts", "doc": doc}
        return await safe_edit(event, upgrade_prompt(doc, "accounts"),
                               [[btn("Keep same", "upg_keep", icon="⏭"),
                                 btn("Cancel", f"client_{cid}", icon="↩️")]])

    # ── tasks ─────────────────────────────────────────────────
    if data == "join":
        if acc_count() == 0:
            return await event.answer("No accounts online", alert=True)
        task_states[event.chat_id] = {"type": "join", "step": "link"}
        return await safe_edit(event, card(E_CHANNEL, "Join Channels", [
            S("Send channel links, one per line (max 50)."),
            "",
            f"{DOT} <code>https://t.me/channel</code>",
            f"{DOT} <code>https://t.me/+inviteHash</code>",
        ]), [[btn("Cancel", "home", icon="↩️")]])

    if data in ("react_view", "multi_react"):
        if acc_count() == 0:
            return await event.answer("No accounts online", alert=True)
        task_states[event.chat_id] = {"type": data, "step": "link"}
        title = "Multi React" if data == "multi_react" else "React + View"
        return await safe_edit(event, card(E_FIRE, title, [
            S("Send post links, one per line (max 50)."),
            "",
            f"{DOT} <code>https://t.me/channel/123</code>",
            f"{DOT} <code>https://t.me/c/1234567890/123</code>",
        ]), [[btn("Cancel", "home", icon="↩️")]])

    if data == "views_menu":
        return await safe_edit(event, card(E_EYE, "Real Views", [
            S("Increment the real view counter of a post."),
            "",
            f"{E_PAGE} <b>{S('Single')}</b> — {S('one post, choose how many accounts')}",
            f"{E_CHART} <b>{S('Batch')}</b> — {S('up to 20 posts from every account')}",
        ]), [[btn("Single Post", "views_single", icon="📄"),
              btn("Batch Posts", "views_batch", icon="📚")],
             [btn("Home", "home", icon="🏠")]])

    if data == "views_single":
        if acc_count() == 0:
            return await event.answer("No accounts online", alert=True)
        task_states[event.chat_id] = {"type": "views", "subtype": "single",
                                      "step": "link"}
        return await safe_edit(event, card(E_EYE, "Single Post Views", [
            S("Send the post link."),
            "",
            f"{DOT} <code>https://t.me/channel/123</code>",
            f"{DOT} <code>https://t.me/c/1234567890/123</code>",
            "",
            f"{E_WARN} {S('For a private channel the accounts must already be members.')}",
        ]), [[btn("Cancel", "views_menu", icon="↩️")]])

    if data == "views_batch":
        if acc_count() == 0:
            return await event.answer("No accounts online", alert=True)
        task_states[event.chat_id] = {"type": "views", "subtype": "batch",
                                      "step": "links"}
        return await safe_edit(event, card(E_CHART, "Batch Views", [
            S("Send up to 20 post links, one per line."),
            "",
            f"{E_SPARKLE} {S('Every online account will view every post.')}",
        ]), [[btn("Cancel", "views_menu", icon="↩️")]])

    if data == "leave":
        if acc_count() == 0:
            return await event.answer("No accounts online", alert=True)
        return await safe_edit(event, card(E_TRASH, "Leave Chats", [
            f"{E_CHANNEL} <b>{S('Leave one')}</b> — {S('a single channel')}",
            f"{E_WARN} <b>{S('Leave all')}</b> — {S('every channel and group')}",
        ]), [[btn("Leave One", "leave_specific", icon="📤")],
             [btn("Leave All", "leave_all", icon="⚠️")],
             [btn("Home", "home", icon="🏠")]])

    if data == "leave_specific":
        task_states[event.chat_id] = {"type": "leave_specific", "step": "link"}
        return await safe_edit(event, card(E_TRASH, "Leave One Channel", [
            S("Send the channel link to leave."),
        ]), [[btn("Cancel", "leave", icon="↩️")]])

    if data == "leave_all":
        return await safe_edit(event, card(E_WARN, "Danger Zone", [
            f"{S('Every one of')} <b>{acc_count()}</b> "
            f"{S('accounts will leave every channel and group.')}",
            "",
            f"{E_CROSS} {S('This cannot be undone.')}",
        ]), [[btn("I am sure", "exec_leave_all", icon="🚫")],
             [btn("Cancel", "leave", icon="↩️")]])

    if data == "exec_leave_all":
        await safe_edit(event, card(E_REFRESH, "Working", [S("Starting...")]))
        return await execute_leave_all(event)

    # ── quantity pickers ──────────────────────────────────────
    if data.startswith("qty_") or data.startswith("vqty_"):
        return await handle_qty(event, data)

    # ── live stream audio ─────────────────────────────────────
    if data == "audio_menu":
        return await show_audio_menu(event)

    if data == "audio_set":
        task_states[event.chat_id] = {"type": "set_audio"}
        return await safe_edit(event, card(E_ROCKET, "Set Live Audio", [
            S("Send an MP3 file now."),
            "",
            f"{E_BELL} {S('Every account plays this in a live stream.')}",
            f"{E_REFRESH} {S('It repeats forever, so a long file is fine.')}",
        ]), [[btn("Cancel", "audio_menu", icon="↩️")]])

    if data == "audio_del":
        try:
            os.unlink(LIVE_AUDIO_PATH)
            await event.answer("Audio removed")
        except FileNotFoundError:
            await event.answer("No audio was set")
        except Exception as e:
            await event.answer(str(e)[:100], alert=True)
        return await show_audio_menu(event)

    if data == "go_live":
        if not TGCALLS_OK:
            return await event.answer(f"py-tgcalls missing: {TGCALLS_ERR}"[:180],
                                      alert=True)
        if not os.path.exists(LIVE_AUDIO_PATH):
            return await event.answer("Set an audio file first", alert=True)
        if acc_count() == 0:
            return await event.answer("No accounts online", alert=True)
        task_states[event.chat_id] = {"type": "go_live", "step": "link"}
        return await safe_edit(event, card(E_ROCKET, "Go Live", [
            S("Send the channel link whose live stream to join."),
            "",
            f"{DOT} <code>https://t.me/channel</code>",
            f"{DOT} <code>https://t.me/+inviteHash</code>",
            "",
            f"{E_WARN} {S('The stream must already be running.')}",
        ]), [[btn("Cancel", "home", icon="↩️")]])

    if data == "live_now":
        return await show_live_now(event)

    if data == "live_cap":
        return await show_live_cap(event)

    if data.startswith("live_cap_"):
        await save_live_cap(int(data[len("live_cap_"):]))
        await event.answer(f"Limit: {LIVE_CAP or 'unlimited'}")
        return await show_live_cap(event)

    if data == "live_rot":
        return await show_live_rotation(event)

    if data.startswith("live_rot_"):
        await save_live_rotate(int(data[len("live_rot_"):]))
        await event.answer(f"Rotation: {LIVE_ROTATE // 60}m" if LIVE_ROTATE
                           else "Rotation off")
        return await show_live_rotation(event)

    # Swap a call's accounts right now instead of waiting for the timer.
    if data.startswith("live_rotnow_"):
        chat = int(data[len("live_rotnow_"):])
        sess = LIVE_SESSIONS.get(chat)
        if sess is None:
            return await event.answer("That stream is no longer active", alert=True)
        sess["rotated"] = time.monotonic()
        await event.answer("Rotating...")
        out, into = await rotate_live_accounts(chat)
        if not out:
            await event.answer("No rested account in the pool to swap in",
                               alert=True)
        return await show_live_now(event)

    if data == "live_stop_all":
        n = 0
        for chat, keys in list(LIVE_AUDIO.items()):
            for key in list(keys):
                await stop_live_audio(key, chat)
                n += 1
            end_live_session(chat)
        await event.answer(f"Stopped {n} stream(s)")
        return await show_audio_menu(event)

    # Pull every account out of one specific call, leaving other calls running.
    if data.startswith("live_stop_"):
        chat = int(data[len("live_stop_"):])
        keys = list(LIVE_AUDIO.get(chat, set()))
        for key in keys:
            await stop_live_audio(key, chat)
        end_live_session(chat)
        await event.answer(f"{len(keys)} account(s) left the stream")
        return await show_live_now(event)

    # ── access ────────────────────────────────────────────────
    if data == "approve":
        users = await get_approved_users()
        lines = [f"{E_PERSON} <code>{u['user_id']}</code>" for u in users[:12]] \
            or [S("No approved users yet.")]
        if len(users) > 12:
            lines.append(f"<i>+ {len(users) - 12} {S('more')}</i>")
        return await safe_edit(event, card(E_SHIELD, "Access Control", lines + [
            "",
            f"{E_PLUS} <code>/add 123456789</code>",
            f"{E_CROSS} <code>/remove 123456789</code>",
            f"{E_PAGE} <code>/list</code>",
        ]), [[btn("Refresh", "approve", icon="🔄")],
             [btn("Home", "home", icon="🏠")]])

    await event.answer("Unknown button — going home", alert=True)
    await show_menu(event, uid)


# ═══════════════════════ QUANTITY PICKER ═══════════════════════
def qty_buttons(prefix: str, total: int, cancel: str = "home") -> list:
    choices = [n for n in (10, 25, 50, 100, 250, 500) if n < total]
    rows, row = [], []
    for n in choices:
        row.append(Button.inline(str(n), f"{prefix}_{n}".encode()))
        if len(row) == 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([Button.inline(f"⚡ ALL ({total})", f"{prefix}_all".encode())])
    rows.append([btn("Custom", f"{prefix}_custom", icon="🔢"),
                 btn("Cancel", cancel, icon="↩️")])
    return rows


def qty_prompt(total: int) -> str:
    return card(E_PERSON, "How Many Accounts", [
        field(E_GREEN, "Online now", str(total)),
        "",
        S("Pick a preset or enter a custom number."),
    ])


async def handle_qty(event, data):
    state = task_states.get(event.chat_id)
    if not state:
        await event.answer("This task expired", alert=True)
        return await show_menu(event, event.sender_id)

    prefix, _, choice = data.rpartition("_")
    total = acc_count()

    if choice == "custom":
        state["step"] = "custom_qty"
        return await safe_edit(event, card(E_TARGET, "Custom Amount", [
            f"{S('Send a number between')} <b>1</b> {S('and')} <b>{total}</b>.",
        ]), [[btn("Cancel", "home", icon="↩️")]])

    count = total if choice == "all" else min(int(choice), total)
    try:
        await event.delete()
    except Exception:
        pass
    task_states.pop(event.chat_id, None)
    await run_task(event, state, count)


async def run_task(event, state, count):
    t = state["type"]
    if t == "join":
        await execute_join(event, state, count)
    elif t == "react_view":
        await execute_react_view(event, state, count, multi=False)
    elif t == "multi_react":
        await execute_react_view(event, state, count, multi=True)
    elif t == "views":
        await execute_views(event, state["links"], count)


# ═══════════════════════ UPGRADE FLOW ═══════════════════════
UPG_STEPS = [
    ("accounts", "accounts_count", E_PERSON, "Accounts", "Step 1 of 5"),
    ("reactions", "reactions_per_post", E_THUMB, "Reactions per post", "Step 2 of 5"),
    ("views", "views_per_post", E_EYE, "Views per post", "Step 3 of 5"),
    ("livestream", "livestream_accounts", E_ROCKET, "Live stream accounts", "Step 4 of 5"),
    ("days", "subscription_days", E_CAL, "Extra days", "Step 5 of 5"),
]
KEEP_TOKENS = {"-", ".", "0", "skip", "same", "keep"}


def upgrade_prompt(doc: dict, step: str) -> str:
    for key, dbfield, icon, label, pos in UPG_STEPS:
        if key != step:
            continue
        current = doc.get(dbfield, 0)
        extra = []
        if key == "livestream":
            extra = [f"{E_ROCKET} {S('How many accounts join live streams automatically.')}",
                     f"{E_BELL} {S('0 means live stream join is off.')}"]
        if key == "days":
            extra = [f"{E_CAL} {S('Current expiry')}: "
                     f"<b>{doc['expires_at'].strftime('%d %b %Y')}</b>",
                     f"{S('Send how many days to')} <b>{S('add')}</b>."]
        # Every step shows the online total so the number can be chosen here
        # instead of backing out to the home screen to look it up.
        head = [f"{S(pos)} {DOT} {S(label)}",
                "",
                field(E_GREEN, "Accounts online", str(acc_count())),
                field(icon, "Current", str(current))]
        return card(icon, "Upgrade Package", head + extra + [
            "",
            f"{S('Send a new number, or')} <code>+5</code> {S('to add to the current value.')}",
            f"{E_SHIELD} {S('Send')} <code>-</code> {S('or press Keep same to leave it as is.')}",
        ])
    return card(E_WARN, "Upgrade", [S("Unknown step.")])


def parse_upgrade_value(text: str, current: int) -> Optional[int]:
    """Absolute (`50`), relative (`+10`) or keep (`-`). None means invalid."""
    t = text.strip().lower()
    if t in KEEP_TOKENS:
        return current
    if t.startswith("+") and t[1:].isdigit():
        return current + int(t[1:])
    if t.isdigit():
        return int(t)
    return None


async def upgrade_advance(event, state, keep=False, value=None):
    """Move the upgrade wizard to the next step (or to the confirmation)."""
    order = [s[0] for s in UPG_STEPS]
    step = state["step"]
    idx = order.index(step)
    dbfield = UPG_STEPS[idx][1]
    doc = state["doc"]

    if keep:
        value = doc.get(dbfield, 0) if step != "days" else 0
    state.setdefault("new", {})[step] = value

    if idx + 1 < len(order):
        nxt = order[idx + 1]
        state["step"] = nxt
        cid = state["client_id"]
        return await respond_or_edit(
            event, upgrade_prompt(doc, nxt),
            [[btn("Keep same", "upg_keep", icon="⏭"),
              btn("Cancel", f"client_{cid}", icon="↩️")]])

    state["step"] = "confirm"
    return await respond_or_edit(event, upgrade_summary(state), [
        [btn("Apply Upgrade", "upg_apply", icon="✅")],
        [btn("Cancel", f"client_{state['client_id']}", icon="↩️")],
    ])


def upgrade_summary(state) -> str:
    doc, new = state["doc"], state["new"]
    lines = [field(E_PERSON, "Client", esc(str(doc.get("client_name", "Unknown")))),
             field(E_CHANNEL, "Channel", esc(str(doc["channel_link"]))), ""]
    changed = False
    for key, dbfield, icon, label, _ in UPG_STEPS:
        old = int(doc.get(dbfield, 0) or 0)
        if key == "days":
            add = int(new.get("days", 0) or 0)
            if add:
                changed = True
                newexp = (max(doc["expires_at"], utcnow())
                          + datetime.timedelta(days=add))
                lines.append(f"{icon} {S('Days')}: <b>+{add}</b> "
                             f"{DOT} {S('expires')} "
                             f"<b>{newexp.strftime('%d %b %Y')}</b>")
            else:
                lines.append(f"{icon} {S('Days')}: <i>{S('unchanged')}</i>")
            continue
        val = int(new.get(key, old) or 0)
        if val != old:
            changed = True
            lines.append(f"{icon} {S(label)}: <s>{old}</s> ➜ <b>{val}</b>")
        else:
            lines.append(f"{icon} {S(label)}: <b>{old}</b> <i>({S('unchanged')})</i>")

    n_acc = int(new.get("accounts", doc.get("accounts_count", 0)) or 0)
    n_react = int(new.get("reactions", doc.get("reactions_per_post", 0)) or 0)
    n_views = int(new.get("views", doc.get("views_per_post", 0)) or 0)
    n_live = int(new.get("livestream", doc.get("livestream_accounts", 0)) or 0)
    warn = []
    need = max(n_react, n_views)
    if need > n_acc:
        warn.append(f"{E_WARN} {S('Accounts raised to')} <b>{need}</b> "
                    f"{S('so reactions and views fit.')}")
    if need > acc_count():
        warn.append(f"{E_WARN} {S('Only')} <b>{acc_count()}</b> "
                    f"{S('accounts are online — the rest cannot join yet.')}")
    if not changed:
        warn.append(f"{E_CROSS} {S('Nothing would change.')}")

    return card(E_ROCKET, "Confirm Upgrade", lines + ([""] + warn if warn else []),
                footer=f"{E_SHIELD} {S('Nothing is saved until you press Apply.')}")


async def apply_upgrade(event, state):
    doc = state["doc"]
    new = state["new"]
    cid = state["client_id"]

    n_acc = int(new.get("accounts", doc.get("accounts_count", 0)) or 0)
    n_react = int(new.get("reactions", doc.get("reactions_per_post", 0)) or 0)
    n_views = int(new.get("views", doc.get("views_per_post", 0)) or 0)
    n_live = int(new.get("livestream", doc.get("livestream_accounts", 0)) or 0)
    add_days = int(new.get("days", 0) or 0)
    # Reactions/views can never exceed the account count, so raise it to fit
    # instead of silently capping the package the customer paid for.
    n_acc = max(n_acc, n_react, n_views)

    if n_react == 0 and n_views == 0:
        return await respond_or_edit(event, card(E_CROSS, "Invalid Package", [
            S("Reactions and views cannot both be zero."),
        ]), [[btn("Back to Client", f"client_{cid}", icon="⬅️")]])

    await respond_or_edit(event, card(E_REFRESH, "Applying Upgrade",
                                       [S("Updating package and joining accounts...")]))

    update = {
        "accounts_count": n_acc,
        "reactions_per_post": n_react,
        "views_per_post": n_views,
        "livestream_accounts": n_live,
        "updated_at": utcnow(),
    }
    if add_days:
        base = max(doc["expires_at"], utcnow())
        update["expires_at"] = base + datetime.timedelta(days=add_days)
        update["subscription_days"] = int(doc.get("subscription_days", 0) or 0) + add_days
        update["status"] = "active"

    # Join any extra accounts the bigger package needs.
    joined = list(doc.get("joined_accounts", []))
    newly = 0
    if n_acc > len(joined):
        spec = doc.get("channel_username")
        # Least-busy first, same reason as pick_accounts(): acc_keys() order
        # would hand every upgrade the same front-of-the-list sessions.
        load = await account_load()
        candidates = [k for k in joinable_keys()
                      if k not in joined
                      and load.get(k, 0) < MAX_CHANNELS_PER_ACCOUNT]
        random.shuffle(candidates)
        candidates.sort(key=lambda k: load.get(k, 0))
        need = n_acc - len(joined)
        sem = asyncio.Semaphore(JOIN_CONCURRENCY)

        async def join_one(key):
            nonlocal newly
            async with sem:
                client = acc_client(key)
                if not client:
                    return
                try:
                    if spec:
                        await client(JoinChannelRequest(spec))
                    elif doc.get("channel_type") == "private":
                        _, invite = parse_target(doc["channel_link"])
                        await client(ImportChatInviteRequest(invite))
                    else:
                        await client(JoinChannelRequest(
                            channel_peer(doc["channel_id"])))
                    joined.append(key)
                    newly += 1
                    await log_activity(key, "CLIENT_JOIN", doc["channel_link"],
                                       "upgrade")
                except UserAlreadyParticipantError:
                    joined.append(key)
                    newly += 1
                except Exception as e:
                    if is_frozen_account_error(e):
                        mark_frozen(key)
                    await log_activity(key, "CLIENT_JOIN", doc["channel_link"],
                                       str(e)[:40])
                await asyncio.sleep(0.4)

        await asyncio.gather(*(join_one(k) for k in candidates[:need]))
        update["joined_accounts"] = joined

    await col_clients.update_one({"_id": ObjectId(cid)}, {"$set": update})
    task_states.pop(event.chat_id, None)
    invalidate_peer_cache()
    await setup_channel_monitors()

    fresh = await get_client_doc(cid)
    receipt = card(E_CHECK, "Package Upgraded", [
        field(E_PERSON, "Client", esc(str(fresh.get("client_name", "Unknown")))),
        field(E_CHANNEL, "Channel", esc(str(fresh["channel_link"]))),
        "",
        field(E_PERSON, "Accounts", str(fresh["accounts_count"])),
        field(E_THUMB, "Reactions / post", str(fresh["reactions_per_post"])),
        field(E_EYE, "Views / post", str(fresh["views_per_post"])),
        field(E_ROCKET, "Live stream accounts", str(fresh.get("livestream_accounts", 0))),
        field(E_CAL, "Expires", fresh["expires_at"].strftime("%d %b %Y %H:%M")),
        field(E_PLUS, "Newly joined", str(newly)),
    ], footer=f"{E_SPARKLE} {S('The new package is live from the next post.')}")
    await respond_or_edit(event, receipt, [
        [btn("Back to Client", f"client_{cid}", icon="⬅️"),
         btn("Home", "home", icon="🏠")]])

    try:
        await bot.send_message(int(fresh["client_user_id"]), card(
            E_GIFT, "Package Upgraded", [
                field(E_CHANNEL, "Channel", esc(str(fresh["channel_link"]))),
                field(E_THUMB, "Reactions / post", str(fresh["reactions_per_post"])),
                field(E_EYE, "Views / post", str(fresh["views_per_post"])),
                field(E_ROCKET, "Live stream accounts", str(fresh.get("livestream_accounts", 0))),
                field(E_CAL, "Expires", fresh["expires_at"].strftime("%d %b %Y")),
            ], footer=f"{E_HEART} {S('Thank you!')}"))
    except Exception as e:
        logger.info(f"could not notify client: {e}")


async def respond_or_edit(event, text, buttons=None):
    if hasattr(event, "edit") and getattr(event, "query", None) is not None:
        return await safe_edit(event, text, buttons)
    try:
        return await event.respond(text, buttons=buttons)
    except Exception:
        return await safe_edit(event, text, buttons)


# ═══════════════════════ MESSAGE / CONVERSATION HANDLER ═══════════════════════
@bot.on(events.NewMessage(incoming=True))
async def on_message(event):
    if not event.is_private:
        return
    if not await is_user_approved(event.sender_id):
        return

    chat_id = event.chat_id
    state = task_states.get(chat_id)

    # a ZIP can arrive with no caption, so check documents before the text FSM
    if event.file and (event.file.name or "").lower().endswith(".zip"):
        if state and state.get("type") == "import_zip":
            task_states.pop(chat_id, None)
            return await handle_zip_import(event)
        return await event.respond(card(E_WARN, "Unexpected File", [
            S("Press Import ZIP first, then send the file."),
        ]), buttons=[[btn("Import ZIP", "import_zip", icon="📦")]])

    # Photo upload for acc_photo_set flow
    if event.photo and state and state.get("type") == "acc_photo_set":
        key = state["key"]
        acc = ACCOUNTS.get(key)
        task_states.pop(chat_id, None)
        if not acc or not acc.client:
            return await event.respond(card(E_CROSS, "Account offline", []), buttons=kb_nav("list_accounts"))
        try:
            tmp = tempfile.mktemp(suffix=".jpg")
            await event.download_media(tmp)
            uploaded = await acc.client.upload_file(tmp)
            await acc.client(UploadProfilePhotoRequest(file=uploaded))
            try:
                os.unlink(tmp)
            except Exception:
                pass
            return await event.respond(card(E_CHECK, "Photo Updated", [
                field(E_PHONE, "Account", esc(acc.label)),
                "",
                S("Profile photo has been changed."),
            ]), buttons=[[btn("Back to Account", f"acc_mgmt_{key}", icon="⬅️")]])
        except Exception as e:
            return await event.respond(card(E_CROSS, "Photo Failed", [
                f"<code>{esc(str(e)[:200])}</code>",
            ]), buttons=[[btn("Back", f"acc_mgmt_{key}", icon="⬅️")]])

    # Audio upload for the live-stream file
    if state and state.get("type") == "set_audio" and (event.audio or event.file):
        name = (event.file.name or "").lower() if event.file else ""
        if not (event.audio or name.endswith((".mp3", ".m4a", ".ogg", ".wav"))):
            return await event.respond(card(E_WARN, "Not Audio", [
                S("Send an MP3 file."),
            ]), buttons=[[btn("Cancel", "audio_menu", icon="↩️")]])
        task_states.pop(chat_id, None)
        msg = await event.respond(card(E_REFRESH, "Saving Audio",
                                        [S("Downloading...")]))
        try:
            os.makedirs(AUDIO_DIR, exist_ok=True)
            tmp = LIVE_AUDIO_PATH + ".part"
            await event.download_media(tmp)
            # Only replace the live file once the download finished, so a
            # failed transfer cannot leave accounts with a truncated track.
            os.replace(tmp, LIVE_AUDIO_PATH)
            size = os.path.getsize(LIVE_AUDIO_PATH) / (1024 * 1024)
        except Exception as e:
            return await msg.edit(card(E_CROSS, "Save Failed", [
                f"<code>{esc(str(e)[:200])}</code>",
            ]), buttons=[[btn("Back", "audio_menu", icon="⬅️")]])
        return await msg.edit(card(E_CHECK, "Audio Saved", [
            field(E_PAGE, "Size", f"{size:.1f} MB"),
            field(E_REFRESH, "Repeat", S("loops forever")),
            "",
            S("Accounts will play this in every live stream."),
        ]), buttons=[[btn("Go Live", "go_live", icon="🎤")],
                     [btn("Audio Menu", "audio_menu", icon="🎵"),
                      btn("Home", "home", icon="🏠")]])

    text = (event.raw_text or "").strip()
    if not state:
        return
    if text.startswith("/"):
        # a command mid-flow means the user walked away from it
        task_states.pop(chat_id, None)
        return

    try:
        await route_message(event, state, text)
    except Exception as e:
        logger.error(f"message flow {state.get('type')}: {e}", exc_info=True)
        task_states.pop(chat_id, None)
        await event.respond(card(E_CROSS, "Something Broke", [
            f"<code>{esc(str(e)[:200])}</code>",
        ]), buttons=kb_nav())


async def bad(event, msg: str, cancel="home"):
    await event.respond(card(E_WARN, "Try Again", [S(msg)]),
                        buttons=[[btn("Cancel", cancel, icon="↩️")]])


async def route_message(event, state, text):
    chat_id = event.chat_id
    t = state["type"]

    # The quantity step expects a button press, but typing a number there is the
    # obvious thing to do — accept it instead of silently ignoring the message.
    if state.get("step") == "qty" and t in ("join", "react_view", "multi_react",
                                            "views"):
        if text.isdigit():
            return await finish_custom_qty(event, state, text)
        return await bad(event, "Tap one of the buttons above, or send a number.")

    # ── 2FA change ────────────────────────────────────────────
    if t == "acc_2fa_set":
        key = state["key"]
        acc = ACCOUNTS.get(key)
        if not acc or not acc.client:
            task_states.pop(chat_id, None)
            return await event.respond(card(E_CROSS, "Account offline", []), buttons=kb_nav("list_accounts"))
        new_pwd = text.strip()
        remove = new_pwd == "-"
        try:
            if remove:
                await acc.client.edit_2fa(current_password=None, new_password="")
            else:
                await acc.client.edit_2fa(new_password=new_pwd)
            task_states.pop(chat_id, None)
            result = S("2FA password removed.") if remove else S("2FA password updated successfully.")
            return await event.respond(card(E_CHECK, "2FA Updated", [
                field(E_PHONE, "Account", esc(acc.label)),
                "",
                result,
            ]), buttons=[[btn("Back to Account", f"acc_mgmt_{key}", icon="⬅️")]])
        except Exception as e:
            task_states.pop(chat_id, None)
            return await event.respond(card(E_CROSS, "2FA Failed", [
                f"<code>{esc(str(e)[:200])}</code>",
            ]), buttons=[[btn("Back", f"acc_mgmt_{key}", icon="⬅️")]])

    # ── Name change ────────────────────────────────────────────
    if t == "acc_name_set":
        key = state["key"]
        acc = ACCOUNTS.get(key)
        if not acc or not acc.client:
            task_states.pop(chat_id, None)
            return await event.respond(card(E_CROSS, "Account offline", []), buttons=kb_nav("list_accounts"))
        parts = text.strip().split(None, 1)
        first_name = parts[0] if parts else text.strip()
        last_name = parts[1] if len(parts) > 1 else ""
        try:
            await acc.client(UpdateProfileRequest(first_name=first_name, last_name=last_name))
            acc.name = first_name
            task_states.pop(chat_id, None)
            return await event.respond(card(E_CHECK, "Name Updated", [
                field(E_PHONE, "Account", esc(acc.label)),
                field(E_PERSON, "New name", esc(f"{first_name} {last_name}".strip())),
            ]), buttons=[[btn("Back to Account", f"acc_mgmt_{key}", icon="⬅️")]])
        except Exception as e:
            task_states.pop(chat_id, None)
            return await event.respond(card(E_CROSS, "Name Change Failed", [
                f"<code>{esc(str(e)[:200])}</code>",
            ]), buttons=[[btn("Back", f"acc_mgmt_{key}", icon="⬅️")]])

    # ── login ─────────────────────────────────────────────────
    if t == "login":
        return await flow_login(event, state, text)

    # ── go live ───────────────────────────────────────────────
    if t == "go_live":
        return await flow_go_live(event, state, text)

    # ── create client ─────────────────────────────────────────
    if t == "create_client":
        return await flow_create_client(event, state, text)

    # ── extend ────────────────────────────────────────────────
    if t == "extend_client":
        if not text.isdigit() or int(text) < 1:
            return await bad(event, "Send a whole number of days.",
                             f"client_{state['client_id']}")
        doc = await extend_client_subscription(state["client_id"], int(text))
        task_states.pop(chat_id, None)
        if not doc:
            return await event.respond(card(E_CROSS, "Failed",
                                             [S("Client not found.")]),
                                       buttons=kb_nav("manage_clients"))
        await event.respond(card(E_CHECK, "Subscription Extended", [
            field(E_PERSON, "Client", esc(str(doc.get("client_name", "Unknown")))),
            field(E_PLUS, "Added", f"{text} {S('days')}"),
            field(E_CAL, "New expiry", doc["expires_at"].strftime("%d %b %Y %H:%M")),
        ]), buttons=[[btn("Back to Client", f"client_{state['client_id']}", icon="⬅️"),
                      btn("Home", "home", icon="🏠")]])
        try:
            await bot.send_message(int(doc["client_user_id"]), card(
                E_GIFT, "Subscription Extended", [
                    field(E_PLUS, "Added", f"{text} {S('days')}"),
                    field(E_CAL, "New expiry",
                          doc["expires_at"].strftime("%d %b %Y %H:%M")),
                ]))
        except Exception:
            pass
        return

    # ── upgrade ───────────────────────────────────────────────
    if t == "upgrade_client":
        step = state["step"]
        if step == "confirm":
            return await bad(event, "Press Apply Upgrade or Cancel.",
                             f"client_{state['client_id']}")
        dbfield = dict((s[0], s[1]) for s in UPG_STEPS)[step]
        current = 0 if step == "days" else int(state["doc"].get(dbfield, 0) or 0)
        val = parse_upgrade_value(text, current)
        if val is None or val < 0:
            return await bad(event, "Send a number, +N, or - to keep it.",
                             f"client_{state['client_id']}")
        return await upgrade_advance(event, state, value=val)

    # ── join ──────────────────────────────────────────────────
    if t == "join":
        if state["step"] == "link":
            valid = []
            for line in text.splitlines():
                ltype, target = parse_target(line)
                if ltype in ("public", "private"):
                    valid.append({"link": line.strip(), "type": ltype,
                                  "target": target})
            if not valid:
                return await bad(event, "No valid channel link found.")
            if len(valid) > 50:
                return await bad(event, "Maximum 50 links at a time.")
            state["links_data"] = valid
            state["step"] = "delay"
            return await event.respond(card(E_CHECK, "Links Accepted", [
                field(E_CHANNEL, "Links", str(len(valid))),
                "",
                S("Send the delay in seconds between each join (1-10 is safe)."),
            ]), buttons=[[btn("Cancel", "home", icon="↩️")]])

        if state["step"] == "delay":
            try:
                delay = float(text.replace(",", "."))
                if delay < 0:
                    raise ValueError
            except ValueError:
                return await bad(event, "Send a number like 2 or 1.5.")
            state["delay"] = delay
            state["step"] = "qty"
            return await event.respond(qty_prompt(acc_count()),
                                       buttons=qty_buttons("qty", acc_count()))

        if state["step"] == "custom_qty":
            return await finish_custom_qty(event, state, text)

    # ── react / multi react ───────────────────────────────────
    if t in ("react_view", "multi_react"):
        if state["step"] == "link":
            valid = [{"link": l.strip()} for l in text.splitlines()
                     if is_post_link(l)]
            if not valid:
                return await bad(event, "No valid post link found.")
            if len(valid) > 50:
                return await bad(event, "Maximum 50 posts at a time.")
            state["links_data"] = valid
            state["step"] = "emoji"
            if t == "react_view":
                body = [S("Send one emoji to react with."), "",
                        f"{S('Example')}: ❤️  🔥  👍  🎉"]
            else:
                body = [S("Send several emojis separated by commas."), "",
                        f"{S('Example')}: <code>❤️,🔥,👍</code>"]
            return await event.respond(card(E_HEART, "Choose Emoji",
                                             [field(E_PAGE, "Posts", str(len(valid))),
                                              ""] + body),
                                       buttons=[[btn("Cancel", "home", icon="↩️")]])

        if state["step"] == "emoji":
            if t == "react_view":
                if not text:
                    return await bad(event, "Send one emoji.")
                state["emoji"] = text.split()[0]
            else:
                emojis = [e.strip() for e in re.split(r"[,\s]+", text) if e.strip()]
                if not emojis:
                    return await bad(event, "Send at least one emoji.")
                state["emojis"] = emojis[:10]
            state["step"] = "speed"
            return await event.respond(card(E_CLOCK, "Speed", [
                S("Send the delay in seconds between accounts."),
                "",
                f"{S('Fast')}: <code>0.1</code>  {DOT}  {S('Safe')}: <code>0.5</code>",
            ]), buttons=[[btn("Cancel", "home", icon="↩️")]])

        if state["step"] == "speed":
            try:
                speed = float(text.replace(",", "."))
                if speed < 0:
                    raise ValueError
            except ValueError:
                return await bad(event, "Send a number like 0.2.")
            state["speed"] = speed
            state["step"] = "qty"
            return await event.respond(qty_prompt(acc_count()),
                                       buttons=qty_buttons("qty", acc_count()))

        if state["step"] == "custom_qty":
            return await finish_custom_qty(event, state, text)

    # ── views ─────────────────────────────────────────────────
    if t == "views":
        if state["step"] == "custom_qty":
            return await finish_custom_qty(event, state, text)

        if state["subtype"] == "single" and state["step"] == "link":
            if not is_post_link(text):
                return await bad(event, "That is not a valid post link.",
                                 "views_menu")
            state["links"] = [text.strip()]
            state["step"] = "qty"
            return await event.respond(qty_prompt(acc_count()),
                                       buttons=qty_buttons("qty", acc_count(),
                                                           "views_menu"))

        if state["subtype"] == "batch" and state["step"] == "links":
            links = [l.strip() for l in text.splitlines() if is_post_link(l)]
            if not links:
                return await bad(event, "No valid post link found.", "views_menu")
            if len(links) > 20:
                return await bad(event, "Maximum 20 posts at a time.", "views_menu")
            state["links"] = links
            state["step"] = "qty"
            return await event.respond(card(E_CHECK, "Posts Accepted", [
                field(E_PAGE, "Posts", str(len(links))),
            ]) + "\n\n" + qty_prompt(acc_count()),
                buttons=qty_buttons("qty", acc_count(), "views_menu"))

    # ── leave one ─────────────────────────────────────────────
    if t == "leave_specific":
        task_states.pop(chat_id, None)
        return await execute_leave_specific(event, text)


async def finish_custom_qty(event, state, text):
    if not text.isdigit() or int(text) < 1:
        return await bad(event, "Send a whole number of accounts.")
    count = min(int(text), acc_count())
    task_states.pop(event.chat_id, None)
    await run_task(event, state, count)


# ═══════════════════════ GO LIVE FLOW ═══════════════════════
async def flow_go_live(event, state, text):
    if state["step"] == "link":
        ltype, target = parse_target(text)
        if ltype not in ("public", "private"):
            return await bad(event, "That is not a valid channel link.", "home")
        state.update({"link": text.strip(), "ltype": ltype, "target": target,
                      "step": "count"})
        return await event.respond(card(E_PERSON, "Go Live", [
            field(E_CHANNEL, "Channel", esc(text.strip())),
            field(E_GREEN, "Accounts online", str(acc_count())),
            field(E_CHECK, "Free for live", str(len(free_live_keys(acc_keys())))),
            field(E_SHIELD, "Stream limit", cap_text()),
            field(E_REFRESH, "Rotation", rotate_text()),
            "",
            f"{S('How many accounts should join the live stream?')} "
            f"(1-{live_limit(joinable_count())})",
        ]), buttons=[[btn("Cancel", "home", icon="↩️")]])

    if state["step"] == "count":
        if not text.isdigit() or int(text) < 1:
            return await bad(event, "Send a whole number.", "home")
        n = live_limit(min(int(text), joinable_count()))
        task_states.pop(event.chat_id, None)
        return await execute_go_live(event, state, n)


async def execute_go_live(event, state, count):
    msg = await event.respond(card(E_REFRESH, "Going Live", [
        S("Finding the live stream..."),
    ]))

    # Resolving needs a member, so join the channel first where necessary.
    # With rotation on, pull extra accounts into the channel as well: they stay
    # out of the call and become the pool to swap the tired ones for.
    # Skip accounts another client's live already has — they cannot serve both.
    # Least-busy first so a Go Live does not lean on the accounts that are
    # already carrying the most client channels.
    load = await account_load()
    candidates = free_live_keys(acc_keys())
    random.shuffle(candidates)
    candidates.sort(key=lambda k: load.get(k, 0))
    pool_size = min(len(candidates),
                    count * LIVE_POOL_MULT if LIVE_ROTATE else count)
    keys = candidates[:pool_size]
    if not keys:
        return await msg.edit(card(E_WARN, "No Free Accounts", [
            S("Every account is already streaming in another live call."),
            "",
            S("Stop one from Live Now, or add more accounts."),
        ]), buttons=[[btn("Live Now", "live_now", icon="🎤")],
                     [btn("Home", "home", icon="🏠")]])
    ltype, target = state["ltype"], state["target"]
    spec = target if ltype == "public" else None
    chat_id = None
    usable: List[str] = []

    frozen_hit = 0

    for key in keys:
        client = acc_client(key)
        if not client or is_frozen(key):
            continue
        try:
            ent = None
            if ltype == "public":
                try:
                    await client(JoinChannelRequest(target))
                except UserAlreadyParticipantError:
                    pass
                ent = await client.get_entity(target)
            else:
                try:
                    res = await client(ImportChatInviteRequest(target))
                    chats = chats_from_join_result(res)
                    ent = chats[0] if chats else None
                except UserAlreadyParticipantError:
                    ent = None
                if ent is None:
                    # Already a member, or the new ChatInviteJoinResult layer
                    # gave us no chats — resolve it the normal way. Falling back
                    # to a chat_id another account already found keeps the whole
                    # batch from failing on one odd response.
                    if chat_id is not None:
                        ent = await client.get_entity(channel_peer(chat_id))
                    else:
                        ent = await client.get_entity(state["link"])
            chat_id = utils.get_peer_id(ent)
            usable.append(key)
        except Exception as e:
            if is_frozen_account_error(e):
                mark_frozen(key)
                frozen_hit += 1
            else:
                logger.warning(f"go live join {key}: {type(e).__name__}: {e}")
        await asyncio.sleep(0.3)

    if frozen_hit:
        logger.warning(f"go live: {frozen_hit} frozen account(s) skipped")

    if chat_id is None or not usable:
        lines = [S("No account could join or resolve that channel.")]
        if frozen_hit:
            lines += ["",
                      field(E_WARN, "Frozen accounts", str(frozen_hit)),
                      S("Those accounts are frozen by Telegram and cannot join "
                        "anything. Add fresh accounts.")]
        return await msg.edit(card(E_CROSS, "Cannot Reach Channel", lines),
                              buttons=kb_nav("home"))

    call = await get_active_call(chat_id, usable)
    if call is None:
        return await msg.edit(card(E_WARN, "No Live Stream", [
            field(E_CHANNEL, "Channel", esc(state["link"])),
            "",
            S("That channel has no live stream running right now."),
        ]), buttons=[[btn("Try Again", "go_live", icon="🔄")],
                     [btn("Home", "home", icon="🏠")]])

    streamers = usable[:count]
    await msg.edit(card(E_REFRESH, "Going Live", [
        f"{S('Joining with')} <b>{len(streamers)}</b> {S('accounts...')}",
    ]))
    # Mark it handled so the watcher does not fire a second join for this call.
    active_calls[chat_id] = call.id
    register_live_session(chat_id, usable, len(streamers), state["link"])
    ok, errors = await join_live_with_audio(streamers, chat_id, state["link"])

    lines = [field(E_CHANNEL, "Channel", esc(state["link"])),
             field(E_CHECK, "Streaming", f"{ok} / {len(streamers)}")]
    if LIVE_ROTATE:
        lines.append(field(E_REFRESH, "Rotation",
                           f"{rotate_text()} ({len(usable)} {S('in pool')})"))
    if errors:
        first = list(errors.items())[:3]
        lines += [""] + [f"{E_CROSS} <code>{esc(k)}</code> — {esc(v)}"
                         for k, v in first]
        if len(errors) > 3:
            lines.append(f"<i>+ {len(errors) - 3} {S('more failed')}</i>")
    await msg.edit(card(E_ROCKET if ok else E_CROSS,
                        "Live" if ok else "Join Failed", lines,
                        footer=f"{E_SHIELD} {S('Accounts stay until the stream ends.')}"),
                   buttons=[[btn("Live Now", "live_now", icon="🎤"),
                             btn("Audio Menu", "audio_menu", icon="🎵")],
                            [btn("Home", "home", icon="🏠")]])


# ═══════════════════════ LOGIN FLOW ═══════════════════════
async def send_login_code(event, state, phone):
    """Request an OTP for `phone` and park the flow on the code step."""
    old = state.get("relogin_stem")
    if old:
        # Reuse the existing file: signing in re-authorises it in place, so a
        # failed attempt leaves the original session untouched.
        path = os.path.join(SESSIONS_DIR, old + ".session")
    else:
        stem = re.sub(r"[^0-9A-Za-z_+-]", "", phone)
        path = os.path.join(SESSIONS_DIR, stem + ".session")
        if os.path.exists(path):
            return await bad(event, "That number already has a session in the folder.",
                             "menu_accounts")

    client = TelegramClient(os.path.splitext(path)[0], API_ID, API_HASH,
                            **ANDROID_PROFILE)
    try:
        await client.connect()
        sent = await client.send_code_request(phone)
    except Exception as e:
        try:
            await client.disconnect()
        except Exception:
            pass
        return await bad(event, f"Could not send the code: {str(e)[:80]}",
                         "menu_accounts")

    state.update({"client": client, "phone": phone, "path": path,
                  "hash": sent.phone_code_hash, "step": "otp"})
    return await event.respond(card(E_PHONE, "Code Sent", [
        field(E_PHONE, "Number", esc(phone)),
        "",
        S("Send the login code you received."),
    ]), buttons=[[btn("Cancel", "menu_accounts", icon="↩️")]])


async def start_relogin(event, stem):
    """Re-authorise a Needs Review session by sending it a fresh OTP."""
    reason = PROBLEM_SESSIONS.get(stem, "")
    if "locked" in reason:
        return await event.answer(
            "This file is locked by another process, not logged out. "
            "Stop the other bot copy first.", alert=True)

    path = os.path.join(SESSIONS_DIR, stem + ".session")
    if not os.path.exists(path):
        PROBLEM_SESSIONS.pop(stem, None)
        await event.answer("Session file is gone — entry cleared", alert=True)
        return await show_problem_sessions(event, 1)

    phone = str(meta_get(read_meta(path), "phone") or "").strip()
    if not phone:
        guess = re.sub(r"[^\d+]", "", stem)
        phone = guess if len(guess) >= 7 else ""

    state = {"type": "login", "step": "phone", "relogin_stem": stem}
    task_states[event.chat_id] = state
    if not phone:
        await event.answer()
        return await safe_edit(event, card(E_PHONE, "Re-login", [
            field(E_PAGE, "Session", esc(stem)),
            "",
            S("The phone number for this session is unknown."),
            S("Send the number with country code to receive a code."),
        ]), [[btn("Cancel", "problem_sessions", icon="↩️")]])

    await event.answer("Sending code...")
    return await send_login_code(event, state, phone)


async def flow_login(event, state, text):
    chat_id = event.chat_id
    step = state["step"]

    if step == "phone":
        phone = re.sub(r"[^\d+]", "", text)
        if len(phone) < 7:
            return await bad(event, "Send a valid phone number with country code.",
                             "menu_accounts")
        return await send_login_code(event, state, phone)

    if step == "otp":
        code = re.sub(r"\D", "", text)
        try:
            await state["client"].sign_in(phone=state["phone"], code=code,
                                         phone_code_hash=state["hash"])
        except SessionPasswordNeededError:
            state["step"] = "2fa"
            return await event.respond(card(E_LOCK, "Two-Step Password", [
                S("This account has a 2FA password. Send it."),
            ]), buttons=[[btn("Cancel", "menu_accounts", icon="↩️")]])
        except Exception as e:
            return await bad(event, f"Login failed: {str(e)[:80]}", "menu_accounts")
        return await finish_login(event, state)

    if step == "2fa":
        try:
            await state["client"].sign_in(password=text)
        except Exception as e:
            return await bad(event, f"Wrong password: {str(e)[:60]}", "menu_accounts")
        return await finish_login(event, state)

    if step == "string":
        msg = await event.respond(card(E_REFRESH, "Checking Session",
                                        [S("Connecting...")]))
        acc = await import_string_session(text)
        task_states.pop(chat_id, None)
        if not acc:
            return await msg.edit(card(E_CROSS, "Rejected", [
                S("That string session is not valid or not authorised."),
            ]), buttons=[[btn("Try Another", "add_string", icon="🔑")],
                         [btn("Back", "menu_accounts", icon="⬅️")]])
        await setup_channel_monitors()
        # Put it into every active client channel so it starts earning straight
        # away instead of sitting idle until the next subscription is created.
        schedule_onboarding([acc.key], notify=chat_id)
        return await msg.edit(card(E_CHECK, "Account Added", [
            field(E_PHONE, "Account", esc(acc.label)),
            field(E_PERSON, "Name", esc(acc.name)),
            field(E_GREEN, "Total online", str(acc_count())),
            "",
            f"{E_REFRESH} {S('Joining it to all client channels in the background...')}",
        ]), buttons=add_another_kb("add_string"))


async def finish_login(event, state):
    client = state["client"]
    path = state["path"]
    try:
        me = await client.get_me()
        await client(UpdateStatusRequest(offline=False))
    except Exception:
        me = None
    write_meta(path, api_id=API_ID, api_hash=API_HASH, phone=state["phone"],
               user_id=getattr(me, "id", 0), first_name=getattr(me, "first_name", ""),
               **ANDROID_PROFILE)
    # hand the live client over to the store without reopening the file
    stem = os.path.splitext(os.path.basename(path))[0]
    acc = Account(stem, path, API_ID, API_HASH, dict(ANDROID_PROFILE))
    acc.client = client
    acc.user_id = getattr(me, "id", 0) or 0
    acc.phone = state["phone"]
    acc.name = (getattr(me, "first_name", "") or "").strip() or "Account"
    acc.key = acc.phone
    acc.state = "alive"
    ACCOUNTS[acc.key] = acc
    PROBLEM_SESSIONS.pop(stem, None)
    PROBLEM_SESSIONS.pop(state.get("relogin_stem") or "", None)

    task_states.pop(event.chat_id, None)
    await setup_channel_monitors()
    # Re-login lands back on the review list; a fresh login offers another go.
    if state.get("relogin_stem"):
        buttons = [[btn("Needs Review", "problem_sessions", icon="⚠️")],
                   [btn("Accounts", "menu_accounts", icon="⬅️"),
                    btn("Home", "home", icon="🏠")]]
    else:
        buttons = add_another_kb("add_phone")
    lines = [
        field(E_PHONE, "Number", esc(acc.phone)),
        field(E_PERSON, "Name", esc(acc.name)),
        field(E_GREEN, "Total online", str(acc_count())),
    ]
    # A re-login is an account the fleet already had, so it is already inside
    # its channels; only a genuinely new login needs onboarding.
    if not state.get("relogin_stem"):
        schedule_onboarding([acc.key], notify=event.chat_id)
        lines += ["", f"{E_REFRESH} {S('Joining it to all client channels in the background...')}"]
    await event.respond(card(E_CHECK, "Account Added", lines,
                             footer=f"{E_SHIELD} {S('Saved into the sessions folder.')}"),
                        buttons=buttons)


# ═══════════════════════ CREATE CLIENT FLOW ═══════════════════════
async def flow_create_client(event, state, text):
    chat_id = event.chat_id
    step = state["step"]

    if step == "user_id":
        if not text.lstrip("-").isdigit():
            return await bad(event, "Send the numeric Telegram user ID.")
        state["client_user_id"] = int(text)
        state["step"] = "channel_link"
        return await event.respond(card(E_CHANNEL, "Create Client", [
            S("Step 2 of 6"),
            "",
            S("Send the channel link."),
            "",
            f"{DOT} <code>https://t.me/channel</code>",
            f"{DOT} <code>https://t.me/+inviteHash</code>",
        ]), buttons=[[btn("Cancel", "home", icon="↩️")]])

    if step == "channel_link":
        ltype, target = parse_target(text)
        if ltype not in ("public", "private"):
            return await bad(event, "That is not a valid channel link.")
        # A client may hold many channels, but not the same one twice — the two
        # subscriptions would both fire on every post and double the reactions.
        if await sub_exists_for_channel(state["client_user_id"], text.strip(),
                                        target if ltype == "public" else None):
            return await bad(event, "This client already has a subscription on "
                                    "that channel. Upgrade or extend it instead.",
                             f"cuser_{state['client_user_id']}")
        state.update({"channel_link": text.strip(), "channel_type": ltype,
                      "channel_target": target, "step": "accounts_count"})
        return await event.respond(card(E_PERSON, "Create Client", [
            S("Step 3 of 6"),
            "",
            field(E_GREEN, "Accounts online", str(acc_count())),
            "",
            f"{S('How many accounts should join?')} (1-{joinable_count()})",
        ]), buttons=[[btn("Cancel", "home", icon="↩️")]])

    if step == "accounts_count":
        if not text.isdigit():
            return await bad(event, "Numbers only.")
        n = int(text)
        # Frozen accounts cannot join, so promising them to a client would
        # under-deliver the package from day one.
        if n < 1 or n > joinable_count():
            return await bad(event, f"Enter between 1 and {joinable_count()}.")
        state["accounts_count"] = n
        state["step"] = "reactions"
        return await event.respond(card(E_THUMB, "Create Client", [
            S("Step 4 of 6"),
            "",
            field(E_GREEN, "Accounts online", str(acc_count())),
            field(E_PERSON, "This package", str(n)),
            "",
            f"{S('How many reactions per post?')} (0-{n})",
            "",
            f"{E_BELL} {S('0 means reactions off.')}",
        ]), buttons=[[btn("Cancel", "home", icon="↩️")]])

    if step == "reactions":
        if not text.isdigit():
            return await bad(event, "Numbers only.")
        n = int(text)
        if n > state["accounts_count"]:
            return await bad(event, f"Maximum {state['accounts_count']}.")
        state["reactions_per_post"] = n
        state["step"] = "views"
        return await event.respond(card(E_EYE, "Create Client", [
            S("Step 5 of 6"),
            "",
            field(E_GREEN, "Accounts online", str(acc_count())),
            field(E_PERSON, "This package", str(state["accounts_count"])),
            "",
            f"{S('How many views per post?')} (0-{state['accounts_count']})",
            "",
            f"{E_BELL} {S('0 means views off.')}",
        ]), buttons=[[btn("Cancel", "home", icon="↩️")]])

    if step == "views":
        if not text.isdigit():
            return await bad(event, "Numbers only.")
        n = int(text)
        if n > state["accounts_count"]:
            return await bad(event, f"Maximum {state['accounts_count']}.")
        if n == 0 and state["reactions_per_post"] == 0:
            return await bad(event, "Reactions and views cannot both be zero.")
        state["views_per_post"] = n
        state["step"] = "livestream"
        return await event.respond(card(E_ROCKET, "Create Client", [
            S("Step 6 of 6"),
            "",
            field(E_GREEN, "Accounts online", str(acc_count())),
            field(E_PERSON, "This package", str(state["accounts_count"])),
            "",
            f"{S('How many accounts should join live streams?')} (0-{state['accounts_count']})",
            "",
            f"{E_BELL} {S('0 means live stream joining is off.')}",
            f"{E_ROCKET} {S('When channel goes live, these accounts auto-join.')}",
        ]), buttons=[[btn("Cancel", "home", icon="↩️")]])

    if step == "livestream":
        if not text.isdigit():
            return await bad(event, "Numbers only.")
        n = int(text)
        if n > state["accounts_count"]:
            return await bad(event, f"Maximum {state['accounts_count']}.")
        state["livestream_accounts"] = n
        state["step"] = "days"
        return await event.respond(card(E_CAL, "Subscription Length", [
            S("Final Step"),
            "",
            S("Send how many days the subscription runs."),
            "",
            f"{S('Example')}: <code>30</code>",
        ]), buttons=[[btn("Cancel", "home", icon="↩️")]])

    if step == "days":
        if not text.isdigit() or int(text) < 1:
            return await bad(event, "Send at least 1 day.")
        state["subscription_days"] = int(text)
        task_states.pop(chat_id, None)
        return await create_client_now(event, state)


async def create_client_now(event, state):
    msg = await event.respond(card(E_REFRESH, "Creating Subscription", [
        f"{S('Joining')} <b>{state['accounts_count']}</b> {S('accounts...')}",
    ]))
    prog = Progress(msg)

    # Least-busy accounts, not the first N: see pick_accounts().
    keys, load = await pick_accounts(state["accounts_count"])
    if not keys:
        return await prog.show(card(E_CROSS, "No Usable Account", [
            S("Every account is already in the maximum number of channels."),
        ]), force=True)
    ltype = state["channel_type"]
    target = state["channel_target"]
    joined: List[str] = []
    channel_id = None
    channel_username = target if ltype == "public" else None
    done = 0
    sem = asyncio.Semaphore(JOIN_CONCURRENCY)

    async def join_one(key):
        nonlocal channel_id, done
        async with sem:
            client = acc_client(key)
            if not client:
                done += 1
                return
            try:
                if ltype == "public":
                    res = await client(JoinChannelRequest(target))
                else:
                    res = await client(ImportChatInviteRequest(target))
                if channel_id is None:
                    for chat in chats_from_join_result(res):
                        channel_id = utils.get_peer_id(chat)
                        break
                joined.append(key)
                await log_activity(key, "CLIENT_JOIN", state["channel_link"], "Success")
            except UserAlreadyParticipantError:
                joined.append(key)
            except FloodWaitError as e:
                await log_activity(key, "CLIENT_JOIN", state["channel_link"],
                                   f"flood {e.seconds}s")
            except Exception as e:
                if is_frozen_account_error(e):
                    mark_frozen(key)
                await log_activity(key, "CLIENT_JOIN", state["channel_link"],
                                   str(e)[:40])
            if channel_id is None:
                try:
                    ent = await client.get_entity(
                        target if ltype == "public" else state["channel_link"])
                    channel_id = utils.get_peer_id(ent)
                except Exception:
                    pass
            done += 1
            await asyncio.sleep(0.4)
            await prog.show(progress_card("Joining Channel", done, len(keys), [
                field(E_CHECK, "Joined", str(len(joined))),
            ]))

    await asyncio.gather(*(join_one(k) for k in keys))
    invalidate_peer_cache()

    if not joined:
        return await prog.show(card(E_CROSS, "Nothing Joined", [
            S("No account could join that channel."),
            "",
            f"{E_WARN} {S('Check the link, or whether the invite is still valid.')}",
        ]), force=True)

    client_name = "Unknown"
    try:
        ent = await bot.get_entity(int(state["client_user_id"]))
        client_name = " ".join(x for x in [getattr(ent, "first_name", ""),
                                           getattr(ent, "last_name", "")] if x) \
            or "Unknown"
    except Exception:
        pass

    sub = await create_client_subscription({
        "client_user_id": state["client_user_id"],
        "client_name": client_name,
        "channel_link": state["channel_link"],
        "channel_id": channel_id,
        "channel_username": channel_username,
        "channel_type": ltype,
        "accounts_count": state["accounts_count"],
        "reactions_per_post": state["reactions_per_post"],
        "views_per_post": state["views_per_post"],
        "livestream_accounts": state.get("livestream_accounts", 0),
        "subscription_days": state["subscription_days"],
        "joined_accounts": joined,
    })
    await setup_channel_monitors()

    warn = []
    if channel_id is None:
        warn.append(f"{E_WARN} {S('Channel id could not be resolved — auto posts may not trigger. Press Reload in Accounts after the first post.')}")
    if len(joined) < state["accounts_count"]:
        warn.append(f"{E_WARN} {len(joined)}/{state['accounts_count']} "
                    f"{S('accounts joined.')}")
    if load:
        picked = [load.get(k, 0) for k in keys]
        warn.append(f"{E_SHIELD} {S('Picked the least-busy accounts')} "
                    f"({min(picked)}-{max(picked)} {S('channels each')}).")

    await prog.show(card(E_CHECK, "Subscription Created", [
        field(E_PERSON, "Client", esc(client_name)),
        f"{E_STAR} {S('User ID')}: <code>{state['client_user_id']}</code>",
        field(E_CHANNEL, "Channel", esc(state["channel_link"])),
        "",
        field(E_PERSON, "Accounts joined",
              f"{len(joined)}/{state['accounts_count']}"),
        field(E_THUMB, "Reactions / post", str(state["reactions_per_post"])),
        field(E_EYE, "Views / post", str(state["views_per_post"])),
        field(E_ROCKET, "Live stream accounts", str(state.get("livestream_accounts", 0))),
        field(E_CAL, "Duration", f"{state['subscription_days']} {S('days')}"),
        field(E_CLOCK, "Expires", sub["expires_at"].strftime("%d %b %Y %H:%M")),
    ] + ([""] + warn if warn else []),
        footer=f"{E_ROCKET} {S('Auto reactions, views and live stream join are live.')}"), force=True)
    try:
        await msg.edit(buttons=[
            [btn("Open", f"client_{sub['_id']}", icon="📄"),
             btn("Add Channel", f"cadd_{state['client_user_id']}", icon="➕")],
            [btn("Home", "home", icon="🏠")]])
    except Exception:
        pass

    try:
        await bot.send_message(int(state["client_user_id"]), card(
            E_GIFT, "Your Subscription Is Active", [
                field(E_CHANNEL, "Channel", esc(state["channel_link"])),
                field(E_THUMB, "Reactions / post", str(state["reactions_per_post"])),
                field(E_EYE, "Views / post", str(state["views_per_post"])),
                field(E_ROCKET, "Live stream accounts", str(state.get("livestream_accounts", 0))),
                field(E_CAL, "Duration", f"{state['subscription_days']} {S('days')}"),
                field(E_CLOCK, "Expires", sub["expires_at"].strftime("%d %b %Y %H:%M")),
            ], footer=f"{E_SPARKLE} {S('New posts get reactions, views and live joins automatically.')}"))
    except Exception as e:
        logger.info(f"could not notify client: {e}")


# ═══════════════════════ ZIP IMPORT ═══════════════════════
async def handle_zip_import(event):
    msg = await event.respond(card(E_GIFT, "Import ZIP",
                                    [S("Downloading...")]))
    prog = Progress(msg)
    temp_dir = None
    try:
        temp_dir = tempfile.mkdtemp()
        zip_path = os.path.join(temp_dir, "import.zip")
        await event.download_media(zip_path)

        extract_dir = os.path.join(temp_dir, "x")
        os.makedirs(extract_dir, exist_ok=True)
        with zipfile.ZipFile(zip_path, "r") as z:
            z.extractall(extract_dir)

        # collect .session files plus any sidecar json sitting next to them
        found = []
        strings: List[str] = []
        for root, _dirs, files in os.walk(extract_dir):
            for f in files:
                if f.endswith(".session"):
                    found.append(os.path.join(root, f))
                elif f.lower() in ("string.txt", "strings.txt", "sessions.txt"):
                    try:
                        with open(os.path.join(root, f), "r",
                                  encoding="utf-8", errors="ignore") as fh:
                            strings += [l.strip() for l in fh if l.strip()]
                    except Exception:
                        pass

        await prog.show(card(E_GIFT, "Import ZIP", [
            field(E_PAGE, "Session files", str(len(found))),
            field(E_LOCK, "String sessions", str(len(strings))),
            "",
            S("Copying into the sessions folder..."),
        ]), force=True)

        stats = {"alive": 0, "dead": 0, "unknown": 0, "no_meta": 0,
                 "desktop": 0, "android": 0}
        new_keys: List[str] = []

        for i, src in enumerate(found, 1):
            stem = os.path.splitext(os.path.basename(src))[0]
            dest = os.path.join(SESSIONS_DIR, stem + ".session")
            n = 1
            while os.path.exists(dest):
                dest = os.path.join(SESSIONS_DIR, f"{stem}_{n}.session")
                n += 1
            try:
                shutil.copy2(src, dest)
            except Exception as e:
                logger.warning(f"copy {src}: {e}")
                continue

            # A session's auth key is bound to the api_id that made it, and
            # Telegram checks the device signature against that api_id. Carry
            # the bundle's own json across or the session gets revoked in a day
            # or two — this is the single biggest cause of ZIP accounts dying.
            meta = {}
            for cand in (os.path.splitext(src)[0] + ".json",
                         os.path.join(os.path.dirname(src), stem + ".json")):
                if os.path.exists(cand):
                    try:
                        with open(cand, "r", encoding="utf-8") as fh:
                            loaded = json.load(fh)
                        if isinstance(loaded, dict):
                            meta = loaded
                            break
                    except Exception:
                        pass

            api_id, api_hash = creds_from_meta(meta)
            if not meta_get(meta, "api_id"):
                stats["no_meta"] += 1
            device = device_profile_for(api_id, meta)
            if device["device_model"] == DESKTOP_PROFILE["device_model"]:
                stats["desktop"] += 1
            else:
                stats["android"] += 1
            write_meta(dest, api_id=api_id, api_hash=api_hash,
                       user_id=meta_get(meta, "user_id") or 0,
                       first_name=meta_get(meta, "first_name") or "",
                       phone=meta_get(meta, "phone") or "", **device)

            # NB: do not call this `state` — that name is the import flow's own
            # variable and shadowing it here broke nothing yet only by luck.
            res_state, _acc = await probe_and_register(dest)
            stats[res_state] += 1
            if res_state == "alive" and _acc and _acc.key:
                new_keys.append(_acc.key)
            await asyncio.sleep(0.2)
            await prog.show(progress_card("Importing", i, len(found), [
                field(E_GREEN, "Online", str(stats["alive"])),
                field(E_RED, "Not authorised", str(stats["dead"])),
                field(E_WARN, "Unclear", str(stats["unknown"])),
            ]))

        for s in strings:
            imported = await import_string_session(s)
            if imported:
                stats["alive"] += 1
                if imported.key:
                    new_keys.append(imported.key)
            else:
                stats["dead"] += 1
            await asyncio.sleep(0.3)

        await setup_channel_monitors()
        # Every account that came in from this ZIP now walks into all the
        # active client channels, in the background, so the fleet that just grew
        # is actually usable instead of idling until the next subscription.
        schedule_onboarding(new_keys, notify=event.chat_id)

        lines = [
            field(E_GREEN, "Now online", str(stats["alive"])),
            field(E_RED, "Not authorised", str(stats["dead"])),
            field(E_WARN, "Unclear", str(stats["unknown"])),
            "",
            field(E_PERSON, "Total accounts online", str(acc_count())),
            "",
            field(E_DIAMOND, "Desktop profile", str(stats["desktop"])),
            field(E_PHONE, "Android profile", str(stats["android"])),
        ]
        if stats["no_meta"]:
            lines += ["", f"{E_WARN} <b>{stats['no_meta']}</b> "
                          f"{S('sessions had no .json, so the bot api_id was used. Those are the ones most likely to be revoked — include the json files next time.')}"]
        if stats["unknown"]:
            lines += [f"{E_SHIELD} {S('Nothing was deleted. See Needs Review under Accounts.')}"]

        await prog.show(card(E_CHECK, "Import Complete", lines), force=True)
        try:
            await msg.edit(buttons=[[btn("Accounts List", "list_accounts", icon="📋"),
                                     btn("Home", "home", icon="🏠")]])
        except Exception:
            pass

    except zipfile.BadZipFile:
        await prog.show(card(E_CROSS, "Bad ZIP",
                             [S("That file is not a readable ZIP.")]), force=True)
    except Exception as e:
        logger.error(f"zip import: {e}", exc_info=True)
        await prog.show(card(E_CROSS, "Import Failed",
                             [f"<code>{esc(str(e)[:200])}</code>"]), force=True)
    finally:
        if temp_dir:
            shutil.rmtree(temp_dir, ignore_errors=True)


# ═══════════════════════ MAIN ═══════════════════════
async def main():
    print("=" * 58)
    print("  REACTION & VIEWS BOT")
    print("=" * 58)
    print(f"  sessions dir : {SESSIONS_DIR}")
    print(f"  owners       : {OWNER_IDS}")

    try:
        await mongo_client.admin.command("ping")
        print("  mongodb      : connected (clients only)")
    except Exception as e:
        print(f"  mongodb      : FAILED - {e}")
        return

    await load_settings()
    await bot.start(bot_token=BOT_TOKEN)
    me = await bot.get_me()
    print(f"  bot          : @{me.username}")
    print("=" * 58)

    tally = await load_all_sessions()
    print(f"  accounts     : {acc_count()} online "
          f"(dead {tally['dead']}, unclear {tally['unknown']})")
    if PROBLEM_SESSIONS:
        print(f"  needs review : {len(PROBLEM_SESSIONS)} "
              f"(nothing deleted — see Accounts > Needs Review)")

    await setup_channel_monitors()
    asyncio.create_task(monitor_task())
    asyncio.create_task(livestream_watch_task())
    asyncio.create_task(live_keepalive_task())
    asyncio.create_task(reminder_task())
    asyncio.create_task(expiry_check_task())
    asyncio.create_task(keep_alive_task())
    asyncio.create_task(onboard_sweep_task())

    if not TGCALLS_OK:
        print(f"  live audio   : UNAVAILABLE — {TGCALLS_ERR}")
        print("                 pip install py-tgcalls && apt install -y ffmpeg")
    elif not os.path.exists(LIVE_AUDIO_PATH):
        print("  live audio   : no file set (Home > Audio > Set Audio)")
    else:
        mb = os.path.getsize(LIVE_AUDIO_PATH) / (1024 * 1024)
        spare = fd_headroom()
        cap = f"max {LIVE_CAP} streams" if LIVE_CAP else "no stream limit"
        fds = f", {spare} fds free" if spare < 10 ** 6 else ""
        print(f"  live audio   : ready ({mb:.1f} MB, loops, {cap}{fds})")
        rot = (f"every {LIVE_ROTATE // 60}m, "
               f"{int(LIVE_ROTATE_FRACTION * 100)}% swapped"
               if LIVE_ROTATE else "off")
        print(f"  rotation     : {rot}")

    print("  status       : ready")
    print("=" * 58)
    await bot.run_until_disconnected()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nstopped")
