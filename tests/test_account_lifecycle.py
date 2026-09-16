"""Lifecycle tests for view.py — dead / frozen account handling.

Run with the interpreter that has telethon installed:

    python tests/test_account_lifecycle.py

These tests exist because the bot's own detection was silently broken: every
"dead account" marker was matched against ``str(exception)``, which for Telethon
is a human sentence ("The user has been deleted/deactivated") and not the API
code, so no error ever matched and dead logins stayed in the fleet forever.
The first test below fails outright on that old behaviour.
"""

import asyncio
import os
import sqlite3
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import view  # noqa: E402  (import after sys.path is set up)

from telethon.errors import (  # noqa: E402
    AuthKeyDuplicatedError,
    AuthKeyInvalidError,
    AuthKeyUnregisteredError,
    FloodWaitError,
    FrozenMethodInvalidError,
    FrozenParticipantMissingError,
    PeerIdInvalidError,
    PhoneNumberBannedError,
    SessionExpiredError,
    SessionRevokedError,
    UserDeactivatedBanError,
    UserDeactivatedError,
)
from telethon.tl.functions.updates import GetStateRequest  # noqa: E402


class FakeClient:
    """Just enough TelegramClient for the store's bookkeeping."""

    def __init__(self, me="user", error=None):
        self.disconnected = 0
        self.me = me
        self.error = error

    async def get_me(self):
        if self.error is not None:
            raise self.error
        return self.me

    async def disconnect(self):
        self.disconnected += 1


def account(key, stem=None, client=None):
    acc = view.Account(stem or key.replace("+", ""), f"/tmp/{stem or key}.session",
                       view.API_ID, view.API_HASH, dict(view.ANDROID_PROFILE))
    acc.key = key
    acc.state = "alive"
    acc.client = client if client is not None else FakeClient()
    return acc


def reset():
    view.ACCOUNTS.clear()
    view.PROBLEM_SESSIONS.clear()
    view.FROZEN_ACCOUNTS.clear()
    view.LIVE_AUDIO.clear()
    view.TGCALLS.clear()


def check_dead_errors():
    """Every way Telegram says "this login is gone" must be recognised."""
    cases = [
        UserDeactivatedError(GetStateRequest()),
        UserDeactivatedBanError(GetStateRequest()),
        AuthKeyUnregisteredError(GetStateRequest()),
        AuthKeyDuplicatedError(GetStateRequest()),
        AuthKeyInvalidError(GetStateRequest()),
        SessionRevokedError(GetStateRequest()),
        SessionExpiredError(GetStateRequest()),
        PhoneNumberBannedError(GetStateRequest()),
    ]
    for err in cases:
        assert view.is_dead_account_error(err), type(err).__name__
        assert not view.is_frozen_account_error(err), type(err).__name__

    # The raw server message must work too, for codes Telethon has no class for.
    plain = view.RPCError if hasattr(view, "RPCError") else None
    if plain is None:
        from telethon.errors import RPCError
        raw = RPCError(GetStateRequest(), "USER_DEACTIVATED", 401)
        assert view.is_dead_account_error(raw)


def check_frozen_errors():
    for err in (FrozenMethodInvalidError(GetStateRequest()),
                FrozenParticipantMissingError(GetStateRequest())):
        assert view.is_frozen_account_error(err), type(err).__name__
        assert not view.is_dead_account_error(err), type(err).__name__


def check_transient_errors():
    """A blip must never look like a deleted account."""
    cases = [
        TimeoutError("timed out"),
        ConnectionError("connection reset by peer"),
        sqlite3.OperationalError("database is locked"),
        PeerIdInvalidError(GetStateRequest()),
        FloodWaitError(request=GetStateRequest(), capture=42),
        ValueError("something else entirely"),
    ]
    for err in cases:
        assert not view.is_dead_account_error(err), repr(err)
        assert not view.is_frozen_account_error(err), repr(err)


async def check_dead_account_leaves_the_pool():
    reset()
    view._notify_owners_soon = lambda text: None          # no Telegram in tests
    acc = account("+910000000001")
    view.ACCOUNTS[acc.key] = acc
    assert view.acc_keys() == [acc.key]
    assert view.usable_keys() == [acc.key]

    first = await view.mark_dead(acc.key, "dead — UserDeactivatedError")
    assert first is True
    # Out of every pool, but the entry (and its file) stay for review.
    assert view.acc_keys() == []
    assert view.usable_keys() == []
    assert view.all_acc_keys() == [acc.key]
    assert view.ACCOUNTS[acc.key].state == "dead"
    assert acc.client is None                     # disconnected
    assert not os.path.exists(acc.path)           # file never deleted
    reason = view.PROBLEM_SESSIONS[acc.stem]
    assert view.is_dead_problem(reason), reason
    assert acc.key in view.dead_keys()
    # Marking twice is a no-op, not a second disconnect.
    assert await view.mark_dead(acc.key, "again") is False

    # A dead account is never handed work again.
    assert view.pick_workers([acc.key], 1) == []
    assert view.joinable_keys([acc.key]) == []
    assert view.free_live_keys([acc.key]) == []


async def check_frozen_account_is_parked_not_deleted():
    reset()
    view._notify_owners_soon = lambda text: None
    acc = account("+910000000002")
    view.ACCOUNTS[acc.key] = acc

    view.mark_frozen(acc.key, "FrozenMethodInvalidError")
    assert view.is_frozen(acc.key)
    assert view.FROZEN_ACCOUNTS[acc.key]
    # Still connected and still known — a freeze is temporary, not a death.
    assert acc.key in view.acc_keys()
    assert acc.state == "alive"
    assert view.usable_keys() == []
    assert view.pick_workers([acc.key], 1) == []
    assert view.joinable_keys([acc.key]) == []
    assert view.free_live_keys([acc.key]) == []
    assert view.frozen_keys() == [acc.key]
    # ...and it must never be reported as dead.
    assert not view.is_dead_problem("file locked")
    assert acc.key not in view.PROBLEM_SESSIONS


async def check_reload_drops_dead_registered_accounts():
    """A file that stops authorising must stop the client that was using it."""
    reset()
    view._notify_owners_soon = lambda text: None
    acc = account("+910000000003", stem="session_x")
    view.ACCOUNTS[acc.key] = acc

    await view.detach_dead_by_stem("session_x", "not authorised")
    assert view.acc_keys() == []
    assert view.all_acc_keys() == [acc.key]
    assert view.ACCOUNTS[acc.key].state == "dead"
    assert view.is_dead_problem(view.PROBLEM_SESSIONS["session_x"])


async def check_error_reporting_helper():
    reset()
    view._notify_owners_soon = lambda text: None
    acc = account("+910000000004")
    view.ACCOUNTS[acc.key] = acc

    assert await view.note_account_error(acc.key, UserDeactivatedError(
        GetStateRequest())) is True
    assert view.ACCOUNTS[acc.key].state == "dead"

    other = account("+910000000005")
    view.ACCOUNTS[other.key] = other
    assert await view.note_account_error(other.key, FrozenMethodInvalidError(
        GetStateRequest())) is True
    assert view.is_frozen(other.key)
    assert other.state == "alive"

    third = account("+910000000006")
    view.ACCOUNTS[third.key] = third
    assert await view.note_account_error(third.key, TimeoutError()) is False
    assert third.state == "alive" and not view.is_frozen(third.key)


def check_dead_problem_classification():
    reset()
    view.PROBLEM_SESSIONS.update({
        "dead1": "dead — not authorised",
        "dead2": "dead — UserDeactivatedError: The user has been deleted/deactivated",
        "locked1": "file locked",
        "dup1": "duplicate of other",
        "weird1": "ConnectionError",
    })
    assert sorted(view.dead_problem_stems()) == ["dead1", "dead2"]


async def check_health_probe_tells_them_apart():
    """get_me() is the server's answer: None means the login is gone."""
    reset()
    view._notify_owners_soon = lambda text: None

    gone = account("+920000000001", client=FakeClient(me=None))
    view.ACCOUNTS[gone.key] = gone
    assert await view.check_account_health(gone.key) == "dead"
    assert view.ACCOUNTS[gone.key].state == "dead"

    frozen_acc = account("+920000000002", client=FakeClient(
        error=FrozenMethodInvalidError(GetStateRequest())))
    view.ACCOUNTS[frozen_acc.key] = frozen_acc
    assert await view.check_account_health(frozen_acc.key) == "frozen"
    assert view.is_frozen(frozen_acc.key)
    assert frozen_acc.state == "alive"

    blip = account("+920000000003", client=FakeClient(error=TimeoutError()))
    view.ACCOUNTS[blip.key] = blip
    assert await view.check_account_health(blip.key) == "unclear"
    assert blip.state == "alive" and not view.is_frozen(blip.key)
    assert view.account_usable(blip.key)          # a blip changes nothing

    # The scan walks whatever is still connected: the account above already left
    # the pool when the probe found it, so add one more to be found by scanning.
    fresh = account("+920000000004", client=FakeClient(me=None))
    view.ACCOUNTS[fresh.key] = fresh
    assert gone.key not in view.acc_keys()        # already removed, not re-scanned

    dead, frozen = await view.scan_dead_accounts()
    assert fresh.key in dead and frozen_acc.key in frozen
    assert blip.key not in dead and blip.key not in frozen
    assert view.ACCOUNTS[fresh.key].state == "dead"


async def main():
    check_dead_errors()
    check_frozen_errors()
    check_transient_errors()
    check_dead_problem_classification()
    await check_dead_account_leaves_the_pool()
    await check_frozen_account_is_parked_not_deleted()
    await check_reload_drops_dead_registered_accounts()
    await check_error_reporting_helper()
    await check_health_probe_tells_them_apart()
    print("all lifecycle checks passed")


if __name__ == "__main__":
    asyncio.run(main())
