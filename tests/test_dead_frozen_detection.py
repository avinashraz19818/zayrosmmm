"""Dead / frozen account detection checks for the Heroku worker (view.py).

Run from the repository root on a machine with the requirements installed:

    python tests/test_dead_frozen_detection.py

Why this file exists: the bot classified Telegram errors by matching API codes
against ``str(exception)``. Telethon's ``str()`` is the human sentence ("The
user has been deleted/deactivated"), so no code ever matched and dead or frozen
sessions stayed in the fleet, were picked for every job and failed silently.
The first test fails outright on that old behaviour.
"""

import asyncio
import os
import sqlite3
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import view  # noqa: E402  (import after sys.path is set up)

from telethon.errors import (  # noqa: E402
    AuthKeyDuplicatedError,
    AuthKeyUnregisteredError,
    FloodWaitError,
    FrozenMethodInvalidError,
    FrozenParticipantMissingError,
    PeerIdInvalidError,
    SessionRevokedError,
    UserDeactivatedBanError,
    UserDeactivatedError,
)
from telethon.tl.functions.updates import GetStateRequest  # noqa: E402


class FakeClient:
    """Just enough TelegramClient for the account store's bookkeeping."""

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
    view.PEER_CACHE.clear()


def check_classifiers():
    """Every way Telegram says "this login is gone" must be recognised."""
    for err in (UserDeactivatedError(GetStateRequest()),
                UserDeactivatedBanError(GetStateRequest()),
                AuthKeyUnregisteredError(GetStateRequest()),
                AuthKeyDuplicatedError(GetStateRequest()),
                SessionRevokedError(GetStateRequest())):
        assert view.is_dead_account_error(err), type(err).__name__
        assert not view.is_frozen_account_error(err), type(err).__name__

    for err in (FrozenMethodInvalidError(GetStateRequest()),
                FrozenParticipantMissingError(GetStateRequest())):
        assert view.is_frozen_account_error(err), type(err).__name__
        assert not view.is_dead_account_error(err), type(err).__name__

    # Blips and unrelated errors must never look like a dead account.
    for err in (TimeoutError("timed out"),
                ConnectionError("connection reset by peer"),
                sqlite3.OperationalError("database is locked"),
                PeerIdInvalidError(GetStateRequest()),
                FloodWaitError(request=GetStateRequest(), capture=42),
                ValueError("something else entirely")):
        assert not view.is_dead_account_error(err), repr(err)
        assert not view.is_frozen_account_error(err), repr(err)


def check_dead_problem_classification():
    reset()
    view.PROBLEM_SESSIONS.update({
        "dead1": "dead — not authorised",
        "dead2": "dead — UserDeactivatedError: The user has been deleted/deactivated",
        "locked1": "file locked",
        "dup1": "duplicate of other",
    })
    assert sorted(view.dead_problem_stems()) == ["dead1", "dead2"]


async def check_dead_account_leaves_the_pool():
    reset()
    view._notify_owners_soon = lambda text: None       # no Telegram in tests
    acc = account("+910000000001")
    view.ACCOUNTS[acc.key] = acc
    assert view.acc_keys() == [acc.key]
    assert view.usable_keys() == [acc.key]

    first = await view.mark_dead(acc.key, "dead — UserDeactivatedError")
    assert first is True
    assert view.acc_keys() == []                 # out of every pool
    assert view.usable_keys() == []
    assert view.all_acc_keys() == [acc.key]      # entry kept for review
    assert view.ACCOUNTS[acc.key].state == "dead"
    assert acc.client is None                    # client disconnected
    assert not os.path.exists(acc.path)          # file never deleted
    assert view.is_dead_problem(view.PROBLEM_SESSIONS[acc.stem])
    assert acc.key in view.dead_keys()
    assert await view.mark_dead(acc.key, "again") is False

    assert view.pick_workers([acc.key], 1) == []
    assert view.joinable_keys([acc.key]) == []
    assert view.free_live_keys([acc.key]) == []


async def check_frozen_is_parked_not_deleted():
    reset()
    view._notify_owners_soon = lambda text: None
    acc = account("+910000000002")
    view.ACCOUNTS[acc.key] = acc

    view.mark_frozen(acc.key, "FrozenMethodInvalidError")
    assert view.is_frozen(acc.key)
    assert view.FROZEN_ACCOUNTS[acc.key]
    assert acc.key in view.acc_keys()            # still connected, still known
    assert acc.state == "alive"
    assert view.usable_keys() == []
    assert view.pick_workers([acc.key], 1) == []
    assert view.joinable_keys([acc.key]) == []
    assert view.free_live_keys([acc.key]) == []
    assert view.frozen_keys() == [acc.key]
    assert acc.key not in view.PROBLEM_SESSIONS  # frozen is not dead


async def check_error_reporting():
    reset()
    view._notify_owners_soon = lambda text: None
    acc = account("+910000000003")
    view.ACCOUNTS[acc.key] = acc
    assert await view.note_account_error(
        acc.key, UserDeactivatedError(GetStateRequest())) is True
    assert view.ACCOUNTS[acc.key].state == "dead"

    other = account("+910000000004")
    view.ACCOUNTS[other.key] = other
    assert await view.note_account_error(
        other.key, FrozenMethodInvalidError(GetStateRequest())) is True
    assert view.is_frozen(other.key) and other.state == "alive"

    third = account("+910000000005")
    view.ACCOUNTS[third.key] = third
    assert await view.note_account_error(third.key, TimeoutError()) is False
    assert third.state == "alive" and not view.is_frozen(third.key)


async def check_health_probe_and_scan():
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
    assert frozen_acc.state == "alive" and view.is_frozen(frozen_acc.key)

    blip = account("+920000000003", client=FakeClient(error=TimeoutError()))
    view.ACCOUNTS[blip.key] = blip
    assert await view.check_account_health(blip.key) == "unclear"
    assert view.account_usable(blip.key)          # a blip changes nothing

    fresh = account("+920000000004", client=FakeClient(me=None))
    view.ACCOUNTS[fresh.key] = fresh
    dead, frozen = await view.scan_dead_accounts()
    assert fresh.key in dead and frozen_acc.key in frozen
    assert blip.key not in dead and blip.key not in frozen


async def main():
    check_classifiers()
    check_dead_problem_classification()
    await check_dead_account_leaves_the_pool()
    await check_frozen_is_parked_not_deleted()
    await check_error_reporting()
    await check_health_probe_and_scan()
    print("all dead/frozen detection checks passed")


if __name__ == "__main__":
    asyncio.run(main())
