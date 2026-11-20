"""Regression tests for frozen/disabled/deleted account filtering and low worker load."""

import asyncio
import os
import sys
from types import SimpleNamespace

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import view  # noqa: E402
from telethon.errors import (  # noqa: E402
    AuthKeyPermEmptyError,
    PhoneNumberUnoccupiedError,
    UserRestrictedError,
)
from telethon.tl.functions.updates import GetStateRequest  # noqa: E402


class FakeClient:
    def __init__(self, me=None):
        self.me = me
        self.disconnected = 0
        self.calls = []

    async def get_me(self):
        return self.me

    async def disconnect(self):
        self.disconnected += 1

    async def __call__(self, req):
        self.calls.append(req)
        raise RuntimeError("should not be called")


def _make_acc(key, stem=None, client=None):
    acc = view.Account(
        stem or key.replace("+", ""),
        f"/tmp/{stem or key}.session",
        view.API_ID,
        view.API_HASH,
        dict(view.ANDROID_PROFILE),
    )
    acc.key = key
    acc.state = "alive"
    acc.client = client if client is not None else FakeClient(me=SimpleNamespace(id=1, deleted=False))
    return acc


def _reset():
    view.ACCOUNTS.clear()
    view.PROBLEM_SESSIONS.clear()
    view.FROZEN_ACCOUNTS.clear()
    view.LIVE_AUDIO.clear()
    view.TGCALLS.clear()
    view.PEER_CACHE.clear()
    view.ACCOUNT_SLOT_BY_KEY.clear()


def test_extra_dead_and_frozen_classifiers():
    for err in (
        AuthKeyPermEmptyError(GetStateRequest()),
        PhoneNumberUnoccupiedError(GetStateRequest()),
    ):
        assert view.is_dead_account_error(err), type(err).__name__
        assert not view.is_frozen_account_error(err), type(err).__name__

    restricted = UserRestrictedError(GetStateRequest())
    assert view.is_frozen_account_error(restricted)
    assert not view.is_dead_account_error(restricted)


def test_deleted_user_flag_marks_dead():
    async def run():
        _reset()
        view._notify_owners_soon = lambda text: None
        acc = _make_acc("+910000000010", client=FakeClient(me=SimpleNamespace(id=10, deleted=True)))
        view.ACCOUNTS[acc.key] = acc
        res = await view.check_account_health(acc.key)
        assert res == "dead"
        assert acc.state == "dead"
        assert acc.client is None

    asyncio.run(run())


def test_worker_task_keys_and_live_keys_skip_frozen_and_dead():
    _reset()
    view._notify_owners_soon = lambda text: None
    ok_acc = _make_acc("+910000000011")
    frozen_acc = _make_acc("+910000000012")
    dead_acc = _make_acc("+910000000013")
    dead_acc.state = "dead"

    view.ACCOUNTS[ok_acc.key] = ok_acc
    view.ACCOUNTS[frozen_acc.key] = frozen_acc
    view.ACCOUNTS[dead_acc.key] = dead_acc
    view.mark_frozen(frozen_acc.key, "FrozenMethodInvalidError")

    assert view.worker_task_keys() == [ok_acc.key]
    assert view.ordered_live_account_keys() == [ok_acc.key]


def test_get_active_call_empty_keys_does_not_fallback_to_all_accounts():
    async def run():
        _reset()
        client = FakeClient(me=SimpleNamespace(id=14, deleted=False))
        acc = _make_acc("+910000000014", client=client)
        view.ACCOUNTS[acc.key] = acc

        call = await view.get_active_call(-1001234567890, [])
        assert call is None
        assert client.calls == []

    asyncio.run(run())


def test_resolve_peer_handles_none_client():
    async def run():
        _reset()
        assert await view.resolve_peer("+910000000015", None, "@somechannel") is None

    asyncio.run(run())
