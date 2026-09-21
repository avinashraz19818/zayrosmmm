"""Post package split across Heroku worker shards (view.py).

Run from the repository root on a machine with the requirements installed:

    python tests/test_post_package_split.py
    python -m pytest tests/test_post_package_split.py -q

Why this file exists: with account sharding ON, every Heroku worker only
loads the sessions of its own shard, so ``ACCOUNTS`` / ``account_usable()`` /
``local_worker_account()`` are worker-local. ``process_new_post`` used to
filter the subscription's joined list through that local-only predicate
BEFORE slicing the package. Each of the N workers then delivered
``min(package, shard_size)``, so a 10-reaction package became 30 on a
three-dyno fleet — the client paid for one package and the log claimed the
delivery matched, while Telegram saw three. The checks below pin the fixed
maths: one global selection, identical on every worker, slices disjoint and
their sizes summing to the package exactly.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import view  # noqa: E402  (import after sys.path is set up)


class FakeClient:
    """Just enough TelegramClient for acc_client()/local_worker_account()."""

    async def disconnect(self):
        pass


def _account(key):
    acc = view.Account(key.replace("+", ""), f"/tmp/{key}.session",
                       view.API_ID, view.API_HASH, dict(view.ANDROID_PROFILE))
    acc.key = key
    acc.state = "alive"
    acc.client = FakeClient()
    return acc


def _fleet(workers, per_worker):
    """Deterministic fake fleet: key list, slot map, per-worker key sets."""
    keys, slot_by_key, owned = [], {}, {}
    serial = 0
    for w in range(workers):
        owned[w] = []
        for _ in range(per_worker):
            key = f"+91990000{serial:04d}"
            slot_by_key[key] = w
            keys.append(key)
            owned[w].append(key)
            serial += 1
    return keys, slot_by_key, owned


def _patch_sharding(slot_by_key, my_slot):
    """Pretend this process is dyno worker.<my_slot + 1> in a sharded fleet."""
    saved = {
        "ACCOUNT_SHARDING": view.ACCOUNT_SHARDING,
        "DYNO_NAME": view.DYNO_NAME,
        "WORKER_SLOT": view.WORKER_SLOT,
        "ACCOUNT_SLOT_BY_KEY": view.ACCOUNT_SLOT_BY_KEY,
    }
    view.ACCOUNT_SHARDING = True
    view.DYNO_NAME = f"worker.{my_slot + 1}"
    view.WORKER_SLOT = my_slot
    view.ACCOUNT_SLOT_BY_KEY = dict(slot_by_key)
    return saved


def _restore(saved):
    for name, value in saved.items():
        setattr(view, name, value)


def _fleet_order(slot_by_key):
    """The global joined order every worker must agree on."""
    saved = _patch_sharding(slot_by_key, 0)
    try:
        return view.globally_ordered_joined(list(slot_by_key))
    finally:
        _restore(saved)


def _old_split_total(local_pools, package):
    """Reproduce the pre-fix maths: every worker sliced only its own pool."""
    return sum(min(package, len(pool)) for pool in local_pools)


def test_global_selection_identical_on_every_worker():
    """Every dyno must agree on the same fleet order and the same first-N."""
    keys, slot_by_key, owned = _fleet(3, 10)
    agreed = None
    for w in range(3):
        saved = _patch_sharding(slot_by_key, w)
        try:
            joined = view.globally_ordered_joined(keys)  # noqa: F841
            if agreed is None:
                agreed = joined
            assert joined == agreed, f"worker {w}: fleet order differs"
            global_pick, _local = view.split_package_for_shard(
                joined, 10, lambda key: key in owned[w])
            assert global_pick == agreed[:10]
        finally:
            _restore(saved)


def test_slices_are_disjoint_and_sum_to_the_package():
    """No two workers deliver for the same account; totals are exact."""
    for package in (5, 10, 25, 40):
        keys, slot_by_key, owned = _fleet(3, 10)
        joined = _fleet_order(slot_by_key)
        slices = []
        for w in range(3):
            _global, local = view.split_package_for_shard(
                joined, package, lambda key: key in owned[w])
            slices.append(local)
        flat = [key for local in slices for key in local]
        assert len(flat) == len(set(flat)), \
            f"package {package}: two workers would double-send some accounts"
        assert sum(len(local) for local in slices) == min(package, len(keys))
        assert flat == joined[:min(package, len(keys))]


def test_old_maths_overdelivered_and_new_maths_is_exact():
    """The regression this fix kills: 3 dynos x 10 sessions, all healthy."""
    _keys, slot_by_key, owned = _fleet(3, 10)
    joined = _fleet_order(slot_by_key)
    pools = [owned[w] for w in range(3)]
    # Pre-fix: each worker delivered min(package, shard_size).
    assert _old_split_total(pools, 10) == 30    # 3x the package sold
    assert _old_split_total(pools, 5) == 15
    assert _old_split_total(pools, 20) == 30    # also unstable as usage moved
    for package in (1, 5, 10, 17, 25, 30, 45):
        delivered = 0
        for w in range(3):
            _global, local = view.split_package_for_shard(
                joined, package, lambda key: key in owned[w])
            delivered += len(local)
        assert delivered == min(package, len(joined)), \
            f"package {package}: fleet total must equal the package"


def test_local_slice_keeps_global_order_for_the_emoji_plan():
    """process_new_post maps emoji indexes by global position — order is load-bearing."""
    _keys, slot_by_key, owned = _fleet(3, 10)
    joined = _fleet_order(slot_by_key)
    global_pick, local = view.split_package_for_shard(
        joined, 21, lambda key: key in owned[1])
    positions = [global_pick.index(key) for key in local]
    assert positions == sorted(positions)
    index_by_key = {key: i for i, key in enumerate(global_pick)}
    assert all(index_by_key[key] < len(global_pick) for key in local)


def test_split_against_the_real_local_worker_account():
    """End-to-end against the production predicate with a seeded ACCOUNTS."""
    _keys, slot_by_key, owned = _fleet(3, 10)
    saved = _patch_sharding(slot_by_key, 1)
    saved_accounts = dict(view.ACCOUNTS)
    try:
        view.ACCOUNTS.clear()
        for key in owned[1]:
            view.ACCOUNTS[key] = _account(key)
        joined = view.globally_ordered_joined(list(slot_by_key))
        global_pick, local = view.split_package_for_shard(
            joined, 15, view.local_worker_account)
        assert len(global_pick) == 15
        # Only the accounts this dyno owns are executed here…
        assert local == [k for k in global_pick if k in owned[1]]
        assert all(view.local_worker_account(k) for k in local)
        # …and if one selected session failed to load on this dyno it drops
        # out of the local slice (its global slot stays in global_pick, so no
        # other worker double-executes it — deliver()/pick_workers guard
        # dead/frozen accounts via account_usable() at send time instead).
        view.ACCOUNTS[local[0]].client = None
        _g2, local2 = view.split_package_for_shard(
            joined, 15, view.local_worker_account)
        assert local2 == [k for k in local if k != local[0]]
        assert len(local2) == len(local) - 1
    finally:
        view.ACCOUNTS.clear()
        view.ACCOUNTS.update(saved_accounts)
        _restore(saved)


def test_single_process_slice_is_unchanged():
    """Non-sharded deploys (old VPS flavour) keep the plain first-N slice."""
    keys = [f"+91880000{i:04d}" for i in range(7)]
    global_pick, local = view.split_package_for_shard(keys, 3)
    assert global_pick == keys[:3] and local == keys[:3]
    assert view.split_package_for_shard(keys, 0, lambda k: True) == ([], [])
    assert view.split_package_for_shard(keys, 99, lambda k: False) == (keys, [])


def main():
    test_global_selection_identical_on_every_worker()
    test_slices_are_disjoint_and_sum_to_the_package()
    test_old_maths_overdelivered_and_new_maths_is_exact()
    test_local_slice_keeps_global_order_for_the_emoji_plan()
    test_split_against_the_real_local_worker_account()
    test_single_process_slice_is_unchanged()
    print("all post package split checks passed")


if __name__ == "__main__":
    main()
