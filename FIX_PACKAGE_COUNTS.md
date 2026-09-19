# FIX: Package counts multiplied across Heroku worker shards

## Symptom (kya galat ho raha tha)

With account sharding ON (Heroku, multiple worker dynos), a client's per-post
package did **not** deliver the counts they paid for:

- Package = 10 reactions/post on a 3-dyno fleet → **30 reactions** landed
  (workers × package). Views behaved the same way.
- Counts also drifted with usage/dead sessions, because each dyno counted only
  its own shard. The client-visible numbers ("reactions/views per post")
  stopped meaning anything exact, and the extra Telegram traffic added flood
  risk for the whole fleet.

The **dashboard manual React/Views** actions and the **live (audio) package**
were already correct — both select globally, then execute the local slice.
Only the automatic **per-post** delivery (`process_new_post`) was wrong.

## Root cause (wajah)

Every Heroku worker loads only its own shard's sessions, so `ACCOUNTS`,
`account_usable()` and `local_worker_account()` are **worker-local**.

`process_new_post` filtered the subscription's joined list through the local
`account_usable()` **before** slicing the package:

```python
joined  = [k for k in recorded if account_usable(k)]   # local-only sessions!
n_react = min(sub["reactions_per_post"], len(joined))  # per-worker pool size
react_keys = [k for k in joined[:n_react] if local_worker_account(k)]
```

So each of the N workers computed `min(package, len(own_shard))` and delivered
that much. Summed over the fleet: `N × min(package, shard_size)` instead of
`package` — e.g. `3 × min(10, 10) = 30` for a 10-reaction package.

## Fix (kya badla) — `view.py`

1. **Fleet-wide pool in sharded mode.** Workers now count the subscription's
   WHOLE joined list (`globally_ordered_joined`, deterministic fleet order)
   before slicing. Local usability is still enforced per account inside
   `deliver()` and by the spare-account failover (dead/frozen sessions get
   skipped and replaced there, same as before).
2. **One shared split: `split_package_for_shard()`.** Takes the same global
   first-N selection on every worker, then keeps only the keys the calling
   worker owns. Slices are disjoint, keep global order (the emoji plan indexes
   still match), and sum to the package exactly:
   `Σ workers = min(package, joined_total)`. Non-sharded deploys keep the old
   plain first-N slice (`is_local=None`).
3. **Honest per-shard logging.** Each worker now compares its deliveries with
   ITS OWN slice (`len(react_keys)` / `len(view_keys)`) instead of the whole
   package — otherwise every dyno would log "SHORT" even when the fleet
   together delivered everything. Workers that own none of the selected keys
   stay quiet instead of logging `0 reactions, 0 views` on every post.

## Dashboard — `web.py`

`/api/plans` now also returns `joined` (accounts actually inside the channel)
and the Plans table shows it in a **Joined** column, highlighted red when
`joined < accounts` — the delivered post package can never exceed the number
of accounts that are actually in the channel, so a gap there is the one thing
an owner had no way to see.

## Tests — `tests/test_post_package_split.py`

Six checks (pytest + direct run) pin the maths: identical global selection on
every worker, disjoint slices, exact totals for package sizes 1–45, preserved
slice order for the emoji plan, behaviour with the real
`local_worker_account()` predicate, and unchanged single-process semantics.
One check reproduces the old maths (`3 × min(10,10) = 30`) so the regression
cannot come back silently.

Run before/after deploy:

```bash
python tests/test_post_package_split.py        # or: python -m pytest tests/ -q
python tests/test_dead_frozen_detection.py     # existing suite, untouched
```

## Deploy & rollback

- Deploy branch: `arena/01a015ce-zayrosmmm` (the live Heroku worker branch)
  after this PR merges — Heroku Dashboard → app → **Deploy** → **Manual
  deploy** → pick the branch → **Deploy Branch** (or `git push heroku
  arena/01a015ce-zayrosmmm:main`).
- No config/env changes, no data migration. Existing subscriptions work as-is;
  only the delivered counts change — back to what the package says.
- Rollback: `heroku releases:rollback -a <app>` reverts to the previous slug.
