# Patch: dead / frozen account fix for the Heroku worker branch

Ye folder me ek **ready patch** hai jo aapke **live Heroku version** (branch
`arena/01a015ce-zayrosmmm`) ke liye banaya gaya hai — kyunki wahi branch Heroku
par deploy hoti hai (Flask dashboard + MongoDB/GridFS sessions), aur usme
dead/frozen detection ka bug abhi bhi hai.

| File | Kya hai |
|---|---|
| `dead_frozen_fix_for_heroku.patch` | `git apply` se lagne wala patch (`view.py` + naya test file) |

Patch sirf **do cheezein** badalta hai:
* `view.py` — dead/frozen detection ka poora fix
* `tests/test_dead_frozen_detection.py` — naya test (naya file, kuch todta nahi)

## Patch kya theek karta hai

1. **Error pehchaan** — Telethon ke typed errors ka `str()` sirf English line
   deta hai ("The user has been deleted/deactivated"), isliye
   `USER_DEACTIVATED` / `FROZEN_METHOD_INVALID` jaisa koi marker **kabhi match
   hi nahi hota tha**. Ab error ko exception class ke naam se classify kiya
   jaata hai (`error_codes()`), plus `err.message` aur `str()` bhi scan hote hain.
   (Yehi asli bug tha: isliye dead/frozen accounts fleet me pade rehte the.)
2. **Health probe** — pehle `UpdateStatusRequest(offline=False)` bheja jaata
   tha, jiska jawab **frozen aur deleted login bhi normal deta hai**. Ab
   `get_me()` se poocha jaata hai (aur `is_user_authorized()` ki jagah bhi
   `get_me()`), jo deleted/banned/key-revoked login ke liye `None` deta hai:
   * account load par (`probe_and_register`)
   * keep-alive sweeper me (har cycle 30 accounts ka rolling check)
   * manual **Accounts → Scan Accounts**
3. **Pool se nikalna** — `mark_dead()` account ko har picker se hataata hai,
   client disconnect karta hai, aur reason **Needs Review** me likhta hai.
   `mark_frozen()` freeze ko 6 ghante ke liye park karta hai (session chalta
   rehta hai, sirf join/reaction band). **Session file kabhi delete nahi hoti.**
4. **Har job par gate** — reaction, views, join, onboarding/top-up joins,
   monitors aur poora live stack ab `account_usable()` / `usable_keys()` se
   guzarta hai; mid-job error aane par `note_account_error()` account ko
   turant pool se hata deta hai (pehle sirf request fail hoti thi).
5. **UI** — Accounts menu me "Usable / Dead / Frozen" counters, buttons
   **Scan Accounts**, **Trash Dead**, **Trash Frozen**, aur scan result me dead
   aur frozen ki alag list. Trash Dead sirf "dead" entries ko hi chhoota hai —
   locked/duplicate/unreadable files kabhi nahi.

## Kaise lagau (Heroku wale repo me)

```bash
# 1) apna repo clone/update karo, aur live branch par jao
git fetch origin
git checkout arena/01a015ce-zayrosmmm
git pull

# 2) patch lagao (file ka path yahi patch file ho)
git apply /path/to/dead_frozen_fix_for_heroku.patch

# lagta hai ya nahi, pehle check karna ho to:
git apply --check /path/to/dead_frozen_fix_for_heroku.patch

# 3) sanity check + commit + push
python -m py_compile view.py                 # syntax OK aana chahiye
python tests/test_dead_frozen_detection.py   # "all dead/frozen detection checks passed"
git add view.py tests/test_dead_frozen_detection.py
git commit -m "Fix dead/frozen account detection, cleanup and usage gating"
git push origin arena/01a015ce-zayrosmmm
```

Uske baad Heroku khud deploy karega (agar auto-deploy ON hai), warna
Dashboard → Deploy → branch chuno → **Deploy Branch**, phir
**More → Restart all dynos**.

### Agar `git apply` fail ho jaye

Us branch par `view.py` me koi aur change aa gaya ho to patch conflict dega.
Tab do raste hain:

1. `git apply --3way dead_frozen_fix_for_heroku.patch` try karo (conflict
   markers dikhayega), ya
2. Ye poora patch + problem statement kisi bhi coding assistant ko do
   (jaise doosri Arena chat me `arena/01a015ce-zayrosmmm` waali session):
   *"is patch ko current branch me merge karo aur dead/frozen fix laga do"*.

## Verify

Patch isi codebase ke copy par test kiya gaya hai:

| Check | Result |
|---|---|
| `python -m py_compile view.py` | OK |
| `pyflakes view.py` | baseline ke barabar (9 purane findings, koi naya nahi) |
| `python tests/test_dead_frozen_detection.py` | 6/6 pass |
| `git apply --check` (pristine branch par) | clean |

## Yaad rahe

* Patch us branch ko **replace nahi** karta — dashboard, MongoDB/GridFS
  session storage, sharding, sab waise hi rehta hai; sirf detection aur
  gating badalti hai.
* Heroku ka disk ephemeral hai, lekin is branch me sessions GridFS me rehte
  hain, to restart par accounts bache rahenge (jaisa aapne bataya).
* Patch ke `Procfile`/`runtime.txt`/`app.json` ko **chhedna nahi** — wo is
  branch ke apne hain aur Heroku unhi se chalta hai.
