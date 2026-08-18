# Heroku pe host karne ka SARAL TARIKA (5 step, bina command ke)

> VPS pe jitna aasan tha, Heroku pe bhi utna hi aasan hai. Bas ye 5 steps follow karo.

---

## Step 0 — Sabse pehle: VPS wala bot BAND karo ⚠️

VPS pe (jahan bot chalta hai) ye command chalao:

```
pkill -f view.py
```

Agar dono (VPS + Heroku) ek saath chale, to Telegram pe accounts ban ho sakte hain. Isliye ye step miss mat karna.

---

## Step 1 — Heroku account banao

1. Browser me kholo: **https://heroku.com**
2. **Sign up** karo (email + password)
3. Email me aaya verify link dabao
4. Heroku me login karo → **Account settings → Billing** me credit card add karo
   - Heroku ka free plan band ho chuka hai, isliye **Eco plan (~$5/month)** select karna padega
   - Pehle 30 din me $5 free credit bhi milta hai kabhi-kabhi, check kar lena

---

## Step 2 — Ek click me app banao (Deploy button)

**Is link pe click karo:**

👉 **https://heroku.com/deploy?template=https://github.com/avinashraz19818/zayrosmmm**

1. **App name** me kuch bhi likho (jaise: `telegram-smm-bot`) — ye naam unique hona chahiye
2. **Region** me **United States** chhodo
3. Neeche **Deploy app** ka button dabao
4. Build hone me 2-5 minute lagenge (green tick aayega)

> Ye button app.json ki wajah se kaam karta hai — ffmpeg aur python dono buildpacks khud set ho jate hain. Kuch aur set karne ki zaroorat nahi.

---

## Step 3 — Worker ON karo (yehi bot ko chalata hai)

1. App open hua hai → upar **Resources** tab pe click karo
2. Neeche scroll karo → **worker** dikhega (web nahi)
3. Uske bagal me **pencil ✏️ icon** pe click karo
4. Toggle ko **ON** karo → **Confirm** karo

---

## Step 4 — Bot check karo

Telegram me apne bot ko kholo (`@tumhara_bot`) aur **/start** bhejo.

Bot ne jo msg likha hai wo ya to "Access Denied" hoga (agar tumhara ID approved nahi hai) ya menu dikhega. Agar 1-2 minute me reply nahi aaya to Step 5 dekho.

---

## Step 5 — Accounts upload karo (sirf pehli baar)

Heroku me sessions folder khaali hota hai (naya system hai na!). Isliye:

1. Bot me **Accounts** menu kholo
2. **Import ZIP** option chuno
3. Wahi ZIP file do jo tumne VPS pe use ki thi (sessions wali)

Ho gaya! Ab accounts load ho jayenge, aur **har 5 minute me MongoDB me backup** hota rahega — restart/deploy ke baad bhi accounts apne aap wapas aa jayenge.

---

## Problem aaye to kya karein

| Problem | Solution |
|---|---|
| Bot reply nahi de raha | heroku.com → apna app → **More** (upar right) → **View logs** → wahan error dikhega |
| Log me `MongoDB ... FAILED` | Atlas me Network Access me `0.0.0.0/0` allow hai? |
| Log me `live audio : UNAVAILABLE` | Ye theek hai, bot chalta rahega — live audio ke liye ffmpeg chahiye |
| Account "not authorized" dikh raha | ZIP dobara import karo |

---

## Band karna ho to

heroku.com → apna app → **Resources** → worker ko OFF kar do. (Jab tak OFF hai, bot band hai.)
