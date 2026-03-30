# Stock Market Scanner Bot — Setup Guide

A Python bot that scans all US equities during pre-market, regular hours, and after-hours, then emails you a report every day at 9:00 AM ET with the top gainers, losers, volume spikes, and pre-market movers.

---

## 1. Get Your API Keys

### Alpaca (market data)
1. Sign up free at https://alpaca.markets
2. Go to **Paper Trading** → **API Keys** → Generate a new key
3. Copy your **API Key** and **Secret Key**

### Gmail App Password (for sending email)
1. Enable 2-Factor Authentication on your Google account
2. Go to https://myaccount.google.com/apppasswords
3. Create an app password for "Mail"
4. Copy the 16-character password shown

---

## 2. Choose Where to Run

### Option A — Railway (easiest, ~$5/mo)
1. Push this folder to a GitHub repo
2. Go to https://railway.app → New Project → Deploy from GitHub
3. Add your environment variables in Railway's dashboard (Settings → Variables)
4. Railway auto-detects Python and runs `scanner.py`
5. **Set your Railway server timezone**: add env var `TZ=America/New_York`

### Option B — DigitalOcean Droplet (~$6/mo, most control)
Follow the deployment steps in Section 4 below.

### Option C — Your own computer (not recommended for 24/7)
Only do this if your computer never sleeps. Skip to Section 3.

---

## 3. Configure Your Environment

Copy the example env file and fill in your values:

```bash
cp .env.example .env
nano .env   # or open in any text editor
```

Fill in:
- `ALPACA_API_KEY` and `ALPACA_SECRET_KEY` from step 1
- `SMTP_USER` — your Gmail address
- `SMTP_PASSWORD` — the App Password from step 1 (not your real password)
- `REPORT_TO` — the email address to send reports to

---

## 4. Install & Run

### Install dependencies

```bash
# Create a virtual environment (recommended)
python3 -m venv venv
source venv/bin/activate        # Mac/Linux
# venv\Scripts\activate         # Windows

pip install -r requirements.txt
```

### Run the bot

```bash
# Load env vars and start
export $(cat .env | xargs)
python scanner.py
```

You should see:
```
Database ready: market_data.db
Stock bot starting…
Universe: 8000+ symbols
Scheduler running. Scans every 5 min; report at 09:00 ET daily.
Starting scan — session: premarket
Saved 7842 snapshots (session=premarket)
```

---

## 5. Deploy as a Background Service (DigitalOcean / Linux server)

```bash
# 1. Copy files to your server
scp -r stock_bot/ ubuntu@YOUR_SERVER_IP:/home/ubuntu/

# 2. SSH in
ssh ubuntu@YOUR_SERVER_IP

# 3. Install Python + create venv
sudo apt update && sudo apt install python3 python3-venv -y
cd /home/ubuntu/stock_bot
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 4. Create your .env file
cp .env.example .env
nano .env   # fill in your keys

# 5. Set server timezone to Eastern Time
sudo timedatectl set-timezone America/New_York

# 6. Install the systemd service
sudo cp stock_bot.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable stock_bot
sudo systemctl start stock_bot

# 7. Check it's running
sudo systemctl status stock_bot
sudo journalctl -u stock_bot -f   # live logs
```

---

## 6. What the Bot Does, Hour by Hour

| Time (ET)       | Action                                      |
|-----------------|---------------------------------------------|
| 4:00 AM         | Pre-market scanning begins (every 5 min)    |
| 9:00 AM         | **Daily report emailed to you**             |
| 9:30 AM         | Switches to regular-hours scanning          |
| 4:00 PM         | Switches to after-hours scanning            |
| 8:00 PM         | Scanning stops for the night                |

---

## 7. Customizing the Report

Edit these values in your `.env` file:

| Variable                  | Default | Effect                                      |
|---------------------------|---------|---------------------------------------------|
| `TOP_N`                   | 10      | How many gainers/losers to show             |
| `VOLUME_SPIKE_MULTIPLIER` | 3.0     | Flag stocks with volume 3× their 30d avg    |

---

## 8. Checking the Database

The bot stores every snapshot in `market_data.db` (SQLite). You can query it directly:

```bash
sqlite3 market_data.db

# See today's top snapshots
SELECT symbol, price, change_pct, session
FROM snapshots
WHERE date(ts) = date('now')
ORDER BY change_pct DESC
LIMIT 20;
```

---

## Troubleshooting

**Bot starts but no data comes in**
- Check your Alpaca keys are correct in `.env`
- Make sure it's during a valid session (4am–8pm ET on weekdays)
- Run `python scanner.py` directly and watch the logs

**Email not sending**
- Make sure you used a Gmail **App Password**, not your real password
- Check that 2FA is enabled on your Google account
- Try `SMTP_PORT=465` with `smtplib.SMTP_SSL` if port 587 is blocked

**Report shows "No data collected today"**
- The bot may not have had time to collect data before 9am on its first day
- Wait until the next morning after a full day of scanning

---

*Data from Alpaca Markets. Not financial advice.*
