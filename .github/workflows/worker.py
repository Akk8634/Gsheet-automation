#!/usr/bin/env python3
import os, time, json, re, subprocess, requests
from google.oauth2.service_account import Credentials
from google.auth.transport.requests import AuthorizedSession
import gspread

# env / defaults
SHEET_ID = os.environ.get("SHEET_ID")
BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE","20"))

SCOPES = [
    'https://www.googleapis.com/auth/drive.readonly',
    'https://www.googleapis.com/auth/spreadsheets'
]

# init Google auth + gspread
creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPES)
authed = AuthorizedSession(creds)
gc = gspread.authorize(creds)
ws = gc.open_by_key(SHEET_ID).sheet1

def extract_id(text):
    if not text: return None
    s=str(text).strip()
    m = re.search(r'(?:id=|/d/)([A-Za-z0-9_-]{10,})', s)
    if m: return m.group(1)
    if re.match(r'^[A-Za-z0-9_-]{10,}$', s): return s
    return None

def download_drive(file_id,out):
    url = f"https://www.googleapis.com/drive/v3/files/{file_id}?alt=media"
    r = authed.get(url, stream=True, timeout=300)
    r.raise_for_status()
    with open(out,'wb') as f:
        for c in r.iter_content(1024*1024):
            if c: f.write(c)

def convert_with_ffmpeg(inp, out):
    """
    Convert using ffmpeg to H.264 + AAC-LC, web-optimized.
    Uses veryfast preset for speed. Adjust -crf for quality/speed tradeoff.
    """
    cmd = [
        "ffmpeg",
        "-y",
        "-i", inp,
        "-c:v", "libx264",
        "-preset", "veryfast",
        "-crf", "23",
        "-vf", "fps=30,format=yuv420p",
        "-c:a", "aac",
        "-b:a", "128k",
        "-ar", "44100",
        "-movflags", "+faststart",
        "-threads", "0",
        out
    ]
    # Run and raise on error
    subprocess.run(cmd, check=True)

def tg_upload(path):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendVideo"
    with open(path,"rb") as f:
        r = requests.post(url, data={"chat_id": CHAT_ID}, files={"video": f}, timeout=300)
    j = r.json()
    if not j.get("ok"):
        # fallback to sendDocument
        with open(path,"rb") as f:
            r2 = requests.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendDocument",
                                data={"chat_id": CHAT_ID}, files={"document": f}, timeout=300)
        j = r2.json()
        if not j.get("ok"):
            raise Exception("TG upload failed: " + json.dumps(j))
    res = j['result']
    fid = None
    if 'video' in res and 'file_id' in res['video']:
        fid = res['video']['file_id']
    elif 'document' in res and 'file_id' in res['document']:
        fid = res['document']['file_id']
    else:
        raise Exception("no file_id in tg response")
    gf = requests.get(f"https://api.telegram.org/bot{BOT_TOKEN}/getFile?file_id={fid}", timeout=60).json()
    return f"https://api.telegram.org/file/bot{BOT_TOKEN}/{gf['result']['file_path']}"

def process_row(idx, row):
    # columns: A:file_id, B:folder, C:path, D:drive_url, E:tg_old, F:status, G:final_url
    final = row[6] if len(row)>6 else ""
    if final and str(final).strip(): return False
    file_id = extract_id(row[0]) or extract_id(row[3]) or None
    if not file_id:
        ws.update_cell(idx+1, 6, "NO_FILEID")
        return True

    ws.update_cell(idx+1, 6, "DOWNLOADING")
    in_path = f"/tmp/in_{idx}.mp4"
    out_path = f"/tmp/out_{idx}.mp4"
    try:
        download_drive(file_id, in_path)
        ws.update_cell(idx+1, 6, "CONVERT")
        # use ffmpeg conversion (replaced HandBrake)
        convert_with_ffmpeg(in_path, out_path)
        ws.update_cell(idx+1, 6, "UPLOAD")
        public_url = tg_upload(out_path)
        ws.update_cell(idx+1, 7, public_url)
        ws.update_cell(idx+1, 6, "DONE")
    except Exception as e:
        # write a concise error to sheet for debugging
        err_text = str(e)
        ws.update_cell(idx+1, 6, "ERROR: "+err_text[:250])
    finally:
        for p in (in_path,out_path):
            try: os.remove(p)
            except: pass
    return True

def main():
    rows = ws.get_all_values()
    processed = 0
    for i in range(1, len(rows)):
        if processed >= BATCH_SIZE: break
        row = rows[i] + [""]*10
        if process_row(i, row):
            processed += 1
        time.sleep(1)

if __name__ == "__main__":
    main()
