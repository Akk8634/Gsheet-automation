import os, time, json, requests, re, subprocess
from google.oauth2.service_account import Credentials
from google.auth.transport.requests import AuthorizedSession
import gspread

SHEET_ID = os.environ["SHEET_ID"]
BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
BATCH_SIZE = int(os.environ.get("BATCH_SIZE","20"))

SCOPES = [
    'https://www.googleapis.com/auth/drive.readonly',
    'https://www.googleapis.com/auth/spreadsheets'
]

creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPES)
authed = AuthorizedSession(creds)
gc = gspread.authorize(creds)
ws = gc.open_by_key(SHEET_ID).sheet1

def extract_id(text):
    if not text: return None
    text=str(text)
    m = re.search(r'(?:id=|/d/)([A-Za-z0-9_-]{10,})',text)
    if m: return m.group(1)
    if re.match(r'^[A-Za-z0-9_-]{10,}$',text): return text
    return None

def download_drive(file_id,out):
    url = f"https://www.googleapis.com/drive/v3/files/{file_id}?alt=media"
    r = authed.get(url,stream=True,timeout=200)
    r.raise_for_status()
    with open(out,'wb') as f:
        for c in r.iter_content(1024*1024):
            f.write(c)

def convert_video(inp,out):
    cmd = [
        "HandBrakeCLI","-i",inp,"-o",out,
        "-e","x264","-q","22",
        "--aencoder","av_aac",
        "--mixdown","stereo",
        "--arate","44.1",
        "--ab","128",
        "--optimize"
    ]
    subprocess.run(cmd,check=True)

def tg_upload(path):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendVideo"
    with open(path,"rb") as f:
        r = requests.post(url,data={"chat_id":CHAT_ID},files={"video":f})
    j = r.json()
    if not j.get("ok"):
        raise Exception(j)
    file_id = j["result"]["video"]["file_id"]
    gf = requests.get(f"https://api.telegram.org/bot{BOT_TOKEN}/getFile?file_id={file_id}").json()
    file_path = gf["result"]["file_path"]
    return f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file_path}"

def process_row(idx,row):
    # row: [fileId,_,_,url,...]
    final = row[6] if len(row)>6 else ""
    if final.strip(): return False

    file_id = extract_id(row[0]) or extract_id(row[3])
    if not file_id:
        ws.update_cell(idx+1,6,"NO FILE ID")
        return True

    ws.update_cell(idx+1,6,"DOWN")

    inp=f"/tmp/in_{idx}.mp4"
    out=f"/tmp/out_{idx}.mp4"

    try:
        download_drive(file_id,inp)
        ws.update_cell(idx+1,6,"CONVERT")
        convert_video(inp,out)
        ws.update_cell(idx+1,6,"UPLOAD")
        url=tg_upload(out)
        ws.update_cell(idx+1,7,url)
        ws.update_cell(idx+1,6,"DONE")
    except Exception as e:
        ws.update_cell(idx+1,6,"ERR: "+str(e)[:150])
    finally:
        for p in [inp,out]:
            try: os.remove(p)
            except: pass

    return True

def main():
    rows=ws.get_all_values()
    processed=0
    for i in range(1,len(rows)):
        if processed>=BATCH_SIZE: break
        row=rows[i]+[""]*10
        if process_row(i,row):
            processed+=1
        time.sleep(1)

main()
