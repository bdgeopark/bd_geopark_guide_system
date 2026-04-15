import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time
import calendar
import os
from fpdf import FPDF
import base64
import json

# =========================================================
# 1. 초기 설정 및 상수
# =========================================================
st.set_page_config(page_title="지질공원 통합관리", page_icon="🪨", layout="wide")

st.markdown("""
    <style>
    html, body, [class*="css"] { font-size: 18px !important; }
    div[data-testid="stDataEditor"] table { font-size: 16px !important; }
    div[data-testid="stSelectbox"] * { font-size: 18px !important; }
    div[data-testid="stForm"] { border: 2px solid #f0f2f6; padding: 20px; border-radius: 10px; }
    div[data-testid="stMetricValue"] { font-size: 28px !important; font-weight: bold; }
    </style>
    """, unsafe_allow_html=True)

SPREADSHEET_NAME = "지질공원_운영일지_DB"
LOCATIONS = {
    "백령도": ["두무진 안내소", "콩돌해안 안내소", "사곶해변 안내소", "용기포신항 안내소", "진촌리 현무암 안내소", "용틀임바위 안내소", "임시지질공원센터"],
    "대청도": ["서풍받이 안내소", "옥죽동 해안사구 안내소", "농여해변 안내소", "선진동 선착장 안내소"],
    "소청도": ["분바위 안내소", "탑동 선착장 안내소"],
    "시청": ["인천시청", "지질공원팀 사무실"]
}
DAY_MAP = {0: "월", 1: "화", 2: "수", 3: "목", 4: "금", 5: "토", 6: "일"}

# --- [추가] 토큰 암호화/복호화 함수 ---
def encode_token(uid, upw):
    data = json.dumps({"uid": uid, "upw": upw}).encode('utf-8')
    return base64.urlsafe_b64encode(data).decode('utf-8')

def decode_token(token):
    try:
        data = base64.urlsafe_b64decode(token.encode('utf-8'))
        return json.loads(data.decode('utf-8'))
    except:
        return None
# ------------------------------------

def get_kst_now():
    return datetime.utcnow() + timedelta(hours=9)

def safe_int(val, default=0):
    try:
        n = pd.to_numeric(val, errors='coerce')
        return int(n) if pd.notna(n) else default
    except:
        return default

def round_time_30min(t_str):
    if not t_str or str(t_str).strip() == "" or str(t_str).strip() == "-": return ""
    try:
        raw_t = str(t_str).split('(')[0].strip()
        t = datetime.strptime(raw_t, "%H:%M")
        if t.minute == 0:
            return t.strftime("%H:%M")
        elif 1 <= t.minute <= 30:
            t = t.replace(minute=30)
        else:
            t = (t + timedelta(hours=1)).replace(minute=0)
        return t.strftime("%H:%M")
    except:
        return str(t_str).split('(')[0].strip()

def format_time_with_rounded(t_raw):
    raw = str(t_raw).split('(')[0].strip()
    if not raw or raw == "-": return ""
    rnd = round_time_30min(raw)
    if raw == rnd: return raw
    return f"{raw} ({rnd})"

def calc_working_hours(c_in, c_out):
    r_in = round_time_30min(c_in)
    r_out = round_time_30min(c_out)
    if not r_in or not r_out or r_in == "-" or r_out == "-": return ""
    try:
        t_in = datetime.strptime(r_in, "%H:%M")
        t_out = datetime.strptime(r_out, "%H:%M")
        
        h_in = t_in.hour + t_in.minute / 60.0
        h_out = t_out.hour + t_out.minute / 60.0
        if h_out < h_in: h_out += 24.0
        
        lunch_start = 12.0
        lunch_end = 13.0
        overlap = max(0, min(h_out, lunch_end) - max(h_in, lunch_start))
        
        hours = (h_out - h_in) - overlap
        if hours < 0: hours = 0
        
        return str(int(hours)) if float(hours).is_integer() else str(round(hours, 1))
    except:
        return ""

if 'logged_in' not in st.session_state: st.session_state['logged_in'] = False
if 'user_info' not in st.session_state: st.session_state['user_info'] = {}

if 'cur_year' not in st.session_state: st.session_state['cur_year'] = get_kst_now().year
if 'cur_month' not in st.session_state: st.session_state['cur_month'] = get_kst_now().month

# =========================================================
# 2. 데이터 함수
# =========================================================
@st.cache_resource
def get_client():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    try:
        if os.path.exists("geopark_key.json"):
            creds = ServiceAccountCredentials.from_json_keyfile_name("geopark_key.json", scope)
        else:
            key_dict = st.secrets["gcp_service_account"]
            creds = ServiceAccountCredentials.from_json_keyfile_dict(key_dict, scope)
        return gspread.authorize(creds)
    except: return None

client = get_client()

@st.cache_data(ttl=60, show_spinner=False)
def load_data(sheet_name, year=None, month=None, island=None):
    try:
        try: sh = client.open(SPREADSHEET_NAME).worksheet(sheet_name)
        except: return pd.DataFrame()
            
        vals = sh.get_all_values()
        if not vals or len(vals) < 2: return pd.DataFrame()
        
        headers = [str(h).strip() for h in vals[0]]
        df = pd.DataFrame(vals[1:], columns=headers)
        
        if '일자' in df.columns: df.rename(columns={'일자': '날짜'}, inplace=True)
        
        for c in ["날짜", "이름", "장소", "섬"]:
            if c not in df.columns: df[c] = ""

        if sheet_name == "활동계획":
            for c in ['대타여부', '기존해설사', '상태', '활동여부']:
                if c not in df.columns: df[c] = ""
        elif sheet_name == "활동일지":
            for c in ["출근시간", "퇴근시간"]:
                if c not in df.columns: df[c] = ""
        elif sheet_name == "운영일지":
            for c in ["입력시간", "탐방객수", "청취자수", "해설횟수", "특이사항", "공동해설", "타임스탬프", "년", "월"]:
                if c not in df.columns: df[c] = ""

        if not df.empty and '날짜' in df.columns:
            df['날짜_dt'] = pd.to_datetime(df['날짜'], errors='coerce')
            df = df.dropna(subset=['날짜_dt'])
            
            if not df.empty:
                mask = pd.Series(True, index=df.index)
                if year: mask &= (df['날짜_dt'].dt.year == int(year))
                if month: mask &= (df['날짜_dt'].dt.month == int(month))
                df = df[mask]
                
            df = df.drop(columns=['날짜_dt'], errors='ignore')
        
        if island and '섬' in df.columns and not df.empty:
            df = df[df['섬'] == island]
            
        return df
    except Exception as e:
        return pd.DataFrame()

def save_data(sheet_name, row_dicts):
    try:
        doc = client.open(SPREADSHEET_NAME)
        try: sh = doc.worksheet(sheet_name)
        except:
            sh = doc.add_worksheet(sheet_name, 1000, len(row_dicts[0]))
            sh.append_row(list(row_dicts[0].keys()))
            
        vals = sh.get_all_values()
        if not vals:
            headers = list(row_dicts[0].keys())
            sh.append_row(headers)
            df = pd.DataFrame(columns=headers)
        else:
            headers = [str(h).strip() for h in vals[0]]
            df = pd.DataFrame(vals[1:], columns=headers)
            
        if '일자' in df.columns: df.rename(columns={'일자': '날짜'}, inplace=True)
        
        new_df = pd.DataFrame(row_dicts)
        def make_key(d): return str(d.get('날짜','')) + str(d.get('이름','')) + str(d.get('장소',''))

        if not df.empty: df['key'] = df.apply(make_key, axis=1)
        else: df['key'] = []
        new_df['key'] = new_df.apply(make_key, axis=1)
            
        keys_to_remove = new_df['key'].tolist()
        final_df = df[~df['key'].isin(keys_to_remove)].copy() if not df.empty else df
        
        final_df = final_df.drop(columns=['key'], errors='ignore')
        new_df = new_df.drop(columns=['key'], errors='ignore')
        
        for col in list(row_dicts[0].keys()):
            if col not in final_df.columns: final_df[col] = ""
            
        combined = pd.concat([final_df, new_df], ignore_index=True).fillna("")
        
        if '날짜' in combined.columns:
            temp_dt = pd.to_datetime(combined['날짜'], errors='coerce')
            combined['sort_key'] = temp_dt.fillna(pd.Timestamp('1900-01-01'))
            combined = combined.sort_values('sort_key').drop(columns=['sort_key'])
            combined['날짜'] = temp_dt.dt.strftime("%Y-%m-%d").fillna(combined['날짜'])
            
        final_cols = headers.copy()
        for c in combined.columns:
            if c not in final_cols: final_cols.append(c)
            
        sh.clear()
        sh.update([final_cols] + combined[final_cols].astype(str).values.tolist())
        st.cache_data.clear()
        return True
    except Exception as e:
        st.error(f"저장 오류: {e}")
        return False

def append_data(sheet_name, row_dict):
    try:
        doc = client.open(SPREADSHEET_NAME)
        try: sh = doc.worksheet(sheet_name)
        except:
            sh = doc.add_worksheet(sheet_name, 1000, len(row_dict))
            sh.append_row(list(row_dict.keys()))
        
        vals = sh.get_all_values()
        if not vals:
            headers = list(row_dict.keys())
            sh.append_row(headers)
            df = pd.DataFrame(columns=headers)
        else:
            headers = [str(h).strip() for h in vals[0]]
            df = pd.DataFrame(vals[1:], columns=headers)
            
        if '일자' in df.columns: df.rename(columns={'일자': '날짜'}, inplace=True)
        
        new_df = pd.DataFrame([row_dict])
        combined = pd.concat([df, new_df], ignore_index=True).fillna("")
        
        if '날짜' in combined.columns:
            temp_dt = pd.to_datetime(combined['날짜'], errors='coerce')
            combined['sort_key'] = temp_dt.fillna(pd.Timestamp('1900-01-01'))
            combined = combined.sort_values('sort_key').drop(columns=['sort_key'])
            combined['날짜'] = temp_dt.dt.strftime("%Y-%m-%d").fillna(combined['날짜'])
            
        final_cols = headers.copy()
        for c in combined.columns:
            if c not in final_cols: final_cols.append(c)
            
        sh.clear()
        sh.update([final_cols] + combined[final_cols].astype(str).values.tolist())
        st.cache_data.clear()
        return True
    except Exception as e:
        st.error(f"데이터 추가 중 오류 발생: {e}")
        return False

def get_users(island):
    try:
        sh = client.open(SPREADSHEET_NAME).worksheet("사용자")
        users = sh.get_all_records()
        return [u['이름'] for u in users if u.get('섬') == island]
    except: return []

# =========================================================
# 3. PDF 및 데이터 가공 로직
# =========================================================

def get_display_data(df_plan, df_act, date_list, show_place=False, is_pdf=False, is_plan_only=False):
    disp_rows = []
    unique_workers = []

    if not df_plan.empty: df_plan['d_str'] = pd.to_datetime(df_plan['날짜'], errors='coerce').dt.strftime('%Y-%m-%d')
    if not df_act.empty: df_act['d_str'] = pd.to_datetime(df_act['날짜'], errors='coerce').dt.strftime('%Y-%m-%d')

    if not df_plan.empty:
        orig_workers = df_plan[df_plan['대타여부'] != 'O']['이름'].astype(str).str.strip().unique().tolist()
        for w in orig_workers:
            if w and w not in unique_workers: unique_workers.append(w)
        
        replaced_workers = df_plan[df_plan['대타여부'] == 'O']['기존해설사'].astype(str).str.strip().unique().tolist()
        for w in replaced_workers:
            if w and w not in unique_workers: unique_workers.append(w)

    pure_subs = []
    if not df_plan.empty:
        all_subs = df_plan[df_plan['대타여부'] == 'O']['이름'].astype(str).str.strip().unique().tolist()
        pure_subs = [s for s in all_subs if s not in unique_workers]

    if not df_act.empty:
        act_workers = df_act['이름'].astype(str).str.strip().unique().tolist()
        for w in act_workers:
            if w and w not in unique_workers and w not in pure_subs:
                unique_workers.append(w)

    workers_list = unique_workers[:4]
    while len(workers_list) < 4:
        workers_list.append("")
        
    for d in date_list:
        if isinstance(d, str): d_obj = datetime.strptime(d, "%Y-%m-%d")
        else: d_obj = d
        d_str = d_obj.strftime("%Y-%m-%d"); w_day = DAY_MAP[d_obj.weekday()]
        row_dat = {"날짜": d_str, "요일": w_day}
        
        day_plan = df_plan[df_plan['d_str'] == d_str] if not df_plan.empty else pd.DataFrame()
        day_act = df_act[df_act['d_str'] == d_str] if not df_act.empty else pd.DataFrame()
        
        for i, w in enumerate(workers_list):
            if not w:
                row_dat[f'plan_{i}'] = ""
                row_dat[f'res_{i}'] = ""
                continue

            p_text, r_text = "", ""
            
            orig_plan = day_plan[(day_plan['이름'].astype(str).str.strip() == w) & (day_plan['대타여부'] != 'O')] if not day_plan.empty else pd.DataFrame()
            sub_plan = day_plan[(day_plan['기존해설사'].astype(str).str.strip() == w) & (day_plan['대타여부'] == 'O')] if not day_plan.empty else pd.DataFrame()
            orig_act = day_act[day_act['이름'].astype(str).str.strip() == w] if not day_act.empty else pd.DataFrame()
            
            def get_hours(act_df):
                if act_df.empty: return ""
                c_in = str(act_df.iloc[-1].get('출근시간', '')).split('(')[0].strip()
                c_out = str(act_df.iloc[-1].get('퇴근시간', '')).split('(')[0].strip()
                if c_in and c_out:
                    h = calc_working_hours(c_in, c_out)
                    return f"{h}H" if h else ""
                elif c_in:
                    return "근무중"
                return "미출근"
            
            p_place = ""
            if show_place and not orig_plan.empty:
                p_place = f" [{str(orig_plan.iloc[-1].get('장소','')).replace(' 안내소', '')}]"

            if not orig_plan.empty:
                op = orig_plan.iloc[-1]
                plan_time = str(op.get('활동여부', '')).strip()
                stat = str(op.get('상태', '')).strip()
                
                appr_sub = sub_plan[sub_plan['상태'] == '승인완료'] if not sub_plan.empty else pd.DataFrame()
                pend_sub = sub_plan[sub_plan['상태'] == '승인대기'] if not sub_plan.empty else pd.DataFrame()
                
                if stat == '취소승인':
                    p_text = f"~~{plan_time}{p_place}~~\n취소"
                    r_text = ""
                elif not appr_sub.empty:
                    sub_name = str(appr_sub.iloc[-1]['이름']).strip()
                    p_text = f"~~{plan_time}{p_place}~~\n대타({sub_name})"
                    if is_plan_only:
                        r_text = ""
                    else:
                        sub_act = day_act[day_act['이름'].astype(str).str.strip() == sub_name] if not day_act.empty else pd.DataFrame()
                        h = get_hours(sub_act)
                        r_text = f"{sub_name}\n{h}" if h else ""
                else:
                    if is_pdf:
                        p_text = f"{plan_time}{p_place}"
                    else:
                        if stat == '취소대기':
                            p_text = f"{plan_time}{p_place} (취소요청)"
                        elif not pend_sub.empty:
                            sub_name = str(pend_sub.iloc[-1]['이름']).strip()
                            p_text = f"{plan_time}{p_place} (대타요청:{sub_name})"
                        elif stat == '승인대기':
                            p_text = f"{plan_time}{p_place} (대기)"
                        else:
                            p_text = f"{plan_time}{p_place}"
                            
                    if is_plan_only:
                        r_text = ""
                    else:
                        r_text = get_hours(orig_act)
            else:
                if not orig_act.empty:
                    if show_place:
                        p_place = f" [{str(orig_act.iloc[-1].get('장소','')).replace(' 안내소', '')}]"
                    p_text = f"(계획없음){p_place}" if not is_pdf else ""
                    if is_plan_only:
                        r_text = ""
                    else:
                        r_text = get_hours(orig_act)
                        
            row_dat[f'plan_{i}'] = p_text
            row_dat[f'res_{i}'] = r_text
            
        disp_rows.append(row_dat)
    return disp_rows, workers_list

def generate_official_journal_month_pdf(df_act, df_op, p_year, p_month, target_place, p_range):
    font_path = "NanumGothic.ttf"
    if not os.path.exists(font_path): return None

    pdf = FPDF(orientation='P', unit='mm', format='A4')
    pdf.set_margins(15, 15, 15)
    pdf.set_auto_page_break(True, margin=15)
    pdf.add_font("Nanum", "", font_path)
    pdf.add_font("Nanum", "B", font_path)

    dates_act = set(pd.to_datetime(df_act['날짜'], errors='coerce').dt.strftime('%Y-%m-%d').dropna().unique()) if not df_act.empty else set()
    dates_op = set(pd.to_datetime(df_op['날짜'], errors='coerce').dt.strftime('%Y-%m-%d').dropna().unique()) if not df_op.empty else set()
    all_dates = sorted(list(dates_act | dates_op))
    
    if "전반기" in p_range:
        all_dates = [d for d in all_dates if int(d.split('-')[2]) <= 15]
    elif "후반기" in p_range:
        all_dates = [d for d in all_dates if int(d.split('-')[2]) >= 16]
        
    if not all_dates: return None

    for target_date in all_dates:
        pdf.add_page()
        d_obj = datetime.strptime(target_date, "%Y-%m-%d")
        w_day = DAY_MAP[d_obj.weekday()]

        pdf.set_font("Nanum", "B", 18)
        pdf.cell(180, 10, "【서식 3】 지질공원 안내소 운영일지", 0, 1, 'C')
        pdf.ln(5)

        pdf.set_font("Nanum", "B", 11)
        pdf.cell(180, 8, f"({d_obj.year}년 {d_obj.month}월 {d_obj.day}일) {w_day}요일", 0, 1, 'R')

        pdf.set_fill_color(240, 240, 240)
        pdf.set_font("Nanum", "B", 10)
        
        pdf.cell(30, 8, "안내소", 1, 0, 'C', True)
        pdf.set_font("Nanum", "", 10)
        pdf.cell(40, 8, str(target_place), 1, 0, 'C')
        pdf.set_font("Nanum", "B", 10)
        pdf.cell(30, 8, "지시사항", 1, 0, 'C', True)
        pdf.cell(80, 8, "", 1, 1, 'C')

        pdf.cell(50, 8, "해설사 성명", 1, 0, 'C', True)
        pdf.cell(100, 8, "활동 시간 (출퇴근)", 1, 0, 'C', True)
        pdf.cell(30, 8, "합계 시간", 1, 1, 'C', True)

        pdf.set_font("Nanum", "", 10)
        
        day_act = pd.DataFrame()
        if not df_act.empty:
            df_act['d_str'] = pd.to_datetime(df_act['날짜'], errors='coerce').dt.strftime('%Y-%m-%d')
            day_act = df_act[df_act['d_str'] == target_date]
        
        guides = day_act.to_dict('records') if not day_act.empty else []
        for i in range(2):
            if i < len(guides):
                g = guides[i]
                g_name = str(g.get('이름', ''))
                c_in = str(g.get('출근시간', '')).split('(')[0].strip()
                c_out = str(g.get('퇴근시간', '')).split('(')[0].strip()
                
                r_in = round_time_30min(c_in)
                r_out = round_time_30min(c_out)
                
                t_display = f"{r_in} ~ {r_out}" if r_in and r_out else (r_in if r_in else "")
                h_total = calc_working_hours(c_in, c_out) 
                
                pdf.cell(50, 8, g_name, 1, 0, 'C')
                pdf.cell(100, 8, t_display, 1, 0, 'C')
                pdf.cell(30, 8, h_total, 1, 1, 'C')
            else:
                pdf.cell(50, 8, "", 1, 0, 'C')
                pdf.cell(100, 8, "", 1, 0, 'C')
                pdf.cell(30, 8, "", 1, 1, 'C')
                
        pdf.ln(5)

        pdf.set_font("Nanum", "B", 10)
        pdf.cell(30, 8, "시간", 1, 0, 'C', True)
        pdf.cell(35, 8, "지질명소 탐방객(명)", 1, 0, 'C', True)
        pdf.cell(35, 8, "해설 청취자(명)", 1, 0, 'C', True)
        pdf.cell(30, 8, "해설 횟수(회)", 1, 0, 'C', True)
        pdf.cell(50, 8, "비고(내용 및 특이사항)", 1, 1, 'C', True)

        pdf.set_font("Nanum", "", 9)
        time_slots = [f"{h:02d}:00~{h+1:02d}:00" for h in range(6, 21)]
        
        slot_data = {t: {'vis': 0, 'lis': 0, 'cnt': 0, 'note': []} for t in time_slots}
        day_op = pd.DataFrame()
        if not df_op.empty:
            df_op['d_str'] = pd.to_datetime(df_op['날짜'], errors='coerce').dt.strftime('%Y-%m-%d')
            day_op = df_op[df_op['d_str'] == target_date]
        
        if not day_op.empty:
            for _, r in day_op.iterrows():
                in_time = str(r.get('입력시간', ''))
                try: h = int(in_time.split(':')[0])
                except: h = 8
                
                if h < 6: slot_k = "06:00~07:00"
                elif h >= 20: slot_k = "20:00~21:00"
                else: slot_k = f"{h:02d}:00~{h+1:02d}:00"
                
                is_joint = (str(r.get('공동해설', '')).strip() == 'True' or str(r.get('공동해설', '')).strip() == 'O')
                v = safe_int(r.get('탐방객수', 0))
                l = safe_int(r.get('청취자수', 0))
                c = safe_int(r.get('해설횟수', 0))
                if c == 0 and l > 0: c = 1
                
                if not is_joint: 
                    slot_data[slot_k]['vis'] += v
                    slot_data[slot_k]['lis'] += l
                
                slot_data[slot_k]['cnt'] += c
                
                note_txt = str(r.get('특이사항', '')).strip()
                if is_joint: note_txt = f"(공동) {note_txt}".strip()
                if note_txt: slot_data[slot_k]['note'].append(note_txt)
        
        t_vis = 0; t_lis = 0; t_cnt = 0
        for t in time_slots:
            d = slot_data[t]
            v_str = str(d['vis']) if d['vis'] > 0 else ""
            l_str = str(d['lis']) if d['lis'] > 0 else ""
            c_str = str(d['cnt']) if d['cnt'] > 0 else ""
            n_str = ", ".join(d['note'])
            if len(n_str) > 25: n_str = n_str[:23] + ".."
            
            t_vis += d['vis']; t_lis += d['lis']; t_cnt += d['cnt']
            
            pdf.cell(30, 7, t, 1, 0, 'C') 
            pdf.cell(35, 7, v_str, 1, 0, 'C')
            pdf.cell(35, 7, l_str, 1, 0, 'C')
            pdf.cell(30, 7, c_str, 1, 0, 'C')
            pdf.cell(50, 7, n_str, 1, 1, 'L')

        pdf.set_font("Nanum", "B", 10)
        pdf.cell(30, 8, "합계", 1, 0, 'C', True)
        pdf.cell(35, 8, str(t_vis), 1, 0, 'C')
        pdf.cell(35, 8, str(t_lis), 1, 0, 'C')
        pdf.cell(30, 8, str(t_cnt), 1, 0, 'C')
        pdf.cell(50, 8, "", 1, 1, 'C')

        pdf.ln(3)
        pdf.set_font("Nanum", "B", 10)
        pdf.cell(30, 15, "총 특이사항", 1, 0, 'C', True)
        
        pdf.set_font("Nanum", "", 9)
        all_notes = []
        if not day_op.empty and '특이사항' in day_op.columns:
            all_notes = [str(x).strip() for x in day_op['특이사항'].dropna() if str(x).strip()]
        note_base = " / ".join(all_notes)
        if len(note_base) > 65: note_base = note_base[:63] + "..."
        pdf.cell(150, 15, note_base, 1, 1, 'L')
        
        pdf.ln(8)
        pdf.set_font("Nanum", "", 12)
        pdf.cell(90, 10, "조장 확인 :                         (인/서명)", 0, 0, 'C')
        pdf.cell(90, 10, "면 담당 확인 :                         (인/서명)", 0, 1, 'C')

    return bytes(pdf.output())

def generate_plan_result_pdf(doc_title, target_place, special_note, p_year, p_month, p_range, disp_rows, workers_list):
    font_path = "NanumGothic.ttf"
    if not os.path.exists(font_path): return None

    pdf = FPDF(orientation='P', unit='mm', format='A4')
    pdf.set_margins(15, 15, 15); pdf.set_auto_page_break(True, margin=10); pdf.add_page()
    pdf.add_font("Nanum", "", font_path); pdf.add_font("Nanum", "B", font_path)

    pdf.set_font("Nanum", "B", 22); pdf.set_line_width(0.4)
    pdf.cell(180, 15, doc_title, 1, 1, 'C'); pdf.ln(3)

    sy = pdf.get_y(); sx = pdf.get_x()
    pdf.set_line_width(0.12); lh = 7; pdf.set_fill_color(245, 245, 245)
    def p_row(l, v, nl=False):
        pdf.set_font("Nanum", "B", 10); pdf.cell(30, lh, l, 1, 0, 'C', True)
        pdf.set_font("Nanum", "", 10); pdf.cell(60, lh, str(v).replace("nan",""), 1, 0, 'L')
        if nl: pdf.ln()
    p_row("안내소", target_place); p_row("특이사항", special_note, True)
    p_row("활동월", f"{p_year}년 {p_month}월"); p_row("활동기간", str(p_range), True)
    pdf.set_line_width(0.4); pdf.rect(sx, sy, 180, pdf.get_y()-sy, style="D"); pdf.set_y(pdf.get_y()+5)

    w_d=12; w_w=12; w_h=(180-24)/2; w_c=w_h/4
    def draw_header():
        sy = pdf.get_y(); sx = pdf.get_x()
        pdf.set_line_width(0.12); pdf.set_font("Nanum", "B", 10); pdf.set_fill_color(235, 235, 235)
        pdf.cell(w_d, 14, "일", 1, 0, 'C', True); pdf.cell(w_w, 14, "요일", 1, 0, 'C', True)
        pdf.set_xy(sx+24, sy); pdf.cell(w_h, 7, "활동 계획", 1, 0, 'C', True)
        pdf.cell(w_h, 7, "활동 결과", 1, 1, 'C', True)
        y2 = sy+7; bx = sx+24
        for i in range(8):
            pdf.set_xy(bx+(i*w_c) if i<4 else bx+w_h+((i-4)*w_c), y2)
            w_name = workers_list[i % 4] if (i % 4) < len(workers_list) else ""
            pdf.cell(w_c, 7, w_name, 1, 0, 'C', True)
        pdf.set_xy(sx, sy+14); pdf.set_line_width(0.4); pdf.rect(sx, sy, 180, 14, style="D"); pdf.set_line_width(0.12)

    draw_header()
    
    row_h = 8; body_sy = pdf.get_y()
    for row in disp_rows:
        if pdf.get_y() > 275:
            pdf.set_line_width(0.4); pdf.rect(15, body_sy, 180, pdf.get_y()-body_sy, style="D"); pdf.set_line_width(0.12)
            pdf.add_page(); draw_header(); body_sy = pdf.get_y()

        yc = pdf.get_y(); xc = pdf.get_x()
        pdf.set_font("Nanum", "B", 9)
        d_str = str(row['날짜'])
        day_str = d_str.split('-')[2] if '-' in d_str else d_str
        pdf.cell(w_d, row_h, day_str, 1, 0, 'C')
        pdf.cell(w_w, row_h, str(row.get('요일', '')), 1, 0, 'C')
        pdf.set_font("Nanum", "", 7)

        bx = xc + 24
        for i in range(4):
            pdf.set_xy(bx+(i*w_c), yc)
            final_txt = row.get(f"plan_{i}", "")
            clean_txt = final_txt.replace("~~", "")
            
            if "\n" in clean_txt:
                pdf.multi_cell(w_c, 4, clean_txt, 1, 'C')
                pdf.set_xy(bx+(i*w_c), yc); pdf.rect(bx+(i*w_c), yc, w_c, row_h)
                if "~~" in final_txt:
                    pdf.line(bx+(i*w_c)+2, yc+2, bx+(i*w_c)+w_c-2, yc+2)
            else:
                pdf.cell(w_c, row_h, clean_txt, 1, 0, 'C')
                if "~~" in final_txt:
                    pdf.line(bx+(i*w_c)+2, yc+(row_h/2), bx+(i*w_c)+w_c-2, yc+(row_h/2))
                
        bx += w_h
        for i in range(4):
            pdf.set_xy(bx+(i*w_c), yc)
            txt = row.get(f"res_{i}", "")
            if "\n" in txt:
                pdf.multi_cell(w_c, 4, txt, 0, 'C'); pdf.set_xy(bx+(i*w_c), yc); pdf.rect(bx+(i*w_c), yc, w_c, row_h)
            else: pdf.cell(w_c, row_h, txt, 1, 0, 'C')
        pdf.set_xy(xc, yc+row_h)

    pdf.set_line_width(0.4); pdf.rect(15, body_sy, 180, pdf.get_y()-body_sy, style="D")
    pdf.ln(5); pdf.set_font("Nanum", "", 12)
    pdf.cell(90, 10, "조장 확인 :                         (인/서명)", 0, 0, 'C')
    pdf.cell(90, 10, "면 담당 확인 :                         (인/서명)", 0, 1, 'C')
    return bytes(pdf.output())

# =========================================================
# 4. UI 탭별 함수
# =========================================================

def ui_journal_write(name, island, role):
    st.header("📝 일지 작성")
    
    is_manual = st.toggle("📅 지난 일지 작성 / 수기 수정 모드", key="jw_manual")
    
    now = get_kst_now()
    today_str = now.strftime("%Y-%m-%d")
    
    if is_manual:
        st.info("💡 과거 날짜의 일지를 작성하거나, 대상자/시간을 직접 수정할 수 있습니다.")
        c1, c2, c3 = st.columns(3)
        with c1: target_date = st.date_input("날짜 선택", value=now.date())
        with c2: place_act = st.selectbox("근무 안내소", LOCATIONS.get(island, []), key="jw_p_man")
        with c3:
            if role in ["조장", "관리자"]:
                all_users = get_users(island)
                target_name = st.selectbox("대상 해설사", all_users, index=all_users.index(name) if name in all_users else 0, key="jw_u_man")
            else:
                st.text_input("대상 해설사", value=name, disabled=True)
                target_name = name
        
        t_str = target_date.strftime("%Y-%m-%d")
        
        df_act = load_data("활동일지", target_date.year, target_date.month, island)
        my_act = pd.DataFrame()
        if not df_act.empty:
            df_act['d_str'] = pd.to_datetime(df_act['날짜'], errors='coerce').dt.strftime('%Y-%m-%d').fillna("")
            my_act = df_act[(df_act['d_str'] == t_str) & (df_act['이름'].astype(str).str.strip() == target_name.strip()) & (df_act['장소'] == place_act)]
        
        default_in = datetime.strptime("09:00", "%H:%M").time()
        default_out = datetime.strptime("18:00", "%H:%M").time()
        
        if not my_act.empty:
            try: default_in = datetime.strptime(str(my_act.iloc[-1].get('출근시간','')).split('(')[0].strip(), "%H:%M").time()
            except: pass
            try: default_out = datetime.strptime(str(my_act.iloc[-1].get('퇴근시간','')).split('(')[0].strip(), "%H:%M").time()
            except: pass
            
        c_in_val = st.time_input("출근 시간", value=default_in)
        c_out_val = st.time_input("퇴근 시간", value=default_out)
        
        if st.button("💾 수기 일지 저장", use_container_width=True):
            in_str = c_in_val.strftime("%H:%M")
            out_str = c_out_val.strftime("%H:%M")
            row_dict = {
                "날짜": t_str, "섬": island, "장소": place_act, "이름": target_name,
                "출근시간": in_str, "퇴근시간": out_str, "타임스탬프": str(get_kst_now()),
                "년": target_date.year, "월": target_date.month
            }
            save_data("활동일지", [row_dict])
            st.success(f"{t_str} [{target_name}] 일지가 수정/저장되었습니다.")
            time.sleep(1)
            st.rerun()
            
    else:
        c1, c2 = st.columns(2)
        with c1: place_act = st.selectbox("근무 안내소", LOCATIONS.get(island, []), key="jw_p_btn")
        with c2: st.text_input("현재 날짜", value=today_str, disabled=True)
        
        df_act = load_data("활동일지", now.year, now.month, island)
        my_act = pd.DataFrame()
        if not df_act.empty:
            df_act['d_str'] = pd.to_datetime(df_act['날짜'], errors='coerce').dt.strftime('%Y-%m-%d').fillna("")
            my_act = df_act[(df_act['d_str'] == today_str) & (df_act['이름'].astype(str).str.strip() == name.strip()) & (df_act['장소'] == place_act)]
            
        c_in = ""; c_out = ""
        if not my_act.empty:
            c_in = str(my_act.iloc[-1].get('출근시간', '')).split('(')[0].strip()
            c_out = str(my_act.iloc[-1].get('퇴근시간', '')).split('(')[0].strip()
            
        st.markdown(f"**현재 상태:** 출근 `[{c_in if c_in else '미등록'}]` / 퇴근 `[{c_out if c_out else '미등록'}]`")
        
        col_btn1, col_btn2 = st.columns(2)
        with col_btn1:
            if not c_in:
                if st.button("🟢 출근하기", use_container_width=True):
                    curr_t = time.time()
                    if 'btn_lock' not in st.session_state or (curr_t - st.session_state['btn_lock']) > 3:
                        st.session_state['btn_lock'] = curr_t
                        now_time = get_kst_now().strftime("%H:%M")
                        row_dict = {
                            "날짜": today_str, "섬": island, "장소": place_act, "이름": name,
                            "출근시간": now_time, "퇴근시간": "", "타임스탬프": str(get_kst_now()),
                            "년": now.year, "월": now.month
                        }
                        save_data("활동일지", [row_dict])
                        st.success(f"{now_time} 출근 완료!"); time.sleep(0.5); st.rerun()
            else:
                st.button("🟢 출근 완료", disabled=True, use_container_width=True)
                
        with col_btn2:
            if c_in and not c_out:
                if st.button("🔴 퇴근하기", use_container_width=True):
                    curr_t = time.time()
                    if 'btn_lock' not in st.session_state or (curr_t - st.session_state['btn_lock']) > 3:
                        st.session_state['btn_lock'] = curr_t
                        now_time = get_kst_now().strftime("%H:%M")
                        row_dict = {
                            "날짜": today_str, "섬": island, "장소": place_act, "이름": name,
                            "출근시간": c_in, "퇴근시간": now_time, "타임스탬프": str(get_kst_now()),
                            "년": now.year, "월": now.month
                        }
                        save_data("활동일지", [row_dict])
                        st.success(f"{now_time} 퇴근 완료!"); time.sleep(0.5); st.rerun()
            elif c_out:
                st.button("🔴 퇴근 완료", disabled=True, use_container_width=True)

    st.divider()
    st.subheader("해설 실적 등록 (운영일지)")
    
    time_slots = [f"{h:02d}:00 ~ {h+1:02d}:00" for h in range(6, 21)]
    curr_h = now.hour
    default_idx = 0
    if 6 <= curr_h < 21:
        default_idx = curr_h - 6
    
    if is_manual:
        op_date_str = t_str
        op_y = target_date.year
        op_m = target_date.month
        op_target_name = target_name
    else:
        op_date_str = today_str
        op_y = now.year
        op_m = now.month
        op_target_name = name
        
    with st.form("op_form", clear_on_submit=True):
        place_op = st.selectbox("해설 장소", LOCATIONS.get(island, []), key="op_p")
        selected_slot = st.selectbox("실적 시간대 선택", time_slots, index=default_idx)
        
        c1, c2, c3 = st.columns(3)
        vis = c1.number_input("탐방객 수 (명)", min_value=0, step=1)
        lis = c2.number_input("해설 청취자 수 (명)", min_value=0, step=1)
        cnt = c3.number_input("해설 횟수 (건)", min_value=0, step=1, value=1)
        
        is_joint = st.checkbox("공동해설 (보조 해설사 - 인원통계 미포함)")
        note = st.text_input("특이사항 (교육, 정비 등 내용 입력)")
        
        if st.form_submit_button("💾 실적 등록", use_container_width=True):
            curr_t = time.time()
            if 'op_lock' not in st.session_state or (curr_t - st.session_state['op_lock']) > 3:
                st.session_state['op_lock'] = curr_t
                row_dict = {
                    "날짜": op_date_str, "섬": island, "장소": place_op, "이름": op_target_name,
                    "입력시간": selected_slot,
                    "탐방객수": vis, "청취자수": lis, "해설횟수": cnt,
                    "특이사항": note, "공동해설": str(is_joint),
                    "타임스탬프": str(get_kst_now()), "년": op_y, "월": op_m
                }
                
                if append_data("운영일지", row_dict):
                    msg = f"[{selected_slot}] 실적 등록 완료! (횟수: {cnt}회)"
                    if is_joint: msg += " (공동해설)"
                    st.success(msg)
                    time.sleep(1.5); st.rerun()
            else:
                st.warning("데이터를 저장하는 중입니다. 연속해서 누르지 마세요!")

def ui_view_journal(scope, name, island, role=""):
    st.header("🔍 내 활동 조회" if scope=="me" else "🔍 활동 조회")
    
    c1, c2, c3 = st.columns(3)
    with c1: 
        vy = st.number_input("연도", value=st.session_state['cur_year'], key="vj_y")
        st.session_state['cur_year'] = vy
    with c2: 
        vm = st.number_input("월", value=st.session_state['cur_month'], key="vj_m")
        st.session_state['cur_month'] = vm
        
    t_isl = island if scope != "all" else None
    
    if scope == "all" or scope == "team":
        place_options = ["전체"] + [p for locs in LOCATIONS.values() for p in locs] if scope == "all" else ["전체"] + LOCATIONS.get(island, [])
        with c3: sel_place = st.selectbox("안내소 선택", place_options, key="vj_p")
    else:
        sel_place = "전체"
        
    df_act = load_data("활동일지", vy, vm, t_isl)
    df_op = load_data("운영일지", vy, vm, t_isl)
    
    st.subheader("🕒 출퇴근 내역 (활동일지)")
    if df_act.empty:
        st.info("출퇴근 기록이 없습니다.")
    else:
        edit_cols = ["날짜", "이름", "장소", "출근시간", "퇴근시간"]
        for c in edit_cols:
            if c not in df_act.columns: df_act[c] = ""
        
        filter_act = df_act.copy()
        if sel_place != "전체":
            filter_act = filter_act[filter_act['장소'].astype(str).str.strip() == sel_place.strip()]
        if scope == "me": 
            filter_act = filter_act[filter_act['이름'].astype(str).str.strip() == name.strip()]
        
        if filter_act.empty: 
            st.info("조건에 맞는 출퇴근 기록이 없습니다.")
        else:
            disp_df = filter_act[edit_cols].copy()
            disp_df['날짜'] = pd.to_datetime(disp_df['날짜'], errors='coerce').dt.strftime('%Y-%m-%d').fillna("")
            
            disp_df['활동시간(H)'] = disp_df.apply(lambda r: calc_working_hours(str(r.get('출근시간','')).split('(')[0].strip(), str(r.get('퇴근시간','')).split('(')[0].strip()), axis=1)
            
            disp_df['출근시간'] = disp_df['출근시간'].apply(format_time_with_rounded)
            disp_df['퇴근시간'] = disp_df['퇴근시간'].apply(format_time_with_rounded)
            
            tot_hours = sum([float(x) for x in disp_df['활동시간(H)'] if x])
            tot_hours_str = str(int(tot_hours)) if tot_hours.is_integer() else str(round(tot_hours, 1))
            
            disp_df.insert(0, '삭제', False)
            sum_dict = {
                '삭제': False, '날짜': '합계', '이름': '-', '장소': '-', 
                '출근시간': '-', '퇴근시간': '-', '활동시간(H)': tot_hours_str
            }
            disp_df = pd.concat([disp_df, pd.DataFrame([sum_dict])], ignore_index=True)
            
            for col in disp_df.columns:
                if col == '삭제':
                    disp_df[col] = disp_df[col].astype(bool)
                else:
                    disp_df[col] = disp_df[col].astype(str)
            
            with st.form("edit_act_form"):
                col_config = {
                    "활동시간(H)": st.column_config.Column(disabled=True),
                    "삭제": st.column_config.CheckboxColumn("🗑️ 삭제", default=False)
                }
                edited_act = st.data_editor(disp_df, hide_index=True, use_container_width=True, column_config=col_config)
                st.markdown(f"<div style='text-align:right; font-weight:bold; font-size:18px;'>총 활동시간 합계: {tot_hours_str} H</div>", unsafe_allow_html=True)
                
                if st.form_submit_button("변경사항 및 삭제 저장"):
                    sh_act = client.open(SPREADSHEET_NAME).worksheet("활동일지")
                    full_act_vals = sh_act.get_all_values()
                    headers_act = [str(h).strip() for h in full_act_vals[0]]
                    full_df_act = pd.DataFrame(full_act_vals[1:], columns=headers_act)
                    
                    if '일자' in full_df_act.columns: full_df_act.rename(columns={'일자': '날짜'}, inplace=True)
                    full_df_act['날짜'] = pd.to_datetime(full_df_act['날짜'], errors='coerce').dt.strftime("%Y-%m-%d").fillna("")

                    rows_to_delete = edited_act[edited_act['삭제'] == True]
                    rows_to_update = edited_act[(edited_act['삭제'] == False) & (edited_act['날짜'] != '합계')]

                    has_changes = False

                    for _, r in rows_to_update.iterrows():
                        if role not in ["조장", "관리자"] and str(r['이름']).strip() != name.strip(): continue 
                        idx = full_df_act[(full_df_act['날짜'] == r['날짜']) & (full_df_act['이름'].astype(str).str.strip() == str(r['이름']).strip()) & (full_df_act['장소'].astype(str).str.strip() == str(r['장소']).strip())].index
                        if not idx.empty:
                            full_df_act.loc[idx, '출근시간'] = str(r['출근시간']).split('(')[0].strip()
                            full_df_act.loc[idx, '퇴근시간'] = str(r['퇴근시간']).split('(')[0].strip()
                            has_changes = True

                    for _, r in rows_to_delete.iterrows():
                        if r['날짜'] == '합계': continue
                        if role not in ["조장", "관리자"] and str(r['이름']).strip() != name.strip(): continue
                        mask = (full_df_act['날짜'] == r['날짜']) & (full_df_act['이름'].astype(str).str.strip() == str(r['이름']).strip()) & (full_df_act['장소'].astype(str).str.strip() == str(r['장소']).strip())
                        full_df_act = full_df_act[~mask]
                        has_changes = True
                    
                    if has_changes:
                        sh_act.clear()
                        sh_act.update([full_df_act.columns.values.tolist()] + full_df_act.astype(str).values.tolist())
                        st.cache_data.clear()
                        st.success("활동일지가 업데이트되었습니다."); time.sleep(0.5); st.rerun()
                        
    st.divider()
    st.subheader("📋 운영 실적 내역 (운영일지)")
    if df_op.empty:
        st.info("등록된 운영 실적이 없습니다.")
    else:
        filter_op = df_op.copy()
        if sel_place != "전체":
            filter_op = filter_op[filter_op['장소'].astype(str).str.strip() == sel_place.strip()]
        if scope == "me": 
            filter_op = filter_op[filter_op['이름'].astype(str).str.strip() == name.strip()]
        
        if filter_op.empty: 
            st.info("조건에 맞는 운영 실적 데이터가 없습니다.")
        else:
            show_cols = ["날짜", "장소", "이름", "입력시간", "탐방객수", "청취자수", "해설횟수", "공동해설", "특이사항"]
            for c in show_cols:
                if c not in filter_op.columns: filter_op[c] = ""
            
            disp_op = filter_op[show_cols].copy()
            disp_op['날짜'] = pd.to_datetime(disp_op['날짜'], errors='coerce').dt.strftime('%Y-%m-%d').fillna("")
            
            disp_op['탐방객수'] = disp_op['탐방객수'].apply(safe_int)
            disp_op['청취자수'] = disp_op['청취자수'].apply(safe_int)
            
            disp_op['공동해설'] = disp_op['공동해설'].apply(lambda x: 'O' if str(x).strip() == 'True' else '')
            
            def calc_vis(row):
                return 0 if row.get('공동해설') == 'O' else safe_int(row.get('탐방객수'))
            def calc_lis(row):
                return 0 if row.get('공동해설') == 'O' else safe_int(row.get('청취자수'))
            
            disp_op['계산용_탐방객'] = disp_op.apply(calc_vis, axis=1)
            disp_op['계산용_청취자'] = disp_op.apply(calc_lis, axis=1)
            disp_op['해설횟수'] = disp_op.apply(lambda x: safe_int(x['해설횟수']) if pd.notna(x['해설횟수']) and str(x['해설횟수'])!='' else (1 if safe_int(x['청취자수']) > 0 else 0), axis=1)
            
            tot_vis = disp_op['계산용_탐방객'].sum()
            tot_lis = disp_op['계산용_청취자'].sum()
            tot_cnt = disp_op['해설횟수'].sum()
            
            final_cols = ["날짜", "장소", "이름", "입력시간", "탐방객수", "청취자수", "해설횟수", "공동해설", "특이사항"]
            disp_op = disp_op[final_cols]
            
            disp_op = disp_op.sort_values(by=['날짜', '입력시간']).reset_index(drop=True)
            
            disp_op.insert(0, '삭제', False)
            sum_dict_op = {
                '삭제': False, '날짜': '합계', '장소': '-', '이름': '-', '입력시간': '-',
                '탐방객수': int(tot_vis), '청취자수': int(tot_lis), '해설횟수': int(tot_cnt),
                '공동해설': '-', '특이사항': '-'
            }
            disp_op = pd.concat([disp_op, pd.DataFrame([sum_dict_op])], ignore_index=True)
            
            for col in ['날짜', '장소', '이름', '입력시간', '공동해설', '특이사항']:
                disp_op[col] = disp_op[col].fillna("").astype(str)
            for col in ['탐방객수', '청취자수', '해설횟수']:
                disp_op[col] = pd.to_numeric(disp_op[col], errors='coerce').fillna(0).astype(int)
            disp_op['삭제'] = disp_op['삭제'].astype(bool)
            
            with st.form("edit_op_form"):
                col_config_op = {
                    "삭제": st.column_config.CheckboxColumn("🗑️ 삭제", default=False),
                    "날짜": st.column_config.Column(disabled=True),
                    "장소": st.column_config.Column(disabled=True),
                    "이름": st.column_config.Column(disabled=True),
                    "입력시간": st.column_config.Column(disabled=True),
                    "탐방객수": st.column_config.Column(disabled=True),
                    "청취자수": st.column_config.Column(disabled=True),
                    "해설횟수": st.column_config.Column(disabled=True),
                    "공동해설": st.column_config.Column(disabled=True),
                    "특이사항": st.column_config.Column(disabled=True)
                }
                edited_op = st.data_editor(disp_op, hide_index=True, use_container_width=True, column_config=col_config_op)
                
                if st.form_submit_button("선택한 운영일지 삭제"):
                    sh_op = client.open(SPREADSHEET_NAME).worksheet("운영일지")
                    full_op_vals = sh_op.get_all_values()
                    headers_op = [str(h).strip() for h in full_op_vals[0]]
                    full_df_op = pd.DataFrame(full_op_vals[1:], columns=headers_op)
                    
                    if '일자' in full_df_op.columns: full_df_op.rename(columns={'일자': '날짜'}, inplace=True)
                    full_df_op['날짜'] = pd.to_datetime(full_df_op['날짜'], errors='coerce').dt.strftime("%Y-%m-%d").fillna("")
                    
                    rows_to_delete = edited_op[edited_op['삭제'] == True]
                    
                    has_del = False
                    for _, r in rows_to_delete.iterrows():
                        if r['날짜'] == '합계': continue
                        if role not in ["조장", "관리자"] and str(r['이름']).strip() != name.strip(): continue
                        
                        mask = (full_df_op['날짜'] == r['날짜']) & (full_df_op['이름'].astype(str).str.strip() == str(r['이름']).strip()) & (full_df_op['장소'].astype(str).str.strip() == str(r['장소']).strip()) & (full_df_op['입력시간'].astype(str).str.strip() == str(r['입력시간']).strip())
                        full_df_op = full_df_op[~mask]
                        has_del = True
                        
                    if has_del:
                        sh_op.clear()
                        sh_op.update([full_df_op.columns.values.tolist()] + full_df_op.astype(str).values.tolist())
                        st.cache_data.clear()
                        st.success("선택한 운영일지가 삭제되었습니다."); time.sleep(0.5); st.rerun()

def ui_plan_input(name, island):
    st.header("✍️ 계획 작성")
    now = get_kst_now()
    nm = now.replace(day=28) + pd.Timedelta(days=4)
    c1,c2,c3=st.columns([1,1,2])
    with c1: py=st.number_input("연도", value=nm.year, key="pi_y")
    with c2: pm=st.number_input("월", value=nm.month, key="pi_m")
    with c3: pr=st.radio("기간", ["전반기(1~15일)", "후반기(16~말일)"], horizontal=True, key="pi_r")
    place = st.selectbox("장소", LOCATIONS.get(island, []), key="pi_p")
    
    _, last = calendar.monthrange(py, pm)
    dates = [datetime(py, pm, d).strftime("%Y-%m-%d") for d in (range(1, 16) if "전반기" in pr else range(16, last+1))]
    
    df = load_data("활동계획", py, pm, island)
    if not df.empty: 
        my_df = df[(df['이름'].astype(str).str.strip()==name.strip())]
    else:
        my_df = pd.DataFrame()
        
    st.divider()
    st.subheader("📊 이번 달 계획 현황")
    
    month_plans = 0
    weekly_warns = []
    
    if not my_df.empty:
        my_df_act = my_df[my_df['활동여부'].astype(str).str.strip() != ""]
        month_plans = len(my_df_act)
        
        week_counts = {}
        for d_str in my_df_act['날짜'].dropna().astype(str):
            try:
                d_obj = datetime.strptime(d_str[:10], "%Y-%m-%d")
                w_num = d_obj.strftime("%U")
                week_counts[w_num] = week_counts.get(w_num, 0) + 1
            except: pass
            
        for w, count in week_counts.items():
            if count > 6:
                weekly_warns.append(f"⚠️ 일~토 기준 주 {count}일 근무 배정 (연속 6일 이하 권장)")
                
    st.markdown(f"**🔹 이달의 누적 활동일:** `{month_plans}`일 / 최대 20일 (잔여: `{max(0, 20 - month_plans)}`일)")
    if weekly_warns:
        for w in weekly_warns: st.warning(w)
    else:
        st.markdown("**🔹 주간 활동일 (일~토 기준):** 모두 6일 이하로 정상입니다.")
        
    st.info("💡 일 8시간, 주 6일, 월 20일을 초과하여 계획을 작성할 경우 조장 및 관리자 승인이 필요할 수 있습니다.")
    
    st.divider()
    mode = st.radio("모드", ["📅 하루씩", "🗓️ 전체"], horizontal=True, key="pi_md")
    
    if "하루씩" in mode:
        c1, c2 = st.columns([1, 1.5])
        with c1:
            try: pick = st.date_input("날짜", value=datetime.strptime(dates[0],"%Y-%m-%d").date(), min_value=datetime.strptime(dates[0],"%Y-%m-%d").date(), max_value=datetime.strptime(dates[-1],"%Y-%m-%d").date(), key="pi_pk")
            except: pick = datetime.strptime(dates[0],"%Y-%m-%d").date()
            pick_s = pick.strftime("%Y-%m-%d")
        ps="활동 없음"; etc=""
        if not my_df.empty:
            my_df['d_str'] = pd.to_datetime(my_df['날짜'], errors='coerce').dt.strftime('%Y-%m-%d')
            r = my_df[(my_df['d_str'] == pick_s) & (my_df['장소'] == place)]
            if not r.empty:
                val = r.iloc[0].get('활동여부', '')
                if val=="종일": ps="종일 (8시간)"
                elif "오전" in val: ps="오전 (4시간)"
                elif "오후" in val: ps="오후 (4시간)"
                elif val: ps="기타"; etc=val
        with c2: st.markdown(f"**{pick.day}일 ({DAY_MAP[pick.weekday()]})**")
        with st.form("pi_d"):
            sel = st.radio("계획", ["활동 없음", "종일 (8시간)", "오전 (4시간)", "오후 (4시간)", "기타"], index=["활동 없음", "종일 (8시간)", "오전 (4시간)", "오후 (4시간)", "기타"].index(ps))
            ein = st.text_input("기타 입력", value=etc)
            if st.form_submit_button("💾 제출 (승인대기)", use_container_width=True):
                stat = ""
                if "종일" in sel: stat="종일"
                elif "오전" in sel: stat="오전(4시간)"
                elif "오후" in sel: stat="오후(4시간)"
                elif "기타" in sel: stat=ein if ein else "미정"
                
                row_dict = {
                    "날짜": pick_s, "섬": island, "장소": place, "이름": name,
                    "활동여부": stat, "비고": "", "타임스탬프": str(get_kst_now()),
                    "년": py, "월": pm, "상태": "승인대기", "대타여부": "", "기존해설사": ""
                }
                save_data("활동계획", [row_dict]);
