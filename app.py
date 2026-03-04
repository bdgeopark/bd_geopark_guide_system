import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time
import calendar
import os
from fpdf import FPDF
import extra_streamlit_components as stx

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

# [누락되었던 함수 추가] 쿠키 매니저 로드
@st.cache_resource(experimental_allow_widgets=True)
def get_manager():
    return stx.CookieManager()

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
            for c in ["입력시간", "탐방객수", "청취자수", "특이사항", "공동해설", "해설횟수"]:
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
        pdf.cell(30, 10, "시간", 1, 0, 'C', True)
        pdf.cell(35, 10, "지질명소 탐방객(명)", 1, 0, 'C', True)
        pdf.cell(35, 10, "해설 청취자(명)", 1, 0, 'C', True)
        pdf.cell(30, 10, "해설 횟수(회)", 1, 0, 'C', True)
        pdf.cell(50, 10, "비고(내용 및 특이사항)", 1, 1, 'C', True)

        pdf.set_font("Nanum", "", 9)
        time_slots = [
            "08:00~09:00", "09:00~10:00", "10:00~11:00", "11:00~12:00",
            "12:00~13:00", "13:00~14:00", "14:00~15:00", "15:00~16:00", 
            "16:00~17:00", "17:00~18:00"
        ]
        
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
                
                if h < 8: slot_k = "08:00~09:00"
                elif h >= 17: slot_k = "17:00~18:00"
                else: slot_k = f"{h:02d}:00~{h+1:02d}:00"
                
                is_joint = (str(r.get('공동해설', '')).strip() == 'True')
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
            
            pdf.cell(30, 8, t, 1, 0, 'C')
            pdf.cell(35, 8, v_str, 1, 0, 'C')
            pdf.cell(35, 8, l_str, 1, 0, 'C')
            pdf.cell(30, 8, c_str, 1, 0, 'C')
            pdf.cell(50, 8, n_str, 1, 1, 'L')

        pdf.set_font("Nanum", "B", 10)
        pdf.cell(30, 10, "합계", 1, 0, 'C', True)
        pdf.cell(35, 10, str(t_vis), 1, 0, 'C')
        pdf.cell(35, 10, str(t_lis), 1, 0, 'C')
        pdf.cell(30, 10, str(t_cnt), 1, 0, 'C')
        pdf.cell(50, 10, "", 1, 1, 'C')

        pdf.ln(5)
        pdf.set_font("Nanum", "B", 10)
        pdf.cell(30, 15, "총 특이사항", 1, 0, 'C', True)
        
        pdf.set_font("Nanum", "", 9)
        all_notes = []
        if not day_op.empty and '특이사항' in day_op.columns:
            all_notes = [str(x).strip() for x in day_op['특이사항'].dropna() if str(x).strip()]
        note_base = " / ".join(all_notes)
        if len(note_base) > 65: note_base = note_base[:63] + "..."
        pdf.cell(150, 15, note_base, 1, 1, 'L')
        
        pdf.ln(10)
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

def ui_journal_write(name, island):
    st.header("📝 일지 작성")
    
    is_manual = st.toggle("📅 지난 일지 작성 / 수기 수정 모드", key="jw_manual")
    
    now = get_kst_now()
    today_str = now.strftime("%Y-%m-%d")
    
    if is_manual:
        st.info("💡 과거 날짜의 일지를 작성하거나, 잘못 입력된 시간을 직접 수정할 수 있습니다.")
        c1, c2 = st.columns(2)
        with c1: target_date = st.date_input("날짜 선택", value=now.date())
        with c2: place_act = st.selectbox("근무 안내소", LOCATIONS.get(island, []), key="jw_p_man")
        
        t_str = target_date.strftime("%Y-%m-%d")
        
        df_act = load_data("활동일지", target_date.year, target_date.month, island)
        my_act = pd.DataFrame()
        if not df_act.empty:
            df_act['d_str'] = pd.to_datetime(df_act['날짜'], errors='coerce').dt.strftime('%Y-%m-%d')
            my_act = df_act[(df_act['d_str'] == t_str) & (df_act['이름'].astype(str).str.strip() == name.strip()) & (df_act['장소'] == place_act)]
        
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
                "날짜": t_str, "섬": island, "장소": place_act, "이름": name,
                "출근시간": in_str, "퇴근시간": out_str, "타임스탬프": str(get_kst_now()),
                "년": target_date.year, "월": target_date.month
            }
            save_data("활동일지", [row_dict])
            st.success(f"{t_str} 일지가 수정/저장되었습니다.")
            time.sleep(1)
            st.rerun()
            
    else:
        c1, c2 = st.columns(2)
        with c1: place_act = st.selectbox("근무 안내소", LOCATIONS.get(island, []), key="jw_p_btn")
        with c2: st.text_input("현재 날짜", value=today_str, disabled=True)
        
        df_act = load_data("활동일지", now.year, now.month, island)
        my_act = pd.DataFrame()
        if not df_act.empty:
            df_act['d_str'] = pd.to_datetime(df_act['날짜'], errors='coerce').dt.strftime('%Y-%m-%d')
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
    
    time_slots = [f"{h:02d}:00 ~ {h+1:02d}:00" for h in range(8, 18)]
    curr_h = now.hour
    default_idx = 0
    if 8 <= curr_h < 18:
        default_idx = curr_h - 8
    
    if is_manual:
        st.info("📝 지난 실적이나, 한 번에 여러 건의 실적을 입력할 때 유용합니다.")
        op_date_str = t_str
        op_y = target_date.year
        op_m = target_date.month
    else:
        op_date_str = today_str
        op_y = now.year
        op_m = now.month
        
    with st.form("op_form"):
        place_op = st.selectbox("해설 장소", LOCATIONS.get(island, []), key="op_p")
        selected_slot = st.selectbox("실적 시간대 선택", time_slots, index=default_idx)
        
        c1, c2, c3 = st.columns(3)
        vis = c1.number_input("탐방객 수 (명)", min_value=0, step=1)
        lis = c2.number_input("해설 청취자 수 (명)", min_value=0, step=1)
        cnt = c3.number_input("해설 횟수 (건)", min_value=0, step=1, value=1)
        
        is_joint = st.checkbox("공동해설 (보조 해설사 - 인원통계 미포함)")
        note = st.text_input("특이사항 (교육, 정비 등 내용 입력)")
        
        if st.form_submit_button("💾 실적 등록", use_container_width=True):
            row_dict = {
                "날짜": op_date_str, "섬": island, "장소": place_op, "이름": name,
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
            disp_df['날짜'] = pd.to_datetime(disp_df['날짜'], errors='coerce').dt.strftime('%Y-%m-%d')
            
            disp_df['활동시간(H)'] = disp_df.apply(lambda r: calc_working_hours(str(r.get('출근시간','')).split('(')[0].strip(), str(r.get('퇴근시간','')).split('(')[0].strip()), axis=1)
            
            disp_df['출근시간'] = disp_df['출근시간'].apply(format_time_with_rounded)
            disp_df['퇴근시간'] = disp_df['퇴근시간'].apply(format_time_with_rounded)
            
            tot_hours = sum([float(x) for x in disp_df['활동시간(H)'] if x])
            tot_hours_str = str(int(tot_hours)) if tot_hours.is_integer() else str(round(tot_hours, 1))
            
            with st.form("edit_act_form"):
                col_config = {"활동시간(H)": st.column_config.Column(disabled=True)}
                edited_act = st.data_editor(disp_df, hide_index=True, use_container_width=True, column_config=col_config)
                st.markdown(f"<div style='text-align:right; font-weight:bold; font-size:18px;'>총 활동시간 합계: {tot_hours_str} H</div>", unsafe_allow_html=True)
                
                if st.form_submit_button("변경사항 저장"):
                    df_act_dates = pd.to_datetime(df_act['날짜'], errors='coerce').dt.strftime("%Y-%m-%d")
                    for _, r in edited_act.iterrows():
                        if r['날짜'] == '합계': continue
                        if role not in ["조장", "관리자"] and str(r['이름']).strip() != name.strip(): continue 
                            
                        d_str = r['날짜']
                        idx = df_act[(df_act_dates == d_str) & (df_act['이름'] == r['이름']) & (df_act['장소'] == r['장소'])].index
                        if not idx.empty:
                            df_act.loc[idx, '출근시간'] = str(r['출근시간']).split('(')[0].strip()
                            df_act.loc[idx, '퇴근시간'] = str(r['퇴근시간']).split('(')[0].strip()
                    
                    sh = client.open(SPREADSHEET_NAME).worksheet("활동일지")
                    df_act['날짜'] = pd.to_datetime(df_act['날짜'], errors='coerce').dt.strftime("%Y-%m-%d")
                    sh.clear()
                    sh.update([df_act.columns.values.tolist()] + df_act.astype(str).values.tolist())
                    st.cache_data.clear()
                    st.success("출퇴근 시간이 수정되었습니다."); time.sleep(0.5); st.rerun()
                        
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
            disp_op['날짜'] = pd.to_datetime(disp_op['날짜'], errors='coerce').dt.strftime('%Y-%m-%d')
            
            disp_op['탐방객수'] = disp_op['탐방객수'].apply(safe_int)
            disp_op['청취자수'] = disp_op['청취자수'].apply(safe_int)
            
            def calc_vis(row):
                return 0 if str(row.get('공동해설')).strip() == 'True' else safe_int(row.get('탐방객수'))
            def calc_lis(row):
                return 0 if str(row.get('공동해설')).strip() == 'True' else safe_int(row.get('청취자수'))
            
            disp_op['계산용_탐방객'] = disp_op.apply(calc_vis, axis=1)
            disp_op['계산용_청취자'] = disp_op.apply(calc_lis, axis=1)
            disp_op['해설횟수'] = disp_op.apply(lambda x: safe_int(x['해설횟수']) if pd.notna(x['해설횟수']) and str(x['해설횟수'])!='' else (1 if safe_int(x['청취자수']) > 0 else 0), axis=1)
            
            tot_vis = disp_op['계산용_탐방객'].sum()
            tot_lis = disp_op['계산용_청취자'].sum()
            tot_cnt = disp_op['해설횟수'].sum()
            
            final_cols = ["날짜", "장소", "이름", "입력시간", "탐방객수", "청취자수", "해설횟수", "공동해설", "특이사항"]
            disp_op = disp_op[final_cols]
            disp_op.loc['합계'] = ['합계', '-', '-', '-', tot_vis, tot_lis, tot_cnt, '-', '-']
            
            st.dataframe(disp_op, use_container_width=True, hide_index=True)

    if role in ["조장", "관리자"]:
        st.divider()
        st.subheader("📥 일지 및 결과보고서 다운로드")
        
        if sel_place == "전체":
            st.warning("⚠️ PDF를 다운로드하려면 상단의 '안내소 선택'에서 특정 안내소를 지정해주세요.")
        elif df_act.empty and df_op.empty:
            st.info("다운로드할 데이터가 없습니다.")
        else:
            day_act = df_act[df_act['장소'].astype(str).str.strip() == sel_place.strip()] if not df_act.empty else pd.DataFrame()
            day_op = df_op[df_op['장소'].astype(str).str.strip() == sel_place.strip()] if not df_op.empty else pd.DataFrame()
            
            c_dl1, c_dl2 = st.columns(2)
            with c_dl1:
                st.markdown("**1. 운영일지 (서식3)**")
                st.caption("※ 한 달 전체 데이터가 1일 1장씩 출력됩니다.")
                if not (day_act.empty and day_op.empty):
                    pdf_data_op = generate_official_journal_month_pdf(day_act, day_op, vy, vm, sel_place, "월간 전체")
                    if pdf_data_op:
                        st.download_button(
                            label=f"📄 {vy}년 {vm}월 {sel_place} 운영일지 다운로드", 
                            data=pdf_data_op, 
                            file_name=f"운영일지_{sel_place}_{vy}년{vm}월.pdf", 
                            mime="application/pdf", 
                            use_container_width=True
                        )
            
            with c_dl2:
                st.markdown("**[활동결과보고서]**")
                pr_res = st.radio("보고서 기간 선택", ["전반기(1~15일)", "후반기(16~말일)"], horizontal=True, key="dl_res_pr")
                
                df_plan_res = load_data("활동계획", vy, vm, t_isl)
                if not df_plan_res.empty:
                    df_plan_res = df_plan_res[df_plan_res['장소'].astype(str).str.strip() == sel_place.strip()]
                
                _, last = calendar.monthrange(vy, vm)
                res_dates = [datetime(vy, vm, d).strftime("%Y-%m-%d") for d in (range(1, 16) if "전반기" in pr_res else range(16, last+1))]
                disp_rows_res, workers_list_res = get_display_data(df_plan_res, day_act, res_dates, is_pdf=True, is_plan_only=False)
                
                pdf_data_res = generate_plan_result_pdf("지질공원 안내소 활동결과보고서", sel_place, "", vy, vm, pr_res, disp_rows_res, workers_list_res)
                if pdf_data_res:
                    pr_label = "전반기" if "전반기" in pr_res else "후반기"
                    st.download_button(
                        label=f"📊 {vy}년 {vm}월 {sel_place} 활동결과보고서 ({pr_label})", 
                        data=pdf_data_res, 
                        file_name=f"활동결과보고서_{sel_place}_{vy}년{vm}월_{pr_label}.pdf", 
                        mime="application/pdf", 
                        use_container_width=True
                    )

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
                save_data("활동계획", [row_dict]); st.success("승인 대기 상태로 저장되었습니다."); st.rerun()
    else:
        grid = []
        d_map = {}
        if not my_df.empty:
            my_df['d_str'] = pd.to_datetime(my_df['날짜'], errors='coerce').dt.strftime('%Y-%m-%d')
            for _, r in my_df[my_df['장소']==place].iterrows(): d_map[r['d_str']] = r
        for d in dates:
            curr = d_map.get(d, "") if isinstance(d_map.get(d, ""), str) else d_map.get(d, {}).get('활동여부', "")
            grid.append({
                "날짜": d, "요일": DAY_MAP[datetime.strptime(d, "%Y-%m-%d").weekday()],
                "종일": curr=="종일", "오전": "오전" in curr, "오후": "오후" in curr, "기타": curr if curr not in ["종일","오전(4시간)","오후(4시간)",""] else ""
            })
        with st.form("pi_m"):
            edited = st.data_editor(pd.DataFrame(grid), hide_index=True, use_container_width=True, height=600)
            if st.form_submit_button("💾 전체 제출 (승인대기)"):
                rows = []
                for _, r in edited.iterrows():
                    s = ""
                    if r['종일']: s="종일"
                    elif r['오전']: s="오전(4시간)"
                    elif r['오후']: s="오후(4시간)"
                    elif r['기타']: s=str(r['기타'])
                    
                    rows.append({
                        "날짜": r['날짜'], "섬": island, "장소": place, "이름": name,
                        "활동여부": s, "비고": "", "타임스탬프": str(get_kst_now()),
                        "년": py, "월": pm, "상태": "승인대기", "대타여부": "", "기존해설사": ""
                    })
                save_data("활동계획", rows); st.success("승인 대기 상태로 저장되었습니다."); st.rerun()

def ui_view_plan(scope, name, island, role=""):
    st.header("🗓️ 내 계획 조회 및 수정" if scope=="me" else "🗓️ 계획 조회 및 수정")
    c1, c2 = st.columns(2)
    with c1: 
        py = st.number_input("연도", value=st.session_state['cur_year'], key="vp_y")
        st.session_state['cur_year'] = py
    with c2: 
        pm = st.number_input("월", value=st.session_state['cur_month'], key="vp_m")
        st.session_state['cur_month'] = pm
    
    sel_place = None
    if scope == "team" or scope == "all":
        t_isl = island if scope == "team" else st.selectbox("섬", list(LOCATIONS.keys()), key="vp_i")
        place_list = ["전체"] + LOCATIONS.get(t_isl, [])
        sel_place = st.selectbox("안내소 선택 (상세조회)", place_list, key="vp_p")
    else:
        t_isl = island

    df_plan = load_data("활동계획", py, pm, t_isl)
    df_act = load_data("활동일지", py, pm, t_isl)

    if scope == "me" and not df_plan.empty:
        sub_mask = (df_plan['대타여부'] == 'O') & (df_plan['기존해설사'].astype(str).str.strip() == name.strip())
        sub_rows = df_plan[sub_mask]
        sub_dates = pd.to_datetime(sub_rows['날짜'], errors='coerce').dt.strftime('%Y-%m-%d')
        sub_keys = sub_dates + "_" + sub_rows['장소'].astype(str).str.strip()
        
        df_plan = df_plan[df_plan['이름'].astype(str).str.strip() == name.strip()].copy()
        
        my_dates = pd.to_datetime(df_plan['날짜'], errors='coerce').dt.strftime('%Y-%m-%d')
        df_plan['match_key'] = my_dates + "_" + df_plan['장소'].astype(str).str.strip()
        
        df_plan = df_plan[~df_plan['match_key'].isin(sub_keys)]
        df_plan = df_plan[~df_plan['상태'].isin(['취소대기', '취소승인'])]
        df_plan = df_plan.drop(columns=['match_key'])
        
        if not df_act.empty:
            df_act = df_act[df_act['이름'].astype(str).str.strip() == name.strip()]

    if sel_place and sel_place != "전체":
        if not df_plan.empty: df_plan = df_plan[df_plan['장소'] == sel_place]
        if not df_act.empty and '장소' in df_act.columns: df_act = df_act[df_act['장소'] == sel_place]

    _, last = calendar.monthrange(py, pm)
    full_month_dates = [datetime(py, pm, d).strftime("%Y-%m-%d") for d in range(1, last+1)]
    
    show_place_flag = (sel_place in [None, "전체"])
    disp_rows, workers_list = get_display_data(df_plan, df_act, full_month_dates, show_place=show_place_flag, is_pdf=False, is_plan_only=False)
    
    df_disp = pd.DataFrame(disp_rows)
    cols = ["날짜", "요일", "plan_0", "plan_1", "plan_2", "plan_3", "res_0", "res_1", "res_2", "res_3"]
    for c in cols:
        if c not in df_disp.columns: df_disp[c] = ""
        
    col_config = {
        "날짜": st.column_config.Column(width="medium"),
        "요일": st.column_config.Column(width="small"),
    }
    for i in range(4):
        w_name = workers_list[i] if i < len(workers_list) and workers_list[i] else f"빈칸"
        col_config[f"plan_{i}"] = st.column_config.Column(f"계획 ({w_name})", width="small")
        col_config[f"res_{i}"] = st.column_config.Column(f"결과 ({w_name})", width="small")
    
    st.dataframe(df_disp[cols], use_container_width=True, hide_index=True, column_config=col_config)

    if sel_place and sel_place != "전체":
        st.divider()
        st.subheader("📥 활동계획서 다운로드")
        pr_plan = st.radio("출력 기간 선택", ["전반기(1~15일)", "후반기(16~말일)"], horizontal=True, key="dl_plan_pr")
        
        plan_dates = [datetime(py, pm, d).strftime("%Y-%m-%d") for d in (range(1, 16) if "전반기" in pr_plan else range(16, last+1))]
        disp_rows_plan, workers_list_plan = get_display_data(df_plan, df_act, plan_dates, is_pdf=True, is_plan_only=True)
        
        pdf_data_plan = generate_plan_result_pdf("지질공원 안내소 활동계획서", sel_place, "", py, pm, pr_plan, disp_rows_plan, workers_list_plan)
        if pdf_data_plan:
            pr_label = "전반기" if "전반기" in pr_plan else "후반기"
            st.download_button(
                label=f"📄 {py}년 {pm}월 {sel_place} 활동계획서 ({pr_label}) 다운로드",
                data=pdf_data_plan,
                file_name=f"활동계획서_{sel_place}_{py}년{pm}월_{pr_label}.pdf",
                mime="application/pdf",
                use_container_width=True
            )
    
    if disp_rows:
        st.divider()
        st.subheader("🛠️ 계획 수정 (대타/취소 요청)")
        with st.expander("수정 메뉴", expanded=True):
            c1, c2 = st.columns(2)
            avail_dates = sorted(list(set(pd.to_datetime(df_plan['날짜'], errors='coerce').dropna().dt.strftime('%Y-%m-%d'))))
            if not avail_dates:
                st.info("수정할 계획 데이터가 없습니다.")
            else:
                with c1: target_d = st.selectbox("날짜", avail_dates, key="md_d")
                
                df_plan_edit = load_data("활동계획", py, pm, t_isl)
                df_plan_edit['d_str'] = pd.to_datetime(df_plan_edit['날짜'], errors='coerce').dt.strftime('%Y-%m-%d')
                day_p = df_plan_edit[df_plan_edit['d_str'] == target_d]
                
                if sel_place and sel_place != "전체":
                    day_p = day_p[day_p['장소'] == sel_place]
                
                pls = day_p['이름'].unique().tolist()
                with c2: target_u = st.selectbox("대상자 (현재 DB 등록자)", pls, key="md_u")
                
                act = st.radio("동작", ["대타 지정 (추가)", "취소 (삭제)"], horizontal=True, key="md_act")
                new_u = None
                if "대타" in act:
                    all_u = get_users(t_isl)
                    new_u = st.selectbox("교체 해설사", [u for u in all_u if u != target_u], key="md_n")
                
                if st.button("수정 요청 적용"):
                    try:
                        if target_u not in pls:
                            st.error("해당 날짜에 선택한 대상자의 계획이 없습니다.")
                        else:
                            tr = day_p[day_p['이름']==target_u].iloc[0]
                            t_place = tr['장소']; t_stat = tr.get('활동여부', '')
                            origin = tr.get('기존해설사', '')
                            if not origin: origin = target_u 
                            
                            if "대타" in act and new_u:
                                row_dict = {
                                    "날짜": target_d, "섬": t_isl, "장소": t_place, "이름": new_u,
                                    "활동여부": t_stat, "비고": "대타요청", "타임스탬프": str(get_kst_now()),
                                    "년": py, "월": pm, "상태": "승인대기", "대타여부": "O", "기존해설사": origin
                                }
                                save_data("활동계획", [row_dict])
                                st.success("대타 지정 요청 완료! (조장 승인 대기)"); time.sleep(1); st.rerun()
                                
                            elif "취소" in act:
                                sh = client.open(SPREADSHEET_NAME).worksheet("활동계획")
                                ald = pd.DataFrame(sh.get_all_records())
                                ald.columns = [str(c).strip() for c in ald.columns]
                                if '일자' in ald.columns: ald.rename(columns={'일자': '날짜'}, inplace=True)
                                ald['d_str'] = pd.to_datetime(ald['날짜'], errors='coerce').dt.strftime("%Y-%m-%d")
                                
                                mask = (ald['d_str']==target_d) & (ald['이름'].astype(str).str.strip()==str(target_u).strip()) & (ald['장소']==t_place)
                                ald.loc[mask, '상태'] = '취소대기'
                                rem = ald.drop(columns=['d_str'])
                                sh.clear(); sh.update([rem.columns.values.tolist()] + rem.astype(str).values.tolist())
                                st.cache_data.clear()
                                st.success("취소 요청 완료! (조장 승인 대기)"); time.sleep(1); st.rerun()
                                
                    except Exception as e: st.error(f"오류: {e}")

def ui_approve(island, role):
    st.header("✅ 계획 승인")
    now = get_kst_now()
    nm = now.replace(day=28) + pd.Timedelta(days=4)
    c1,c2,c3=st.columns([1,1,2])
    with c1: py=st.number_input("연도", value=nm.year, key="ap_y")
    with c2: pm=st.number_input("월", value=nm.month, key="ap_m")
    with c3: pr=st.radio("기간", ["전반기(1~15일)", "후반기(16~말일)"], horizontal=True, key="ap_r")
    
    tis = island
    if role == "관리자": tis = st.selectbox("섬", list(LOCATIONS.keys()), key="ap_isl")
    c4, c5 = st.columns([2,1])
    with c4: tpl = st.selectbox("장소", LOCATIONS.get(tis, []), key="ap_p")
    
    _, last = calendar.monthrange(py, pm)
    dates = [datetime(py, pm, d) for d in (range(1, 16) if "전반기" in pr else range(16, last+1))]
    dates_str = [d.strftime("%Y-%m-%d") for d in dates]
    
    df = load_data("활동계획", py, pm, tis)
    if not df.empty: df = df[df['장소'] == tpl]
    j_df = load_data("활동일지", py, pm, tis)
    
    disp_rows, workers_list = get_display_data(df, j_df, dates_str, is_pdf=False, is_plan_only=False)
    df_disp = pd.DataFrame(disp_rows)
    cols = ["날짜", "요일", "plan_0", "plan_1", "plan_2", "plan_3", "res_0", "res_1", "res_2", "res_3"]
    for c in cols:
        if c not in df_disp.columns: df_disp[c] = ""
        
    edited = st.data_editor(df_disp[cols], hide_index=True, use_container_width=True)
    
    if st.button("💾 승인 완료 저장", use_container_width=True):
        try:
            raw_df = load_data("활동계획", py, pm, tis)
            if raw_df.empty:
                st.warning("저장할 데이터가 없습니다.")
            else:
                raw_df['d_temp'] = pd.to_datetime(raw_df['날짜'], errors='coerce').dt.strftime("%Y-%m-%d")
                
                cancel_mask = (raw_df['장소'] == tpl) & (raw_df['d_temp'].isin(dates_str)) & (raw_df['상태'] == '취소대기')
                raw_df.loc[cancel_mask, '상태'] = "취소승인"
                
                approve_mask = (raw_df['장소'] == tpl) & (raw_df['d_temp'].isin(dates_str)) & (raw_df['상태'] == '승인대기')
                raw_df.loc[approve_mask, '상태'] = "승인완료"
                
                save_rows = []
                for _, r in raw_df.iterrows():
                    d_s = r['날짜'].strftime("%Y-%m-%d") if isinstance(r['날짜'], pd.Timestamp) else str(r['날짜'])
                    save_rows.append({
                        "날짜": d_s, "섬": r['섬'], "장소": r['장소'], "이름": r['이름'],
                        "활동여부": r.get('활동여부',''), "비고": r.get('비고',''), "타임스탬프": str(r.get('타임스탬프','')),
                        "년": r.get('년',''), "월": r.get('월',''), "상태": r.get('상태',''), "대타여부": r.get('대타여부',''), "기존해설사": r.get('기존해설사','')
                    })
                
                sh = client.open(SPREADSHEET_NAME).worksheet("활동계획")
                cols = ["날짜","섬","장소","이름","활동여부","비고","타임스탬프","년","월","상태","대타여부","기존해설사"]
                sh.clear()
                
                final_save_df = pd.DataFrame(save_rows)
                for c in cols:
                    if c not in final_save_df.columns: final_save_df[c] = ""
                final_save_df = final_save_df[cols]
                
                sh.update([cols] + final_save_df.astype(str).values.tolist())
                st.cache_data.clear()
                st.success("✅ 승인 완료! (취소된 일정은 취소선으로 표시됩니다)")
                time.sleep(1.5); st.rerun()
        except Exception as e:
            st.error(f"저장 중 오류 발생: {e}")

def ui_report_download(island, role):
    st.header("📥 보고서 다운로드")
    
    c1, c2, c3, c4 = st.columns(4)
    with c1: 
        vy = st.number_input("연도", value=st.session_state['cur_year'], key="rd_y")
    with c2: 
        vm = st.number_input("월", value=st.session_state['cur_month'], key="rd_m")
    with c3: 
        pr = st.radio("기간", ["전반기(1~15일)", "후반기(16~말일)", "월간 전체"], key="rd_r")
    with c4:
        t_isl = island if role == "조장" else st.selectbox("섬", list(LOCATIONS.keys()), key="rd_isl")
        place_options = ["선택하세요"] + LOCATIONS.get(t_isl, [])
        sel_place = st.selectbox("안내소 선택", place_options, key="rd_p")
        
    if sel_place != "선택하세요":
        df_act = load_data("활동일지", vy, vm, t_isl)
        df_op = load_data("운영일지", vy, vm, t_isl)
        df_plan = load_data("활동계획", vy, vm, t_isl)
        
        day_act = df_act[df_act['장소'].astype(str).str.strip() == sel_place] if not df_act.empty else pd.DataFrame()
        day_op = df_op[df_op['장소'].astype(str).str.strip() == sel_place] if not df_op.empty else pd.DataFrame()
        day_plan = df_plan[df_plan['장소'].astype(str).str.strip() == sel_place] if not df_plan.empty else pd.DataFrame()
        
        st.divider()
        st.subheader(f"📄 {vy}년 {vm}월 {sel_place} 보고서")
        
        col_btn1, col_btn2, col_btn3 = st.columns(3)
        
        with col_btn1:
            st.markdown("**1. 운영일지 (서식3)**")
            if not day_act.empty or not day_op.empty:
                pdf_op = generate_official_journal_month_pdf(day_act, day_op, vy, vm, sel_place, pr)
                if pdf_op:
                    st.download_button(f"📥 운영일지 ({pr})", pdf_op, f"운영일지_{sel_place}_{vy}년{vm}월_{pr}.pdf", "application/pdf", use_container_width=True)
            else:
                st.info("데이터 없음")
                
        with col_btn2:
            st.markdown("**2. 활동계획서**")
            _, last = calendar.monthrange(vy, vm)
            if "전반기" in pr: p_dates = [datetime(vy, vm, d).strftime("%Y-%m-%d") for d in range(1, 16)]
            elif "후반기" in pr: p_dates = [datetime(vy, vm, d).strftime("%Y-%m-%d") for d in range(16, last+1)]
            else: p_dates = [datetime(vy, vm, d).strftime("%Y-%m-%d") for d in range(1, last+1)]
            
            disp_plan, workers_list = get_display_data(day_plan, day_act, p_dates, is_pdf=True, is_plan_only=True)
            pdf_plan = generate_plan_result_pdf("지질공원 안내소 활동계획서", sel_place, "", vy, vm, pr, disp_plan, workers_list)
            if pdf_plan:
                st.download_button(f"📥 활동계획서 ({pr})", pdf_plan, f"활동계획서_{sel_place}_{vy}년{vm}월_{pr}.pdf", "application/pdf", use_container_width=True)

        with col_btn3:
            st.markdown("**3. 활동결과보고서**")
            disp_res, workers_list_res = get_display_data(day_plan, day_act, p_dates, is_pdf=True, is_plan_only=False)
            pdf_res = generate_plan_result_pdf("지질공원 안내소 활동결과보고서", sel_place, "", vy, vm, pr, disp_res, workers_list_res)
            if pdf_res:
                st.download_button(f"📥 활동결과보고서 ({pr})", pdf_res, f"활동결과보고서_{sel_place}_{vy}년{vm}월_{pr}.pdf", "application/pdf", use_container_width=True)

def ui_stats():
    st.header("📊 통계")
    
    c1, c2 = st.columns(2)
    with c1: 
        sy = st.number_input("연도", value=st.session_state['cur_year'], key="st_y")
        st.session_state['cur_year'] = sy
    with c2: 
        sm = st.number_input("월", value=st.session_state['cur_month'], key="st_m")
        st.session_state['cur_month'] = sm
    
    if st.button("통계 불러오기", use_container_width=True):
        df_op = load_data("운영일지", sy, sm, None)
        df_legacy_act = load_data("활동일지", sy, sm, None)
        
        total_v = 0; total_l = 0; total_c = 0
        
        if not df_op.empty:
            df_op['탐방객수'] = df_op['탐방객수'].apply(safe_int)
            df_op['청취자수'] = df_op['청취자수'].apply(safe_int)
            
            valid_stats = df_op[df_op['공동해설'].astype(str) != 'True']
            total_v += int(valid_stats['탐방객수'].sum())
            total_l += int(valid_stats['청취자수'].sum())
            
            if '입력시간' in df_op.columns:
                valid_ops = df_op[(df_op['입력시간'] != "") & (df_op['청취자수'].apply(safe_int) > 0)]
                total_c += len(valid_ops) 
                
        if not df_legacy_act.empty:
            if '청취자수' in df_legacy_act.columns and '입력시간' not in df_legacy_act.columns:
                total_l += int(pd.to_numeric(df_legacy_act['청취자수'], errors='coerce').fillna(0).sum())
            if '해설횟수' in df_legacy_act.columns:
                total_c += int(pd.to_numeric(df_legacy_act['해설횟수'], errors='coerce').fillna(0).sum())
            
        c_m1, c_m2, c_m3 = st.columns(3)
        c_m1.metric("총 탐방객 (합계)", f"{total_v:,}명")
        c_m2.metric("총 청취자 (합계)", f"{total_l:,}명")
        c_m3.metric("총 해설횟수 (누적)", f"{total_c:,}회")
        
        st.divider()
        if not df_op.empty and '장소' in df_op.columns:
            st.subheader("📍 장소별 통계")
            valid_loc_stats = df_op[df_op['공동해설'].astype(str) != 'True']
            st.dataframe(valid_loc_stats.groupby('장소')[['탐방객수', '청취자수']].sum().reset_index(), use_container_width=True)
            
            st.subheader("👤 해설사별 실적")
            def get_cnt(r):
                c = safe_int(r.get('해설횟수'))
                if c > 0: return c
                return 1 if safe_int(r.get('청취자수')) > 0 else 0
                
            df_op['cal_cnt'] = df_op.apply(get_cnt, axis=1)
            sum_grp = df_op.groupby('이름')[['탐방객수', '청취자수', 'cal_cnt']].sum().reset_index()
            sum_grp.rename(columns={'cal_cnt': '해설횟수'}, inplace=True)
            
            st.dataframe(sum_grp, use_container_width=True)

# =========================================================
# 5. 메인 실행
# =========================================================
def main():
    stx_manager = get_manager()
    cookie_val = stx_manager.get(cookie="geopark_login")
    
    if cookie_val and not st.session_state['logged_in']:
        try:
            sh = client.open(SPREADSHEET_NAME).worksheet("사용자")
            users = sh.get_all_records()
            found = next((u for u in users if str(u['아이디']) == str(cookie_val)), None)
            if found:
                st.session_state['logged_in'] = True
                st.session_state['user_info'] = found
                st.rerun()
        except:
            pass

    if not st.session_state['logged_in']:
        st.markdown("## 🔐 로그인")
        with st.form("login"):
            uid = st.text_input("아이디")
            upw = st.text_input("비밀번호", type="password")
            auto_login = st.checkbox("✅ 자동 로그인 유지 (30일)")
            
            if st.form_submit_button("로그인"):
                try:
                    sh = client.open(SPREADSHEET_NAME).worksheet("사용자")
                    users = sh.get_all_records()
                    found = next((u for u in users if str(u['아이디']) == uid and str(u['비번']) == upw), None)
                    if found:
                        st.session_state['logged_in'] = True
                        st.session_state['user_info'] = found
                        
                        if auto_login:
                            stx_manager.set("geopark_login", uid, expires_at=datetime.now() + timedelta(days=30))
                        
                        st.rerun()
                    else: 
                        st.error("로그인 실패: 아이디 또는 비밀번호를 확인해주세요.")
                except Exception as e: 
                    st.error("구글 서버 연결 실패. 잠시 후 다시 시도해주세요.")
    else:
        user = st.session_state['user_info']
        name = user['이름']
        role = user['직책']
        island = user['섬']
        
        with st.sidebar:
            st.info(f"{name} ({role})")
            if st.button("로그아웃"):
                stx_manager.delete("geopark_login")
                st.session_state['logged_in'] = False
                st.rerun()
                
        if role == "관리자":
            t1, t2, t3, t4, t5, t6, t7 = st.tabs(["📝 일지 작성", "🔍 활동 조회", "✍️ 계획 작성", "🗓️ 계획 조회 및 수정", "✅ 계획 승인", "📥 보고서 다운로드", "📊 통계"])
            with t1: ui_journal_write(name, island)
            with t2: ui_view_journal("all", name, island, role)
            with t3: ui_plan_input(name, island)
            with t4: ui_view_plan("all", name, island, role)
            with t5: ui_approve(island, role)
            with t6: ui_report_download(island, role)
            with t7: ui_stats()
            
        elif role == "조장":
            t1, t2, t3, t4, t5, t6 = st.tabs(["📝 일지 작성", "🔍 활동 조회", "✍️ 계획 작성", "🗓️ 계획 조회 및 수정", "✅ 계획 승인", "📥 보고서 다운로드"])
            with t1: ui_journal_write(name, island)
            with t2: ui_view_journal("team", name, island, role)
            with t3: ui_plan_input(name, island)
            with t4: ui_view_plan("team", name, island, role)
            with t5: ui_approve(island, role)
            with t6: ui_report_download(island, role)
            
        else: # 조원
            t1, t2, t3, t4 = st.tabs(["📝 일지 작성", "🔍 내 활동 조회", "✍️ 계획 작성", "🗓️ 내 계획 조회 및 수정"])
            with t1: ui_journal_write(name, island)
            with t2: ui_view_journal("me", name, island, role)
            with t3: ui_plan_input(name, island)
            with t4: ui_view_plan("me", name, island, role)

if __name__ == "__main__":
    main()
