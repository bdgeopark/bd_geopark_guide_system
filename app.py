import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time
import calendar
import os
from fpdf import FPDF

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
            for c in ["입력시간", "탐방객수", "청취자수", "특이사항"]:
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
                
                v = safe_int(r.get('탐방객수', 0))
                l = safe_int(r.get('청취자수', 0))
                
                slot_data[slot_k]['vis'] += v
                slot_data[slot_k]['lis'] += l
                if l > 0: slot_data[slot_k]['cnt'] += 1 
                if str(r.get('특이사항')).strip(): slot_data[slot_k]['note'].append(str(r.get('특이사항')))
        
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

def generate_plan_result_pdf(doc_title, target_place, special_note, p_year, p_month, p_range, disp_rows):
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
            pdf.cell(w_c, 7, "", 1, 0, 'C', True)
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
            raw_txt = row.get(f"plan_{i}", "").replace("(대기)", "").replace("(취소요청)", "").strip()
            if "~~" in raw_txt:
                parts = raw_txt.split("~~")
                final_txt = f"(취소){parts[1]}\n{parts[2].strip()}" if len(parts) >= 3 else raw_txt
            else: final_txt = raw_txt
            if "\n" in final_txt:
                pdf.multi_cell(w_c, 4, final_txt, 1, 'C'); pdf.set_xy(bx+(i*w_c), yc); pdf.rect(bx+(i*w_c), yc, w_c, row_h)
            else: pdf.cell(w_c, row_h, final_txt, 1, 0, 'C')
                
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

def get_display_data(df_plan, df_act, date_list):
    disp_rows = []
    if not df_plan.empty: df_plan['d_str'] = pd.to_datetime(df_plan['날짜'], errors='coerce').dt.strftime('%Y-%m-%d')
    if not df_act.empty: df_act['d_str'] = pd.to_datetime(df_act['날짜'], errors='coerce').dt.strftime('%Y-%m-%d')
        
    for d in date_list:
        if isinstance(d, str): d_obj = datetime.strptime(d, "%Y-%m-%d")
        else: d_obj = d
        d_str = d_obj.strftime("%Y-%m-%d"); w_day = DAY_MAP[d_obj.weekday()]
        row_dat = {"날짜": d_str, "요일": w_day}
        
        day_plans_all = pd.DataFrame()
        if not df_plan.empty: day_plans_all = df_plan[df_plan['d_str'] == d_str]
        
        final_slots = []
        if not day_plans_all.empty:
            subs = day_plans_all[day_plans_all['대타여부'] == 'O']
            origs = day_plans_all[day_plans_all['대타여부'] != 'O']
            replaced_planners = subs['기존해설사'].astype(str).str.strip().unique().tolist()
            
            for _, r in subs.iterrows():
                stat_tag = ""
                if r.get('상태') == '승인대기': stat_tag = "(대기)"
                elif r.get('상태') == '취소대기': stat_tag = "(취소요청)"
                final_slots.append({'plan_display': f"~~{r['기존해설사']}~~ {r['이름']} {stat_tag}", 'worker_name': str(r['이름']).strip(), 'is_sub': True})
                
            for _, r in origs.iterrows():
                if str(r['이름']).strip() not in replaced_planners:
                    stat_tag = ""
                    if r.get('상태') == '승인대기': stat_tag = "(대기)"
                    elif r.get('상태') == '취소대기': stat_tag = "(취소요청)"
                    final_slots.append({'plan_display': f"{r['이름']} {stat_tag}", 'worker_name': str(r['이름']).strip(), 'is_sub': False})
        
        day_acts = pd.DataFrame()
        if not df_act.empty: day_acts = df_act[df_act['d_str'] == d_str]
        
        planned_workers = [s['worker_name'] for s in final_slots]
        if not day_acts.empty:
            for _, log in day_acts.iterrows():
                w_name = str(log.get('이름', '')).strip()
                if w_name and w_name not in planned_workers:
                    final_slots.append({
                        'plan_display': f"{w_name} (계획없음)",
                        'worker_name': w_name,
                        'is_sub': False
                    })
                    planned_workers.append(w_name)

        used_log_indices = set()
        for i in range(4):
            p_key = f"plan_{i}"; r_key = f"res_{i}"; p_val = ""; r_val = ""
            if i < len(final_slots):
                slot = final_slots[i]; p_val = slot['plan_display']; target_worker = slot['worker_name']
                if not day_acts.empty:
                    for idx, log in day_acts.iterrows():
                        if idx not in used_log_indices and str(log.get('이름', '')).strip() == target_worker:
                            c_in = str(log.get('출근시간', '')).split('(')[0].strip()
                            c_out = str(log.get('퇴근시간', '')).split('(')[0].strip()
                            
                            if c_in and c_out: 
                                h = calc_working_hours(c_in, c_out)
                                r_in = round_time_30min(c_in)
                                r_out = round_time_30min(c_out)
                                t_val = f"{h}H ({r_in}~{r_out})" if h else "완료"
                            elif c_in: t_val = "근무중"
                            else: t_val = "미출근"
                            
                            r_val = f"{target_worker}\n{t_val}" if slot['is_sub'] else f"{t_val}"
                            used_log_indices.add(idx)
                            break
            row_dat[p_key] = p_val; row_dat[r_key] = r_val
        disp_rows.append(row_dat)
    return disp_rows

# =========================================================
# 4. UI 탭별 함수
# =========================================================

def ui_journal_write(name, island):
    st.header("📝 일지 작성")
    
    t_act, t_op = st.tabs(["🕒 출퇴근 기록 (활동일지)", "📊 해설 실적 등록 (운영일지)"])
    now = get_kst_now()
    today_str = now.strftime("%Y-%m-%d")
    
    with t_act:
        st.subheader("출퇴근 기록")
        st.info("안내소에 도착하면 [출근], 업무가 끝나면 [퇴근]을 눌러주세요.")
        
        c1, c2 = st.columns(2)
        with c1: place_act = st.selectbox("근무 안내소", LOCATIONS.get(island, []), key="act_p")
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

    with t_op:
        st.subheader("해설 실적 등록")
        st.info("💡 해설을 진행할 때마다 실적을 등록하세요. **청취자가 1명 이상일 경우에만 '해설 횟수'가 카운트**됩니다.")
        
        with st.form("op_form"):
            place_op = st.selectbox("해설 장소", LOCATIONS.get(island, []), key="op_p")
            c_op1, c_op2 = st.columns(2)
            vis = c_op1.number_input("탐방객 수 (명)", min_value=0, step=1)
            lis = c_op2.number_input("해설 청취자 수 (명)", min_value=0, step=1)
            note = st.text_input("특이사항 (교육, 정비 등 내용 입력)")
            
            if st.form_submit_button("💾 실적 1건 등록", use_container_width=True):
                now_time = get_kst_now().strftime("%H:%M")
                row_dict = {
                    "날짜": today_str, "섬": island, "장소": place_op, "이름": name,
                    "입력시간": now_time, "탐방객수": vis, "청취자수": lis, "특이사항": note,
                    "타임스탬프": str(get_kst_now()), "년": now.year, "월": now.month
                }
                
                if append_data("운영일지", row_dict):
                    if lis > 0:
                        st.success(f"[{now_time}] 실적이 등록되었습니다! (해설 횟수 +1 증가)")
                    else:
                        st.success(f"[{now_time}] 방문객 정보가 등록되었습니다! (청취자 0명이므로 해설횟수는 증가 안함)")
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
            
            disp_df['활동시간(H)'] = disp_df.apply(lambda r: calc_working_hours(r.get('출근시간',''), r.get('퇴근시간','')), axis=1)
            
            disp_df['출근시간'] = disp_df['출근시간'].apply(format_time_with_rounded)
            disp_df['퇴근시간'] = disp_df['퇴근시간'].apply(format_time_with_rounded)
            
            tot_hours = sum([float(x) for x in disp_df['활동시간(H)'] if x])
            tot_hours_str = str(int(tot_hours)) if tot_hours.is_integer() else str(round(tot_hours, 1))
            
            if role in ["조장", "관리자"]:
                with st.form("edit_act_form"):
                    col_config = {"활동시간(H)": st.column_config.Column(disabled=True)}
                    edited_act = st.data_editor(disp_df, hide_index=True, use_container_width=True, column_config=col_config)
                    st.markdown(f"<div style='text-align:right; font-weight:bold; font-size:18px;'>총 활동시간 합계: {tot_hours_str} H</div>", unsafe_allow_html=True)
                    if st.form_submit_button("변경사항 저장"):
                        df_act_dates = pd.to_datetime(df_act['날짜'], errors='coerce').dt.strftime("%Y-%m-%d")
                        for _, r in edited_act.iterrows():
                            if r['날짜'] == '합계': continue
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
            else:
                sum_row_act = pd.DataFrame([{"날짜": "합계", "이름": "-", "장소": "-", "출근시간": "-", "퇴근시간": "-", "활동시간(H)": f"{tot_hours_str}"}])
                disp_df = pd.concat([disp_df, sum_row_act], ignore_index=True)
                st.dataframe(disp_df, hide_index=True, use_container_width=True)
                        
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
            show_cols = ["날짜", "장소", "이름", "입력시간", "탐방객수", "청취자수", "특이사항"]
            for c in show_cols:
                if c not in filter_op.columns: filter_op[c] = ""
            
            disp_op = filter_op[show_cols].copy()
            disp_op['날짜'] = pd.to_datetime(disp_op['날짜'], errors='coerce').dt.strftime('%Y-%m-%d')
            
            disp_op['탐방객수'] = disp_op['탐방객수'].apply(safe_int)
            disp_op['청취자수'] = disp_op['청취자수'].apply(safe_int)
            disp_op['해설횟수'] = disp_op['청취자수'].apply(lambda x: 1 if x > 0 else 0)
            
            tot_vis = disp_op['탐방객수'].sum()
            tot_lis = disp_op['청취자수'].sum()
            tot_cnt = disp_op['해설횟수'].sum()
            
            final_cols = ["날짜", "장소", "이름", "입력시간", "탐방객수", "청취자수", "해설횟수", "특이사항"]
            disp_op = disp_op[final_cols]
            
            sum_row = pd.DataFrame([{"날짜": "합계", "장소": "-", "이름": "-", "입력시간": "-", "탐방객수": tot_vis, "청취자수": tot_lis, "해설횟수": tot_cnt, "특이사항": "-"}])
            disp_op = pd.concat([disp_op, sum_row], ignore_index=True)
            
            st.dataframe(disp_op, use_container_width=True, hide_index=True)

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
    
    if df_plan.empty and df_act.empty: 
        st.info("데이터 없음"); return

    if scope == "me" and not df_plan.empty:
        sub_mask = (df_plan['대타여부'] == 'O') & (df_plan['기존해설사'].astype(str).str.strip() == name.strip())
        sub_rows = df_plan[sub_mask]
        sub_dates = pd.to_datetime(sub_rows['날짜'], errors='coerce').dt.strftime('%Y-%m-%d')
        sub_keys = sub_dates + "_" + sub_rows['장소'].astype(str).str.strip()
        
        df_plan = df_plan[df_plan['이름'].astype(str).str.strip() == name.strip()].copy()
        
        my_dates = pd.to_datetime(df_plan['날짜'], errors='coerce').dt.strftime('%Y-%m-%d')
        df_plan['match_key'] = my_dates + "_" + df_plan['장소'].astype(str).str.strip()
        
        df_plan = df_plan[~df_plan['match_key'].isin(sub_keys)]
        df_plan = df_plan[df_plan['상태'] != '취소대기']
        df_plan = df_plan.drop(columns=['match_key'])
        
        if not df_act.empty:
            df_act = df_act[df_act['이름'].astype(str).str.strip() == name.strip()]

    if sel_place and sel_place != "전체":
        if not df_plan.empty: df_plan = df_plan[df_plan['장소'] == sel_place]
        if not df_act.empty and '장소' in df_act.columns: df_act = df_act[df_act['장소'] == sel_place]
    
    if df_plan.empty and df_act.empty: 
        st.info("조건에 맞는 데이터 없음")
        return

    _, last = calendar.monthrange(py, pm)
    full_month_dates = [datetime(py, pm, d).strftime("%Y-%m-%d") for d in range(1, last+1)]
    
    disp_rows = get_display_data(df_plan, df_act, full_month_dates)
    df_disp = pd.DataFrame(disp_rows)
    cols = ["날짜", "요일", "plan_0", "plan_1", "plan_2", "plan_3", "res_0", "res_1", "res_2", "res_3"]
    for c in cols:
        if c not in df_disp.columns: df_disp[c] = ""
    
    st.dataframe(
        df_disp[cols],
        use_container_width=True, 
        hide_index=True,
        column_config={
            "날짜": st.column_config.Column(width="medium"),
            "요일": st.column_config.Column(width="small"),
            "plan_0": st.column_config.Column("계획 1", width="small"),
            "plan_1": st.column_config.Column("계획 2", width="small"),
            "plan_2": st.column_config.Column("계획 3", width="small"),
            "plan_3": st.column_config.Column("계획 4", width="small"),
            "res_0": st.column_config.Column("결과 1", width="small"),
            "res_1": st.column_config.Column("결과 2", width="small"),
            "res_2": st.column_config.Column("결과 3", width="small"),
            "res_3": st.column_config.Column("결과 4", width="small"),
        }
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
                                st.success("취소 요청 완료! (조장 승인 시 삭제됨)"); time.sleep(1); st.rerun()
                                
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
    
    disp_rows = get_display_data(df, j_df, dates_str)
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
                raw_df = raw_df[~cancel_mask]
                
                raw_df['d_temp'] = pd.to_datetime(raw_df['날짜'], errors='coerce').dt.strftime("%Y-%m-%d")
                approve_mask = (raw_df['장소'] == tpl) & (raw_df['d_temp'].isin(dates_str))
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
                st.success("✅ 승인 완료! (취소 요청된 일정은 완전히 삭제되었습니다)")
                time.sleep(1.5); st.rerun()
        except Exception as e:
            st.error(f"저장 중 오류 발생: {e}")

# [새로 추가] 다운로드 전용 허브 탭
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
            
            disp_plan = get_display_data(day_plan, day_act, p_dates)
            pdf_plan = generate_plan_result_pdf("지질공원 안내소 활동계획서", sel_place, "", vy, vm, pr, disp_plan)
            if pdf_plan:
                st.download_button(f"📥 활동계획서 ({pr})", pdf_plan, f"활동계획서_{sel_place}_{vy}년{vm}월_{pr}.pdf", "application/pdf", use_container_width=True)

        with col_btn3:
            st.markdown("**3. 활동결과보고서**")
            pdf_res = generate_plan_result_pdf("지질공원 안내소 활동결과보고서", sel_place, "", vy, vm, pr, disp_plan)
            if pdf_res:
                st.download_button(f"📥 활동결과보고서 ({pr})", pdf_res, f"활동결과보고서_{sel_place}_{vy}년{vm}월_{pr}.pdf", "application/pdf", use_container_width=True)

# =========================================================
# 5. 메인 실행
# =========================================================
def main():
    if not st.session_state['logged_in']:
        st.markdown("## 🔐 로그인")
        with st.form("login"):
            uid = st.text_input("아이디")
            upw = st.text_input("비밀번호", type="password")
            if st.form_submit_button("로그인"):
                try:
                    sh = client.open(SPREADSHEET_NAME).worksheet("사용자")
                    users = sh.get_all_records()
                    found = next((u for u in users if str(u['아이디']) == uid and str(u['비번']) == upw), None)
                    if found:
                        st.session_state['logged_in'] = True
                        st.session_state['user_info'] = found
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
                st.session_state['logged_in'] = False; st.rerun()
                
        # 탭 권한 분리 및 순서 재배치
        if role == "관리자":
            t1, t2, t3, t4, t5, t6 = st.tabs(["📝 일지 작성", "🔍 활동 조회", "✍️ 계획 작성", "🗓️ 계획 조회 및 수정", "✅ 계획 승인", "📥 보고서 다운로드"])
            with t1: ui_journal_write(name, island)
            with t2: ui_view_journal("all", name, island, role)
            with t3: ui_plan_input(name, island)
            with t4: ui_view_plan("all", name, island, role)
            with t5: ui_approve(island, role)
            with t6: ui_report_download(island, role)
            
        elif role == "조장":
            t1, t2, t3, t4, t5, t6 = st.tabs(["📝 일지 작성", "🔍 내 활동 조회", "✍️ 계획 작성", "🗓️ 계획 조회 및 수정", "✅ 계획 승인", "📥 보고서 다운로드"])
            with t1: ui_journal_write(name, island)
            with t2: ui_view_journal("team", name, island, role)
            with t3: ui_plan_input(name, island)
            with t4: ui_view_plan("team", name, island, role)
            with t5: ui_approve(island, role)
            with t6: ui_report_download(island, role)
            
        else: # 일반 해설사(조원)
            t1, t2, t3, t4 = st.tabs(["📝 일지 작성", "🔍 내 활동 조회", "✍️ 계획 작성", "🗓️ 내 계획 조회 및 수정"])
            with t1: ui_journal_write(name, island)
            with t2: ui_view_journal("me", name, island, role)
            with t3: ui_plan_input(name, island)
            with t4: ui_view_plan("me", name, island, role)

if __name__ == "__main__":
    main()

