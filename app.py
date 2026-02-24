import streamlit as st
import pandas as pd
from datetime import datetime
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

if 'logged_in' not in st.session_state: st.session_state['logged_in'] = False
if 'user_info' not in st.session_state: st.session_state['user_info'] = {}

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
            
        data = sh.get_all_records()
        if not data: return pd.DataFrame()
        
        df = pd.DataFrame(data)
        df.columns = [str(c).strip() for c in df.columns]
        if '일자' in df.columns: df.rename(columns={'일자': '날짜'}, inplace=True)
        
        if sheet_name == "활동계획":
            for c in ['대타여부', '기존해설사', '상태']:
                if c not in df.columns: df[c] = ""

        if '날짜' in df.columns:
            df['날짜'] = pd.to_datetime(df['날짜'], errors='coerce')
            df = df.dropna(subset=['날짜'])
            df['_y'] = df['날짜'].dt.year
            df['_m'] = df['날짜'].dt.month
            
            if year: df = df[df['_y'] == int(year)]
            if month: df = df[df['_m'] == int(month)]
            df = df.drop(columns=['_y', '_m'])
        
        if island and '섬' in df.columns:
            df = df[df['섬'] == island]
            
        return df
    except Exception as e:
        return pd.DataFrame()

def save_data(sheet_name, new_rows, header_list):
    try:
        try: sh = client.open(SPREADSHEET_NAME).worksheet(sheet_name)
        except:
            doc = client.open(SPREADSHEET_NAME)
            sh = doc.add_worksheet(sheet_name, 1000, len(header_list))
            sh.append_row(header_list)
            
        existing = sh.get_all_records()
        old_df = pd.DataFrame(existing) if existing else pd.DataFrame(columns=header_list)
        new_df = pd.DataFrame(new_rows, columns=header_list)
        
        old_df.columns = [str(c).strip() for c in old_df.columns]
        if '일자' in old_df.columns: old_df.rename(columns={'일자': '날짜'}, inplace=True)
        
        def make_key(d):
            return str(d.get('날짜','')) + str(d.get('이름','')) + str(d.get('장소',''))

        if not old_df.empty: old_df['key'] = old_df.apply(make_key, axis=1)
        else: old_df['key'] = []
        new_df['key'] = new_df.apply(make_key, axis=1)
            
        keys_to_remove = new_df['key'].tolist()
        if not old_df.empty:
            final_df = old_df[~old_df['key'].isin(keys_to_remove)].copy()
        else:
            final_df = old_df
        
        final_df = final_df.drop(columns=['key'], errors='ignore')
        new_df = new_df.drop(columns=['key'], errors='ignore')
        
        for col in header_list:
            if col not in final_df.columns: final_df[col] = ""
            
        final_df = final_df[header_list]
        new_df = new_df[header_list]
        
        combined = pd.concat([final_df, new_df], ignore_index=True).fillna("")
        
        if '날짜' in combined.columns:
            combined['날짜'] = pd.to_datetime(combined['날짜'], errors='coerce')
            combined = combined.sort_values('날짜')
            combined['날짜'] = combined['날짜'].dt.strftime("%Y-%m-%d")
            
        sh.clear()
        sh.update([combined.columns.values.tolist()] + combined.values.tolist())
        st.cache_data.clear()
        return True
    except Exception as e:
        st.error(f"저장 오류: {e}")
        return False

def save_daily_report(act_row, op_row):
    try:
        doc = client.open(SPREADSHEET_NAME)
        
        # 1. 활동일지 저장
        act_cols = ["날짜", "섬", "장소", "이름", "활동시간", "활동내용", "청취자수", "해설횟수", "타임스탬프", "년", "월"]
        try: sh_act = doc.worksheet("활동일지")
        except: 
            sh_act = doc.add_worksheet("활동일지", 1000, len(act_cols))
            sh_act.append_row(act_cols)
        
        df_act = pd.DataFrame(sh_act.get_all_records())
        new_act_df = pd.DataFrame([act_row], columns=act_cols)
        
        if not df_act.empty:
            df_act.columns = [str(c).strip() for c in df_act.columns]
            df_act['key'] = df_act['날짜'].astype(str) + df_act['이름'] + df_act['장소']
            new_act_df['key'] = new_act_df['날짜'].astype(str) + new_act_df['이름'] + new_act_df['장소']
            df_act = df_act[~df_act['key'].isin(new_act_df['key'])].drop(columns=['key'], errors='ignore')
            new_act_df = new_act_df.drop(columns=['key'], errors='ignore')
        
        for col in act_cols:
            if col not in df_act.columns: df_act[col] = ""
        df_act = pd.concat([df_act[act_cols], new_act_df[act_cols]], ignore_index=True).fillna("")
        
        if '날짜' in df_act.columns:
            df_act['날짜'] = pd.to_datetime(df_act['날짜'], errors='coerce')
            df_act = df_act.sort_values('날짜')
            df_act['날짜'] = df_act['날짜'].dt.strftime("%Y-%m-%d")
        
        sh_act.clear()
        sh_act.update([df_act.columns.values.tolist()] + df_act.values.tolist())
        
        # 2. 운영일지 저장 (탐방객 보정)
        op_cols = ["날짜", "섬", "장소", "탐방객수", "특이사항", "타임스탬프", "년", "월"]
        try: sh_op = doc.worksheet("운영일지")
        except: 
            sh_op = doc.add_worksheet("운영일지", 1000, len(op_cols))
            sh_op.append_row(op_cols)
            
        df_op = pd.DataFrame(sh_op.get_all_records())
        in_date, in_island, in_place, in_vis, in_note, in_ts, in_y, in_m = op_row
        in_vis = int(in_vis)
        
        if not df_op.empty:
            df_op.columns = [str(c).strip() for c in df_op.columns]
            df_op['d_str'] = pd.to_datetime(df_op['날짜'], errors='coerce').dt.strftime("%Y-%m-%d")
            mask = (df_op['d_str'] == in_date) & (df_op['장소'] == in_place)
            
            if mask.any():
                idx = df_op[mask].index[0]
                old_vis = int(pd.to_numeric(df_op.at[idx, '탐방객수'], errors='coerce') or 0)
                old_note = str(df_op.at[idx, '특이사항'])
                
                final_vis = min(old_vis, in_vis) if (old_vis > 0 and in_vis > 0) else max(old_vis, in_vis)
                final_note = old_note
                if in_note and in_note not in old_note:
                    final_note = f"{old_note} / {in_note}" if old_note else in_note
                    
                df_op.at[idx, '탐방객수'] = final_vis
                df_op.at[idx, '특이사항'] = final_note
                df_op.at[idx, '타임스탬프'] = in_ts
                df_op = df_op.drop(columns=['d_str'])
            else:
                new_op_df = pd.DataFrame([op_row], columns=op_cols)
                df_op = pd.concat([df_op.drop(columns=['d_str'], errors='ignore'), new_op_df], ignore_index=True)
        else:
            df_op = pd.DataFrame([op_row], columns=op_cols)
            
        for col in op_cols:
            if col not in df_op.columns: df_op[col] = ""
        df_op = df_op.fillna("")[op_cols]
        
        if '날짜' in df_op.columns:
            df_op['날짜'] = pd.to_datetime(df_op['날짜'], errors='coerce')
            df_op = df_op.sort_values('날짜')
            df_op['날짜'] = df_op['날짜'].dt.strftime("%Y-%m-%d")
            
        sh_op.clear()
        sh_op.update([df_op.columns.values.tolist()] + df_op.values.tolist())
        
        st.cache_data.clear()
        return True
    except Exception as e:
        st.error(f"일지 저장 중 오류 발생: {e}")
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

# [수정] 조장 및 관리자가 다운받는 '서식 3 지질공원 안내소 운영일지' 생성 함수
def generate_official_journal_pdf(df_merged):
    """
    제공된 '25_8_1_용틀임.pdf' 서식을 기반으로 1일 1장 분량의 운영일지를 생성합니다.
    """
    font_path = "NanumGothic.ttf"
    if not os.path.exists(font_path): return None

    pdf = FPDF(orientation='P', unit='mm', format='A4')
    pdf.set_margins(15, 15, 15)
    pdf.set_auto_page_break(True, margin=15)
    pdf.add_font("Nanum", "", font_path)
    pdf.add_font("Nanum", "B", font_path)

    # 날짜와 장소별로 그룹화 (안내소마다 하루에 1장씩 출력)
    df_merged['d_str'] = pd.to_datetime(df_merged['날짜']).dt.strftime('%Y-%m-%d')
    grouped = df_merged.groupby(['d_str', '장소'])

    for (date_str, place), group in grouped:
        pdf.add_page()
        
        d_obj = datetime.strptime(date_str, "%Y-%m-%d")
        w_day = DAY_MAP[d_obj.weekday()]

        # 1. 타이틀
        pdf.set_font("Nanum", "B", 18)
        pdf.cell(180, 10, "【서식 3】 지질공원 안내소 운영일지", 0, 1, 'C')
        pdf.ln(5)

        # 2. 날짜 표시
        pdf.set_font("Nanum", "B", 11)
        pdf.cell(180, 8, f"({d_obj.year}년 {d_obj.month}월 {d_obj.day}일) {w_day}요일", 0, 1, 'R')

        # 3. 상단 헤더 테이블 (안내소, 지시사항, 해설사 정보)
        pdf.set_fill_color(240, 240, 240)
        pdf.set_font("Nanum", "B", 10)
        
        # 첫 번째 행
        pdf.cell(30, 8, "안내소", 1, 0, 'C', True)
        pdf.set_font("Nanum", "", 10)
        pdf.cell(40, 8, str(place), 1, 0, 'C')
        pdf.set_font("Nanum", "B", 10)
        pdf.cell(30, 8, "지시사항", 1, 0, 'C', True)
        pdf.cell(80, 8, "", 1, 1, 'C')

        # 두 번째 행
        pdf.cell(30, 8, "해설사", 1, 0, 'C', True)
        pdf.cell(40, 8, "성명", 1, 0, 'C', True)
        pdf.cell(80, 8, "활동 시간", 1, 0, 'C', True)
        pdf.cell(30, 8, "합계 시간", 1, 1, 'C', True)

        # 해설사 정보 행 (최대 2명까지 표시, 그 이상은 병합 처리)
        pdf.set_font("Nanum", "", 10)
        guides = group.to_dict('records')
        for i in range(2):
            # 첫 번째 칸은 빈칸 (해설사 아래)
            pdf.cell(30, 8, "", 1, 0, 'C')
            if i < len(guides):
                g = guides[i]
                g_name = str(g.get('이름', ''))
                g_time = str(g.get('활동시간', ''))
                t_display = "08:00~17:00" if g_time == "8" else ("08:00~12:00" if g_time == "4" else "")
                
                pdf.cell(40, 8, g_name, 1, 0, 'C')
                pdf.cell(80, 8, t_display, 1, 0, 'C')
                pdf.cell(30, 8, g_time, 1, 1, 'C')
            else:
                pdf.cell(40, 8, "", 1, 0, 'C')
                pdf.cell(80, 8, "", 1, 0, 'C')
                pdf.cell(30, 8, "", 1, 1, 'C')
                
        pdf.ln(5)

        # 4. 메인 데이터 테이블 (시간별)
        pdf.set_font("Nanum", "B", 10)
        pdf.cell(30, 10, "시간", 1, 0, 'C', True)
        pdf.cell(35, 10, "지질명소 탐방객(명)", 1, 0, 'C', True)
        pdf.cell(35, 10, "해설 청취자(명)", 1, 0, 'C', True)
        pdf.cell(30, 10, "해설 횟수(회)", 1, 0, 'C', True)
        pdf.cell(50, 10, "비고(환경정비, 시설점검 등)", 1, 1, 'C', True)

        pdf.set_font("Nanum", "", 9)
        time_slots = [
            "08:00~09:00", "09:00~10:00", "10:00~11:00", "11:00~12:00",
            "12:00~13:00", "13:00~14:00", "14:00~15:00", "15:00~16:00", 
            "16:00~17:00", "17:00~18:00"
        ]
        
        # 시스템상 시간별 데이터가 없으므로 공란으로 출력
        for t in time_slots:
            pdf.cell(30, 8, t, 1, 0, 'C')
            pdf.cell(35, 8, "", 1, 0, 'C')
            pdf.cell(35, 8, "", 1, 0, 'C')
            pdf.cell(30, 8, "", 1, 0, 'C')
            pdf.cell(50, 8, "", 1, 1, 'C')

        # 합계 계산
        t_vis = str(group['탐방객수'].iloc[0]) if not group['탐방객수'].isna().all() and str(group['탐방객수'].iloc[0]) != "" else "0"
        l_sum = int(pd.to_numeric(group['청취자수'], errors='coerce').fillna(0).sum())
        c_sum = int(pd.to_numeric(group['해설횟수'], errors='coerce').fillna(0).sum())

        pdf.set_font("Nanum", "B", 10)
        pdf.cell(30, 10, "합계", 1, 0, 'C', True)
        pdf.cell(35, 10, t_vis, 1, 0, 'C')
        pdf.cell(35, 10, str(l_sum), 1, 0, 'C')
        pdf.cell(30, 10, str(c_sum), 1, 0, 'C')
        pdf.cell(50, 10, "", 1, 1, 'C')

        # 5. 특이사항 및 서명란
        pdf.ln(5)
        pdf.set_font("Nanum", "B", 10)
        pdf.cell(30, 15, "특이사항", 1, 0, 'C', True)
        
        pdf.set_font("Nanum", "", 9)
        # 특이사항 텍스트 결합 (운영일지 특이사항 + 개인 활동내용)
        note_base = str(group['특이사항'].iloc[0])
        acts = [str(x).strip() for x in group['활동내용'].dropna().unique() if str(x).strip()]
        if acts:
            note_base += " / [활동] " + ", ".join(acts)
            
        # 너무 길면 자르기 (1줄 제한)
        if len(note_base) > 65: note_base = note_base[:63] + "..."
        pdf.cell(150, 15, note_base, 1, 1, 'L')
        
        pdf.ln(10)
        pdf.set_font("Nanum", "", 12)
        pdf.cell(90, 10, "조장 확인 :                         (인/서명)", 0, 0, 'C')
        pdf.cell(90, 10, "면 담당 확인 :                         (인/서명)", 0, 1, 'C')

    return bytes(pdf.output())

def get_display_data(df_plan, df_log, date_list):
    disp_rows = []
    if df_log.empty and '날짜' not in df_log.columns: df_log['날짜'] = []
    
    for d in date_list:
        if isinstance(d, str): d_obj = datetime.strptime(d, "%Y-%m-%d")
        else: d_obj = d
        d_str = d_obj.strftime("%Y-%m-%d"); w_day = DAY_MAP[d_obj.weekday()]
        row_dat = {"날짜": d_str, "요일": w_day}
        
        if not df_plan.empty: day_plans_all = df_plan[df_plan['날짜'] == pd.to_datetime(d_str)]
        else: day_plans_all = pd.DataFrame()
        
        final_slots = []
        if not day_plans_all.empty:
            subs = day_plans_all[day_plans_all['대타여부'] == 'O']
            origs = day_plans_all[day_plans_all['대타여부'] != 'O']
            replaced_planners = subs['기존해설사'].unique().tolist()
            
            for _, r in subs.iterrows():
                stat_tag = ""
                if r.get('상태') == '승인대기': stat_tag = "(대기)"
                elif r.get('상태') == '취소대기': stat_tag = "(취소요청)"
                final_slots.append({'plan_display': f"~~{r['기존해설사']}~~ {r['이름']} {stat_tag}", 'worker_name': r['이름'], 'is_sub': True})
                
            for _, r in origs.iterrows():
                if r['이름'] not in replaced_planners:
                    stat_tag = ""
                    if r.get('상태') == '승인대기': stat_tag = "(대기)"
                    elif r.get('상태') == '취소대기': stat_tag = "(취소요청)"
                    final_slots.append({'plan_display': f"{r['이름']} {stat_tag}", 'worker_name': r['이름'], 'is_sub': False})
        
        day_logs = pd.DataFrame()
        if not df_log.empty: day_logs = df_log[df_log['날짜'] == pd.to_datetime(d_str)]
        used_log_indices = set()
        
        for i in range(4):
            p_key = f"plan_{i}"; r_key = f"res_{i}"; p_val = ""; r_val = ""
            if i < len(final_slots):
                slot = final_slots[i]; p_val = slot['plan_display']; target_worker = slot['worker_name']
                if not day_logs.empty:
                    for idx, log in day_logs.iterrows():
                        if idx not in used_log_indices and log['이름'] == target_worker:
                            t_val = str(log.get('활동시간', ''))
                            r_val = f"{target_worker}({t_val}H)" if slot['is_sub'] else f"{t_val}H"
                            used_log_indices.add(idx)
                            break
            row_dat[p_key] = p_val; row_dat[r_key] = r_val
        disp_rows.append(row_dat)
    return disp_rows

def generate_pdf(target_place, special_note, p_year, p_month, p_range, disp_rows, current_island):
    font_path = "NanumGothic.ttf"
    if not os.path.exists(font_path): return None

    pdf = FPDF(orientation='P', unit='mm', format='A4')
    pdf.set_margins(15, 15, 15); pdf.set_auto_page_break(True, margin=10); pdf.add_page()
    pdf.add_font("Nanum", "", font_path); pdf.add_font("Nanum", "B", font_path)

    pdf.set_font("Nanum", "B", 22); pdf.set_line_width(0.4)
    pdf.cell(180, 15, "지질공원 안내소 운영계획표", 1, 1, 'C'); pdf.ln(3)

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
        pdf.cell(w_d, row_h, row['날짜'].split('-')[2], 1, 0, 'C')
        pdf.cell(w_w, row_h, row['요일'], 1, 0, 'C')
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
                pdf.multi_cell(w_c, 3, txt, 0, 'C'); pdf.set_xy(bx+(i*w_c), yc); pdf.rect(bx+(i*w_c), yc, w_c, row_h)
            else: pdf.cell(w_c, row_h, txt, 1, 0, 'C')
        pdf.set_xy(xc, yc+row_h)

    pdf.set_line_width(0.4); pdf.rect(15, body_sy, 180, pdf.get_y()-body_sy, style="D")
    pdf.ln(5); pdf.set_font("Nanum", "", 12)
    pdf.cell(90, 10, "조장 :                         (인/서명)", 0, 0, 'C')
    pdf.cell(90, 10, "면 담당 :                         (인/서명)", 0, 1, 'C')
    return bytes(pdf.output())

# =========================================================
# 4. UI 탭별 함수
# =========================================================

def ui_journal_write(name, island):
    st.header("📝 운영일지 작성")
    st.info("💡 **중복 입력 주의**: 같은 날/장소 2명 이상 근무 시 **'탐방객 수'는 한 명만 대표로 입력**해주세요. (시스템이 자동으로 작은 수를 반영합니다.)")
    
    now = datetime.now()
    c1, c2, c3 = st.columns([1,1,2])
    with c1: jy = st.number_input("년", value=now.year, key="jw_y")
    with c2: jm = st.number_input("월", value=now.month, key="jw_m")
    with c3: place = st.selectbox("장소", LOCATIONS.get(island, []), key="jw_p")
    
    st.divider()
    mode = st.radio("입력 모드", ["📅 하루씩 입력 (모바일)", "🗓️ 월간 전체 입력 (PC)"], horizontal=True, key="jw_mode")
    
    _, last = calendar.monthrange(jy, jm)
    dates = [datetime(jy, jm, d).strftime("%Y-%m-%d") for d in range(1, last+1)]
    
    df_act = load_data("활동일지", jy, jm, island)
    if not df_act.empty: df_act = df_act[(df_act['이름']==name) & (df_act['장소']==place)]
    
    df_op = load_data("운영일지", jy, jm, island)
    
    if "하루씩" in mode:
        c_d1, c_d2 = st.columns([1, 1.5])
        with c_d1:
            def_d = now.date()
            if def_d.month != jm: def_d = datetime(jy, jm, 1).date()
            pick = st.date_input("날짜", value=def_d, min_value=datetime(jy, jm, 1), max_value=datetime(jy, jm, last), key="jw_pk")
            pick_s = pick.strftime("%Y-%m-%d")
        
        pt="활동 없음"; p_acts=[]; pv=0; pl=0; pc=0; pspec=""
        if not df_act.empty:
            r = df_act[df_act['날짜']==pd.to_datetime(pick_s)]
            if not r.empty:
                r = r.iloc[0]
                tv = str(r['활동시간'])
                if tv=="8": pt="종일 (8시간)"
                elif tv=="4": pt="반일 (4시간)"
                raw_act = str(r.get('활동내용', ''))
                p_acts = [x.strip() for x in raw_act.split(',')] if raw_act else []
                pl = int(r.get('청취자수', 0) or 0)
                pc = int(r.get('해설횟수', 0) or 0)
                
        if not df_op.empty:
            r_op = df_op[(df_op['날짜']==pd.to_datetime(pick_s)) & (df_op['장소']==place)]
            if not r_op.empty:
                pv = int(pd.to_numeric(r_op.iloc[0].get('탐방객수', 0), errors='coerce') or 0)
                pspec = str(r_op.iloc[0].get('특이사항', ''))
                
        with c_d2: st.markdown(f"**{pick.day}일 ({DAY_MAP[pick.weekday()]})**")
        
        with st.form("jw_form"):
            st.markdown("**1. 활동 시간**")
            st_sel = st.radio("시간", ["활동 없음", "종일 (8시간)", "반일 (4시간)"], index=["활동 없음", "종일 (8시간)", "반일 (4시간)"].index(pt), horizontal=True)
            
            st.markdown("**2. 활동 내용 (체크)**")
            act_opts = ["시설점검", "환경정비", "교육"]
            cols_act = st.columns(3)
            sel_acts = []
            for idx, opt in enumerate(act_opts):
                if cols_act[idx].checkbox(opt, value=(opt in p_acts)):
                    sel_acts.append(opt)
            
            st.markdown("**3. 실적 입력**")
            c_n1, c_n2, c_n3 = st.columns(3)
            iv = c_n1.number_input("탐방객(명) *장소통합*", value=pv, min_value=0)
            il = c_n2.number_input("청취자(명) *본인실적*", value=pl, min_value=0)
            ic = c_n3.number_input("해설횟수(회) *본인실적*", value=pc, min_value=0)
            
            st.markdown("**4. 특이사항 *장소통합***")
            ispec = st.text_area("내용 입력", value=pspec, height=80)
            
            if st.form_submit_button("💾 저장", use_container_width=True):
                ft = 8 if "8시간" in st_sel else (4 if "4시간" in st_sel else "")
                act_str = ",".join(sel_acts)
                
                act_r = [pick_s, island, place, name, ft, act_str, il, ic, str(datetime.now()), jy, jm]
                op_r = [pick_s, island, place, iv, ispec, str(datetime.now()), jy, jm]
                
                if save_daily_report(act_r, op_r):
                    st.success("저장 완료!"); time.sleep(0.5); st.rerun()
    else:
        st.info("PC 모드 간편 입력 (※ 탐방객/특이사항은 모바일 하루씩 입력을 권장합니다)")
        grid = []
        d_map = {}
        if not df_act.empty:
            for _, r in df_act.iterrows(): d_map[r['날짜'].strftime("%Y-%m-%d")] = r
        for d in dates:
            cur = d_map.get(d, {})
            tv = str(cur.get('활동시간',''))
            grid.append({
                "날짜": d, "요일": DAY_MAP[datetime.strptime(d, "%Y-%m-%d").weekday()],
                "종일": tv=="8", "반일": tv=="4",
                "청취자": cur.get('청취자수',0), "횟수": cur.get('해설횟수',0)
            })
        with st.form("jw_m_form"):
            edited = st.data_editor(pd.DataFrame(grid), hide_index=True, use_container_width=True)
            if st.form_submit_button("💾 저장"):
                for _, r in edited.iterrows():
                    ft = 8 if r['종일'] else (4 if r['반일'] else "")
                    act_r = [r['날짜'], island, place, name, ft, "", r['청취자'], r['횟수'], str(datetime.now()), jy, jm]
                    op_r = [r['날짜'], island, place, 0, "", str(datetime.now()), jy, jm]
                    save_daily_report(act_r, op_r)
                st.success("완료"); st.rerun()

def ui_view_journal(scope, name, island):
    st.header("🔍 활동 조회")
    
    # 상단 검색 필터
    c1, c2, c3 = st.columns(3)
    with c1: vy = st.number_input("연도", value=datetime.now().year, key="vj_y")
    with c2: vm = st.number_input("월", value=datetime.now().month, key="vj_m")
    
    t_isl = island if scope != "all" else None
    
    if scope == "all" or scope == "team":
        place_options = ["전체"] + [p for locs in LOCATIONS.values() for p in locs] if scope == "all" else ["전체"] + LOCATIONS.get(island, [])
        with c3: sel_place = st.selectbox("안내소 선택", place_options, key="vj_p")
    else:
        sel_place = "전체"
    
    df_act = load_data("활동일지", vy, vm, t_isl)
    df_op = load_data("운영일지", vy, vm, t_isl)
    
    if df_act.empty and not df_op.empty and '이름' in df_op.columns:
        df_merged = df_op
    else:
        if not df_act.empty and not df_op.empty:
            df_act['d_str'] = pd.to_datetime(df_act['날짜'], errors='coerce').dt.strftime('%Y-%m-%d')
            df_op['d_str'] = pd.to_datetime(df_op['날짜'], errors='coerce').dt.strftime('%Y-%m-%d')
            op_sub = df_op[['d_str', '장소', '탐방객수', '특이사항']].drop_duplicates(['d_str', '장소'])
            df_merged = pd.merge(df_act, op_sub, on=['d_str', '장소'], how='left')
            df_merged = df_merged.drop(columns=['d_str'])
        elif not df_act.empty:
            df_merged = df_act.copy()
            if '탐방객수' not in df_merged.columns: df_merged['탐방객수'] = ""
            if '특이사항' not in df_merged.columns: df_merged['특이사항'] = ""
        else:
            df_merged = pd.DataFrame()
            
    if df_merged.empty:
        st.info("데이터가 없습니다.")
        return
        
    if scope == "me" and '이름' in df_merged.columns:
        df_merged = df_merged[df_merged['이름'] == name]
        
    if sel_place != "전체" and '장소' in df_merged.columns:
        df_merged = df_merged[df_merged['장소'] == sel_place]
        
    if df_merged.empty:
        st.info("조건에 맞는 활동 데이터가 없습니다.")
        return

    display_cols = ["날짜", "섬", "장소", "이름", "활동시간", "활동내용", "탐방객수", "청취자수", "해설횟수", "특이사항"]
    display_cols = [c for c in display_cols if c in df_merged.columns]
    
    st.dataframe(df_merged[display_cols], use_container_width=True, hide_index=True, column_config={
        "날짜": st.column_config.DateColumn("날짜", format="YYYY-MM-DD")
    })
    
    # [새로운 서식 PDF 다운로드 기능]
    st.divider()
    c_dl1, c_dl2 = st.columns(2)
    with c_dl1:
        csv_data = df_merged[display_cols].to_csv(index=False).encode('utf-8-sig')
        st.download_button("📊 엑셀(CSV) 다운로드", csv_data, f"활동내역_{vy}년{vm}월.csv", "text/csv", use_container_width=True)
        
    with c_dl2:
        if sel_place == "전체":
            st.warning("⚠️ 서식 3 운영일지 PDF를 다운로드하려면 먼저 위에서 '특정 안내소'를 선택해주세요.")
        else:
            journal_pdf = generate_official_journal_pdf(df_merged)
            if journal_pdf:
                st.download_button("📥 【서식 3】 운영일지 PDF 다운로드", journal_pdf, f"운영일지_{sel_place}_{vy}년{vm}월.pdf", "application/pdf", use_container_width=True, key="journal_pdf_dl")

def ui_plan_input(name, island):
    st.header("✍️ 계획 입력")
    now = datetime.now()
    nm = now.replace(day=28) + pd.Timedelta(days=4)
    c1,c2,c3=st.columns([1,1,2])
    with c1: py=st.number_input("연도", value=nm.year, key="pi_y")
    with c2: pm=st.number_input("월", value=nm.month, key="pi_m")
    with c3: pr=st.radio("기간", ["전반기(1~15일)", "후반기(16~말일)"], horizontal=True, key="pi_r")
    place = st.selectbox("장소", LOCATIONS.get(island, []), key="pi_p")
    
    _, last = calendar.monthrange(py, pm)
    dates = [datetime(py, pm, d).strftime("%Y-%m-%d") for d in (range(1, 16) if "전반기" in pr else range(16, last+1))]
    
    df = load_data("활동계획", py, pm, island)
    if not df.empty: df = df[(df['이름']==name) & (df['장소']==place)]
    
    st.divider()
    mode = st.radio("모드", ["📅 하루씩", "🗓️ 전체"], horizontal=True, key="pi_md")
    
    if "하루씩" in mode:
        c1, c2 = st.columns([1, 1.5])
        with c1:
            try: pick = st.date_input("날짜", value=datetime.strptime(dates[0],"%Y-%m-%d").date(), min_value=datetime.strptime(dates[0],"%Y-%m-%d").date(), max_value=datetime.strptime(dates[-1],"%Y-%m-%d").date(), key="pi_pk")
            except: pick = datetime.strptime(dates[0],"%Y-%m-%d").date()
            pick_s = pick.strftime("%Y-%m-%d")
        ps="활동 없음"; etc=""
        if not df.empty:
            r = df[df['날짜']==pd.to_datetime(pick_s)]
            if not r.empty:
                val = r.iloc[0]['활동여부']
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
                row = [pick_s, island, place, name, stat, "", str(datetime.now()), py, pm, "승인대기", "", ""]
                cols = ["날짜","섬","장소","이름","활동여부","비고","타임스탬프","년","월","상태","대타여부","기존해설사"]
                save_data("활동계획", [row], cols); st.success("승인 대기 상태로 저장되었습니다."); st.rerun()
    else:
        grid = []
        d_map = {}
        if not df.empty:
            for _, r in df.iterrows(): d_map[r['날짜'].strftime("%Y-%m-%d")] = r
        for d in dates:
            curr = d_map.get(d, {})
            val = curr.get('활동여부', "")
            grid.append({
                "날짜": d, "요일": DAY_MAP[datetime.strptime(d, "%Y-%m-%d").weekday()],
                "종일": val=="종일", "오전": "오전" in val, "오후": "오후" in val, "기타": val if val not in ["종일","오전(4시간)","오후(4시간)",""] else ""
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
                    rows.append([r['날짜'], island, place, name, s, "", str(datetime.now()), py, pm, "승인대기", "", ""])
                cols = ["날짜","섬","장소","이름","활동여부","비고","타임스탬프","년","월","상태","대타여부","기존해설사"]
                save_data("활동계획", rows, cols); st.success("승인 대기 상태로 저장되었습니다."); st.rerun()

def ui_view_plan(scope, name, island, role=""):
    st.header("🗓️ 계획 조회 및 수정")
    c1, c2 = st.columns(2)
    now = datetime.now()
    with c1: py = st.number_input("연도", value=now.year, key="vp_y")
    with c2: pm = st.number_input("월", value=now.month, key="vp_m")
    
    sel_place = None
    if scope == "team" or scope == "all":
        t_isl = island if scope == "team" else st.selectbox("섬", list(LOCATIONS.keys()), key="vp_i")
        place_list = LOCATIONS.get(t_isl, [])
        sel_place = st.selectbox("안내소 선택 (상세조회)", place_list, key="vp_p")
    else:
        t_isl = island

    df_plan = load_data("활동계획", py, pm, t_isl)
    df_act = load_data("활동일지", py, pm, t_isl)
    
    if df_plan.empty: st.info("데이터 없음"); return
    
    if sel_place:
        df_plan = df_plan[df_plan['장소'] == sel_place]
        if not df_act.empty and '장소' in df_act.columns:
            df_act = df_act[df_act['장소'] == sel_place]
    
    if scope == "me": df_plan = df_plan[df_plan['이름'] == name]
    if df_plan.empty: st.info("조건에 맞는 데이터 없음"); return

    try: dates = sorted(df_plan['날짜'].unique())
    except: dates = []
    
    disp_rows = get_display_data(df_plan, df_act, dates)
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
            avail_dates = [r['날짜'] for r in disp_rows]
            with c1: target_d = st.selectbox("날짜", sorted(list(set(avail_dates))), key="md_d")
            
            day_p = df_plan[df_plan['날짜'] == pd.to_datetime(target_d)]
            pls = day_p['이름'].unique().tolist()
            with c2: target_u = st.selectbox("대상자 (현재 DB 등록자)", pls, key="md_u")
            
            act = st.radio("동작", ["대타 지정 (추가)", "취소 (삭제)"], horizontal=True, key="md_act")
            new_u = None
            if "대타" in act:
                all_u = get_users(t_isl)
                new_u = st.selectbox("교체 해설사", [u for u in all_u if u != target_u], key="md_n")
            
            if st.button("수정 요청 적용"):
                try:
                    tr = day_p[day_p['이름']==target_u].iloc[0]
                    t_place = tr['장소']; t_stat = tr['활동여부']
                    origin = tr.get('기존해설사', '')
                    if not origin: origin = target_u 
                    
                    if "대타" in act and new_u:
                        row = {
                            "날짜": target_d, "섬": t_isl, "장소": t_place, "이름": new_u,
                            "활동여부": t_stat, "비고": "대타요청", "타임스탬프": str(datetime.now()),
                            "년": py, "월": pm, "상태": "승인대기", "대타여부": "O", "기존해설사": origin
                        }
                        cols = ["날짜","섬","장소","이름","활동여부","비고","타임스탬프","년","월","상태","대타여부","기존해설사"]
                        save_data("활동계획", [list(row.values())], cols)
                        st.success("대타 지정 요청 완료! (조장 승인 대기)"); time.sleep(1); st.rerun()
                        
                    elif "취소" in act:
                        sh = client.open(SPREADSHEET_NAME).worksheet("활동계획")
                        ald = pd.DataFrame(sh.get_all_records())
                        ald.columns = [str(c).strip() for c in ald.columns]
                        if '일자' in ald.columns: ald.rename(columns={'일자': '날짜'}, inplace=True)
                        ald['d_str'] = pd.to_datetime(ald['날짜'], errors='coerce').dt.strftime("%Y-%m-%d")
                        
                        mask = (ald['d_str']==target_d) & (ald['이름']==target_u) & (ald['장소']==t_place)
                        ald.loc[mask, '상태'] = '취소대기'
                        rem = ald.drop(columns=['d_str'])
                        sh.clear(); sh.update([rem.columns.values.tolist()] + rem.values.tolist())
                        st.cache_data.clear()
                        st.success("취소 요청 완료! (조장 승인 시 삭제됨)"); time.sleep(1); st.rerun()
                        
                except Exception as e: st.error(f"오류: {e}")

def ui_approve(island, role):
    st.header("✅ 계획 승인")
    now = datetime.now(); nm = now.replace(day=28) + pd.Timedelta(days=4)
    c1,c2,c3=st.columns([1,1,2])
    with c1: py=st.number_input("연도", value=nm.year, key="ap_y")
    with c2: pm=st.number_input("월", value=nm.month, key="ap_m")
    with c3: pr=st.radio("기간", ["전반기(1~15일)", "후반기(16~말일)"], horizontal=True, key="ap_r")
    
    tis = island
    if role == "관리자": tis = st.selectbox("섬", list(LOCATIONS.keys()), key="ap_isl")
    c4, c5 = st.columns([2,1])
    with c4: tpl = st.selectbox("장소", LOCATIONS.get(tis, []), key="ap_p")
    with c5: note = st.text_input("특이사항", key="ap_n")
    
    _, last = calendar.monthrange(py, pm)
    dates = [datetime(py, pm, d) for d in (range(1, 16) if "전반기" in pr else range(16, last+1))]
    dates_str = [d.strftime("%Y-%m-%d") for d in dates]
    
    df = load_data("활동계획", py, pm, tis)
    if not df.empty: df = df[df['장소'] == tpl]
    j_df = load_data("활동일지", py, pm, tis)
    
    disp_rows = get_display_data(df, j_df, dates)
    df_disp = pd.DataFrame(disp_rows)
    cols = ["날짜", "요일", "plan_0", "plan_1", "plan_2", "plan_3", "res_0", "res_1", "res_2", "res_3"]
    for c in cols:
        if c not in df_disp.columns: df_disp[c] = ""
        
    edited = st.data_editor(df_disp[cols], hide_index=True, use_container_width=True)
    
    c_btn1, c_btn2 = st.columns(2)
    with c_btn1:
        if st.button("💾 승인 완료 저장"):
            try:
                raw_df = load_data("활동계획", py, pm, tis)
                if raw_df.empty:
                    st.warning("저장할 데이터가 없습니다.")
                else:
                    raw_df['d_temp'] = raw_df['날짜'].dt.strftime("%Y-%m-%d")
                    
                    cancel_mask = (raw_df['장소'] == tpl) & (raw_df['d_temp'].isin(dates_str)) & (raw_df['상태'] == '취소대기')
                    raw_df = raw_df[~cancel_mask]
                    
                    raw_df['d_temp'] = raw_df['날짜'].dt.strftime("%Y-%m-%d")
                    approve_mask = (raw_df['장소'] == tpl) & (raw_df['d_temp'].isin(dates_str))
                    raw_df.loc[approve_mask, '상태'] = "승인완료"
                    
                    save_rows = []
                    for _, r in raw_df.iterrows():
                        d_s = r['날짜'].strftime("%Y-%m-%d")
                        row = [
                            d_s, r['섬'], r['장소'], r['이름'], r['활동여부'], r['비고'], 
                            str(r['타임스탬프']), r['년'], r['월'], r['상태'], 
                            r['대타여부'], r['기존해설사']
                        ]
                        save_rows.append(row)
                    
                    sh = client.open(SPREADSHEET_NAME).worksheet("활동계획")
                    cols = ["날짜","섬","장소","이름","활동여부","비고","타임스탬프","년","월","상태","대타여부","기존해설사"]
                    sh.clear()
                    sh.update([cols] + save_rows)
                    st.cache_data.clear()
                    st.success("✅ 승인 완료! (취소 요청된 일정은 완전히 삭제되었습니다)")
                    time.sleep(1.5); st.rerun()
            except Exception as e:
                st.error(f"저장 중 오류 발생: {e}")
            
    with c_btn2:
        pdf_data = generate_pdf(tpl, note, py, pm, pr, disp_rows, tis)
        if pdf_data:
            st.download_button("📥 운영계획서 PDF 다운로드", pdf_data, f"운영계획서_{tpl}_{pm}월.pdf", "application/pdf", key="pdf_dl_btn")

def ui_stats():
    st.header("📊 통계")
    
    c1, c2 = st.columns(2)
    with c1: sy = st.number_input("연도", value=datetime.now().year, key="st_y")
    with c2: sm = st.number_input("월", value=datetime.now().month, key="st_m")
    
    if st.button("통계 불러오기"):
        df_op = load_data("운영일지", sy, sm, None)
        total_v = 0
        if not df_op.empty:
            df_op['탐방객수'] = pd.to_numeric(df_op['탐방객수'], errors='coerce').fillna(0)
            total_v = int(df_op['탐방객수'].sum())
            
        df_act = load_data("활동일지", sy, sm, None)
        
        if df_act.empty and not df_op.empty and '이름' in df_op.columns:
            df_act = df_op.copy()
            
        total_l = 0; total_c = 0
        if not df_act.empty:
            df_act['청취자수'] = pd.to_numeric(df_act['청취자수'], errors='coerce').fillna(0)
            df_act['해설횟수'] = pd.to_numeric(df_act['해설횟수'], errors='coerce').fillna(0)
            total_l = int(df_act['청취자수'].sum())
            total_c = int(df_act['해설횟수'].sum())
            
        c_m1, c_m2, c_m3 = st.columns(3)
        c_m1.metric("총 탐방객", f"{total_v:,}명")
        c_m2.metric("총 청취자", f"{total_l:,}명")
        c_m3.metric("총 해설횟수", f"{total_c:,}회")
        
        st.divider()
        if not df_op.empty:
            st.subheader("📍 장소별 통계 (탐방객)")
            st.dataframe(df_op.groupby('장소')['탐방객수'].sum().reset_index(), use_container_width=True)
            
        if not df_act.empty:
            st.subheader("👤 해설사별 실적")
            st.dataframe(df_act.groupby('이름')[['청취자수', '해설횟수']].sum().reset_index(), use_container_width=True)

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
                        st.success(f"{found['이름']}님 환영합니다!")
                        time.sleep(0.5); st.rerun()
                    else: st.error("로그인 실패")
                except: st.error("서버 연결 실패")
    else:
        user = st.session_state['user_info']
        name = user['이름']
        role = user['직책']
        island = user['섬']
        
        with st.sidebar:
            st.info(f"{name} ({role})")
            if st.button("로그아웃"):
                st.session_state['logged_in'] = False; st.rerun()
                
        if role == "관리자":
            t1, t2, t3, t4 = st.tabs(["🔍 활동조회", "🗓️ 계획조회", "📊 통계", "✅ 계획승인"])
            with t1: ui_view_journal("all", name, island)
            with t2: ui_view_plan("all", name, island, role)
            with t3: ui_stats()
            with t4: ui_approve(island, role)
            
        elif role == "조장":
            t1, t2, t3, t4, t5 = st.tabs(["📝 일지작성", "🔍 활동조회", "🗓️ 계획조회", "✍️ 계획입력", "✅ 계획승인"])
            with t1: ui_journal_write(name, island)
            with t2: ui_view_journal("team", name, island)
            with t3: ui_view_plan("team", name, island, role)
            with t4: ui_plan_input(name, island)
            with t5: ui_approve(island, role)
            
        else: # 조원
            t1, t2, t3, t4 = st.tabs(["📝 일지작성", "📅 내 활동", "🗓️ 내 계획", "✍️ 계획입력"])
            with t1: ui_journal_write(name, island)
            with t2: ui_view_journal("me", name, island)
            with t3: ui_view_plan("me", name, island, role)
            with t4: ui_plan_input(name, island)

if __name__ == "__main__":
    main()
