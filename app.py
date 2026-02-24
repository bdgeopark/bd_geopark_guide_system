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

# 한국 표준시(KST) 반환 함수
def get_kst_now():
    return datetime.utcnow() + timedelta(hours=9)

if 'logged_in' not in st.session_state: st.session_state['logged_in'] = False
if 'user_info' not in st.session_state: st.session_state['user_info'] = {}

# 월 상태유지
if 'cur_year' not in st.session_state: st.session_state['cur_year'] = get_kst_now().year
if 'cur_month' not in st.session_state: st.session_state['cur_month'] = get_kst_now().month

# =========================================================
# 2. 데이터 함수 (결측 컬럼 및 레거시 데이터 보호 강화)
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
        
        # 시트별 필수 컬럼 보정
        if sheet_name == "활동계획":
            for c in ['대타여부', '기존해설사', '상태']:
                if c not in df.columns: df[c] = ""
        elif sheet_name == "활동일지":
            for c in ["출근시간", "퇴근시간"]:
                if c not in df.columns: df[c] = ""
        elif sheet_name == "운영일지":
            for c in ["입력시간"]:
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
        
        def make_key(d): return str(d.get('날짜','')) + str(d.get('이름','')) + str(d.get('장소',''))

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

def append_data(sheet_name, row_data, header_list):
    try:
        try: sh = client.open(SPREADSHEET_NAME).worksheet(sheet_name)
        except:
            doc = client.open(SPREADSHEET_NAME)
            sh = doc.add_worksheet(sheet_name, 1000, len(header_list))
            sh.append_row(header_list)
        
        existing = sh.get_all_values()
        if not existing:
            sh.append_row(header_list)
            
        sh.append_row(row_data)
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
def generate_official_journal_pdf(df_act, df_op, target_date, target_place):
    font_path = "NanumGothic.ttf"
    if not os.path.exists(font_path): return None

    pdf = FPDF(orientation='P', unit='mm', format='A4')
    pdf.set_margins(15, 15, 15)
    pdf.set_auto_page_break(True, margin=15)
    pdf.add_font("Nanum", "", font_path)
    pdf.add_font("Nanum", "B", font_path)

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
    
    guides = df_act.to_dict('records') if not df_act.empty else []
    for i in range(2):
        if i < len(guides):
            g = guides[i]
            g_name = str(g.get('이름', ''))
            c_in = str(g.get('출근시간', ''))
            c_out = str(g.get('퇴근시간', ''))
            
            t_display = f"{c_in} ~ {c_out}" if c_in and c_out else (c_in if c_in else "")
            h_total = ""
            if c_in and c_out:
                try:
                    tdelta = datetime.strptime(c_out, "%H:%M") - datetime.strptime(c_in, "%H:%M")
                    h_total = str(tdelta.seconds // 3600)
                except: pass
            
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
    
    if not df_op.empty:
        for _, r in df_op.iterrows():
            in_time = str(r.get('입력시간', ''))
            try: h = int(in_time.split(':')[0])
            except: h = 8
            
            if h < 8: slot_k = "08:00~09:00"
            elif h >= 17: slot_k = "17:00~18:00"
            else: slot_k = f"{h:02d}:00~{h+1:02d}:00"
            
            slot_data[slot_k]['vis'] += int(pd.to_numeric(r.get('탐방객수', 0), errors='coerce') or 0)
            slot_data[slot_k]['lis'] += int(pd.to_numeric(r.get('청취자수', 0), errors='coerce') or 0)
            slot_data[slot_k]['cnt'] += 1 
            
            if r.get('특이사항'):
                slot_data[slot_k]['note'].append(str(r.get('특이사항')))
    
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
    if not df_op.empty and '특이사항' in df_op.columns:
        all_notes = [str(x) for x in df_op['특이사항'].dropna() if str(x).strip()]
    note_base = " / ".join(all_notes)
    if len(note_base) > 65: note_base = note_base[:63] + "..."
    pdf.cell(150, 15, note_base, 1, 1, 'L')
    
    pdf.ln(10)
    pdf.set_font("Nanum", "", 12)
    pdf.cell(90, 10, "조장 확인 :                         (인/서명)", 0, 0, 'C')
    pdf.cell(90, 10, "면 담당 확인 :                         (인/서명)", 0, 1, 'C')

    return bytes(pdf.output())

def get_display_data(df_plan, df_act, date_list):
    disp_rows = []
    if df_act.empty and '날짜' not in df_act.columns: df_act['날짜'] = []
    
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
        
        day_acts = pd.DataFrame()
        if not df_act.empty: day_acts = df_act[df_act['날짜'] == pd.to_datetime(d_str)]
        used_log_indices = set()
        
        for i in range(4):
            p_key = f"plan_{i}"; r_key = f"res_{i}"; p_val = ""; r_val = ""
            if i < len(final_slots):
                slot = final_slots[i]; p_val = slot['plan_display']; target_worker = slot['worker_name']
                if not day_acts.empty:
                    for idx, log in day_acts.iterrows():
                        if idx not in used_log_indices and log['이름'] == target_worker:
                            c_in = log.get('출근시간', '')
                            c_out = log.get('퇴근시간', '')
                            if c_in and c_out: t_val = "완료"
                            elif c_in: t_val = "근무중"
                            else: t_val = "미출근"
                            
                            r_val = f"{target_worker}({t_val})" if slot['is_sub'] else f"{t_val}"
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
            my_act = df_act[(df_act['날짜'] == pd.to_datetime(today_str)) & (df_act['이름'] == name) & (df_act['장소'] == place_act)]
            
        c_in = ""; c_out = ""
        if not my_act.empty:
            c_in = str(my_act.iloc[0].get('출근시간', ''))
            c_out = str(my_act.iloc[0].get('퇴근시간', ''))
            
        st.markdown(f"**현재 상태:** 출근 `[{c_in if c_in else '미등록'}]` / 퇴근 `[{c_out if c_out else '미등록'}]`")
        
        col_btn1, col_btn2 = st.columns(2)
        with col_btn1:
            if not c_in:
                if st.button("🟢 출근하기", use_container_width=True):
                    now_time = get_kst_now().strftime("%H:%M")
                    cols = ["날짜", "섬", "장소", "이름", "출근시간", "퇴근시간", "타임스탬프", "년", "월"]
                    row = [today_str, island, place_act, name, now_time, "", str(get_kst_now()), now.year, now.month]
                    save_data("활동일지", [row], cols)
                    st.success(f"{now_time} 출근 완료!"); time.sleep(0.5); st.rerun()
            else:
                st.button("🟢 출근 완료", disabled=True, use_container_width=True)
                
        with col_btn2:
            if c_in and not c_out:
                if st.button("🔴 퇴근하기", use_container_width=True):
                    now_time = get_kst_now().strftime("%H:%M")
                    cols = ["날짜", "섬", "장소", "이름", "출근시간", "퇴근시간", "타임스탬프", "년", "월"]
                    row = [today_str, island, place_act, name, c_in, now_time, str(get_kst_now()), now.year, now.month]
                    save_data("활동일지", [row], cols)
                    st.success(f"{now_time} 퇴근 완료!"); time.sleep(0.5); st.rerun()
            elif c_out:
                st.button("🔴 퇴근 완료", disabled=True, use_container_width=True)

    with t_op:
        st.subheader("해설 실적 등록")
        st.info("💡 해설을 진행할 때마다 실적을 등록하세요. **1번 등록할 때마다 '해설 횟수'가 1씩 자동으로 카운트**되며, 시간에 맞춰 PDF에 기록됩니다.")
        
        with st.form("op_form"):
            place_op = st.selectbox("해설 장소", LOCATIONS.get(island, []), key="op_p")
            c_op1, c_op2 = st.columns(2)
            vis = c_op1.number_input("탐방객 수 (명)", min_value=0, step=1)
            lis = c_op2.number_input("해설 청취자 수 (명)", min_value=0, step=1)
            note = st.text_input("특이사항 (교육, 정비 등 내용 입력)")
            
            if st.form_submit_button("💾 실적 1건 등록", use_container_width=True):
                now_time = get_kst_now().strftime("%H:%M")
                cols = ["날짜", "섬", "장소", "이름", "입력시간", "탐방객수", "청취자수", "특이사항", "타임스탬프", "년", "월"]
                row = [today_str, island, place_op, name, now_time, vis, lis, note, str(get_kst_now()), now.year, now.month]
                
                if append_data("운영일지", row, cols):
                    st.success(f"[{now_time}] 실적이 성공적으로 누적 등록되었습니다!")
                    time.sleep(1); st.rerun()

def ui_view_journal(scope, name, island, role=""):
    st.header("🔍 활동 조회")
    
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
    
    if role == "조장" or role == "관리자":
        st.subheader("🛠️ 출퇴근 시간 관리 (활동일지 수정)")
        if df_act.empty:
            st.info("출퇴근 기록이 없습니다.")
        else:
            edit_cols = ["날짜", "이름", "장소", "출근시간", "퇴근시간"]
            for c in edit_cols:
                if c not in df_act.columns: df_act[c] = ""
            
            filter_act = df_act[df_act['장소'] == sel_place] if sel_place != "전체" else df_act
            
            if filter_act.empty: 
                st.info("해당 장소 기록 없음")
            else:
                with st.form("edit_act_form"):
                    edited_act = st.data_editor(filter_act[edit_cols], hide_index=True, use_container_width=True)
                    if st.form_submit_button("변경사항 저장"):
                        for _, r in edited_act.iterrows():
                            d_str = r['날짜'].strftime("%Y-%m-%d") if isinstance(r['날짜'], pd.Timestamp) else str(r['날짜'])
                            idx = df_act[(df_act['날짜'] == pd.to_datetime(d_str)) & (df_act['이름'] == r['이름']) & (df_act['장소'] == r['장소'])].index
                            if not idx.empty:
                                df_act.loc[idx, '출근시간'] = r['출근시간']
                                df_act.loc[idx, '퇴근시간'] = r['퇴근시간']
                        
                        cols = ["날짜", "섬", "장소", "이름", "출근시간", "퇴근시간", "타임스탬프", "년", "월"]
                        for c in cols:
                            if c not in df_act.columns: df_act[c] = ""
                        
                        sh = client.open(SPREADSHEET_NAME).worksheet("활동일지")
                        df_act['날짜'] = pd.to_datetime(df_act['날짜']).dt.strftime("%Y-%m-%d")
                        sh.clear()
                        sh.update([df_act.columns.values.tolist()] + df_act.values.tolist())
                        st.cache_data.clear()
                        st.success("출퇴근 시간이 수정되었습니다."); time.sleep(0.5); st.rerun()
                        
    st.divider()
    st.subheader("📋 운영 실적 내역")
    if df_op.empty:
        st.info("등록된 운영 실적이 없습니다.")
    else:
        filter_op = df_op[df_op['장소'] == sel_place] if sel_place != "전체" else df_op
        if scope == "me": filter_op = filter_op[filter_op['이름'] == name]
        
        if filter_op.empty: st.info("조건에 맞는 데이터가 없습니다.")
        else:
            show_cols = ["날짜", "장소", "이름", "입력시간", "탐방객수", "청취자수", "특이사항"]
            show_cols = [c for c in show_cols if c in filter_op.columns]
            st.dataframe(filter_op[show_cols], use_container_width=True, hide_index=True, column_config={
                "날짜": st.column_config.DateColumn("날짜", format="YYYY-MM-DD")
            })

    if sel_place != "전체" and not df_act.empty:
        st.divider()
        st.subheader("📥 일지 다운로드")
        avail_dates = sorted(df_act[df_act['장소'] == sel_place]['날짜'].dt.strftime('%Y-%m-%d').unique())
        if avail_dates:
            c_d1, c_d2 = st.columns([1, 2])
            with c_d1: target_d = st.selectbox("출력할 날짜 선택", avail_dates)
            with c_d2:
                st.write(""); st.write("")
                day_act = df_act[(df_act['날짜'] == pd.to_datetime(target_d)) & (df_act['장소'] == sel_place)]
                day_op = df_op[(df_op['날짜'] == pd.to_datetime(target_d)) & (df_op['장소'] == sel_place)] if not df_op.empty else pd.DataFrame()
                
                pdf_data = generate_official_journal_pdf(day_act, day_op, target_d, sel_place)
                if pdf_data:
                    st.download_button(f"📄 {target_d} 운영일지 PDF 다운로드", pdf_data, f"운영일지_{sel_place}_{target_d}.pdf", "application/pdf", use_container_width=True)


def ui_plan_input(name, island):
    st.header("✍️ 계획 입력")
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
                row = [pick_s, island, place, name, stat, "", str(get_kst_now()), py, pm, "승인대기", "", ""]
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
                    rows.append([r['날짜'], island, place, name, s, "", str(get_kst_now()), py, pm, "승인대기", "", ""])
                cols = ["날짜","섬","장소","이름","활동여부","비고","타임스탬프","년","월","상태","대타여부","기존해설사"]
                save_data("활동계획", rows, cols); st.success("승인 대기 상태로 저장되었습니다."); st.rerun()

def ui_view_plan(scope, name, island, role=""):
    st.header("🗓️ 계획 조회 및 수정")
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
                            "활동여부": t_stat, "비고": "대타요청", "타임스탬프": str(get_kst_now()),
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
    
    disp_rows = get_display_data(df, j_df, dates)
    df_disp = pd.DataFrame(disp_rows)
    cols = ["날짜", "요일", "plan_0", "plan_1", "plan_2", "plan_3", "res_0", "res_1", "res_2", "res_3"]
    for c in cols:
        if c not in df_disp.columns: df_disp[c] = ""
        
    edited = st.data_editor(df_disp[cols], hide_index=True, use_container_width=True)
    
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
            if '탐방객수' in df_op.columns:
                total_v += int(pd.to_numeric(df_op['탐방객수'], errors='coerce').fillna(0).sum())
            if '청취자수' in df_op.columns:
                total_l += int(pd.to_numeric(df_op['청취자수'], errors='coerce').fillna(0).sum())
            if '입력시간' in df_op.columns:
                total_c += len(df_op[df_op['입력시간'] != ""]) 
                
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
            df_op['탐방객수'] = pd.to_numeric(df_op.get('탐방객수', 0), errors='coerce').fillna(0)
            df_op['청취자수'] = pd.to_numeric(df_op.get('청취자수', 0), errors='coerce').fillna(0)
            st.dataframe(df_op.groupby('장소')[['탐방객수', '청취자수']].sum().reset_index(), use_container_width=True)
            
            st.subheader("👤 해설사별 실적")
            grp = df_op.groupby('이름').agg({'청취자수':'sum', '날짜':'count'}).rename(columns={'날짜':'해설횟수'}).reset_index()
            st.dataframe(grp, use_container_width=True)

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
                
        if role == "관리자":
            t1, t2, t3, t4 = st.tabs(["🔍 활동조회", "🗓️ 계획조회", "📊 통계", "✅ 계획승인"])
            with t1: ui_view_journal("all", name, island, role)
            with t2: ui_view_plan("all", name, island, role)
            with t3: ui_stats()
            with t4: ui_approve(island, role)
            
        elif role == "조장":
            t1, t2, t3, t4, t5 = st.tabs(["📝 일지작성", "🔍 활동조회", "🗓️ 계획조회", "✍️ 계획입력", "✅ 계획승인"])
            with t1: ui_journal_write(name, island)
            with t2: ui_view_journal("team", name, island, role)
            with t3: ui_view_plan("team", name, island, role)
            with t4: ui_plan_input(name, island)
            with t5: ui_approve(island, role)
            
        else: # 조원
            t1, t2, t3, t4 = st.tabs(["📝 일지작성", "📅 내 활동", "🗓️ 내 계획", "✍️ 계획입력"])
            with t1: ui_journal_write(name, island)
            with t2: ui_view_journal("me", name, island, role)
            with t3: ui_view_plan("me", name, island, role)
            with t4: ui_plan_input(name, island)

if __name__ == "__main__":
    main()
