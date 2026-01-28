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

def load_data(sheet_name, year=None, month=None, island=None):
    try:
        sh = client.open(SPREADSHEET_NAME).worksheet(sheet_name)
        data = sh.get_all_records()
        if not data: return pd.DataFrame()
        
        df = pd.DataFrame(data)
        df.columns = [str(c).strip() for c in df.columns]
        if '일자' in df.columns: df.rename(columns={'일자': '날짜'}, inplace=True)
        
        for c in ['대타여부', '기존해설사', '상태']:
            if c not in df.columns: df[c] = ""

        if '날짜' in df.columns:
            df['날짜'] = pd.to_datetime(df['날짜'], errors='coerce')
            df['_y'] = df['날짜'].dt.year
            df['_m'] = df['날짜'].dt.month
            if year: df = df[df['_y'] == int(year)]
            if month: df = df[df['_m'] == int(month)]
            df = df.drop(columns=['_y', '_m'])
        
        if island and '섬' in df.columns:
            df = df[df['섬'] == island]
            
        return df
    except: return pd.DataFrame()

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
        
        # 키 생성: 날짜+이름+장소
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
        
        combined = pd.concat([final_df, new_df], ignore_index=True)
        combined = combined.fillna("")
        
        if '날짜' in combined.columns:
            combined['날짜'] = pd.to_datetime(combined['날짜'], errors='coerce')
            combined = combined.sort_values('날짜')
            combined['날짜'] = combined['날짜'].dt.strftime("%Y-%m-%d")
            
        sh.clear()
        sh.update([combined.columns.values.tolist()] + combined.values.tolist())
        return True
    except Exception as e:
        st.error(f"저장 오류: {e}")
        return False

def get_users(island):
    try:
        sh = client.open(SPREADSHEET_NAME).worksheet("사용자")
        users = sh.get_all_records()
        return [u['이름'] for u in users if u.get('섬') == island]
    except: return []

# =========================================================
# 3. PDF 및 데이터 가공 로직 (요일 에러 수정됨)
# =========================================================
def get_display_data(df_plan, df_log, date_list):
    """
    화면/PDF 표시용 데이터 생성
    [수정] 요일 계산 시 문자열을 datetime으로 변환하는 로직 추가
    """
    disp_rows = []
    
    for d in date_list:
        # [수정] d가 문자열인지 확인하고 변환
        if isinstance(d, str):
            d_obj = datetime.strptime(d, "%Y-%m-%d")
        else:
            d_obj = d
            
        d_str = d_obj.strftime("%Y-%m-%d")
        w_day = DAY_MAP[d_obj.weekday()] # 이제 여기서 에러 안 남
        
        row_dat = {"날짜": d_str, "요일": w_day}
        
        # 1. 해당 날짜 계획 가져오기
        # df_plan['날짜']는 datetime 객체임
        day_plans = df_plan[df_plan['날짜'] == pd.to_datetime(d_str)]
        
        # 2. 대타/원본 분리
        subs = day_plans[day_plans['대타여부'] == 'O']
        origs = day_plans[day_plans['대타여부'] != 'O']
        
        # 3. 슬롯 구성
        final_slots = []
        replaced_planners = []
        
        # 대타 먼저
        if not subs.empty:
            replaced_planners = subs['기존해설사'].unique().tolist()
            for _, r in subs.iterrows():
                final_slots.append({
                    'plan_name': r['기존해설사'], 
                    'worker_name': r['이름'],
                    'is_sub': True
                })
            
        # 원본 (대체되지 않은 사람만)
        if not origs.empty:
            for _, r in origs.iterrows():
                my_name = r['이름']
                if my_name not in replaced_planners:
                    final_slots.append({
                        'plan_name': my_name,
                        'worker_name': my_name,
                        'is_sub': False
                    })
        
        # 4. 실적(로그)
        day_logs = df_log[df_log['날짜'] == pd.to_datetime(d_str)]
        used_log_indices = set()
        
        for i in range(4):
            p_key = f"plan_{i}"; r_key = f"res_{i}"
            p_val = ""; r_val = ""
            
            if i < len(final_slots):
                slot = final_slots[i]
                p_val = slot['plan_name'] # 계획란 (원래주인)
                target_worker = slot['worker_name'] # 수행해야 할 사람
                
                # 로그 매칭
                found = False
                for idx, log in day_logs.iterrows():
                    if idx not in used_log_indices and log['이름'] == target_worker:
                        t_val = str(log.get('활동시간', ''))
                        if slot['is_sub']:
                            r_val = f"{target_worker}({t_val}H)"
                        else:
                            r_val = f"{t_val}H"
                        used_log_indices.add(idx)
                        found = True
                        break
            
            row_dat[p_key] = p_val
            row_dat[r_key] = r_val
            
        disp_rows.append(row_dat)
        
    return disp_rows

def generate_pdf(target_place, special_note, p_year, p_month, p_range, disp_rows, current_island):
    font_path = "NanumGothic.ttf"
    if not os.path.exists(font_path): st.error("폰트 없음"); return None

    pdf = FPDF(orientation='P', unit='mm', format='A4')
    pdf.set_margins(15, 15, 15); pdf.set_auto_page_break(True, margin=10)
    pdf.add_page()
    pdf.add_font("Nanum", "", font_path); pdf.add_font("Nanum", "B", font_path)

    pdf.set_font("Nanum", "B", 22); pdf.set_line_width(0.4)
    pdf.cell(180, 15, "지질공원 안내소 운영계획서", 1, 1, 'C'); pdf.ln(3)

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
            pdf.set_xy(bx+(i*w_c), yc); pdf.cell(w_c, row_h, row.get(f"plan_{i}", ""), 1, 0, 'C')
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
    now = datetime.now()
    c1, c2, c3 = st.columns([1,1,2])
    with c1: jy = st.number_input("년", value=now.year, key="jw_y")
    with c2: jm = st.number_input("월", value=now.month, key="jw_m")
    with c3: place = st.selectbox("장소", LOCATIONS.get(island, []), key="jw_p")
    
    st.divider()
    mode = st.radio("입력 모드", ["📅 하루씩 입력 (모바일)", "🗓️ 월간 전체 입력 (PC)"], horizontal=True, key="jw_mode")
    
    _, last = calendar.monthrange(jy, jm)
    dates = [datetime(jy, jm, d).strftime("%Y-%m-%d") for d in range(1, last+1)]
    
    df = load_data("운영일지", jy, jm, island)
    if not df.empty: df = df[(df['이름']==name) & (df['장소']==place)]
    
    if "하루씩" in mode:
        c_d1, c_d2 = st.columns([1, 1.5])
        with c_d1:
            def_d = now.date()
            if def_d.month != jm: def_d = datetime(jy, jm, 1).date()
            pick = st.date_input("날짜", value=def_d, min_value=datetime(jy, jm, 1), max_value=datetime(jy, jm, last), key="jw_pk")
            pick_s = pick.strftime("%Y-%m-%d")
        
        pt="활동 없음"; p_acts=[]; pv=0; pl=0; pc=0; pspec=""
        if not df.empty:
            r = df[df['날짜']==pd.to_datetime(pick_s)]
            if not r.empty:
                r = r.iloc[0]
                tv = str(r['활동시간'])
                if tv=="8": pt="종일 (8시간)"
                elif tv=="4": pt="반일 (4시간)"
                raw_act = str(r.get('활동내용', ''))
                p_acts = [x.strip() for x in raw_act.split(',')] if raw_act else []
                pv = int(r.get('탐방객수') or 0)
                pl = int(r.get('청취자수', 0) or 0)
                pc = int(r.get('해설횟수', 0) or 0)
                pspec = str(r.get('특이사항', ''))
                
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
            iv = c_n1.number_input("탐방객(명)", value=pv, min_value=0)
            il = c_n2.number_input("청취자(명)", value=pl, min_value=0)
            ic = c_n3.number_input("해설횟수(회)", value=pc, min_value=0)
            
            st.markdown("**4. 특이사항**")
            ispec = st.text_area("내용 입력", value=pspec, height=80)
            
            if st.form_submit_button("💾 저장", use_container_width=True):
                ft = 8 if "8시간" in st_sel else (4 if "4시간" in st_sel else "")
                act_str = ",".join(sel_acts)
                row = [pick_s, island, place, name, ft, act_str, iv, il, ic, ispec, str(datetime.now()), jy, jm, "검토대기", "", ""]
                cols = ["날짜","섬","장소","이름","활동시간","활동내용","탐방객수","청취자수","해설횟수","특이사항","타임스탬프","년","월","상태","대타여부","기존해설사"]
                if save_data("운영일지", [row], cols):
                    st.success("저장 완료!"); time.sleep(0.5); st.rerun()
    else:
        st.info("PC 모드 간편 입력")
        grid = []
        d_map = {}
        if not df.empty:
            for _, r in df.iterrows(): d_map[r['날짜'].strftime("%Y-%m-%d")] = r
        for d in dates:
            cur = d_map.get(d, {})
            tv = str(cur.get('활동시간',''))
            grid.append({
                "날짜": d, "요일": DAY_MAP[datetime.strptime(d, "%Y-%m-%d").weekday()],
                "종일": tv=="8", "반일": tv=="4",
                "탐방객": cur.get('탐방객수',0), "특이사항": cur.get('특이사항','')
            })
        with st.form("jw_m_form"):
            edited = st.data_editor(pd.DataFrame(grid), hide_index=True, use_container_width=True)
            if st.form_submit_button("💾 저장"):
                rows = []
                for _, r in edited.iterrows():
                    ft = 8 if r['종일'] else (4 if r['반일'] else "")
                    rows.append([r['날짜'], island, place, name, ft, "", r['탐방객'], 0, 0, r['특이사항'], str(datetime.now()), jy, jm, "검토대기", "", ""])
                cols = ["날짜","섬","장소","이름","활동시간","활동내용","탐방객수","청취자수","해설횟수","특이사항","타임스탬프","년","월","상태","대타여부","기존해설사"]
                save_data("운영일지", rows, cols); st.success("완료"); st.rerun()

def ui_view_journal(scope, name, island):
    st.header("🔍 활동 조회")
    c1, c2 = st.columns(2)
    with c1: vy = st.number_input("연도", value=datetime.now().year, key="vj_y")
    with c2: vm = st.number_input("월", value=datetime.now().month, key="vj_m")
    
    t_isl = island if scope != "all" else None
    df = load_data("운영일지", vy, vm, t_isl)
    
    if df.empty: st.info("데이터가 없습니다."); return
    if scope == "me": df = df[df['이름'] == name]
    
    st.dataframe(df, use_container_width=True, hide_index=True, column_config={
        "타임스탬프": None, "년": None, "월": None, "키": None, "key": None,
        "날짜": st.column_config.DateColumn("날짜", format="YYYY-MM-DD")
    })

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
            if st.form_submit_button("💾 저장", use_container_width=True):
                stat = ""
                if "종일" in sel: stat="종일"
                elif "오전" in sel: stat="오전(4시간)"
                elif "오후" in sel: stat="오후(4시간)"
                elif "기타" in sel: stat=ein if ein else "미정"
                row = [pick_s, island, place, name, stat, "", str(datetime.now()), py, pm, "", "", ""]
                cols = ["날짜","섬","장소","이름","활동여부","비고","타임스탬프","년","월","상태","대타여부","기존해설사"]
                save_data("활동계획", [row], cols); st.success("완료"); st.rerun()
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
            if st.form_submit_button("💾 저장"):
                rows = []
                for _, r in edited.iterrows():
                    s = ""
                    if r['종일']: s="종일"
                    elif r['오전']: s="오전(4시간)"
                    elif r['오후']: s="오후(4시간)"
                    elif r['기타']: s=str(r['기타'])
                    rows.append([r['날짜'], island, place, name, s, "", str(datetime.now()), py, pm, "", "", ""])
                cols = ["날짜","섬","장소","이름","활동여부","비고","타임스탬프","년","월","상태","대타여부","기존해설사"]
                save_data("활동계획", rows, cols); st.success("완료"); st.rerun()

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
    df_log = load_data("운영일지", py, pm, t_isl)
    
    if df_plan.empty: st.info("데이터 없음"); return
    
    if sel_place:
        df_plan = df_plan[df_plan['장소'] == sel_place]
        if not df_log.empty and '장소' in df_log.columns:
            df_log = df_log[df_log['장소'] == sel_place]
    
    if scope == "me": df_plan = df_plan[df_plan['이름'] == name]
    if df_plan.empty: st.info("조건에 맞는 데이터 없음"); return

    try: dates = sorted(df_plan['날짜'].unique())
    except: dates = []
    
    disp_rows = get_display_data(df_plan, df_log, dates)
    
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
    
    if scope in ["team", "all"] and disp_rows:
        st.divider()
        st.subheader("🛠️ 계획 수정")
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
            
            if st.button("적용"):
                try:
                    tr = day_p[day_p['이름']==target_u].iloc[0]
                    t_place = tr['장소']; t_stat = tr['활동여부']
                    origin = tr.get('기존해설사', '')
                    if not origin: origin = target_u 
                    
                    if "대타" in act and new_u:
                        row = {
                            "날짜": target_d, "섬": t_isl, "장소": t_place, "이름": new_u,
                            "활동여부": t_stat, "비고": "대타변경", "타임스탬프": str(datetime.now()),
                            "년": py, "월": pm, "상태": "", "대타여부": "O", "기존해설사": origin
                        }
                        cols = ["날짜","섬","장소","이름","활동여부","비고","타임스탬프","년","월","상태","대타여부","기존해설사"]
                        save_data("활동계획", [list(row.values())], cols)
                        st.success("완료! (대타 기록 추가됨)"); time.sleep(1); st.rerun()
                        
                    elif "취소" in act:
                        sh = client.open(SPREADSHEET_NAME).worksheet("활동계획")
                        ald = pd.DataFrame(sh.get_all_records())
                        ald.columns = [str(c).strip() for c in ald.columns]
                        if '일자' in ald.columns: ald.rename(columns={'일자': '날짜'}, inplace=True)
                        ald['d_str'] = pd.to_datetime(ald['날짜'], errors='coerce').dt.strftime("%Y-%m-%d")
                        mask = (ald['d_str']==target_d) & (ald['이름']==target_u) & (ald['장소']==t_place)
                        rem = ald[~mask].drop(columns=['d_str'])
                        sh.clear(); sh.update([rem.columns.values.tolist()] + rem.values.tolist())
                        st.success("삭제 완료"); time.sleep(1); st.rerun()
                        
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
    dates = [datetime(py, pm, d).strftime("%Y-%m-%d") for d in (range(1, 16) if "전반기" in pr else range(16, last+1))]
    dates_str = [d.strftime("%Y-%m-%d") for d in dates] # 승인 저장용 문자열 리스트
    
    df = load_data("활동계획", py, pm, tis)
    if not df.empty: df = df[df['장소'] == tpl]
    j_df = load_data("운영일지", py, pm, tis)
    
    disp_rows = get_display_data(df, j_df, dates)
    df_disp = pd.DataFrame(disp_rows)
    cols = ["날짜", "요일", "plan_0", "plan_1", "plan_2", "plan_3", "res_0", "res_1", "res_2", "res_3"]
    for c in cols:
        if c not in df_disp.columns: df_disp[c] = ""
        
    edited = st.data_editor(df_disp[cols], hide_index=True, use_container_width=True)
    
    c_btn1, c_btn2 = st.columns(2)
    with c_btn1:
        if st.button("💾 승인 저장"):
            # [수정] DB 원본을 불러와서 상태값 업데이트 후 재저장
            try:
                # 1. 원본 로드
                raw_df = load_data("활동계획", py, pm, tis)
                if raw_df.empty:
                    st.warning("저장할 데이터가 없습니다.")
                else:
                    # 2. 조건에 맞는 행만 '승인완료'로 변경
                    # 조건: 장소가 일치하고, 날짜가 선택된 기간(dates_str)에 포함되는 행
                    # 날짜 형변환 (비교를 위해)
                    raw_df['d_temp'] = raw_df['날짜'].dt.strftime("%Y-%m-%d")
                    
                    # 마스크 생성
                    mask = (raw_df['장소'] == tpl) & (raw_df['d_temp'].isin(dates_str))
                    
                    # 업데이트
                    raw_df.loc[mask, '상태'] = "승인완료"
                    
                    # 3. 저장 형식으로 변환 (리스트)
                    save_rows = []
                    # 저장 함수가 기대하는 컬럼 순서
                    # ["날짜","섬","장소","이름","활동여부","비고","타임스탬프","년","월","상태","대타여부","기존해설사"]
                    
                    for _, r in raw_df.iterrows():
                        # 해당되는 행은 업데이트된 상태로, 나머지는 그대로 저장
                        # load_data가 날짜를 datetime으로 바꿨으니 다시 문자열로
                        d_s = r['날짜'].strftime("%Y-%m-%d")
                        row = [
                            d_s, r['섬'], r['장소'], r['이름'], r['활동여부'], r['비고'], 
                            str(r['타임스탬프']), r['년'], r['월'], r['상태'], 
                            r['대타여부'], r['기존해설사']
                        ]
                        save_rows.append(row)
                    
                    # 4. 전체 덮어쓰기 (save_data 대신 시트 클리어 후 업데이트)
                    # save_data는 append/update 방식이라 여기선 부적합할 수 있음(전체 상태 변경이므로)
                    # 하지만 save_data가 key 기반이므로, key가 같으면 덮어씀.
                    # 여기선 전체 데이터를 다시 밀어넣는게 안전.
                    
                    sh = client.open(SPREADSHEET_NAME).worksheet("활동계획")
                    cols = ["날짜","섬","장소","이름","활동여부","비고","타임스탬프","년","월","상태","대타여부","기존해설사"]
                    sh.clear()
                    sh.update([cols] + save_rows)
                    
                    st.success("✅ 승인 상태가 저장되었습니다!")
            except Exception as e:
                st.error(f"저장 중 오류 발생: {e}")
            
    with c_btn2:
        pdf_data = generate_pdf(tpl, note, py, pm, pr, disp_rows, tis)
        if pdf_data:
            st.download_button("📥 PDF 다운로드", pdf_data, f"운영계획서_{tpl}_{pm}월.pdf", "application/pdf")

def ui_stats():
    st.header("📊 통계")
    st.info("준비 중입니다.")

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
