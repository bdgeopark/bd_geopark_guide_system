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
# 1. 초기 설정 및 상수 정의
# =========================================================
st.set_page_config(page_title="지질공원 통합관리", page_icon="🪨", layout="wide")

# 스타일 적용 (모바일 가독성 향상)
st.markdown("""
    <style>
    html, body, [class*="css"] { font-size: 18px !important; }
    div[data-testid="stDataEditor"] table { font-size: 16px !important; }
    div[data-testid="stSelectbox"] * { font-size: 18px !important; }
    div[data-testid="stForm"] { border: 2px solid #f0f2f6; padding: 20px; border-radius: 10px; }
    </style>
    """, unsafe_allow_html=True)

# 전역 상수
SPREADSHEET_NAME = "지질공원_운영일지_DB"
LOCATIONS = {
    "백령도": ["두무진 안내소", "콩돌해안 안내소", "사곶해변 안내소", "용기포신항 안내소", "진촌리 현무암 안내소", "용틀임바위 안내소", "임시지질공원센터"],
    "대청도": ["서풍받이 안내소", "옥죽동 해안사구 안내소", "농여해변 안내소", "선진동 선착장 안내소"],
    "소청도": ["분바위 안내소", "탑동 선착장 안내소"],
    "시청": ["인천시청", "지질공원팀 사무실"]
}
DAY_MAP = {0: "월", 1: "화", 2: "수", 3: "목", 4: "금", 5: "토", 6: "일"}

# 세션 상태 초기화
if 'logged_in' not in st.session_state: st.session_state['logged_in'] = False
if 'user_info' not in st.session_state: st.session_state['user_info'] = {}

# =========================================================
# 2. 데이터베이스(Google Sheets) 연결 및 데이터 처리 함수
# =========================================================
@st.cache_resource
def get_client():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    try:
        # 1. 로컬 파일 시도
        if os.path.exists("geopark_key.json"):
            creds = ServiceAccountCredentials.from_json_keyfile_name("geopark_key.json", scope)
        else:
            # 2. Streamlit Cloud Secrets 시도
            key_dict = st.secrets["gcp_service_account"]
            creds = ServiceAccountCredentials.from_json_keyfile_dict(key_dict, scope)
        return gspread.authorize(creds)
    except Exception as e:
        return None

client = get_client()

def load_data(sheet_name, year=None, month=None, island=None):
    """데이터 불러오기 (필터링 옵션 포함)"""
    try:
        sh = client.open(SPREADSHEET_NAME).worksheet(sheet_name)
        data = sh.get_all_records()
        df = pd.DataFrame(data)
        if df.empty: return pd.DataFrame()
        
        # 날짜 타입 변환 시도
        if '날짜' in df.columns:
            df['날짜'] = pd.to_datetime(df['날짜'], errors='coerce')
            
        # 필터링
        if year and '년' in df.columns: df = df[df['년'] == year]
        if month and '월' in df.columns: df = df[df['월'] == month]
        if island and '섬' in df.columns: df = df[df['섬'] == island]
        
        return df
    except:
        return pd.DataFrame()

def save_data_append(sheet_name, new_row_list, header_list):
    """데이터 저장 (기존 데이터 유지, 키 기반 중복 제거 후 저장)"""
    try:
        try: sh = client.open(SPREADSHEET_NAME).worksheet(sheet_name)
        except: 
            doc = client.open(SPREADSHEET_NAME)
            sh = doc.add_worksheet(sheet_name, 1000, len(header_list))
            sh.append_row(header_list)
            
        existing = sh.get_all_records()
        old_df = pd.DataFrame(existing) if existing else pd.DataFrame(columns=header_list)
        new_df = pd.DataFrame(new_row_list, columns=header_list)
        
        # 키 생성 (날짜+이름+장소)로 중복 방지
        # 활동계획과 운영일지의 키 조합이 다를 수 있으나, 공통적으로 날짜/이름/장소는 필수
        old_df['key'] = old_df['날짜'].astype(str) + "_" + old_df['이름'] + "_" + old_df.get('장소', '공통')
        new_df['key'] = new_df['날짜'].astype(str) + "_" + new_df['이름'] + "_" + new_df.get('장소', '공통')
        
        keys_to_remove = new_df['key'].tolist()
        final_df = old_df[~old_df['key'].isin(keys_to_remove)].copy()
        
        final_df = final_df.drop(columns=['key'])
        new_df = new_df.drop(columns=['key'])
        
        combined = pd.concat([final_df, new_df], ignore_index=True)
        sh.clear()
        sh.update([combined.columns.values.tolist()] + combined.values.tolist())
        return True
    except Exception as e:
        st.error(f"저장 중 오류 발생: {e}")
        return False

def get_users_cached(island_name):
    """해당 섬의 사용자 목록 가져오기"""
    try:
        sh = client.open(SPREADSHEET_NAME).worksheet("사용자")
        users = sh.get_all_records()
        return [u['이름'] for u in users if u.get('섬') == island_name]
    except: return []

# =========================================================
# 3. PDF 생성 엔진 (정밀 서식 적용)
# =========================================================
def generate_roster_pdf(target_place, special_note, p_year, p_month, p_range, matrix_df, display_users, current_island):
    font_path = "NanumGothic.ttf"
    if not os.path.exists(font_path):
        st.error("❌ 폰트 파일(NanumGothic.ttf)이 서버에 없습니다.")
        return None

    # 운영일지(결과) 데이터 로드
    journal_df = load_data("운영일지", p_year, p_month, current_island)
    if not journal_df.empty:
        journal_df = journal_df[journal_df['장소'] == target_place]

    # PDF 초기화
    pdf = FPDF(orientation='P', unit='mm', format='A4')
    pdf.set_margins(15, 15, 15)
    pdf.set_auto_page_break(True, margin=10)
    pdf.add_page()

    # 폰트 등록 (Regular, Bold)
    pdf.add_font("Nanum", "", font_path)
    pdf.add_font("Nanum", "B", font_path)

    # --- [1] 제목 ---
    pdf.set_font("Nanum", "B", 22)
    pdf.set_line_width(0.4)
    pdf.cell(180, 15, "지질공원 안내소 운영계획서", border=1, align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(3)

    # --- [2] 상단 정보 ---
    start_y = pdf.get_y(); start_x = pdf.get_x()
    pdf.set_line_width(0.12); lh = 7; pdf.set_fill_color(245, 245, 245)

    def print_row(label, value, new_line=False):
        pdf.set_font("Nanum", "B", 10)
        pdf.cell(30, lh, label, 1, 0, 'C', True)
        pdf.set_font("Nanum", "", 10)
        pdf.cell(60, lh, str(value), 1, 0, 'L')
        if new_line: pdf.ln()

    print_row("안내소", target_place)
    print_row("특이사항", special_note, True)
    print_row("활동월", f"{p_year}년 {p_month}월")
    print_row("활동기간", str(p_range), True)

    # 외곽 굵은 테두리
    pdf.set_line_width(0.4); pdf.set_fill_color(0,0,0,0)
    pdf.rect(start_x, start_y, 180, pdf.get_y()-start_y, style="D")
    pdf.set_y(pdf.get_y() + 5) # 간격 5mm

    # --- [3] 테이블 레이아웃 ---
    w_d = 12; w_w = 12; w_rem = 180 - (w_d + w_w)
    w_half = w_rem / 2
    w_cell = w_half / 4 # 4칸 고정

    def draw_header():
        y_s = pdf.get_y(); x_s = pdf.get_x()
        pdf.set_line_width(0.12); pdf.set_font("Nanum", "B", 10); pdf.set_fill_color(235, 235, 235)
        
        # 1행
        pdf.cell(w_d, 14, "일", 1, 0, 'C', True)
        pdf.cell(w_w, 14, "요일", 1, 0, 'C', True)
        pdf.set_xy(x_s+w_d+w_w, y_s)
        pdf.cell(w_half, 7, "활동 계획", 1, 0, 'C', True)
        pdf.cell(w_half, 7, "활동 결과", 1, 1, 'C', True)
        
        # 2행 (이름)
        y_2 = y_s + 7; base_x = x_s + w_d + w_w
        pdf.set_font("Nanum", "B", 8)
        
        # 계획 이름칸 (4칸)
        for i in range(4):
            u = display_users[i] if i < len(display_users) else ""
            pdf.set_xy(base_x + (i*w_cell), y_2)
            pdf.cell(w_cell, 7, u, 1, 0, 'C', True)
        
        # 결과 이름칸 (4칸)
        base_x += w_half
        for i in range(4):
            u = display_users[i] if i < len(display_users) else ""
            pdf.set_xy(base_x + (i*w_cell), y_2)
            pdf.cell(w_cell, 7, u, 1, 0, 'C', True)
        
        # 헤더 외곽 굵게
        pdf.set_xy(x_s, y_s+14)
        pdf.set_line_width(0.4)
        pdf.rect(x_s, y_s, 180, 14, style="D")
        pdf.set_line_width(0.12)

    draw_header()

    # --- [4] 본문 데이터 ---
    row_h = 8
    body_start_y = pdf.get_y()

    for _, row in matrix_df.iterrows():
        # 페이지 넘김
        if pdf.get_y() > 275:
            # 이전 페이지 테두리 마감
            pdf.set_line_width(0.4)
            pdf.rect(15, body_start_y, 180, pdf.get_y()-body_start_y, style="D")
            pdf.set_line_width(0.12)
            
            pdf.add_page()
            draw_header()
            body_start_y = pdf.get_y()

        y_c = pdf.get_y(); x_c = pdf.get_x()
        d_str = row['raw_date']
        d_obj = datetime.strptime(d_str, "%Y-%m-%d")

        # 날짜/요일 (Bold)
        pdf.set_font("Nanum", "B", 9)
        pdf.cell(w_d, row_h, str(d_obj.day), 1, 0, 'C')
        pdf.cell(w_w, row_h, DAY_MAP[d_obj.weekday()], 1, 0, 'C')

        pdf.set_font("Nanum", "", 8)

        # -- 데이터 준비 --
        plan_txts = [""] * 4
        res_txts = [""] * 4

        # (A) 계획 데이터
        for i in range(4):
            if i < len(display_users):
                u = display_users[i]
                val = row.get(u, "")
                if val:
                    val = val.replace("오전(4시간)", "오전").replace("오후(4시간)", "오후").replace("4시간", "4H").replace("8시간", "8H")
                    plan_txts[i] = val if "기타" not in val else "기타"

        # (B) 결과 데이터 (일지 매칭)
        j_entries = []
        if not journal_df.empty:
            j_rows = journal_df[journal_df['날짜'] == d_obj] # 날짜 비교
            for _, jr in j_rows.iterrows():
                j_entries.append({"n": jr['이름'], "t": str(jr['활동시간'])+"H"})
        
        matched_indices = []
        # 1. 본인 확인
        for i in range(4):
            if i < len(display_users):
                owner = display_users[i]
                for k, ent in enumerate(j_entries):
                    if ent['n'] == owner:
                        res_txts[i] = ent['t'] # 시간만 표시
                        matched_indices.append(k)
                        break
        
        # 2. 대타 확인
        unmatched = [e for k, e in enumerate(j_entries) if k not in matched_indices]
        empty_slots = [i for i in range(4) if res_txts[i] == ""]
        
        for k in range(min(len(unmatched), len(empty_slots))):
            slot = empty_slots[k]
            res_txts[slot] = f"{unmatched[k]['n']}\n({unmatched[k]['t']})"

        # -- 출력 (계획) --
        base_x = x_c + w_d + w_w
        for i in range(4):
            pdf.set_xy(base_x + (i*w_cell), y_c)
            pdf.cell(w_cell, row_h, plan_txts[i], 1, 0, 'C')

        # -- 출력 (결과) --
        base_x += w_half
        for i in range(4):
            c_x = base_x + (i*w_cell)
            txt = res_txts[i]
            pdf.set_xy(c_x, y_c)
            
            if "\n" in txt: # 줄바꿈 있으면
                pdf.set_font("Nanum", "", 7)
                pdf.set_xy(c_x, y_c+1)
                pdf.multi_cell(w_cell, 3, txt, 0, 'C')
                # 테두리 다시 그림
                pdf.set_xy(c_x, y_c)
                pdf.rect(c_x, y_c, w_cell, row_h)
                pdf.set_font("Nanum", "", 8)
            else:
                pdf.cell(w_cell, row_h, txt, 1, 0, 'C')

        pdf.set_xy(x_c, y_c + row_h)

    # 마지막 페이지 테두리 마감
    final_y = pdf.get_y()
    pdf.set_line_width(0.4)
    pdf.rect(15, body_start_y, 180, final_y - body_start_y, style="D")
    pdf.set_line_width(0.12)

    # 서명란
    pdf.ln(5)
    pdf.set_font("Nanum", "", 12)
    pdf.cell(90, 10, "조장 :                         (인/서명)", 0, 0, 'C')
    pdf.cell(90, 10, "면 담당 :                         (인/서명)", 0, 1, 'C')

    return bytes(pdf.output())

# =========================================================
# 4. UI 컴포넌트 함수 (탭별 기능)
# =========================================================

def ui_write_journal(user_name, user_island):
    """운영일지 작성 탭"""
    st.header("📝 운영일지 작성")
    
    # 날짜/장소 선택
    now = datetime.now()
    c1, c2, c3 = st.columns([1, 1, 2])
    with c1: jy = st.number_input("연도", value=now.year)
    with c2: jm = st.number_input("월", value=now.month)
    with c3: 
        places = LOCATIONS.get(user_island, [])
        sel_place = st.selectbox("근무 장소", places)

    # 날짜 계산
    _, last_day = calendar.monthrange(jy, jm)
    date_strs = [datetime(jy, jm, d).strftime("%Y-%m-%d") for d in range(1, last_day+1)]

    # DB 로드
    df = load_data("운영일지", jy, jm, user_island)
    if not df.empty:
        df = df[(df['이름'] == user_name) & (df['장소'] == sel_place)]

    # 모드 선택
    st.divider()
    mode = st.radio("입력 모드", ["📅 하루씩 입력 (모바일)", "🗓️ 월간 전체 입력 (PC)"], horizontal=True)

    if "하루씩" in mode:
        c_d1, c_d2 = st.columns([1, 1.5])
        with c_d1:
            def_d = now.date()
            if def_d.month != jm: def_d = datetime(jy, jm, 1).date()
            try:
                pick_d = st.date_input("날짜", value=def_d, min_value=datetime(jy, jm, 1), max_value=datetime(jy, jm, last_day))
            except: pick_d = def_d # 범위 오류 방지
            pick_d_str = pick_d.strftime("%Y-%m-%d")
        
        # 기존값 찾기
        prev_t="활동 없음"; prev_c=""; prev_v=0; prev_n=""
        if not df.empty:
            row = df[df['날짜'] == pd.to_datetime(pick_d_str)]
            if not row.empty:
                r = row.iloc[0]
                tv = str(r['활동시간'])
                if tv=="8": prev_t="종일 (8시간)"
                elif tv=="4": prev_t="반일 (4시간)"
                prev_c = str(r['활동내용'])
                prev_v = int(r['탐방객수'] or 0)
                prev_n = str(r['비고'])

        with c_d2: st.markdown(f"**{pick_d.day}일 ({DAY_MAP[pick_d.weekday()]})**")

        with st.form("daily_j"):
            st.markdown("**1. 활동 시간**")
            sel_t = st.radio("시간", ["활동 없음", "종일 (8시간)", "반일 (4시간)"], index=["활동 없음", "종일 (8시간)", "반일 (4시간)"].index(prev_t), horizontal=True)
            st.markdown("**2. 활동 내용**")
            in_c = st.text_area("내용", value=prev_c, height=100)
            c_f1, c_f2 = st.columns(2)
            with c_f1: in_v = st.number_input("탐방객(명)", value=prev_v, min_value=0)
            with c_f2: in_n = st.text_input("비고", value=prev_n)
            
            if st.form_submit_button("💾 저장", use_container_width=True):
                ft = 8 if "8시간" in sel_t else (4 if "4시간" in sel_t else "")
                new_row = [jy, jm, pick_d_str, user_island, sel_place, user_name, ft, in_c, in_v, in_n, str(datetime.now())]
                cols = ["년","월","날짜","섬","장소","이름","활동시간","활동내용","탐방객수","비고","타임스탬프"]
                if save_data_append("운영일지", [new_row], cols):
                    st.success("저장 완료!")
                    time.sleep(0.5); st.rerun()

    else: # PC 모드
        grid = []
        data_map = {}
        if not df.empty:
            for _, r in df.iterrows(): data_map[r['날짜'].strftime("%Y-%m-%d")] = r
        
        for d in date_strs:
            curr = data_map.get(d, {})
            tv = str(curr.get('활동시간',''))
            grid.append({
                "날짜": d, "요일": DAY_MAP[datetime.strptime(d, "%Y-%m-%d").weekday()],
                "종일": tv=="8", "반일": tv=="4",
                "활동내용": curr.get('활동내용',''), "탐방객": curr.get('탐방객수',0), "비고": curr.get('비고','')
            })
        
        with st.form("monthly_j"):
            edited = st.data_editor(pd.DataFrame(grid), hide_index=True, use_container_width=True, height=600)
            if st.form_submit_button("💾 일괄 저장"):
                rows = []
                for _, r in edited.iterrows():
                    ft = 8 if r['종일'] else (4 if r['반일'] else "")
                    rows.append([jy, jm, r['날짜'], user_island, sel_place, user_name, ft, r['활동내용'], r['탐방객'], r['비고'], str(datetime.now())])
                cols = ["년","월","날짜","섬","장소","이름","활동시간","활동내용","탐방객수","비고","타임스탬프"]
                if save_data_append("운영일지", rows, cols):
                    st.success("일괄 저장 완료!")
                    time.sleep(1); st.rerun()

def ui_view_activity(scope, user_name, user_island):
    """활동 조회 탭"""
    st.header("🔍 활동 내역 조회")
    
    # 필터
    c1, c2 = st.columns(2)
    with c1: vy = st.number_input("조회 연도", value=datetime.now().year)
    with c2: vm = st.number_input("조회 월", value=datetime.now().month)
    
    df = load_data("운영일지", vy, vm, user_island if scope != "all" else None)
    
    if df.empty:
        st.info("데이터가 없습니다.")
        return

    # 권한별 필터링
    if scope == "me":
        df = df[df['이름'] == user_name]
    elif scope == "team":
        # 이미 user_island로 로드됨
        pass
    elif scope == "all":
        # 관리자는 전체 보기 (섬 선택 옵션 추가 가능)
        pass

    st.dataframe(df, use_container_width=True)

def ui_input_plan(user_name, user_island):
    """활동 계획 입력 탭"""
    st.header("✍️ 다음달 계획 입력")
    
    # 다음달 계산
    today = datetime.now()
    next_m = today.replace(day=28) + pd.Timedelta(days=4)
    
    c1, c2, c3 = st.columns([1, 1, 2])
    with c1: py = st.number_input("계획 연도", value=next_m.year)
    with c2: pm = st.number_input("계획 월", value=next_m.month)
    with c3: pr = st.radio("기간", ["전반기(1~15일)", "후반기(16~말일)"], horizontal=True)
    
    places = LOCATIONS.get(user_island, [])
    sel_place = st.selectbox("계획 장소", places)
    
    # 날짜 범위
    _, last = calendar.monthrange(py, pm)
    dates = [datetime(py, pm, d).strftime("%Y-%m-%d") for d in (range(1, 16) if "전반기" in pr else range(16, last+1))]
    
    # DB 로드
    df = load_data("활동계획", py, pm, user_island)
    if not df.empty:
        df = df[(df['이름']==user_name) & (df['장소']==sel_place)]
    
    st.divider()
    mode = st.radio("입력 방식", ["📅 하루씩 입력 (모바일)", "🗓️ 전체 입력 (PC)"], horizontal=True)
    
    if "하루씩" in mode:
        c_d1, c_d2 = st.columns([1, 1.5])
        with c_d1:
            try: pick_d = st.date_input("날짜", value=datetime.strptime(dates[0], "%Y-%m-%d"), min_value=datetime.strptime(dates[0], "%Y-%m-%d"), max_value=datetime.strptime(dates[-1], "%Y-%m-%d"))
            except: pick_d = datetime.strptime(dates[0], "%Y-%m-%d")
            pick_d_str = pick_d.strftime("%Y-%m-%d")
        
        prev_s = "활동 없음"; etc_v = ""
        if not df.empty:
            row = df[df['날짜'] == pd.to_datetime(pick_d_str)]
            if not row.empty:
                val = row.iloc[0]['활동여부']
                if val=="종일": prev_s="종일 (8시간)"
                elif "오전" in val: prev_s="오전 (4시간)"
                elif "오후" in val: prev_s="오후 (4시간)"
                elif val: prev_s="기타"; etc_v=val

        with c_d2: st.markdown(f"**{pick_d.day}일 ({DAY_MAP[pick_d.weekday()]})**")
        
        with st.form("daily_p"):
            sel = st.radio("계획", ["활동 없음", "종일 (8시간)", "오전 (4시간)", "오후 (4시간)", "기타"], index=["활동 없음", "종일 (8시간)", "오전 (4시간)", "오후 (4시간)", "기타"].index(prev_s))
            etc_in = st.text_input("시간 직접 입력 (기타 선택 시)", value=etc_v)
            
            if st.form_submit_button("💾 저장", use_container_width=True):
                stat = ""
                if "종일" in sel: stat="종일"
                elif "오전" in sel: stat="오전(4시간)"
                elif "오후" in sel: stat="오후(4시간)"
                elif "기타" in sel: stat=etc_in if etc_in else "미정"
                
                row = [py, pm, pick_d_str, user_island, sel_place, user_name, stat, "", "", str(datetime.now())]
                cols = ["년", "월", "일자", "섬", "장소", "이름", "활동여부", "비고", "상태", "타임스탬프"]
                if save_data_append("활동계획", [row], cols):
                    st.success("저장 완료!")
                    time.sleep(0.5); st.rerun()
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
            
        with st.form("monthly_p"):
            edited = st.data_editor(pd.DataFrame(grid), hide_index=True, use_container_width=True, height=600)
            if st.form_submit_button("💾 일괄 저장"):
                rows = []
                for _, r in edited.iterrows():
                    s = ""
                    if r['종일']: s="종일"
                    elif r['오전']: s="오전(4시간)"
                    elif r['오후']: s="오후(4시간)"
                    elif r['기타']: s=str(r['기타'])
                    rows.append([py, pm, r['날짜'], user_island, sel_place, user_name, s, "", "", str(datetime.now())])
                cols = ["년", "월", "일자", "섬", "장소", "이름", "활동여부", "비고", "상태", "타임스탬프"]
                if save_data_append("활동계획", rows, cols):
                    st.success("저장 완료!"); time.sleep(1); st.rerun()

def ui_view_plan(scope, user_name, user_island):
    """활동 계획 조회 탭"""
    st.header("🗓️ 활동 계획 조회")
    c1, c2 = st.columns(2)
    now = datetime.now()
    with c1: py = st.number_input("연도", value=now.year, key="vp_y")
    with c2: pm = st.number_input("월", value=now.month, key="vp_m")
    
    df = load_data("활동계획", py, pm, user_island if scope != "all" else None)
    
    if df.empty: st.info("계획이 없습니다."); return
    if scope == "me": df = df[df['이름'] == user_name]
    
    # 피벗 테이블
    if not df.empty:
        pivot = df.pivot_table(index="일자", columns="이름", values="활동여부", aggfunc="first").fillna("")
        st.dataframe(pivot, use_container_width=True)

def ui_approve_plan(user_island, user_role):
    """계획 승인 및 PDF 출력 탭"""
    st.header("✅ 계획 승인 및 PDF 출력")
    
    # 다음달 기준
    today = datetime.now()
    nm = today.replace(day=28) + pd.Timedelta(days=4)
    
    c1, c2, c3 = st.columns([1, 1, 2])
    with c1: py = st.number_input("연도", value=nm.year, key="ap_y")
    with c2: pm = st.number_input("월", value=nm.month, key="ap_m")
    with c3: pr = st.radio("기간", ["전반기(1~15일)", "후반기(16~말일)"], horizontal=True, key="ap_r")
    
    target_island = user_island
    if user_role == "관리자":
        target_island = st.selectbox("섬 선택", list(LOCATIONS.keys()), key="ap_isl")
    
    c4, c5 = st.columns([2, 1])
    with c4: target_place = st.selectbox("장소 선택", LOCATIONS.get(target_island, []), key="ap_p")
    with c5: note = st.text_input("특이사항(PDF용)", key="ap_n")
    
    # 데이터 준비
    _, last = calendar.monthrange(py, pm)
    dates = [datetime(py, pm, d).strftime("%Y-%m-%d") for d in (range(1, 16) if "전반기" in pr else range(16, last+1))]
    
    df = load_data("활동계획", py, pm, target_island)
    if not df.empty: df = df[df['장소'] == target_place]
    
    users_in_sheet = df['이름'].unique().tolist() if not df.empty else []
    display_users = [u for u in get_users_cached(target_island) if u in users_in_sheet]
    
    if not display_users:
        st.warning("해당 장소에 등록된 계획이 없습니다.")
        return

    # 매트릭스 생성 (수정 가능하도록)
    matrix_data = []
    for d in dates:
        d_obj = datetime.strptime(d, "%Y-%m-%d")
        row = {"날짜": f"{d_obj.day}일 ({DAY_MAP[d_obj.weekday()]})", "raw_date": d}
        for u in display_users:
            val = ""
            if not df.empty:
                chk = df[(df['일자']==d) & (df['이름']==u)]
                if not chk.empty: val = chk.iloc[0]['활동여부']
            row[u] = val
        matrix_data.append(row)
    
    edited = st.data_editor(pd.DataFrame(matrix_data), hide_index=True, use_container_width=True)
    
    # PDF 생성 버튼
    if st.button("✅ 승인 및 운영계획서(PDF) 다운로드"):
        # 1. 승인 상태 저장
        rows = []
        for _, r in edited.iterrows():
            for u in display_users:
                stt = r[u] if r[u] else ""
                rows.append([py, pm, r['raw_date'], target_island, target_place, u, stt, "", "승인완료", str(datetime.now())])
        
        cols = ["년", "월", "일자", "섬", "장소", "이름", "활동여부", "비고", "상태", "타임스탬프"]
        save_data_append("활동계획", rows, cols)
        
        # 2. PDF 생성
        pdf_bytes = generate_roster_pdf(target_place, note, py, pm, pr, edited, display_users, target_island)
        if pdf_bytes:
            st.download_button(
                label="📥 PDF 파일 받기",
                data=pdf_bytes,
                file_name=f"운영계획서_{target_place}_{pm}월.pdf",
                mime="application/pdf"
            )
            st.success("승인 완료 및 PDF 생성 성공!")

def ui_statistics():
    st.header("📊 통합 통계")
    st.info("관리자용 통계 대시보드 (준비중)")
    # 필요시 여기에 그래프 코드 추가

# =========================================================
# 5. 메인 실행 로직 (로그인 및 라우팅)
# =========================================================
def main():
    if not st.session_state['logged_in']:
        st.markdown("## 🔐 백령·대청 지질공원 로그인")
        with st.form("login_form"):
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
                    else:
                        st.error("아이디 또는 비밀번호가 틀렸습니다.")
                except Exception as e:
                    st.error(f"로그인 오류: {e}")
    else:
        user = st.session_state['user_info']
        role = user['직책']
        name = user['이름']
        island = user['섬']

        # 사이드바
        with st.sidebar:
            st.info(f"👤 **{name}** ({role})")
            if st.button("로그아웃"):
                st.session_state['logged_in'] = False
                st.rerun()

        # 직책별 탭 구성
        if role == "관리자":
            t1, t2, t3, t4 = st.tabs(["🔍 활동 조회", "🗓️ 계획 조회", "📊 통계", "✅ 계획 승인(관리)"])
            with t1: ui_view_activity("all", name, island)
            with t2: ui_view_plan("all", name, island)
            with t3: ui_statistics()
            with t4: ui_approve_plan(island, role)

        elif role == "조장":
            t1, t2, t3, t4, t5 = st.tabs(["📝 운영일지 작성", "🔍 활동 조회", "🗓️ 계획 조회", "✍️ 내 계획 입력", "✅ 계획 승인"])
            with t1: ui_write_journal(name, island)
            with t2: ui_view_activity("team", name, island)
            with t3: ui_view_plan("team", name, island)
            with t4: ui_input_plan(name, island)
            with t5: ui_approve_plan(island, role)

        else: # 조원
            t1, t2, t3, t4 = st.tabs(["📝 운영일지 작성", "📅 내 활동 조회", "🗓️ 내 계획 조회", "✍️ 계획 입력"])
            with t1: ui_write_journal(name, island)
            with t2: ui_view_activity("me", name, island)
            with t3: ui_view_plan("me", name, island)
            with t4: ui_input_plan(name, island)

if __name__ == "__main__":
    main()
