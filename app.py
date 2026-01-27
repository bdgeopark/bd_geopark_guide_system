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

# 스타일 설정 (모바일 가독성)
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

# 세션 초기화
if 'logged_in' not in st.session_state: st.session_state['logged_in'] = False
if 'user_info' not in st.session_state: st.session_state['user_info'] = {}

# =========================================================
# 2. 구글 시트 연결 및 데이터 함수
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
    except Exception as e:
        return None

client = get_client()

def load_data(sheet_name, year=None, month=None, island=None):
    """
    데이터 불러오기 (날짜 컬럼 기반 필터링)
    시트 구조가 '날짜' 컬럼을 포함하고 있다고 가정
    """
    try:
        sh = client.open(SPREADSHEET_NAME).worksheet(sheet_name)
        data = sh.get_all_records()
        if not data: return pd.DataFrame()
        
        df = pd.DataFrame(data)
        
        # 1. 컬럼명 공백 제거
        df.columns = [str(c).strip() for c in df.columns]
        
        # 2. 날짜 컬럼 인식 및 변환
        if '날짜' in df.columns:
            df['날짜'] = pd.to_datetime(df['날짜'], errors='coerce')
            
            # 필터링용 임시 컬럼 생성
            df['_year'] = df['날짜'].dt.year
            df['_month'] = df['날짜'].dt.month
            
            # 3. 필터링 적용
            if year: df = df[df['_year'] == int(year)]
            if month: df = df[df['_month'] == int(month)]
            
            # 임시 컬럼 삭제
            df = df.drop(columns=['_year', '_month'])
        
        # 4. 섬 필터링
        if island and '섬' in df.columns:
            df = df[df['섬'] == island]
            
        return df
    except Exception as e:
        # st.error(f"로드 오류: {e}") # 디버깅용
        return pd.DataFrame()

def save_data(sheet_name, new_rows, header_list):
    """
    데이터 저장 (A열이 '날짜'인 구조로 통일)
    """
    try:
        try: sh = client.open(SPREADSHEET_NAME).worksheet(sheet_name)
        except:
            doc = client.open(SPREADSHEET_NAME)
            sh = doc.add_worksheet(sheet_name, 1000, len(header_list))
            sh.append_row(header_list)
            
        existing = sh.get_all_records()
        old_df = pd.DataFrame(existing) if existing else pd.DataFrame(columns=header_list)
        new_df = pd.DataFrame(new_rows, columns=header_list)
        
        # 날짜 통일
        old_df.columns = [str(c).strip() for c in old_df.columns]
        
        # 중복 방지 키 생성 (날짜+이름+장소)
        # 장소 컬럼이 없으면(활동계획 등) 날짜+이름만 사용
        if '장소' in header_list:
            old_df['key'] = old_df['날짜'].astype(str) + old_df['이름'] + old_df['장소']
            new_df['key'] = new_df['날짜'].astype(str) + new_df['이름'] + new_df['장소']
        else:
            old_df['key'] = old_df['날짜'].astype(str) + old_df['이름']
            new_df['key'] = new_df['날짜'].astype(str) + new_df['이름']
            
        keys_to_remove = new_df['key'].tolist()
        final_df = old_df[~old_df['key'].isin(keys_to_remove)].copy()
        
        # 키 삭제 및 병합
        final_df = final_df.drop(columns=['key'], errors='ignore')
        new_df = new_df.drop(columns=['key'], errors='ignore')
        
        combined = pd.concat([final_df, new_df], ignore_index=True)
        
        # 날짜순 정렬 (선택사항)
        if '날짜' in combined.columns:
            combined['날짜'] = pd.to_datetime(combined['날짜'], errors='coerce')
            combined = combined.sort_values('날짜')
            combined['날짜'] = combined['날짜'].dt.strftime("%Y-%m-%d") # 저장할 땐 문자열로
            
        sh.clear()
        sh.update([combined.columns.values.tolist()] + combined.values.tolist())
        return True
    except Exception as e:
        st.error(f"저장 오류: {e}")
        return False

def get_users(island):
    """사용자 목록"""
    try:
        sh = client.open(SPREADSHEET_NAME).worksheet("사용자")
        users = sh.get_all_records()
        return [u['이름'] for u in users if u.get('섬') == island]
    except: return []

# =========================================================
# 3. PDF 생성 (정밀 서식)
# =========================================================
def generate_pdf(target_place, special_note, p_year, p_month, p_range, matrix_df, display_users, current_island):
    font_path = "NanumGothic.ttf"
    if not os.path.exists(font_path):
        st.error("폰트 파일(NanumGothic.ttf)이 없습니다.")
        return None

    # 일지 데이터 로드 (결과 매칭용)
    j_df = load_data("운영일지", p_year, p_month, current_island)
    if not j_df.empty: j_df = j_df[j_df['장소'] == target_place]

    pdf = FPDF(orientation='P', unit='mm', format='A4')
    pdf.set_margins(15, 15, 15)
    pdf.set_auto_page_break(True, margin=10)
    pdf.add_page()

    try:
        pdf.add_font("Nanum", "", font_path)
        pdf.add_font("Nanum", "B", font_path)
    except: return None

    # [제목]
    pdf.set_font("Nanum", "B", 22)
    pdf.set_line_width(0.4)
    pdf.cell(180, 15, "지질공원 안내소 운영계획서", border=1, align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(3)

    # [정보 테이블]
    sy = pdf.get_y(); sx = pdf.get_x()
    pdf.set_line_width(0.12); lh = 7; pdf.set_fill_color(245, 245, 245)

    def p_row(l, v, nl=False):
        pdf.set_font("Nanum", "B", 10)
        pdf.cell(30, lh, l, 1, 0, 'C', True)
        pdf.set_font("Nanum", "", 10)
        pdf.cell(60, lh, str(v), 1, 0, 'L')
        if nl: pdf.ln()

    p_row("안내소", target_place)
    p_row("특이사항", special_note, True)
    p_row("활동월", f"{p_year}년 {p_month}월")
    p_row("활동기간", str(p_range), True)

    # 외곽 테두리
    pdf.set_line_width(0.4); pdf.set_fill_color(0,0,0,0)
    pdf.rect(sx, sy, 180, pdf.get_y()-sy, style="D")
    pdf.set_y(pdf.get_y() + 5)

    # [본문 헤더]
    w_d = 12; w_w = 12; w_rem = 180 - 24
    w_half = w_rem / 2; w_cell = w_half / 4

    def draw_header():
        sy = pdf.get_y(); sx = pdf.get_x()
        pdf.set_line_width(0.12); pdf.set_font("Nanum", "B", 10); pdf.set_fill_color(235, 235, 235)
        
        pdf.cell(w_d, 14, "일", 1, 0, 'C', True)
        pdf.cell(w_w, 14, "요일", 1, 0, 'C', True)
        pdf.set_xy(sx+24, sy)
        pdf.cell(w_half, 7, "활동 계획", 1, 0, 'C', True)
        pdf.cell(w_half, 7, "활동 결과", 1, 1, 'C', True)
        
        pdf.set_font("Nanum", "B", 8)
        y2 = sy+7; bx = sx+24
        
        # 계획 이름
        for i in range(4):
            u = display_users[i] if i < len(display_users) else ""
            pdf.set_xy(bx + (i*w_cell), y2)
            pdf.cell(w_cell, 7, u, 1, 0, 'C', True)
        
        # 결과 이름
        bx += w_half
        for i in range(4):
            u = display_users[i] if i < len(display_users) else ""
            pdf.set_xy(bx + (i*w_cell), y2)
            pdf.cell(w_cell, 7, u, 1, 0, 'C', True)
            
        pdf.set_xy(sx, sy+14)
        pdf.set_line_width(0.4); pdf.rect(sx, sy, 180, 14, style="D"); pdf.set_line_width(0.12)

    draw_header()

    # [데이터]
    row_h = 8; body_sy = pdf.get_y()
    
    for _, row in matrix_df.iterrows():
        if pdf.get_y() > 275:
            pdf.set_line_width(0.4); pdf.rect(15, body_sy, 180, pdf.get_y()-body_sy, style="D"); pdf.set_line_width(0.12)
            pdf.add_page(); draw_header(); body_sy = pdf.get_y()

        yc = pdf.get_y(); xc = pdf.get_x()
        d_obj = datetime.strptime(row['raw_date'], "%Y-%m-%d")
        
        pdf.set_font("Nanum", "B", 9)
        pdf.cell(w_d, row_h, str(d_obj.day), 1, 0, 'C')
        pdf.cell(w_w, row_h, DAY_MAP[d_obj.weekday()], 1, 0, 'C')
        pdf.set_font("Nanum", "", 8)

        # 데이터 매핑
        p_txt = [""]*4; r_txt = [""]*4
        
        # 계획
        for i in range(4):
            if i < len(display_users):
                val = row.get(display_users[i], "")
                if val:
                    val = val.replace("오전(4시간)","오전").replace("오후(4시간)","오후").replace("4시간","4H").replace("8시간","8H")
                    p_txt[i] = val if "기타" not in val else "기타"
        
        # 결과 (일지에서 찾기)
        j_entries = []
        if not j_df.empty:
            day_j = j_df[j_df['날짜'] == d_obj]
            for _, r in day_j.iterrows(): j_entries.append({"n":r['이름'], "t":str(r['활동시간'])+"H"})
            
        matched = []
        for i in range(4):
            if i < len(display_users):
                owner = display_users[i]
                for k, e in enumerate(j_entries):
                    if e['n'] == owner:
                        r_txt[i] = e['t']; matched.append(k); break
        
        unmatched = [e for k, e in enumerate(j_entries) if k not in matched]
        empty = [i for i in range(4) if r_txt[i] == ""]
        for k in range(min(len(unmatched), len(empty))):
            r_txt[empty[k]] = f"{unmatched[k]['n']}\n({unmatched[k]['t']})"

        # 출력 (계획)
        bx = xc + 24
        for i in range(4):
            pdf.set_xy(bx + (i*w_cell), yc)
            pdf.cell(w_cell, row_h, p_txt[i], 1, 0, 'C')
            
        # 출력 (결과)
        bx += w_half
        for i in range(4):
            cx = bx + (i*w_cell); txt = r_txt[i]
            pdf.set_xy(cx, yc)
            if "\n" in txt:
                pdf.set_font("Nanum", "", 7); pdf.set_xy(cx, yc+1)
                pdf.multi_cell(w_cell, 3, txt, 0, 'C')
                pdf.set_xy(cx, yc); pdf.rect(cx, yc, w_cell, row_h)
                pdf.set_font("Nanum", "", 8)
            else:
                pdf.cell(w_cell, row_h, txt, 1, 0, 'C')
        
        pdf.set_xy(xc, yc+row_h)

    pdf.set_line_width(0.4); pdf.rect(15, body_sy, 180, pdf.get_y()-body_sy, style="D"); pdf.set_line_width(0.12)
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
    with c1: jy = st.number_input("년", value=now.year)
    with c2: jm = st.number_input("월", value=now.month)
    with c3: place = st.selectbox("장소", LOCATIONS.get(island, []))
    
    # 모드 선택
    st.divider()
    mode = st.radio("입력 모드", ["📅 하루씩 입력 (모바일)", "🗓️ 월간 전체 입력 (PC)"], horizontal=True)
    
    # 날짜 계산
    _, last = calendar.monthrange(jy, jm)
    dates = [datetime(jy, jm, d).strftime("%Y-%m-%d") for d in range(1, last+1)]
    
    # 데이터 로드
    df = load_data("운영일지", jy, jm, island)
    if not df.empty: df = df[(df['이름']==name) & (df['장소']==place)]
    
    # [모바일 모드]
    if "하루씩" in mode:
        c_d1, c_d2 = st.columns([1, 1.5])
        with c_d1:
            def_d = now.date()
            if def_d.month != jm: def_d = datetime(jy, jm, 1).date()
            pick = st.date_input("날짜", value=def_d, min_value=datetime(jy, jm, 1), max_value=datetime(jy, jm, last))
            pick_s = pick.strftime("%Y-%m-%d")
        
        # 기존값
        pt="활동 없음"; pc=""; pv=0; pn=""
        if not df.empty:
            r = df[df['날짜']==pd.to_datetime(pick_s)]
            if not r.empty:
                r = r.iloc[0]
                tv = str(r['활동시간'])
                if tv=="8": pt="종일 (8시간)"
                elif tv=="4": pt="반일 (4시간)"
                pc = str(r['활동내용'])
                pv = int(r['탐방객수'] or 0)
                pn = str(r['비고'])
                
        with c_d2: st.markdown(f"**{pick.day}일 ({DAY_MAP[pick.weekday()]})**")
        
        with st.form("daily_j"):
            st.markdown("**1. 활동 시간**")
            st_sel = st.radio("시간", ["활동 없음", "종일 (8시간)", "반일 (4시간)"], index=["활동 없음", "종일 (8시간)", "반일 (4시간)"].index(pt), horizontal=True)
            st.markdown("**2. 활동 내용**")
            ic = st.text_area("내용", value=pc, height=100)
            c1, c2 = st.columns(2)
            with c1: iv = st.number_input("탐방객", value=pv, min_value=0)
            with c2: inote = st.text_input("비고", value=pn)
            
            if st.form_submit_button("💾 저장", use_container_width=True):
                ft = 8 if "8시간" in st_sel else (4 if "4시간" in st_sel else "")
                # 스크린샷 구조 반영: [날짜, 섬, 장소, 이름, 활동시간, 활동내용, 탐방객수, 비고, 타임스탬프, 년, 월, 상태]
                row = [pick_s, island, place, name, ft, ic, iv, inote, str(datetime.now()), jy, jm, "검토대기"]
                cols = ["날짜","섬","장소","이름","활동시간","활동내용","탐방객수","비고","타임스탬프","년","월","상태"]
                if save_data("운영일지", [row], cols):
                    st.success("저장 완료!"); time.sleep(0.5); st.rerun()
    
    # [PC 모드]
    else:
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
                "활동내용": cur.get('활동내용',''), "탐방객": cur.get('탐방객수',0), "비고": cur.get('비고','')
            })
            
        with st.form("month_j"):
            edited = st.data_editor(pd.DataFrame(grid), hide_index=True, use_container_width=True, height=600)
            if st.form_submit_button("💾 일괄 저장"):
                rows = []
                for _, r in edited.iterrows():
                    ft = 8 if r['종일'] else (4 if r['반일'] else "")
                    rows.append([r['날짜'], island, place, name, ft, r['활동내용'], r['탐방객'], r['비고'], str(datetime.now()), jy, jm, "검토대기"])
                cols = ["날짜","섬","장소","이름","활동시간","활동내용","탐방객수","비고","타임스탬프","년","월","상태"]
                if save_data("운영일지", rows, cols):
                    st.success("일괄 저장 완료!"); time.sleep(1); st.rerun()

def ui_view_journal(scope, name, island):
    st.header("🔍 활동 조회")
    c1, c2 = st.columns(2)
    with c1: vy = st.number_input("연도", value=datetime.now().year)
    with c2: vm = st.number_input("월", value=datetime.now().month)
    
    target_isl = island if scope != "all" else None
    df = load_data("운영일지", vy, vm, target_isl)
    
    if df.empty: st.info("데이터가 없습니다."); return
    
    if scope == "me": df = df[df['이름'] == name]
    
    # 타임스탬프 등 숨기고 표시
    st.dataframe(
        df, 
        use_container_width=True, 
        hide_index=True,
        column_config={
            "타임스탬프": None, "년": None, "월": None, "키": None, "key": None,
            "날짜": st.column_config.DateColumn("날짜", format="YYYY-MM-DD")
        }
    )

def ui_plan_input(name, island):
    st.header("✍️ 계획 입력")
    # 다음달
    now = datetime.now()
    nm = now.replace(day=28) + pd.Timedelta(days=4)
    
    c1, c2, c3 = st.columns([1,1,2])
    with c1: py = st.number_input("계획 연도", value=nm.year)
    with c2: pm = st.number_input("계획 월", value=nm.month)
    with c3: pr = st.radio("기간", ["전반기(1~15일)", "후반기(16~말일)"], horizontal=True)
    
    place = st.selectbox("장소", LOCATIONS.get(island, []))
    
    _, last = calendar.monthrange(py, pm)
    dates = [datetime(py, pm, d).strftime("%Y-%m-%d") for d in (range(1, 16) if "전반기" in pr else range(16, last+1))]
    
    df = load_data("활동계획", py, pm, island)
    if not df.empty: df = df[(df['이름']==name) & (df['장소']==place)]
    
    st.divider()
    mode = st.radio("입력 모드", ["📅 하루씩 입력 (모바일)", "🗓️ 전체 입력 (PC)"], horizontal=True)
    
    if "하루씩" in mode:
        c1, c2 = st.columns([1, 1.5])
        with c1:
            def_d = datetime.strptime(dates[0], "%Y-%m-%d").date()
            try: pick = st.date_input("날짜", value=def_d, min_value=def_d, max_value=datetime.strptime(dates[-1], "%Y-%m-%d"))
            except: pick = def_d
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
        
        with st.form("daily_p"):
            sel = st.radio("계획", ["활동 없음", "종일 (8시간)", "오전 (4시간)", "오후 (4시간)", "기타"], index=["활동 없음", "종일 (8시간)", "오전 (4시간)", "오후 (4시간)", "기타"].index(ps))
            ein = st.text_input("기타 시간 입력", value=etc)
            
            if st.form_submit_button("💾 저장", use_container_width=True):
                stat = ""
                if "종일" in sel: stat="종일"
                elif "오전" in sel: stat="오전(4시간)"
                elif "오후" in sel: stat="오후(4시간)"
                elif "기타" in sel: stat=ein if ein else "미정"
                
                # 저장 구조 통일: [날짜, 섬, 장소, 이름, 활동여부, 비고, 타임스탬프, 년, 월, 상태]
                row = [pick_s, island, place, name, stat, "", str(datetime.now()), py, pm, ""]
                cols = ["날짜","섬","장소","이름","활동여부","비고","타임스탬프","년","월","상태"]
                if save_data("활동계획", [row], cols):
                    st.success("저장 완료!"); time.sleep(0.5); st.rerun()
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
            
        with st.form("month_p"):
            edited = st.data_editor(pd.DataFrame(grid), hide_index=True, use_container_width=True, height=600)
            if st.form_submit_button("💾 일괄 저장"):
                rows = []
                for _, r in edited.iterrows():
                    s = ""
                    if r['종일']: s="종일"
                    elif r['오전']: s="오전(4시간)"
                    elif r['오후']: s="오후(4시간)"
                    elif r['기타']: s=str(r['기타'])
                    rows.append([r['날짜'], island, place, name, s, "", str(datetime.now()), py, pm, ""])
                cols = ["날짜","섬","장소","이름","활동여부","비고","타임스탬프","년","월","상태"]
                if save_data("활동계획", rows, cols):
                    st.success("일괄 저장 완료!"); time.sleep(1); st.rerun()

def ui_view_plan(scope, name, island):
    st.header("🗓️ 계획 조회")
    c1, c2 = st.columns(2)
    now = datetime.now()
    with c1: py = st.number_input("연도", value=now.year, key="vp_y")
    with c2: pm = st.number_input("월", value=now.month, key="vp_m")
    
    t_isl = island if scope != "all" else None
    df = load_data("활동계획", py, pm, t_isl)
    
    if df.empty: st.info("계획이 없습니다."); return
    if scope == "me": df = df[df['이름'] == name]
    
    # 피벗 보기
    try:
        pivot = df.pivot_table(index="날짜", columns="이름", values="활동여부", aggfunc="first").fillna("")
        st.dataframe(pivot, use_container_width=True)
    except:
        st.dataframe(df)

def ui_approve(island, role):
    st.header("✅ 계획 승인")
    now = datetime.now()
    nm = now.replace(day=28) + pd.Timedelta(days=4)
    
    c1, c2, c3 = st.columns([1,1,2])
    with c1: py = st.number_input("연도", value=nm.year, key="ap_y")
    with c2: pm = st.number_input("월", value=nm.month, key="ap_m")
    with c3: pr = st.radio("기간", ["전반기(1~15일)", "후반기(16~말일)"], horizontal=True, key="ap_r")
    
    tis = island
    if role == "관리자": tis = st.selectbox("섬", list(LOCATIONS.keys()), key="ap_isl")
    
    c4, c5 = st.columns([2,1])
    with c4: tpl = st.selectbox("장소", LOCATIONS.get(tis, []), key="ap_p")
    with c5: note = st.text_input("특이사항", key="ap_n")
    
    _, last = calendar.monthrange(py, pm)
    dates = [datetime(py, pm, d).strftime("%Y-%m-%d") for d in (range(1, 16) if "전반기" in pr else range(16, last+1))]
    
    df = load_data("활동계획", py, pm, tis)
    if not df.empty: df = df[df['장소'] == tpl]
    
    # 사용자 목록
    users = get_users(tis)
    exist_users = df['이름'].unique().tolist() if not df.empty else []
    display_users = [u for u in users if u in exist_users]
    
    if not display_users: st.warning("제출된 계획이 없습니다."); return
    
    # 매트릭스
    data = []
    for d in dates:
        d_obj = datetime.strptime(d, "%Y-%m-%d")
        row = {"날짜": f"{d_obj.day}일 ({DAY_MAP[d_obj.weekday()]})", "raw_date": d}
        for u in display_users:
            val = ""
            if not df.empty:
                chk = df[(df['날짜']==d_obj) & (df['이름']==u)]
                if not chk.empty: val = chk.iloc[0]['활동여부']
            row[u] = val
        data.append(row)
        
    edited = st.data_editor(pd.DataFrame(data), hide_index=True, use_container_width=True)
    
    if st.button("✅ 승인 및 PDF 다운로드"):
        # 승인 저장
        rows = []
        for _, r in edited.iterrows():
            for u in display_users:
                stt = r[u] if r[u] else ""
                rows.append([r['raw_date'], tis, tpl, u, stt, "", str(datetime.now()), py, pm, "승인완료"])
        
        cols = ["날짜","섬","장소","이름","활동여부","비고","타임스탬프","년","월","상태"]
        save_data("활동계획", rows, cols)
        
        # PDF
        pdf_data = generate_pdf(tpl, note, py, pm, pr, edited, display_users, tis)
        if pdf_data:
            st.download_button("📥 PDF 다운로드", pdf_data, f"운영계획서_{tpl}_{pm}월.pdf", "application/pdf")
            st.success("완료!")

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
            with t2: ui_view_plan("all", name, island)
            with t3: ui_stats()
            with t4: ui_approve(island, role)
            
        elif role == "조장":
            t1, t2, t3, t4, t5 = st.tabs(["📝 일지작성", "🔍 활동조회", "🗓️ 계획조회", "✍️ 계획입력", "✅ 계획승인"])
            with t1: ui_journal_write(name, island)
            with t2: ui_view_journal("team", name, island)
            with t3: ui_view_plan("team", name, island)
            with t4: ui_plan_input(name, island)
            with t5: ui_approve(island, role)
            
        else: # 조원
            t1, t2, t3, t4 = st.tabs(["📝 일지작성", "📅 내 활동", "🗓️ 내 계획", "✍️ 계획입력"])
            with t1: ui_journal_write(name, island)
            with t2: ui_view_journal("me", name, island)
            with t3: ui_view_plan("me", name, island)
            with t4: ui_plan_input(name, island)

if __name__ == "__main__":
    main()
