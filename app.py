import streamlit as st
import pandas as pd
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time
import calendar

# ---------------------------------------------------------
# 1. 시스템 설정 & 폰트 크기 확대 (CSS)
# ---------------------------------------------------------
st.set_page_config(page_title="지질공원 통합관리", page_icon="🪨", layout="wide")

st.markdown("""
    <style>
    html, body, [class*="css"] {
        font-size: 18px !important;
    }
    div[data-testid="stDataEditor"] table {
        font-size: 18px !important;
    }
    div[data-testid="stSelectbox"] * {
        font-size: 18px !important;
    }
    </style>
    """, unsafe_allow_html=True)

# 세션 초기화
if 'logged_in' not in st.session_state: st.session_state['logged_in'] = False
if 'user_info' not in st.session_state: st.session_state['user_info'] = {}
# 단계별 데이터 저장소
if 'step1_df' not in st.session_state: st.session_state['step1_df'] = None 
if 'step2_dfs' not in st.session_state: st.session_state['step2_dfs'] = {} 
if 'current_step' not in st.session_state: st.session_state['current_step'] = 1
# ★ 입력 조건 변경 감지용 (이게 있어야 날짜가 바뀝니다!)
if 'last_input_key' not in st.session_state: st.session_state['last_input_key'] = ""

@st.cache_resource
def get_client():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    try:
        creds = ServiceAccountCredentials.from_json_keyfile_name("geopark_key.json", scope)
        return gspread.authorize(creds)
    except:
        try:
            key_dict = st.secrets["gcp_service_account"]
            creds = ServiceAccountCredentials.from_json_keyfile_dict(key_dict, scope)
            return gspread.authorize(creds)
        except:
            return None

client = get_client()
SPREADSHEET_NAME = "지질공원_운영일지_DB"

locations = {
    "백령도": ["두무진 안내소", "콩돌해안 안내소", "사곶해변 안내소", "용기포신항 안내소", "진촌리 현무암 안내소", "용틀임바위 안내소", "임시지질공원센터"],
    "대청도": ["서풍받이 안내소", "옥죽동 해안사구 안내소", "농여해변 안내소", "선진동 선착장 안내소"],
    "소청도": ["분바위 안내소", "탑동 선착장 안내소"],
    "시청": ["인천시청", "지질공원팀 사무실"]
}

# ---------------------------------------------------------
# 2. 기능 함수
# ---------------------------------------------------------
def login(username, password):
    if client is None:
        st.error("서버 연결 실패")
        return
    try:
        sheet = client.open(SPREADSHEET_NAME).worksheet("사용자")
        users = sheet.get_all_records()
        for user in users:
            if str(user['아이디']) == str(username) and str(user['비번']) == str(password):
                st.session_state['logged_in'] = True
                st.session_state['user_info'] = user
                st.success(f"환영합니다, {user['이름']}님!")
                time.sleep(0.5)
                st.rerun()
                return
        st.error("아이디 불일치")
    except: st.error("로그인 오류")

@st.cache_data(ttl=3600)
def get_users_by_island_cached(island_name):
    try:
        if client is None: return []
        sheet = client.open(SPREADSHEET_NAME).worksheet("사용자")
        users = sheet.get_all_records()
        return [u['이름'] for u in users if u.get('섬') == island_name]
    except: return []

def save_bulk(sheet_name, rows):
    try:
        sheet = client.open(SPREADSHEET_NAME).worksheet(sheet_name)
        sheet.append_rows(rows)
        return True
    except: return False

def approve_rows(indices):
    try:
        sheet = client.open(SPREADSHEET_NAME).worksheet("운영일지")
        for idx in indices: sheet.update_cell(idx + 2, 10, "승인완료")
        return True
    except: return False

# ---------------------------------------------------------
# 3. 메인 화면
# ---------------------------------------------------------
if not st.session_state['logged_in']:
    st.markdown("## 🔐 백령·대청 지질공원 로그인")
    with st.form("login"):
        uid = st.text_input("아이디")
        upw = st.text_input("비밀번호", type="password")
        if st.form_submit_button("로그인"): login(uid, upw)
else:
    user = st.session_state['user_info']
    my_name = user['이름']
    my_island = user['섬']
    my_role = user['직책']

    with st.sidebar:
        st.info(f"👤 **{my_name}** ({my_role})")
        if st.button("🔄 처음부터 다시 입력"):
            st.session_state['step1_df'] = None
            st.session_state['step2_dfs'] = {}
            st.session_state['current_step'] = 1
            st.rerun()
        st.divider()
        if st.button("로그아웃"):
            st.session_state['logged_in'] = False
            st.session_state['step1_df'] = None
            st.session_state['step2_dfs'] = {}
            st.session_state['current_step'] = 1
            st.rerun()

    st.title(f"📱 {my_name}님의 업무공간")
    tabs = st.tabs(["📝 활동 입력", "📅 내 활동 조회", "🗓️ 다음달 계획", "👀 조원 검토", "📊 통계"])

    # -----------------------------------------------------
    # 탭 1: 활동 입력
    # -----------------------------------------------------
    with tabs[0]:
        st.subheader("활동 실적 등록")

        c1, c2, c3 = st.columns([1, 1, 2])
        with c1: t_year = st.number_input("년", value=datetime.now().year)
        with c2: t_month = st.number_input("월", value=datetime.now().month)
        with c3: 
            if my_role == "관리자":
                sel_island = st.selectbox("섬 선택", list(locations.keys()))
            else:
                sel_island = my_island
                st.success(f"📍 {sel_island}")
        
        c4, c5 = st.columns([1, 2])
        with c4: period = st.radio("기간", ["전반기(1~15일)", "후반기(16~말일)"], horizontal=True)
        with c5: sel_place = st.selectbox("근무 장소(공통)", locations.get(sel_island, ["-"]))
        
        island_users = get_users_by_island_cached(sel_island)
        
        # ★★★ 핵심 수정: 조건이 바뀌면 표를 초기화하는 로직 ★★★
        # 현재 선택된 조건들을 하나의 문자열로 합쳐서 '키'로 만듭니다.
        current_input_key = f"{t_year}-{t_month}-{sel_island}-{period}-{sel_place}"
        
        # 이전 키와 다르면 (즉, 조건이 바뀌었으면) 초기화!
        if st.session_state['last_input_key'] != current_input_key:
            st.session_state['step1_df'] = None
            st.session_state['step2_dfs'] = {}
            st.session_state['current_step'] = 1
            st.session_state['last_input_key'] = current_input_key
            st.rerun() # 화면 즉시 새로고침
        
        st.divider()

        # =========================================================
        # [STEP 1] 운영 현황 입력
        # =========================================================
        if st.session_state['current_step'] == 1:
            st.markdown("### 1️⃣ 단계: 운영 현황 입력")
            st.info("👇 날짜별 **방문객 통계**와 **근무한 해설사 수**를 입력하세요.")

            # 표 생성 (없을 때만)
            if st.session_state['step1_df'] is None:
                _, last_day = calendar.monthrange(t_year, t_month)
                day_range = range(1, 16) if "전반기" in period else range(16, last_day + 1)
                
                rows = []
                for d in day_range:
                    dt_obj = datetime(t_year, t_month, d)
                    d_str = dt_obj.strftime("%Y-%m-%d")
                    wk = dt_obj.strftime("%a")
                    # 기본값: 0명
                    rows.append([d_str, wk, 0, 0, 0, 0])
                
                st.session_state['step1_df'] = pd.DataFrame(rows, columns=["일자", "요일", "방문자", "청취자", "해설횟수", "활동해설사수"])

            edited_step1 = st.data_editor(
                st.session_state['step1_df'],
                column_config={
                    "일자": st.column_config.TextColumn("일자", disabled=True, width="small"),
                    "요일": st.column_config.TextColumn("요일", disabled=True, width="small"),
                    "방문자": st.column_config.NumberColumn("방문자", min_value=0),
                    "청취자": st.column_config.NumberColumn("청취자", min_value=0),
                    "해설횟수": st.column_config.NumberColumn("해설횟수", min_value=0),
                    "활동해설사수": st.column_config.NumberColumn("활동 해설사 수(명)", min_value=0, max_value=5),
                },
                hide_index=True,
                use_container_width=True
            )

            if st.button("💾 저장 및 다음 단계(해설사 배정)"):
                stats_rows = []
                max_guides = 0
                
                for _, row in edited_step1.iterrows():
                    has_stats = (row["방문자"] > 0 or row["청취자"] > 0 or row["해설횟수"] > 0)
                    g_count = int(row["활동해설사수"])
                    if g_count > max_guides: max_guides = g_count
                    
                    if has_stats:
                        stats_rows.append([
                            row["일자"], sel_island, sel_place, "운영통계", 
                            0, row["방문자"], row["청취자"], row["해설횟수"],
                            str(datetime.now()), "검토대기"
                        ])
                
                if not stats_rows and max_guides == 0:
                    st.warning("⚠️ 입력된 내용이 없습니다.")
                else:
                    if stats_rows:
                        save_bulk("운영일지", stats_rows)
                        st.toast("✅ 운영 통계 저장 완료!")
                    
                    if max_guides > 0:
                        # 2단계 데이터 준비
                        dfs = {}
                        for k in range(1, max_guides + 1):
                            data_k = []
                            for _, row in edited_step1.iterrows():
                                if int(row["활동해설사수"]) >= k:
                                    data_k.append([row["일자"], row["요일"], None, "8시간", 0])
                            dfs[k] = pd.DataFrame(data_k, columns=["일자", "요일", "해설사", "활동시간", "시간(직접)"])
                        
                        st.session_state['step2_dfs'] = dfs
                        st.session_state['current_step'] = 2
                        st.rerun()
                    else:
                        st.success("✅ 통계만 저장되었습니다.")
                        time.sleep(1)
                        st.session_state['step1_df'] = None
                        st.rerun()

        # =========================================================
        # [STEP 2] 해설사 활동
        # =========================================================
        elif st.session_state['current_step'] == 2:
            st.markdown("### 2️⃣ 단계: 해설사 활동 상세 입력")
            
            dfs = st.session_state['step2_dfs']
            total_tables = len(dfs)
            
            for k in range(1, total_tables + 1):
                st.markdown(f"#### 👤 **{k}번 해설사** 활동 입력")
                
                selected_name = st.selectbox(
                    f"👇 {k}번 해설사를 선택하세요 (일괄적용)", 
                    ["선택안함"] + island_users, 
                    key=f"sel_guide_{k}"
                )
                
                df_k = dfs[k]
                if selected_name != "선택안함":
                    df_k["해설사"] = selected_name
                
                edited_k = st.data_editor(
                    df_k,
                    key=f"editor_{k}",
                    column_config={
                        "일자": st.column_config.TextColumn("일자", disabled=True, width="small"),
                        "요일": st.column_config.TextColumn("요일", disabled=True, width="small"),
                        "해설사": st.column_config.TextColumn("해설사", width="medium"),
                        "활동시간": st.column_config.SelectboxColumn("활동시간", options=["8시간", "4시간", "직접입력"], default="8시간"),
                        "시간(직접)": st.column_config.NumberColumn("입력", min_value=0, max_value=24, width="small"),
                    },
                    hide_index=True,
                    use_container_width=True
                )
                st.session_state['step2_dfs'][k] = edited_k
                st.divider()

            c_btn1, c_btn2 = st.columns([1, 1])
            with c_btn1:
                if st.button("✅ 모든 해설사 활동 일괄 저장", type="primary"):
                    all_rows = []
                    missing_name = False
                    
                    for k in range(1, total_tables + 1):
                        df_target = st.session_state['step2_dfs'][k]
                        if df_target['해설사'].isnull().any() or (df_target['해설사'] == "선택안함").any():
                            missing_name = True
                            st.warning(f"⚠️ {k}번 표에 해설사가 선택되지 않은 날짜가 있습니다.")
                            break
                            
                        for _, row in df_target.iterrows():
                            fh = 8
                            if row["활동시간"] == "4시간": fh = 4
                            elif row["활동시간"] == "직접입력": fh = row["시간(직접)"]
                            if row["활동시간"] == "직접입력" and fh == 0: continue
                            
                            all_rows.append([
                                row["일자"], sel_island, sel_place, row["해설사"],
                                fh, 0, 0, 0, # 통계 0
                                str(datetime.now()), "검토대기"
                            ])
                    
                    if not missing_name and all_rows:
                        if save_bulk("운영일지", all_rows):
                            st.success(f"✅ 총 {len(all_rows)}건 저장 완료!")
                            time.sleep(2)
                            st.session_state['step1_df'] = None
                            st.session_state['step2_dfs'] = {}
                            st.session_state['current_step'] = 1
                            st.rerun()
            
            with c_btn2:
                if st.button("🔙 뒤로가기 (1단계 수정)"):
                    st.session_state['current_step'] = 1
                    st.rerun()

    # -----------------------------------------------------
    # 탭 2~5 (기존 동일)
    # -----------------------------------------------------
    with tabs[1]:
        if st.button("내역 조회하기"):
            try:
                wb = client.open(SPREADSHEET_NAME)
                df = pd.DataFrame(wb.worksheet("운영일지").get_all_records())
                my_df = df[df['이름'] == my_name]
                st.dataframe(my_df)
            except: st.error("데이터 없음")

    with tabs[2]:
        st.subheader("계획 등록")
        c1, c2 = st.columns(2)
        with c1: p_year = st.number_input("년도", 2025)
        with c2: p_month = st.number_input("월 ", datetime.now().month)
        p_period = st.radio("기간 ", ["전반기", "후반기"], horizontal=True)
        p_place = st.selectbox("예정지", locations.get(my_island, ["-"]))
        _, last = calendar.monthrange(p_year, p_month)
        rng = range(1, 16) if "전반기" in p_period else range(16, last+1)
        selected_days = st.multiselect("근무일 선택", [f"{d}일" for d in rng])
        if st.button("계획 제출"):
            rows = []
            for s in selected_days:
                d = int(s.replace("일",""))
                dt = datetime(p_year, p_month, d).strftime("%Y-%m-%d")
                rows.append([dt, my_island, p_place, my_name, "", str(datetime.now())])
            if save_bulk("월간계획", rows): st.success("제출 완료")

    if my_role in ["조장", "관리자"]:
        with tabs[3]:
            st.subheader("승인 관리")
            if st.button("검토 목록 새로고침"):
                try:
                    wb = client.open(SPREADSHEET_NAME)
                    df = pd.DataFrame(wb.worksheet("운영일지").get_all_records())
                    if my_role != "관리자": df = df[df['섬'] == my_island]
                    df = df[df['상태'] == "검토대기"]
                    if not df.empty:
                        st.dataframe(df)
                        indices = df.index.tolist()
                        if st.button("조회된 항목 일괄 승인"):
                            approve_rows(indices)
                            st.success("승인 완료")
                    else: st.info("대기 건 없음")
                except: st.error("로드 실패")

    if my_role == "관리자":
        with tabs[4]:
            if st.button("통계 보기"):
                try:
                    wb = client.open(SPREADSHEET_NAME)
                    df = pd.DataFrame(wb.worksheet("운영일지").get_all_records())
                    for col in ['방문자', '해설횟수']:
                        if col in df.columns:
                            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                        else: df[col] = 0
                    st.metric("총 방문객", int(df['방문자'].sum()))
                    if '섬' in df.columns:
                        st.bar_chart(df.groupby("섬")['방문자'].sum())
                except: st.error("로드 실패")
