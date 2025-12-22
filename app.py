import streamlit as st
import pandas as pd
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time
import calendar

# ---------------------------------------------------------
# 1. 시스템 설정
# ---------------------------------------------------------
st.set_page_config(page_title="지질공원 통합관리", page_icon="🪨", layout="wide")

# ★ 화면 기억장치 초기화 (이게 있어야 안 사라집니다!)
if 'logged_in' not in st.session_state: st.session_state['logged_in'] = False
if 'user_info' not in st.session_state: st.session_state['user_info'] = {}
if 'generated_df' not in st.session_state: st.session_state['generated_df'] = None # 만들어진 표 저장

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
    try:
        if client is None:
            st.error("서버 연결 실패")
            return
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
        st.error("아이디 확인 요망")
    except: st.error("로그인 오류")

def get_users_by_island(island_name):
    try:
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
        # ★ 초기화 버튼 (입력하다 꼬이면 이거 누르라고 하세요)
        if st.button("🔄 입력화면 초기화"):
            st.session_state['generated_df'] = None
            st.rerun()
        st.divider()
        if st.button("로그아웃"):
            st.session_state['logged_in'] = False
            st.session_state['generated_df'] = None
            st.rerun()

    st.title(f"📱 {my_name}님의 업무공간")

    tabs = st.tabs(["📝 활동 입력", "📅 내 활동 조회", "🗓️ 다음달 계획", "👀 조원 검토", "📊 통계"])

    # -----------------------------------------------------
    # 탭 1: 활동 입력 (사라짐 방지 기능 적용)
    # -----------------------------------------------------
    with tabs[0]:
        st.subheader("활동 실적 등록")
        
        # 1. 설정값 입력
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

        st.divider()

        # 2. 해설사 선택 및 날짜 체크
        island_users = get_users_by_island(sel_island)
        if not island_users: island_users = ["해설사 없음"]

        if my_role == "관리자":
            # 여기가 사라지지 않게 하려면 multiselect가 session_state와 연결될 필요는 없지만
            # 리런되어도 값이 유지되도록 스트림릿이 알아서 처리합니다.
            # 다만, 섬을 바꾸면 초기화됩니다.
            selected_guides = st.multiselect("📝 이번 기간에 활동한 해설사를 모두 선택하세요", island_users)
        else:
            selected_guides = [my_name]
            st.info(f"👤 **{my_name}**님의 근무일을 체크하세요.")

        # 날짜 범위 계산
        _, last_day = calendar.monthrange(t_year, t_month)
        day_range = range(1, 16) if "전반기" in period else range(16, last_day + 1)

        # 체크박스 데이터 수집용 리스트
        schedule_data = [] 
        
        # ★ 해설사별 체크박스 화면 (여기가 중요!)
        if selected_guides:
            for guide in selected_guides:
                with st.expander(f"🗓️ **{guide}**님 근무일 체크", expanded=True):
                    cols = st.columns(5)
                    for i, day in enumerate(day_range):
                        # 키 값을 유니크하게 해서 상태 유지
                        key = f"chk_{guide}_{day}_{t_month}" 
                        with cols[i % 5]:
                            if st.checkbox(f"{day}일", key=key):
                                dt_obj = datetime(t_year, t_month, day)
                                full_date = dt_obj.strftime("%Y-%m-%d")
                                weekday = dt_obj.strftime("%a")
                                schedule_data.append([full_date, guide, weekday])

        # 3. 표 만들기 버튼
        # (버튼을 누르면 session_state에 데이터를 저장하고 화면을 다시 그립니다)
        if st.button("⬇️ 위에서 체크한 내용으로 표 생성"):
            if not schedule_data:
                st.warning("⚠️ 근무일을 하나 이상 체크해주세요.")
            else:
                # 데이터프레임 생성
                rows = []
                for item in schedule_data:
                    # item = [날짜, 이름, 요일]
                    # 기본값: 8시간, 나머지 0
                    rows.append([item[0], item[2], item[1], "8시간", 0, 0, 0, 0])
                
                # 날짜순, 이름순 정렬
                df = pd.DataFrame(rows, columns=["일자", "요일", "해설사", "활동시간", "시간(직접)", "방문자", "청취자", "해설횟수"])
                df = df.sort_values(by=["일자", "해설사"])
                
                # ★★★ 여기가 핵심: 세션에 저장해둠! ★★★
                st.session_state['generated_df'] = df 
                st.rerun() # 화면 갱신 (이제 사라지지 않음)

        # 4. 생성된 표 보여주기 (세션에 데이터가 있을 때만)
        if st.session_state['generated_df'] is not None:
            st.divider()
            st.success("✅ 표가 생성되었습니다. 내용을 입력하고 저장하세요.")
            st.caption("※ 다시 선택하려면 왼쪽 메뉴의 [🔄 입력화면 초기화] 버튼을 누르세요.")
            
            edited_df = st.data_editor(
                st.session_state['generated_df'],
                column_config={
                    "일자": st.column_config.TextColumn("일자", disabled=True, width="small"),
                    "요일": st.column_config.TextColumn("요일", disabled=True, width="small"),
                    "해설사": st.column_config.TextColumn("해설사", disabled=True, width="medium"),
                    "활동시간": st.column_config.SelectboxColumn("시간", options=["8시간", "4시간", "직접입력"], default="8시간"),
                    "시간(직접)": st.column_config.NumberColumn("입력", min_value=0, max_value=24, width="small"),
                    "방문자": st.column_config.NumberColumn("방문자", min_value=0),
                    "청취자": st.column_config.NumberColumn("청취자", min_value=0),
                    "해설횟수": st.column_config.NumberColumn("횟수", min_value=0),
                },
                hide_index=True,
                use_container_width=True,
                num_rows="dynamic"
            )

            # 저장 버튼
            if st.button("✅ 작성 완료! 일괄 저장하기"):
                rows_to_save = []
                for _, row in edited_df.iterrows():
                    fh = 8
                    if row["활동시간"] == "4시간": fh = 4
                    elif row["활동시간"] == "직접입력": fh = row["시간(직접)"]
                    
                    if row["활동시간"] == "직접입력" and fh == 0: continue

                    rows_to_save.append([
                        row["일자"], sel_island, sel_place, row["해설사"], 
                        fh, row["방문자"], row["청취자"], row["해설횟수"], 
                        str(datetime.now()), "검토대기"
                    ])
                
                if save_bulk("운영일지", rows_to_save):
                    st.success(f"총 {len(rows_to_save)}건이 저장되었습니다!")
                    st.session_state['generated_df'] = None # 저장 후 표 비우기
                    time.sleep(1)
                    st.rerun()

    # -----------------------------------------------------
    # 탭 2: 조회
    # -----------------------------------------------------
    with tabs[1]:
        if st.button("내역 조회하기"):
            try:
                wb = client.open(SPREADSHEET_NAME)
                df = pd.DataFrame(wb.worksheet("운영일지").get_all_records())
                my_df = df[df['이름'] == my_name]
                st.dataframe(my_df)
            except: st.error("데이터 없음")

    # -----------------------------------------------------
    # 탭 3: 계획
    # -----------------------------------------------------
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

    # -----------------------------------------------------
    # 탭 4: 검토 (조장/관리자)
    # -----------------------------------------------------
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

    # -----------------------------------------------------
    # 탭 5: 통계 (관리자)
    # -----------------------------------------------------
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
