import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time
import calendar

# ---------------------------------------------------------
# 1. 시스템 설정
# ---------------------------------------------------------
st.set_page_config(page_title="지질공원 통합관리", page_icon="🪨", layout="wide")

# 세션 상태 초기화 (탭 끊김 방지용)
if 'logged_in' not in st.session_state: st.session_state['logged_in'] = False
if 'user_info' not in st.session_state: st.session_state['user_info'] = {}
if 'input_df' not in st.session_state: st.session_state['input_df'] = None # 입력표 임시저장

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

# 관리자 소속(시청) 포함, 실무 장소만 나열
locations = {
    "백령도": ["두무진 안내소", "콩돌해안 안내소", "사곶해변 안내소", "용기포신항 안내소", "진촌리 현무암 안내소", "용틀임바위 안내소", "임시지질공원센터"],
    "대청도": ["서풍받이 안내소", "옥죽동 해안사구 안내소", "농여해변 안내소", "선진동 선착장 안내소"],
    "소청도": ["분바위 안내소", "탑동 선착장 안내소"],
    "시청": ["인천시청", "지질공원팀 사무실"]
}

# ---------------------------------------------------------
# 2. 핵심 함수
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
        st.error("아이디 불일치")
    except:
        st.error("로그인 오류")

def get_users_by_island(island_name):
    try:
        sheet = client.open(SPREADSHEET_NAME).worksheet("사용자")
        users = sheet.get_all_records()
        # 해당 섬 소속 해설사 이름만 리스트로 반환
        return [u['이름'] for u in users if u.get('섬') == island_name]
    except:
        return []

def save_log_bulk(rows):
    try:
        sheet = client.open(SPREADSHEET_NAME).worksheet("운영일지")
        sheet.append_rows(rows)
        return True
    except:
        return False

def save_plan_bulk(rows):
    try:
        sheet = client.open(SPREADSHEET_NAME).worksheet("월간계획")
        sheet.append_rows(rows)
        return True
    except:
        return False

def update_status_to_approve(target_indices):
    try:
        sheet = client.open(SPREADSHEET_NAME).worksheet("운영일지")
        for idx in target_indices:
            sheet.update_cell(idx + 2, 10, "승인완료") 
        return True
    except:
        return False

# ---------------------------------------------------------
# 3. 화면 구성
# ---------------------------------------------------------
if not st.session_state['logged_in']:
    st.markdown("## 🔐 백령·대청 지질공원 로그인")
    with st.form("login_form"):
        uid = st.text_input("아이디")
        upw = st.text_input("비밀번호", type="password")
        if st.form_submit_button("로그인"):
            login(uid, upw)
else:
    user = st.session_state['user_info']
    my_name = user['이름']
    my_island = user['섬']
    my_role = user['직책']

    with st.sidebar:
        st.info(f"👤 **{my_name}** ({my_role})")
        st.caption(f"📍 소속: {my_island}")
        if st.button("로그아웃"):
            st.session_state['logged_in'] = False
            st.session_state['input_df'] = None
            st.rerun()

    st.title(f"📱 {my_name}님의 업무공간")

    # 탭 구성
    tabs_list = ["📝 활동 입력", "📅 내 활동 조회", "🗓️ 다음달 계획"]
    if my_role in ["조장", "관리자"]:
        tabs_list.append("👀 조원 검토")
    if my_role == "관리자":
        tabs_list.append("📊 통계")

    tabs = st.tabs(tabs_list)

    # -----------------------------------------------------
    # 탭 1: 활동 입력
    # -----------------------------------------------------
    with tabs[0]:
        st.subheader("활동 실적 등록")

        if my_role == "관리자":
            sel_island = st.selectbox("📍 섬 선택", list(locations.keys()))
        else:
            sel_island = my_island
        
        # 해당 섬의 해설사 목록 가져오기 (드롭다운용)
        island_users = get_users_by_island(sel_island)
        if not island_users: island_users = ["등록된 해설사 없음"]

        st.divider()
        input_mode = st.radio("입력 방식", ["하루씩 입력", "일괄 입력 (엑셀형)"], horizontal=True)

        if input_mode == "하루씩 입력":
            # (기존 단건 입력 유지)
            c1, c2 = st.columns(2)
            with c1: input_date = st.date_input("날짜", datetime.now())
            with c2: sel_place = st.selectbox("장소", locations.get(sel_island, ["-"]))
            
            target_name = st.selectbox("해설사", island_users) if my_role=="관리자" else my_name
            
            c3, c4 = st.columns(2)
            with c3: w_hours = st.number_input("활동시간", 8)
            with c4: visitors = st.number_input("방문자", 0)
            
            listeners = st.number_input("청취자", 0)
            counts = st.number_input("해설횟수", 0)

            if st.button("저장하기"):
                row = [str(input_date), sel_island, sel_place, target_name, w_hours, visitors, listeners, counts, str(datetime.now()), "검토대기"]
                if save_log_bulk([row]): st.success("저장 완료!")

        else:
            # ★ 엑셀형 일괄 입력 (시간 선택 기능 포함)
            c1, c2, c3 = st.columns([1,1,2])
            with c1: t_year = st.number_input("년", value=datetime.now().year)
            with c2: t_month = st.number_input("월", value=datetime.now().month)
            with c3: period = st.radio("기간", ["전반기(1-15)", "후반기(16-말일)"], horizontal=True)
            
            sel_place_bulk = st.selectbox("근무 장소(공통)", locations.get(sel_island, ["-"]))

            # [서식 만들기] 버튼을 눌러야 표가 생성됨 (탭 끊김 방지)
            if st.button("📄 빈 서식 만들기 (클릭)"):
                _, last = calendar.monthrange(t_year, t_month)
                rng = range(1, 16) if "전반기" in period else range(16, last + 1)
                
                data = []
                for d in rng:
                    # 날짜, 해설사(빈칸), 시간(8), 나머지0
                    dt_str = datetime(t_year, t_month, d).strftime("%Y-%m-%d")
                    data.append([dt_str, None, "8시간", 0, 0, 0, 0])
                
                # 세션에 저장 (새로고침 되어도 유지)
                st.session_state['input_df'] = pd.DataFrame(data, columns=["일자", "해설사", "활동시간", "시간(직접)", "방문자", "청취자", "해설횟수"])

            # 표 보여주기
            if st.session_state['input_df'] is not None:
                st.info("👇 **해설사**를 선택하고, **시간**을 조정하세요. (동일 날짜 추가는 표 하단 `+` 클릭)")
                
                edited_df = st.data_editor(
                    st.session_state['input_df'],
                    column_config={
                        "일자": st.column_config.TextColumn("일자", width="small"),
                        "해설사": st.column_config.SelectboxColumn("해설사(선택)", options=island_users, width="medium", required=True),
                        # ★ 여기가 시간 선택 핵심입니다!
                        "활동시간": st.column_config.SelectboxColumn("활동시간", options=["8시간", "4시간", "직접입력"], default="8시간"),
                        "시간(직접)": st.column_config.NumberColumn("입력", min_value=0, max_value=24, width="small"),
                        "방문자": st.column_config.NumberColumn("방문자", min_value=0),
                        "청취자": st.column_config.NumberColumn("청취자", min_value=0),
                        "해설횟수": st.column_config.NumberColumn("해설횟수", min_value=0),
                    },
                    num_rows="dynamic", # ★ 행 추가 가능
                    use_container_width=True,
                    hide_index=True
                )

                if st.button("작성한 내용 일괄 저장"):
                    valid_rows = edited_df[edited_df["해설사"].notnull()]
                    
                    if valid_rows.empty:
                        st.warning("⚠️ 해설사가 지정된 데이터가 없습니다.")
                    else:
                        rows_to_save = []
                        for _, row in valid_rows.iterrows():
                            # 시간 계산
                            fh = 8
                            if row["활동시간"] == "4시간": fh = 4
                            elif row["활동시간"] == "직접입력": fh = row["시간(직접)"]

                            rows_to_save.append([
                                row["일자"], sel_island, sel_place_bulk, row["해설사"], 
                                fh, row["방문자"], row["청취자"], row["해설횟수"], 
                                str(datetime.now()), "검토대기"
                            ])
                        
                        if save_log_bulk(rows_to_save):
                            st.success(f"✅ 총 {len(rows_to_save)}건 저장 완료!")
                            st.session_state['input_df'] = None # 저장 후 표 초기화
                            time.sleep(1)
                            st.rerun()

    # -----------------------------------------------------
    # 탭 2: 조회
    # -----------------------------------------------------
    with tabs[1]:
        if st.button("내역 조회"):
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
        with c1: p_year = st.number_input("년", 2025)
        with c2: p_month = st.number_input("월", datetime.now().month)
        p_period = st.radio("기간", ["전반기", "후반기"], key="p_per")
        p_place = st.selectbox("예정지", locations.get(my_island, ["-"]))
        
        _, last = calendar.monthrange(p_year, p_month)
        rng = range(1, 16) if "전반기" in p_period else range(16, last+1)
        
        opts = [f"{d}일" for d in rng]
        sels = st.multiselect("근무일 선택", opts)
        
        if st.button("계획 제출"):
            if sels:
                rows = []
                for s in sels:
                    d_num = int(s.replace("일",""))
                    dt = datetime(p_year, p_month, d_num).strftime("%Y-%m-%d")
                    rows.append([dt, my_island, p_place, my_name, "", str(datetime.now())])
                if save_plan_bulk(rows): st.success("제출 완료")

    # -----------------------------------------------------
    # 탭 4: 검토
    # -----------------------------------------------------
    if my_role in ["조장", "관리자"]:
        with tabs[3]:
            st.subheader("승인 관리")
            if st.button("검토 목록 불러오기"):
                try:
                    wb = client.open(SPREADSHEET_NAME)
                    df = pd.DataFrame(wb.worksheet("운영일지").get_all_records())
                    if my_role != "관리자": df = df[df['섬'] == my_island]
                    df = df[df['상태'] == "검토대기"]
                    
                    if not df.empty:
                        st.dataframe(df)
                        indices = df.index.tolist()
                        if st.button("조회된 모든 항목 일괄 승인"):
                            if update_status_to_approve(indices): st.success("승인 완료")
                    else:
                        st.info("대기중인 건이 없습니다.")
                except: st.error("오류")

    # -----------------------------------------------------
    # 탭 5: 통계
    # -----------------------------------------------------
    if my_role == "관리자":
        with tabs[4]:
            if st.button("통계 산출"):
                try:
                    wb = client.open(SPREADSHEET_NAME)
                    df = pd.DataFrame(wb.worksheet("운영일지").get_all_records())
                    
                    for col in ['방문자', '해설횟수']:
                        if col in df.columns:
                            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                        else:
                            df[col] = 0
                            
                    st.metric("총 방문객", int(df['방문자'].sum()))
                    if '섬' in df.columns:
                        st.bar_chart(df.groupby("섬")['방문자'].sum())
                except: st.error("데이터 로드 실패")
