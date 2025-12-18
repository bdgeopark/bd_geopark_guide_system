import streamlit as st
import pandas as pd
from datetime import datetime, date, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time

# ---------------------------------------------------------
# 1. 시스템 설정 및 연결
# ---------------------------------------------------------
st.set_page_config(page_title="지질공원 통합관리", page_icon="🪨", layout="wide")

# 세션 상태 초기화
if 'logged_in' not in st.session_state:
    st.session_state['logged_in'] = False
if 'user_info' not in st.session_state:
    st.session_state['user_info'] = {}

# 구글 시트 연결 (캐시 적용)
@st.cache_resource
def get_client():
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("geopark_key.json", scope)
        return gspread.authorize(creds)
    except Exception as e:
        st.error(f"⚠️ 키 파일 오류: {e}")
        return None

client = get_client()
SPREADSHEET_NAME = "지질공원_운영일지_DB"

# 장소 데이터
locations = {
    "백령도": ["두무진 안내소", "콩돌해안 안내소", "사곶해변 안내소", "용기포신항 안내소", "진촌리 현무암 안내소", "용틀임바위 안내소", "임시지질공원센터"],
    "대청도": ["서풍받이 안내소", "옥죽동 해안사구 안내소", "농여해변 안내소", "선진동 선착장 안내소"],
    "소청도": ["분바위 안내소", "탑동 선착장 안내소"],
    "본부": ["지질공원 사무실"],
    "시청": ["인천시청", "지질공원팀 사무실"]  # ✅ 여기가 추가되었습니다!
}

# ---------------------------------------------------------
# 2. 기능 함수 모음
# ---------------------------------------------------------
def login(username, password):
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
        st.error("아이디 또는 비밀번호가 틀렸습니다.")
    except Exception as e:
        st.error(f"로그인 오류: {e} ('사용자' 시트가 있는지 확인하세요)")

def save_log(data):
    try:
        sheet = client.open(SPREADSHEET_NAME).worksheet("운영일지")
        sheet.append_row(data)
        return True
    except:
        return False

def save_plan(data):
    try:
        sheet = client.open(SPREADSHEET_NAME).worksheet("월간계획")
        sheet.append_row(data)
        return True
    except:
        return False

# ---------------------------------------------------------
# 3. 메인 화면 로직
# ---------------------------------------------------------

# (A) 로그인 전 화면
if not st.session_state['logged_in']:
    st.markdown("## 🔐 백령·대청 지질공원 로그인")
    with st.form("login_form"):
        uid = st.text_input("아이디")
        upw = st.text_input("비밀번호", type="password")
        if st.form_submit_button("로그인"):
            login(uid, upw)

# (B) 로그인 후 화면
else:
    user = st.session_state['user_info']
    my_name = user['이름']
    my_island = user['섬']
    my_role = user['직책'] # 해설사, 조장, 관리자

    # 사이드바
    with st.sidebar:
        st.info(f"👤 **{my_name}** ({my_role})")
        st.caption(f"📍 소속: {my_island}")
        if st.button("로그아웃"):
            st.session_state['logged_in'] = False
            st.rerun()
    
    st.title(f"📱 {my_name}님의 업무공간")

    # 탭 메뉴 구성 (직책에 따라 다르게 보임)
    tabs_list = ["📝 활동 입력", "📅 내 활동 조회", "🗓️ 다음달 계획"]
    
    if my_role == "조장":
        tabs_list.append("👀 조원 활동 검토")
    if my_role == "관리자":
        tabs_list.append("👀 조원 활동 검토") # 관리자도 검토 탭 볼 수 있게
        tabs_list.append("📊 관리자 통계")

    tabs = st.tabs(tabs_list)

    # -----------------------------------------------------
    # 탭 1: 활동 입력 (공통)
    # -----------------------------------------------------
    with tabs[0]:
        st.subheader("오늘 활동 기록")
        c1, c2 = st.columns(2)
        with c1:
            input_date = st.date_input("날짜", datetime.now())
        with c2:
            # 본부 관리자는 섬을 선택할 수 있게, 해설사는 본인 섬 고정
            if my_role == "관리자":
                sel_island = st.selectbox("섬", list(locations.keys()))
            else:
                sel_island = my_island
                st.success(f"📍 {sel_island} (자동선택)")
        
        sel_place = st.selectbox("장소", locations.get(sel_island, ["장소없음"]))
        
        c3, c4 = st.columns(2)
        with c3:
            w_hours = st.number_input("활동 시간", min_value=0, value=8)
        with c4:
            visitors = st.number_input("방문객(명)", min_value=0)
            
        listeners = st.number_input("해설 청취자(명)", min_value=0)
        counts = st.number_input("해설 횟수(회)", min_value=0)

        if st.button("저장하기", type="primary"):
            row = [str(input_date), sel_island, sel_place, my_name, w_hours, visitors, listeners, counts, str(datetime.now()), "검토대기"]
            if save_log(row):
                st.success("✅ 저장되었습니다!")
            else:
                st.error("저장 실패 (시트 연결 확인 필요)")

    # -----------------------------------------------------
    # 탭 2: 내 활동 조회 (공통)
    # -----------------------------------------------------
    with tabs[1]:
        st.subheader("내 과거 기록 확인")
        if st.button("내역 불러오기"):
            try:
                sheet = client.open(SPREADSHEET_NAME).worksheet("운영일지")
                data = sheet.get_all_records()
                df = pd.DataFrame(data)
                
                # 내 이름으로 필터링
                my_df = df[df['이름'] == my_name]
                
                if not my_df.empty:
                    st.write(f"총 {len(my_df)}건의 활동이 있습니다.")
                    st.dataframe(my_df)
                else:
                    st.info("기록이 없습니다.")
            except:
                st.error("데이터를 불러오지 못했습니다.")

    # -----------------------------------------------------
    # 탭 3: 다음달 계획 (공통)
    # -----------------------------------------------------
    with tabs[2]:
        st.subheader("🗓️ 다음 달 근무 계획 제출")
        st.info("미리 계획을 입력해두면 조장님이 확인합니다.")
        
        p_date = st.date_input("계획 날짜", datetime.now() + timedelta(days=30))
        p_place = st.selectbox("예정 장소", locations.get(my_island, ["-"]))
        p_note = st.text_input("비고 (특이사항)")
        
        if st.button("계획 제출하기"):
            plan_row = [str(p_date), my_island, p_place, my_name, p_note]
            if save_plan(plan_row):
                st.success("계획이 제출되었습니다!")
            else:
                st.error("제출 실패 ('월간계획' 시트 확인)")

    # -----------------------------------------------------
    # 탭 4: 조장/관리자용 검토 (조건부)
    # -----------------------------------------------------
    if my_role in ["조장", "관리자"]:
        with tabs[3]:
            st.subheader(f"👮‍♂️ {my_island} 조원 활동 모니터링")
            
            if st.button("조원 활동내역 조회"):
                try:
                    sheet = client.open(SPREADSHEET_NAME).worksheet("운영일지")
                    data = sheet.get_all_records()
                    df = pd.DataFrame(data)
                    
                    # 관리자는 전체, 조장은 자기 섬만
                    if my_role == "관리자":
                        target_df = df
                    else:
                        target_df = df[df['섬'] == my_island]
                        
                    st.dataframe(target_df)
                    st.info("💡 수정이 필요한 건은 카카오톡으로 안내해주세요.")
                except:
                    st.error("데이터 로드 실패")

    # -----------------------------------------------------
    # 탭 5: 관리자 통계 (관리자만)
    # -----------------------------------------------------
    if my_role == "관리자":
        with tabs[4]:
            st.subheader("📊 전체 운영 통계")
            # (기존 통계 코드 간소화하여 탑재)
            try:
                sheet = client.open(SPREADSHEET_NAME).worksheet("운영일지")
                df = pd.DataFrame(sheet.get_all_records())
                
                if not df.empty:
                    total_v = df['방문자'].sum()
                    st.metric("총 누적 방문객", f"{total_v:,} 명")
                    
                    st.write("▼ 섬별 방문객")
                    st.bar_chart(df.groupby("섬")['방문자'].sum())
            except:
                st.write("데이터가 없습니다.")