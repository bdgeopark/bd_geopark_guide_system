import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
import time

# ---------------------------------------------------------
# 1. 시스템 설정 및 구글 연결 (클라우드/로컬 호환 버전)
# ---------------------------------------------------------
st.set_page_config(page_title="지질공원 통합관리", page_icon="🪨", layout="wide")

if 'logged_in' not in st.session_state:
    st.session_state['logged_in'] = False
if 'user_info' not in st.session_state:
    st.session_state['user_info'] = {}

@st.cache_resource
def get_client():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    try:
        # 1순위: 내 컴퓨터에 파일이 있는지 확인 (로컬용)
        creds = ServiceAccountCredentials.from_json_keyfile_name("geopark_key.json", scope)
    except:
        try:
            # 2순위: 파일이 없으면 스트림릿 클라우드 Secrets에서 키를 찾음 (배포용)
            # secrets.toml에 [private_key] 섹션 아래 json_key가 있어야 함
            key_dict = json.loads(st.secrets["private_key"]["json_key"])
            creds = ServiceAccountCredentials.from_json_keyfile_dict(key_dict, scope)
        except Exception as e:
            st.error(f"⚠️ 인증 키 오류: Secrets 설정을 확인해주세요. ({e})")
            return None
            
    return gspread.authorize(creds)

client = get_client()
SPREADSHEET_NAME = "지질공원_운영일지_DB"

# 장소 데이터
locations = {
    "백령도": ["두무진 안내소", "콩돌해안 안내소", "사곶해변 안내소", "용기포신항 안내소", "진촌리 현무암 안내소", "용틀임바위 안내소", "임시지질공원센터"],
    "대청도": ["서풍받이 안내소", "옥죽동 해안사구 안내소", "농여해변 안내소", "선진동 선착장 안내소"],
    "소청도": ["분바위 안내소", "탑동 선착장 안내소"],
    "본부": ["지질공원 사무실"],
    "시청": ["인천시청", "지질공원팀 사무실"]
}

# ---------------------------------------------------------
# 2. 기능 함수 모음
# ---------------------------------------------------------
def login(username, password):
    try:
        if client is None:
            st.error("구글 시트 연결에 실패했습니다.")
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
        st.error("아이디 또는 비밀번호가 틀렸습니다.")
    except Exception as e:
        st.error(f"로그인 오류: {e}")

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
if not st.session_state['logged_in']:
    st.markdown("## 🔐 백령·대청 지질공원 로그인")
    st.info("관리자 및 해설사 전용 접속 페이지입니다.")
    
    with st.form("login_form"):
        uid = st.text_input("아이디")
        upw = st.text_input("비밀번호", type="password")
        submitted = st.form_submit_button("로그인")
        
        if submitted:
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
            st.rerun()
    
    st.title(f"📱 {my_name}님의 업무공간")

    tabs_list = ["📝 활동 입력", "📅 내 활동 조회", "🗓️ 다음달 계획"]
    if my_role in ["조장", "관리자"]:
        tabs_list.append("👀 조원 활동 검토")
    if my_role == "관리자":
        tabs_list.append("📊 관리자 통계")

    tabs = st.tabs(tabs_list)

    # 탭 1: 활동 입력
    with tabs[0]:
        st.subheader("오늘 활동 기록")
        c1, c2 = st.columns(2)
        with c1:
            input_date = st.date_input("날짜", datetime.now())
        with c2:
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
                st.error("저장 실패")

    # 탭 2: 내 활동 조회
    with tabs[1]:
        st.subheader("내 과거 기록 확인")
        if st.button("내역 불러오기"):
            try:
                sheet = client.open(SPREADSHEET_NAME).worksheet("운영일지")
                df = pd.DataFrame(sheet.get_all_records())
                my_df = df[df['이름'] == my_name]
                if not my_df.empty:
                    st.dataframe(my_df)
                else:
                    st.info("기록이 없습니다.")
            except:
                st.error("데이터 로드 실패")

    # 탭 3: 계획
    with tabs[2]:
        st.subheader("🗓️ 다음 달 근무 계획")
        p_date = st.date_input("계획 날짜", datetime.now() + timedelta(days=30))
        p_place = st.selectbox("예정 장소", locations.get(my_island, ["-"]))
        p_note = st.text_input("비고")
        if st.button("계획 제출"):
            if save_plan([str(p_date), my_island, p_place, my_name, p_note]):
                st.success("제출 완료!")
            else:
                st.error("실패")

    # 탭 4: 검토 (조장/관리자)
    if my_role in ["조장", "관리자"]:
        with tabs[3]:
            st.subheader("조원 활동 모니터링")
            if st.button("조회하기"):
                try:
                    sheet = client.open(SPREADSHEET_NAME).worksheet("운영일지")
                    df = pd.DataFrame(sheet.get_all_records())
                    if my_role != "관리자":
                        df = df[df['섬'] == my_island]
                    st.dataframe(df)
                except:
                    st.error("실패")

    # 탭 5: 통계 (관리자)
    if my_role == "관리자":
        with tabs[4]:
            st.subheader("운영 통계")
            try:
                sheet = client.open(SPREADSHEET_NAME).worksheet("운영일지")
                df = pd.DataFrame(sheet.get_all_records())
                if not df.empty:
                    st.metric("총 방문객", f"{df['방문자'].sum():,}명")
                    st.bar_chart(df.groupby("섬")['방문자'].sum())
            except:
                st.write("데이터 없음")
