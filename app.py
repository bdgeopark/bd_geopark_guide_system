import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time
import calendar

# ---------------------------------------------------------
# 1. 시스템 설정 및 구글 연결
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
        # 1순위: 로컬 파일
        creds = ServiceAccountCredentials.from_json_keyfile_name("geopark_key.json", scope)
    except:
        try:
            # 2순위: 스트림릿 Secrets
            key_dict = st.secrets["gcp_service_account"]
            creds = ServiceAccountCredentials.from_json_keyfile_dict(key_dict, scope)
        except Exception as e:
            st.error(f"⚠️ 인증 키 오류: {e}")
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

def save_plan_bulk(rows):
    try:
        sheet = client.open(SPREADSHEET_NAME).worksheet("월간계획")
        for row in rows:
            sheet.append_row(row)
        return True
    except:
        return False

def update_status_to_approve(target_indices):
    try:
        sheet = client.open(SPREADSHEET_NAME).worksheet("운영일지")
        # 헤더가 1행이므로, 데이터 인덱스 + 2가 실제 행 번호 (0부터 시작하므로)
        for idx in target_indices:
            # 상태 컬럼이 J열(10번째)라고 가정 (날짜,섬,장소,이름,시간,방문,청취,횟수,타임스탬프,상태)
            row_num = idx + 2 
            sheet.update_cell(row_num, 10, "승인완료") 
        return True
    except Exception as e:
        st.error(f"업데이트 실패: {e}")
        return False

# ---------------------------------------------------------
# 3. 메인 화면 로직
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
            st.rerun()
    
    st.title(f"📱 {my_name}님의 업무공간")

    tabs_list = ["📝 활동 입력", "📅 내 활동 조회", "🗓️ 다음달 계획"]
    
    # 조장이나 관리자만 '조원 활동 검토' 탭 보이기
    if my_role in ["조장", "관리자"]:
        tabs_list.append("👀 조원 활동 검토")
    
    if my_role == "관리자":
        tabs_list.append("📊 관리자 통계")

    tabs = st.tabs(tabs_list)

    # -----------------------------------------------------
    # 탭 1: 활동 입력
    # -----------------------------------------------------
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

    # -----------------------------------------------------
    # 탭 2: 내 활동 조회
    # -----------------------------------------------------
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

    # -----------------------------------------------------
    # 탭 3: 계획 (핸드폰 최적화 버전)
    # -----------------------------------------------------
    with tabs[2]:
        st.subheader("🗓️ 근무 계획 일괄 등록")
        
        col_y, col_m = st.columns(2)
        with col_y:
            plan_year = st.number_input("년도", value=datetime.now().year)
        with col_m:
            plan_month = st.number_input("월", value=datetime.now().month, min_value=1, max_value=12)

        # 기간 선택
        period_type = st.radio("기간 선택", ["전반기 (1일 ~ 15일)", "후반기 (16일 ~ 말일)"], horizontal=True)
        
        # 장소 선택
        plan_place = st.selectbox("예정 근무지", locations.get(my_island, ["-"]))
        plan_note = st.text_input("비고 (특이사항)")

        # 날짜 계산
        _, last_day = calendar.monthrange(plan_year, plan_month)
        if "전반기" in period_type:
            day_range = range(1, 16)
        else:
            day_range = range(16, last_day + 1)
        
        # ★ 핸드폰 최적화: 체크박스 대신 '멀티 선택 박스' 사용
        day_options = []
        for d in day_range:
            # 날짜를 "5일 (금)" 형태로 예쁘게 만듦
            dt = datetime(plan_year, plan_month, d)
            day_str = dt.strftime("%d일 (%a)")
            day_options.append(day_str)

        st.write("▼ 근무할 날짜를 터치해서 선택하세요")
        selected_days_str = st.multiselect("날짜 선택 (여러 개 가능)", day_options)

        if st.button(f"{len(selected_days_str)}일치 계획 제출"):
            if not selected_days_str:
                st.warning("날짜를 선택해주세요.")
            else:
                rows_to_add = []
                # 선택된 날짜 문자열("5일 (금)")을 다시 실제 날짜("2025-0
