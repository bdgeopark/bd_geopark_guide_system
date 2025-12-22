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

if 'logged_in' not in st.session_state:
    st.session_state['logged_in'] = False
if 'user_info' not in st.session_state:
    st.session_state['user_info'] = {}

@st.cache_resource
def get_client():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    try:
        creds = ServiceAccountCredentials.from_json_keyfile_name("geopark_key.json", scope)
    except:
        try:
            key_dict = st.secrets["gcp_service_account"]
            creds = ServiceAccountCredentials.from_json_keyfile_dict(key_dict, scope)
        except Exception as e:
            st.error(f"⚠️ 인증 키 오류: {e}")
            return None
    return gspread.authorize(creds)

client = get_client()
SPREADSHEET_NAME = "지질공원_운영일지_DB"

locations = {
    "백령도": ["두무진 안내소", "콩돌해안 안내소", "사곶해변 안내소", "용기포신항 안내소", "진촌리 현무암 안내소", "용틀임바위 안내소", "임시지질공원센터"],
    "대청도": ["서풍받이 안내소", "옥죽동 해안사구 안내소", "농여해변 안내소", "선진동 선착장 안내소"],
    "소청도": ["분바위 안내소", "탑동 선착장 안내소"],
    "본부": ["지질공원 사무실"],
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
        st.error("아이디 또는 비밀번호가 틀렸습니다.")
    except Exception as e:
        st.error(f"로그인 오류: {e}")

# (기존) 단건 저장
def save_log(data):
    try:
        sheet = client.open(SPREADSHEET_NAME).worksheet("운영일지")
        sheet.append_row(data)
        return True
    except Exception as e:
        st.error(f"저장 실패: {e}")
        return False

# (신규) 실적 일괄 저장
def save_log_bulk(rows):
    try:
        sheet = client.open(SPREADSHEET_NAME).worksheet("운영일지")
        sheet.append_rows(rows)
        return True
    except Exception as e:
        st.error(f"일괄 저장 실패: {e}")
        return False

def save_plan_bulk(rows):
    try:
        sheet = client.open(SPREADSHEET_NAME).worksheet("월간계획")
        sheet.append_rows(rows)
        return True
    except gspread.exceptions.WorksheetNotFound:
        st.error("🚨 '월간계획' 시트가 없습니다.")
        return False
    except Exception as e:
        st.error(f"저장 실패: {e}")
        return False

def update_status_to_approve(target_indices):
    try:
        sheet = client.open(SPREADSHEET_NAME).worksheet("운영일지")
        for idx in target_indices:
            row_num = idx + 2 
            sheet.update_cell(row_num, 10, "승인완료") 
        return True
    except Exception as e:
        st.error(f"승인 실패: {e}")
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
            st.rerun()
    
    st.title(f"📱 {my_name}님의 업무공간")

    tabs_list = ["📝 활동 입력", "📅 내 활동 조회", "🗓️ 다음달 계획"]
    if my_role in ["조장", "관리자"]:
        tabs_list.append("👀 조원 활동/계획 검토")
    if my_role == "관리자":
        tabs_list.append("📊 관리자 대시보드")

    tabs = st.tabs(tabs_list)

    # -----------------------------------------------------
    # 탭 1: 활동 입력 (★ 일괄 입력 기능 추가됨)
    # -----------------------------------------------------
    with tabs[0]:
        st.subheader("활동 실적 등록")
        
        # 입력 방식 선택 (라디오 버튼)
        input_mode = st.radio("입력 방식", ["하루씩 입력 (기본)", "기간 일괄 입력 (과거 실적용)"], horizontal=True)
        st.divider()

        if input_mode == "하루씩 입력 (기본)":
            # --- 기존 방식 ---
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
            # --- 일괄 입력 방식 (신규) ---
            st.info("💡 과거 데이터를 입력할 때 유용합니다. 선택한 날짜들에 **동일한 실적**이 입력됩니다.")
            
            col_y, col_m = st.columns(2)
            with col_y:
                target_year = st.number_input("년도", value=datetime.now().year)
            with col_m:
                target_month = st.number_input("월", value=datetime.now().month, min_value=1, max_value=12)

            period_type = st.radio("기간 선택", ["전반기 (1일 ~ 15일)", "후반기 (16일 ~ 말일)"], horizontal=True, key="act_period")
            
            # 공통 정보 입력
            st.markdown("##### 📌 공통 입력 사항")
            if my_role == "관리자":
                sel_island = st.selectbox("섬", list(locations.keys()), key="act_island")
            else:
                sel_island = my_island
            
            sel_place = st.selectbox("장소", locations.get(sel_island, ["장소없음"]), key="act_place")
            
            c1, c2 = st.columns(2)
            with c1:
                w_hours = st.number_input("활동 시간", min_value=0, value=8, key="act_hours")
                listeners = st.number_input("해설 청취자(명)", min_value=0, key="act_listen")
            with c2:
                visitors = st.number_input("방문객(명)", min_value=0, key="act_visit")
                counts = st.number_input("해설 횟수(회)", min_value=0, key="act_count")

            # 날짜 선택기
            _, last_day = calendar.monthrange(target_year, target_month)
            if "전반기" in period_type:
                day_range = range(1, 16)
            else:
                day_range = range(16, last_day + 1)
            
            day_options = []
            for d in day_range:
                dt = datetime(target_year, target_month, d)
                day_str = dt.strftime("%d일 (%a)")
                day_options.append(day_str)

            st.write("▼ **활동했던 날짜**를 모두 선택하세요")
            selected_days_str = st.multiselect("날짜 선택", day_options, key="act_dates")

            if st.button(f"{len(selected_days_str)}건 일괄 등록"):
                if not selected_days_str:
                    st.warning("날짜를 선택해주세요.")
                else:
                    with st.spinner("과거 데이터 입력 중..."):
                        rows_to_add = []
                        for s in selected_days_str:
                            day_num = int(s.split("일")[0])
                            real_date = datetime(target_year, target_month, day_num).strftime("%Y-%m-%d")
                            # 날짜, 섬, 장소, 이름, 시간, 방문, 청취, 횟수, 타임스탬프, 상태
                            rows_to_add.append([
                                real_date, sel_island, sel_place, my_name, 
                                w_hours, visitors, listeners, counts, 
                                str(datetime.now()), "검토대기"
                            ])
                        
                        if save_log_bulk(rows_to_add):
                            st.success(f"✅ 총 {len(rows_to_add)}일치 활동 기록이 등록되었습니다!")
                            time.sleep(1)
                            st.rerun()

    # 탭 2: 내 활동 조회
    with tabs[1]:
        st.subheader("내 과거 기록 확인")
        if st.button("내역 불러오기"):
            try:
                sheet = client.open(SPREADSHEET_NAME).worksheet("운영일지")
                df = pd.DataFrame(sheet.get_all_records())
                my_df = df[df['이름'] == my_name]
                if not my_df.empty:
                    # 날짜순 정렬
                    if '날짜' in my_df.columns:
                        my_df['날짜'] = pd.to_datetime(my_df['날짜'])
                        my_df = my_df.sort_values(by='날짜', ascending=False)
                    st.dataframe(my_df)
                else:
                    st.info("기록이 없습니다.")
            except:
                st.error("데이터 로드 실패")

    # 탭 3: 계획
    with tabs[2]:
        st.subheader("🗓️ 근무 계획 일괄 등록")
        col_y, col_m = st.columns(2)
        with col_y:
            plan_year = st.number_input("년도", value=datetime.now().year, key="plan_y")
        with col_m:
            plan_month = st.number_input("월", value=datetime.now().month, min_value=1, max_value=12, key="plan_m")

        period_type = st.radio("기간 선택", ["전반기 (1일 ~ 15일)", "후반기 (16일 ~ 말일)"], horizontal=True, key="plan_period")
        plan_place = st.selectbox("예정 근무지", locations.get(my_island, ["-"]), key="plan_place")
        plan_note = st.text_input("비고 (특이사항)", key="plan_note")

        _, last_day = calendar.monthrange(plan_year, plan_month)
        if "전반기" in period_type:
            day_range = range(1, 16)
        else:
            day_range = range(16, last_day + 1)
        
        day_options = []
        for d in day_range:
            dt = datetime(plan_year, plan_month, d)
            day_str = dt.strftime("%d일 (%a)")
            day_options.append(day_str)

        st.write("▼ 근무할 날짜를 터치해서 선택하세요")
        selected_days_str = st.multiselect("날짜 선택 (여러 개 가능)", day_options, key="plan_dates")

        if st.button(f"{len(selected_days_str)}일치 계획 제출", key="plan_btn"):
            if not selected_days_str:
                st.warning("⚠️ 날짜를 선택해주세요.")
            else:
                with st.spinner("저장 중..."):
                    rows_to_add = []
                    for s in selected_days_str:
                        day_num = int(s.split("일")[0])
                        real_date = datetime(plan_year, plan_month, day_num).strftime("%Y-%m-%d")
                        rows_to_add.append([real_date, my_island, plan_place, my_name, plan_note, str(datetime.now())])
                    
                    if save_plan_bulk(rows_to_add):
                        st.success(f"✅ {len(rows_to_add)}건 등록 완료!")
                        time.sleep(1)
                        st.rerun()

    # 탭 4: 검토
    if my_role in ["조장", "관리자"]:
        with tabs[3]:
            st.subheader("👀 조원 활동/계획 검토")
            check_type = st.radio("확인할 항목:", ["✅ 활동 내역 (승인)", "📅 월간 계획 (조회)"], horizontal=True)
            st.divider()

            if "활동 내역" in check_type
