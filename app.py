import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time
import calendar

# ---------------------------------------------------------
# 1. 시스템 설정 (에러 확인 강화 버전)
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
# 2. 핵심 함수 (여기를 고쳤습니다!)
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
    except Exception as e:
        st.error(f"저장 실패 원인: {e}") # 에러 메시지 출력
        return False

def save_plan_bulk(rows):
    try:
        # ★ 수정됨: 월간계획 시트가 없으면 에러를 띄움
        sheet = client.open(SPREADSHEET_NAME).worksheet("월간계획")
        
        # ★ 수정됨: 한 줄씩 넣지 않고 '한 방에' 넣기 (속도 10배 향상)
        sheet.append_rows(rows) 
        return True
    except gspread.exceptions.WorksheetNotFound:
        st.error("🚨 오류: 구글 시트에 '월간계획'이라는 탭이 없습니다! 시트를 먼저 만들어주세요.")
        return False
    except Exception as e:
        st.error(f"🚨 저장 중 알 수 없는 오류 발생: {e}")
        return False

def update_status_to_approve(target_indices):
    try:
        sheet = client.open(SPREADSHEET_NAME).worksheet("운영일지")
        for idx in target_indices:
            row_num = idx + 2 
            sheet.update_cell(row_num, 10, "승인완료") 
        return True
    except Exception as e:
        st.error(f"업데이트 실패: {e}")
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
                pass # 에러 메시지는 함수 안에서 출력됨

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

    # 탭 3: 계획 (핸드폰 최적화 + 디버깅 강화)
    with tabs[2]:
        st.subheader("🗓️ 근무 계획 일괄 등록")
        
        col_y, col_m = st.columns(2)
        with col_y:
            plan_year = st.number_input("년도", value=datetime.now().year)
        with col_m:
            plan_month = st.number_input("월", value=datetime.now().month, min_value=1, max_value=12)

        period_type = st.radio("기간 선택", ["전반기 (1일 ~ 15일)", "후반기 (16일 ~ 말일)"], horizontal=True)
        plan_place = st.selectbox("예정 근무지", locations.get(my_island, ["-"]))
        plan_note = st.text_input("비고 (특이사항)")

        _, last_day = calendar.monthrange(plan_year, plan_month)
        if "전반기" in period_type:
            day_range = range(1, 16)
        else:
            day_range = range(16, last_day + 1)
        
        day_options = []
        for d in day_range:
            dt = datetime(plan_year, plan_month, d)
            # 포맷 통일: 05일 (Mon) 형식
            day_str = dt.strftime("%d일 (%a)")
            day_options.append(day_str)

        st.write("▼ 근무할 날짜를 터치해서 선택하세요")
        selected_days_str = st.multiselect("날짜 선택 (여러 개 가능)", day_options)

        if st.button(f"{len(selected_days_str)}일치 계획 제출"):
            if not selected_days_str:
                st.warning("⚠️ 날짜를 하나 이상 선택해주세요.")
            else:
                with st.spinner("저장 중입니다... 잠시만 기다려주세요."):
                    rows_to_add = []
                    for s in selected_days_str:
                        # "05일 (Mon)"에서 숫자만 추출
                        try:
                            day_num = int(s.split("일")[0])
                            real_date = datetime(plan_year, plan_month, day_num).strftime("%Y-%m-%d")
                            rows_to_add.append([real_date, my_island, plan_place, my_name, plan_note, str(datetime.now())])
                        except Exception as e:
                            st.error(f"날짜 변환 오류: {s} / 원인: {e}")
                    
                    # 일괄 저장 시도
                    if save_plan_bulk(rows_to_add):
                        st.success(f"✅ {len(rows_to_add)}건의 계획이 성공적으로 등록되었습니다!")
                        time.sleep(2)
                        st.rerun()

    # 탭 4: 검토
    if my_role in ["조장", "관리자"]:
        with tabs[3]:
            st.subheader("👀 조원 활동 승인")
            try:
                sheet = client.open(SPREADSHEET_NAME).worksheet("운영일지")
                all_data = sheet.get_all_records()
                df = pd.DataFrame(all_data)
                
                if my_role != "관리자":
                    df = df[df['섬'] == my_island]
                
                view_option = st.radio("보기 방식", ["전체 보기", "검토 대기 건만 보기"], horizontal=True)
                
                if view_option == "검토 대기 건만 보기":
                    display_df = df[df['상태'] == "검토대기"]
                else:
                    display_df = df
                
                st.dataframe(display_df)

                st.divider()
                st.write("### 📢 승인 처리")
                pending_df = df[df['상태'] == "검토대기"]
                
                if pending_df.empty:
                    st.info("승인 대기 중인 활동이 없습니다.")
                else:
                    pending_indices = pending_df.index.tolist()
                    selected_indices = st.multiselect(
                        "승인할 목록 선택:",
                        options=pending_indices,
                        format_func=lambda x: f"{df.loc[x]['날짜']} - {df.loc[x]['이름']} ({df.loc[x]['장소']})"
                    )
                    
                    if st.button("선택한 항목 승인하기"):
                        if update_status_to_approve(selected_indices):
                            st.success("승인 완료!")
                            time.sleep(1)
                            st.rerun()
            except Exception as e:
                st.error(f"데이터 로드 실패: {e}")

    # 탭 5: 통계
    if my_role == "관리자":
        with tabs[4]:
            st.subheader("운영 통계")
            try:
                sheet = client.open(SPREADSHEET_NAME).worksheet("운영일지")
                df = pd.DataFrame(sheet.get_all_records())
                if not df.empty:
                    c1, c2 = st.columns(2)
                    c1.metric("총 방문객", f"{df['방문자'].sum():,}명")
                    c1.metric("총 해설 횟수", f"{df['횟수'].sum():,}회")
                    st.bar_chart(df.groupby("섬")['방문자'].sum())
            except:
                st.write("데이터 없음")
