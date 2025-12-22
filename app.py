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

def get_all_users():
    try:
        sheet = client.open(SPREADSHEET_NAME).worksheet("사용자")
        users = sheet.get_all_records()
        return [u['이름'] for u in users]
    except:
        return []

def save_log(data):
    try:
        sheet = client.open(SPREADSHEET_NAME).worksheet("운영일지")
        sheet.append_row(data)
        return True
    except Exception as e:
        st.error(f"저장 실패: {e}")
        return False

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
    # 탭 1: 활동 입력
    # -----------------------------------------------------
    with tabs[0]:
        st.subheader("활동 실적 등록")
        
        input_mode = st.radio("입력 방식", ["하루씩 입력 (기본)", "기간 일괄 입력 (표 형태)"], horizontal=True)
        st.divider()

        # [관리자 전용] 사용자 선택
        target_name = my_name 
        if my_role == "관리자":
            st.markdown("##### 👑 관리자 모드: 해설사 선택")
            all_users = get_all_users()
            if all_users:
                try:
                    default_idx = all_users.index(my_name)
                except:
                    default_idx = 0
                target_name = st.selectbox("누구의 활동을 입력하시겠습니까?", all_users, index=default_idx)
            else:
                st.warning("사용자 목록 로드 실패")
            st.divider()

        if input_mode == "하루씩 입력 (기본)":
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

            if st.button(f"'{target_name}'님 명의로 저장하기", type="primary"):
                # ★ 여기가 수정되었습니다 (줄바꿈으로 안전하게!)
                row = [
                    str(input_date), sel_island, sel_place, target_name, 
                    w_hours, visitors, listeners, counts, 
                    str(datetime.now()), "검토대기"
                ]
                if save_log(row):
                    st.success(f"✅ {target_name}님의 기록이 저장되었습니다!")

        else:
            # --- 표 형태 일괄 입력 ---
            st.info(f"💡 **'{target_name}'** 님의 근무표를 작성합니다. 근무한 날의 **체크박스**를 선택하세요.")
            
            col_y, col_m = st.columns(2)
            with col_y:
                target_year = st.number_input("년도", value=datetime.now().year)
            with col_m:
                target_month = st.number_input("월", value=datetime.now().month, min_value=1, max_value=12)

            period_type = st.radio("기간 선택", ["전반기 (1일 ~ 15일)", "후반기 (16일 ~ 말일)"], horizontal=True, key="act_period")
            
            # 장소 및 공통 정보
            st.markdown("##### 📌 장소 및 방문객 (일괄 적용)")
            c1, c2 = st.columns(2)
            with c1:
                if my_role == "관리자":
                    sel_island = st.selectbox("섬", list(locations.keys()), key="act_island")
                else:
                    sel_island = my_island
                    st.success(f"📍 {sel_island}")
                sel_place = st.selectbox("장소", locations.get(sel_island, ["장소없음"]), key="act_place")
            with c2:
                visitors = st.number_input("방문객(명)", min_value=0, key="act_visit")
                counts = st.number_input("해설 횟수(회)", min_value=0, key="act_count")
                listeners = st.number_input("해설 청취자(명)", min_value=0, key="act_listen")

            # 날짜 데이터 생성
            _, last_day = calendar.monthrange(target_year, target_month)
            if "전반기" in period_type:
                day_range = range(1, 16)
            else:
                day_range = range(16, last_day + 1)
            
            # 데이터프레임 만들기
            data_list = []
            for d in day_range:
                dt = datetime(target_year, target_month, d)
                day_str = dt.strftime("%Y-%m-%d")
                weekday = dt.strftime("%a")
                # [선택, 날짜, 요일, 시간옵션, 직접입력시간]
                data_list.append([False, day_str, weekday, "8시간", 0])
            
            df_input = pd.DataFrame(data_list, columns=["근무여부", "날짜", "요일", "근무시간", "직접입력(시간)"])

            # 에디터 설정
            edited_df = st.data_editor(
                df_input,
                column_config={
                    "근무여부": st.column_config.CheckboxColumn(
                        "체크 (근무일)",
                        help="근무한 날짜에 체크하세요",
                        default=False,
                    ),
                    "날짜": st.column_config.TextColumn("날짜", disabled=True),
                    "요일": st.column_config.TextColumn("요일", disabled=True),
                    "근무시간": st.column_config.SelectboxColumn(
                        "시간 선택",
                        options=["8시간", "4시간", "직접입력"],
                        required=True,
                        default="8시간"
                    ),
                    "직접입력(시간)": st.column_config.NumberColumn(
                        "직접입력(숫자만)",
                        min_value=0,
                        max_value=24,
                        format="%d시간"
                    )
                },
                hide_index=True,
                use_container_width=True
            )

            if st.button(f"선택한 날짜 일괄 등록"):
                selected_rows = edited_df[edited_df["근무여부"] == True]
                
                if selected_rows.empty:
                    st.warning("⚠️ 근무한 날짜를 하나 이상 체크해주세요.")
                else:
                    rows_to_add = []
                    for index, row in selected_rows.iterrows():
                        final_hours = 8
                        if row["근무시간"] == "8시간":
                            final_hours = 8
                        elif row["근무시간"] == "4시간":
                            final_hours = 4
                        elif row["근무시간"] == "직접입력":
                            final_hours = row["직접입력(시간)"]
                            if final_hours == 0:
                                st.warning(f"⚠️ {row['날짜']}: '직접입력'을 선택했는데 시간이 0입니다. 확인해주세요.")
                                continue

                        rows_to_add.append([
                            row["날짜"], sel_island, sel_place, target_name, 
                            final_hours, visitors, listeners, counts, 
                            str(datetime.now()), "검토대기"
                        ])
                    
                    if rows_to_add:
                        if save_log_bulk(rows_to_add):
                            st.success(f"✅ 총 {len(rows_to_add)}건의 활동 기록이 등록되었습니다!")
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
                    if '날짜' in my_df.columns:
                        my_df['날짜'] = pd.
