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

# 캐싱 초기화
if 'cached_logs' not in st.session_state:
    st.session_state['cached_logs'] = None
if 'cached_plans' not in st.session_state:
    st.session_state['cached_plans'] = None
if 'cached_users' not in st.session_state:
    st.session_state['cached_users'] = None

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
                refresh_data()
                time.sleep(0.5)
                st.rerun()
                return
        st.error("아이디 또는 비밀번호가 틀렸습니다.")
    except Exception as e:
        st.error(f"로그인 오류: {e}")

def refresh_data():
    try:
        wb = client.open(SPREADSHEET_NAME)
        st.session_state['cached_logs'] = pd.DataFrame(wb.worksheet("운영일지").get_all_records())
        try:
            st.session_state['cached_plans'] = pd.DataFrame(wb.worksheet("월간계획").get_all_records())
        except:
            st.session_state['cached_plans'] = pd.DataFrame()
        st.session_state['cached_users'] = pd.DataFrame(wb.worksheet("사용자").get_all_records())
        return True
    except Exception as e:
        st.error(f"데이터 로드 실패: {e}")
        return False

def get_data(type="logs"):
    if type == "logs":
        if st.session_state['cached_logs'] is None: refresh_data()
        return st.session_state['cached_logs']
    elif type == "plans":
        if st.session_state['cached_plans'] is None: refresh_data()
        return st.session_state['cached_plans']
    elif type == "users":
        if st.session_state['cached_users'] is None: refresh_data()
        return st.session_state['cached_users']

def get_all_users_full():
    df_users = get_data("users")
    if df_users is not None and not df_users.empty:
        return df_users.to_dict('records')
    return []

def save_log(data):
    try:
        sheet = client.open(SPREADSHEET_NAME).worksheet("운영일지")
        sheet.append_row(data)
        st.session_state['cached_logs'] = None 
        return True
    except Exception as e:
        st.error(f"저장 실패: {e}")
        return False

def save_log_bulk(rows):
    try:
        sheet = client.open(SPREADSHEET_NAME).worksheet("운영일지")
        sheet.append_rows(rows)
        st.session_state['cached_logs'] = None
        return True
    except Exception as e:
        st.error(f"일괄 저장 실패: {e}")
        return False

def save_plan_bulk(rows):
    try:
        sheet = client.open(SPREADSHEET_NAME).worksheet("월간계획")
        sheet.append_rows(rows)
        st.session_state['cached_plans'] = None
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
        # J열(10번째)이 '상태'라고 가정
        for idx in target_indices:
            row_num = idx + 2 
            sheet.update_cell(row_num, 10, "승인완료") 
        st.session_state['cached_logs'] = None
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
        if st.button("🔄 데이터 최신화"):
            with st.spinner("동기화 중..."):
                refresh_data()
            st.success("완료!")
        st.divider()
        if st.button("로그아웃"):
            st.session_state['logged_in'] = False
            for key in ['cached_logs', 'cached_plans', 'cached_users']:
                if key in st.session_state: del st.session_state[key]
            st.rerun()
    
    st.title(f"📱 {my_name}님의 업무공간")

    tabs_list = ["📝 활동 입력", "📅 내 활동 조회", "🗓️ 다음달 계획"]
    if my_role in ["조장", "관리자"]:
        tabs_list.append("👀 조원 활동/계획 검토")
    if my_role == "관리자":
        tabs_list.append("📊 관리자 대시보드")

    tabs = st.tabs(tabs_list)

    # 탭 1: 활동 입력
    with tabs[0]:
        st.subheader("활동 실적 등록")
        
        if my_role == "관리자":
            sel_island = st.selectbox("📍 어느 섬의 활동인가요?", list(locations.keys()))
        else:
            sel_island = my_island

        target_name = my_name
        if my_role == "관리자":
            all_users_info = get_all_users_full()
            filtered_users = [u['이름'] for u in all_users_info if u.get('섬') == sel_island]
            if filtered_users:
                target_name = st.selectbox("👤 해설사 선택", filtered_users)
            else:
                target_name = st.text_input("이름 직접 입력") 
        
        st.divider()
        input_mode = st.radio("입력 방식", ["하루씩 입력 (기본)", "기간 일괄 입력 (엑셀형)"], horizontal=True)
        st.caption(f"현재 **[{sel_island}]** - **[{target_name}]**님의 활동을 입력합니다.")

        if input_mode == "하루씩 입력 (기본)":
            c1, c2 = st.columns(2)
            with c1:
                input_date = st.date_input("날짜", datetime.now())
            with c2:
                sel_place = st.selectbox("장소", locations.get(sel_island, ["장소없음"]))
            
            c3, c4 = st.columns(2)
            with c3:
                w_hours = st.number_input("활동 시간", min_value=0, value=8)
            with c4:
                visitors = st.number_input("방문객(명)", min_value=0)
            
            # 여기서 '해설횟수'로 통일
            listeners = st.number_input("청취자(명)", min_value=0)
            counts = st.number_input("해설횟수(회)", min_value=0)

            if st.button(f"저장하기 ({target_name})", type="primary"):
                # 저장 순서: 날짜, 섬, 장소, 이름, 활동시간, 방문자, 청취자, 해설횟수, 타임스탬프, 상태
                row = [
                    str(input_date), sel_island, sel_place, target_name, 
                    w_hours, visitors, listeners, counts, 
                    str(datetime.now()), "검토대기"
                ]
                if save_log(row):
                    st.success("✅ 저장 완료!")
                    time.sleep(1)
                    refresh_data()
                    st.rerun()

        else:
            # 엑셀형 입력
            col_y, col_m = st.columns(2)
            with col_y:
                target_year = st.number_input("년도", value=datetime.now().year)
            with col_m:
                target_month = st.number_input("월", value=datetime.now().month, min_value=1, max_value=12)

            period_type = st.radio("기간 선택", ["전반기 (1일 ~ 15일)", "후반기 (16일 ~ 말일)"], horizontal=True, key="act_period")
            sel_place = st.selectbox("근무 장소", locations.get(sel_island, ["장소없음"]), key="act_place")
            
            st.info("👇 근무한 날짜를 **체크(v)**하고 실적을 입력하세요.")

            _, last_day = calendar.monthrange(target_year, target_month)
            if "전반기" in period_type:
                day_range = range(1, 16)
            else:
                day_range = range(16, last_day + 1)
            
            data_list = []
            for d in day_range:
                dt = datetime(target_year, target_month, d)
                day_str = dt.strftime("%Y-%m-%d")
                weekday = dt.strftime("%a")
                data_list.append([False, day_str, weekday, "8시간", 0, 0, 0, 0])
            
            df_input = pd.DataFrame(data_list, columns=["근무", "날짜", "요일", "활동시간", "시간(직접)", "방문자", "청취자", "해설횟수"])

            edited_df = st.data_editor(
                df_input,
                column_config={
                    "근무": st.column_config.CheckboxColumn("선택", width="small", default=False),
                    "날짜": st.column_config.TextColumn("날짜", width="small", disabled=True),
                    "요일": st.column_config.TextColumn("요일", width="small", disabled=True),
                    "활동시간": st.column_config.SelectboxColumn("시간", options=["8시간", "4시간", "직접입력"], default="8시간", width="medium"),
                    "시간(직접)": st.column_config.NumberColumn("입력", min_value=0, max_value=24, width="small"),
                    "방문자": st.column_config.NumberColumn("방문자", min_value=0, default=0),
                    "청취자": st.column_config.NumberColumn("청취자", min_value=0, default=0),
                    "해설횟수": st.column_config.NumberColumn("해설(회)", min_value=0, default=0),
                },
                hide_index=True,
                use_container_width=True
            )

            if st.button(f"일괄 저장 ({target_name})"):
                selected_rows = edited_df[edited_df["근무"] == True]
                if selected_rows.empty:
                    st.warning("⚠️ 저장할 날짜를 체크해주세요.")
                else:
                    rows_to_add = []
                    for index, row in selected_rows.iterrows():
                        final_hours = 8
                        if row["활동시간"] == "8시간": final_hours = 8
                        elif row["활동시간"] == "4시간": final_hours = 4
                        elif row["활동시간"] == "직접입력": final_hours = row["시간(직접)"]
                        
                        if row["활동시간"] == "직접입력" and final_hours == 0:
                             st.warning(f"⚠️ {row['날짜']}: 시간 0임. 저장 제외.")
                             continue

                        rows_to_add.append([
                            row["날짜"], sel_island, sel_place, target_name, 
                            final_hours, 
                            row["방문자"], row["청취자"], row["해설횟수"], 
                            str(datetime.now()), "검토대기"
                        ])
                    
                    if rows_to_add:
                        if save_log_bulk(rows_to_add):
                            st.success(f"✅ {len(rows_to_add)}건 저장 완료!")
                            time.sleep(1)
                            refresh_data()
                            st.rerun()

    # 탭 2: 내 활동 조회
    with tabs[1]:
        st.subheader("내 과거 기록 확인")
        df = get_data("logs")
        if df is not None and not df.empty:
            my_df = df[df['이름'] == my_name].copy()
            if not my_df.empty:
                if '날짜' in my_df.columns:
                    my_df['날짜'] = pd.to_datetime(my_df['날짜'])
                    my_df = my_df.sort_values(by='날짜', ascending=False)
                st.dataframe(my_df)
            else:
                st.info("기록이 없습니다.")
        else:
            st.info("데이터 로딩 중...")

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
        
        st.write("▼ 근무할 날짜를 체크하세요")
        selected_days_str = st.multiselect("날짜 선택", day_options, key="plan_dates")

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
                        refresh_data()
                        st.rerun()

    # 탭 4: 검토
    if my_role in ["조장", "관리자"]:
        with tabs[3]:
            st.subheader("👀 조원 활동/계획 검토")
            check_type = st.radio("확인 항목:", ["✅ 활동 내역 (승인)", "📅 월간 계획 (조회)"], horizontal=True)
            st.divider()

            if "활동 내역" in check_type:
                df = get_data("logs")
                if df is not None and not df.empty:
                    # 안전장치: '상태' 컬럼이 없으면 에러 방지
                    if '상태' not in df.columns:
                        st.error("🚨 시트에 '상태' 열이 없습니다! 1행을 확인해주세요.")
                    else:
                        if my_role != "관리자":
                            df = df[df['섬'] == my_island]
                        
                        view_option = st.checkbox("검토 대기 건만 보기", value=True)
                        if view_option:
                            display_df = df[df['상태'] == "검토대기"].copy()
                        else:
                            display_df = df.copy()
                        
                        if '날짜' in display_df.columns:
                            display_df['날짜'] = pd.to_datetime(display_df['날짜'])
                            display_df = display_df.sort_values(by='날짜', ascending=False)
                        st.dataframe(display_df)
                        
                        pending_df = df[df['상태'] == "검토대기"]
                        if not pending_df.empty:
                            st.write("#### 📢 승인 처리")
                            pending_indices = pending_df.index.tolist()
                            selected_indices = st.multiselect(
                                "승인할 목록 선택:",
                                options=pending_indices,
                                format_func=lambda x: f"{df.loc[x]['날짜']} - {df.loc[x]['이름']} ({df.loc[x]['장소']})"
                            )
                            if st.button("선택 항목 승인하기"):
                                if update_status_to_approve(selected_indices):
                                    st.success("승인 완료!")
                                    time.sleep(1)
                                    refresh_data()
                                    st.rerun()

            else:
                df = get_data("plans")
                if df is not None and not df.empty:
                    if my_role != "관리자":
                        df = df[df['섬'] == my_island]
                    if '날짜' in df.columns:
                        df['날짜'] = pd.to_datetime(df['날짜'])
                        df = df.sort_values(by='날짜')
                    st.dataframe(df)
                else:
                    st.info("등록된 계획이 없습니다.")

    # 탭 5: 통계
    if my_role == "관리자":
        with tabs[4]:
            st.subheader("📊 운영 현황 대시보드")
            df = get_data("logs")
            if df is not None and not df.empty:
                df = df.copy()
                # 컬럼 이름 통일 ('횟수' -> '해설횟수')
                
                # 방문자 처리
                if '방문자' in df.columns:
                    df['방문자'] = pd.to_numeric(df['방문자'], errors='coerce').fillna(0)
                else:
                    df['방문자'] = 0
                
                # 해설횟수 처리 (혹시 몰라 횟수/해설횟수 둘다 체크)
                if '해설횟수' in df.columns:
                    df['해설횟수'] = pd.to_numeric(df['해설횟수'], errors='coerce').fillna(0)
                elif '횟수' in df.columns:
                    df['해설횟수'] = pd.to_numeric(df['횟수'], errors='coerce').fillna(0)
                else:
                    df['해설횟수'] = 0

                total_visitors = int(df['방문자'].sum())
                total_counts = int(df['해설횟수'].sum())
                
                m1, m2 = st.columns(2)
                m1.metric("👥 총 방문객", f"{total_visitors:,}명")
                m2.metric("🗣️ 총 해설 횟수", f"{total_counts:,}회")
                
                st.divider()
                st.write("### 📈 상세 분석")
                chart1, chart2 = st.columns(2)
                with chart1:
                    st.write("##### 🏝️ 섬별 방문객")
                    if '섬' in df.columns:
                        island_df = df.groupby("섬")['방문자'].sum()
                        st.bar_chart(island_df)
                with chart2:
                    st.write("##### 🗓️ 일별 활동 추이")
                    if '날짜' in df.columns:
                        try:
                            df['날짜'] = pd.to_datetime(df['날짜'])
                            daily_df = df.groupby("날짜")['방문자'].sum()
                            st.line_chart(daily_df)
                        except:
                            st.caption("날짜 데이터 형식 오류")
            else:
                st.info("데이터가 없습니다.")
