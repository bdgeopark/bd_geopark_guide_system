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

# ★ 수정됨: 관리자 소속인 '시청'을 다시 추가했습니다.
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
        st.error("아이디 또는 비밀번호가 틀렸습니다.")
    except Exception as e:
        st.error(f"로그인 오류: {e}")

# (수정) 이름뿐만 아니라 섬 정보까지 다 가져오는 함수
def get_all_users_full():
    try:
        sheet = client.open(SPREADSHEET_NAME).worksheet("사용자")
        users = sheet.get_all_records()
        return users # 전체 리스트 반환 (이름, 섬, 직책 등 포함)
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
    # 탭 1: 활동 입력 (★ 엑셀 스타일 일괄 입력 강화)
    # -----------------------------------------------------
    with tabs[0]:
        st.subheader("활동 실적 등록")
        
        # 1. 섬 선택
        if my_role == "관리자":
            sel_island = st.selectbox("📍 어느 섬의 활동인가요?", list(locations.keys()))
        else:
            sel_island = my_island

        # 2. 해설사 선택
        target_name = my_name
        if my_role == "관리자":
            all_users_info = get_all_users_full()
            filtered_users = [u['이름'] for u in all_users_info if u.get('섬') == sel_island]
            
            if filtered_users:
                target_name = st.selectbox("👤 해설사 선택", filtered_users)
            else:
                st.warning(f"{sel_island}에 소속된 해설사가 없습니다.")
                target_name = st.text_input("이름 직접 입력") 
        
        st.divider()
        
        # 3. 입력 방식 선택
        input_mode = st.radio("입력 방식", ["하루씩 입력 (기본)", "기간 일괄 입력 (엑셀형)"], horizontal=True)
        st.caption(f"현재 **[{sel_island}]** - **[{target_name}]**님의 활동을 입력합니다.")

        if input_mode == "하루씩 입력 (기본)":
            # --- (기존) 단건 입력 ---
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
                
            listeners = st.number_input("해설 청취자(명)", min_value=0)
            counts = st.number_input("해설 횟수(회)", min_value=0)

            if st.button(f"저장하기 ({target_name})", type="primary"):
                row = [
                    str(input_date), sel_island, sel_place, target_name, 
                    w_hours, visitors, listeners, counts, 
                    str(datetime.now()), "검토대기"
                ]
                if save_log(row):
                    st.success(f"✅ {target_name}님의 기록이 저장되었습니다!")

        else:
            # --- (수정) 표 형태 일괄 입력 ---
            col_y, col_m = st.columns(2)
            with col_y:
                target_year = st.number_input("년도", value=datetime.now().year)
            with col_m:
                target_month = st.number_input("월", value=datetime.now().month, min_value=1, max_value=12)

            period_type = st.radio("기간 선택", ["전반기 (1일 ~ 15일)", "후반기 (16일 ~ 말일)"], horizontal=True, key="act_period")
            
            # 장소는 공통으로 선택 (안내소는 보통 고정이니까요)
            sel_place = st.selectbox("근무 장소", locations.get(sel_island, ["장소없음"]), key="act_place")
            
            st.info("👇 아래 표에서 **근무한 날짜를 체크(v)**하고, **시간**과 **방문객 수**를 각각 입력하세요.")

            # 날짜 생성
            _, last_day = calendar.monthrange(target_year, target_month)
            if "전반기" in period_type:
                day_range = range(1, 16)
            else:
                day_range = range(16, last_day + 1)
            
            # 데이터프레임 생성 (컬럼 확장: 방문객, 청취자, 횟수 추가)
            data_list = []
            for d in day_range:
                dt = datetime(target_year, target_month, d)
                day_str = dt.strftime("%Y-%m-%d")
                weekday = dt.strftime("%a")
                # [체크, 날짜, 요일, 시간옵션, 직접시간, 방문객, 청취자, 횟수]
                data_list.append([False, day_str, weekday, "8시간", 0, 0, 0, 0])
            
            # 컬럼 이름 정의 (사용자에게 보여질 헤더)
            df_input = pd.DataFrame(data_list, columns=[
                "근무", "날짜", "요일", 
                "활동시간", "시간(직접)", 
                "방문객", "청취자", "해설횟수"
            ])

            # ★ 에디터 설정 (여기가 핵심!)
            edited_df = st.data_editor(
                df_input,
                column_config={
                    "근무": st.column_config.CheckboxColumn("선택", width="small", default=False),
                    "날짜": st.column_config.TextColumn("날짜", width="small", disabled=True),
                    "요일": st.column_config.TextColumn("요일", width="small", disabled=True),
                    
                    # [1구역] 해설사 개인 활동
                    "활동시간": st.column_config.SelectboxColumn(
                        "활동 시간",
                        options=["8시간", "4시간", "직접입력"],
                        default="8시간",
                        width="medium"
                    ),
                    "시간(직접)": st.column_config.NumberColumn(
                        "입력",
                        min_value=0, max_value=24,
                        help="직접입력 선택 시 시간 기입",
                        width="small"
                    ),

                    # [2구역] 안내소 운영 통계
                    "방문객": st.column_config.NumberColumn("방문객(명)", min_value=0, default=0),
                    "청취자": st.column_config.NumberColumn("청취자(명)", min_value=0, default=0),
                    "해설횟수": st.column_config.NumberColumn("해설(회)", min_value=0, default=0),
                },
                hide_index=True,
                use_container_width=True
            )

            if st.button(f"선택한 날짜 일괄 저장 ({target_name})"):
                selected_rows = edited_df[edited_df["근무"] == True]
                
                if selected_rows.empty:
                    st.warning("⚠️ 저장할 날짜를 하나 이상 체크해주세요.")
                else:
                    rows_to_add = []
                    for index, row in selected_rows.iterrows():
                        # 시간 계산
                        final_hours = 8
                        if row["활동시간"] == "8시간": final_hours = 8
                        elif row["활동시간"] == "4시간": final_hours = 4
                        elif row["활동시간"] == "직접입력": final_hours = row["시간(직접)"]
                        
                        # 0시간 체크 (직접입력 실수 방지)
                        if row["활동시간"] == "직접입력" and final_hours == 0:
                             st.warning(f"⚠️ {row['날짜']}: 시간을 0으로 입력하셨습니다. 확인해주세요.")
                             continue

                        # 데이터 조립
                        rows_to_add.append([
                            row["날짜"], sel_island, sel_place, target_name, 
                            final_hours, 
                            row["방문객"], row["청취자"], row["해설횟수"], 
                            str(datetime.now()), "검토대기"
                        ])
                    
                    if rows_to_add:
                        if save_log_bulk(rows_to_add):
                            st.success(f"✅ 총 {len(rows_to_add)}건의 실적이 등록되었습니다!")
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

            if "활동 내역" in check_type:
                try:
                    sheet = client.open(SPREADSHEET_NAME).worksheet("운영일지")
                    df = pd.DataFrame(sheet.get_all_records())
                    if my_role != "관리자":
                        df = df[df['섬'] == my_island]
                    view_option = st.checkbox("검토 대기 건만 보기", value=True)
                    if view_option:
                        display_df = df[df['상태'] == "검토대기"]
                    else:
                        display_df = df
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
                                st.rerun()
                except Exception as e:
                    st.error(f"로드 실패: {e}")

            else:
                try:
                    sheet = client.open(SPREADSHEET_NAME).worksheet("월간계획")
                    df = pd.DataFrame(sheet.get_all_records())
                    if df.empty:
                        st.info("등록된 계획이 없습니다.")
                    else:
                        if my_role != "관리자":
                            df = df[df['섬'] == my_island]
                        if '날짜' in df.columns:
                            df['날짜'] = pd.to_datetime(df['날짜'])
                            df = df.sort_values(by='날짜')
                        st.write(f"📊 **{my_island if my_role != '관리자' else '전체'}** 근무 계획")
                        st.dataframe(df)
                except gspread.exceptions.WorksheetNotFound:
                    st.error("🚨 '월간계획' 시트 없음")
                except Exception as e:
                    st.error(f"로드 실패: {e}")

    # 탭 5: 통계
    if my_role == "관리자":
        with tabs[4]:
            st.subheader("📊 운영 현황 대시보드")
            try:
                sheet = client.open(SPREADSHEET_NAME).worksheet("운영일지")
                df = pd.DataFrame(sheet.get_all_records())
                if df.empty:
                    st.info("데이터가 없습니다.")
                else:
                    df['방문자'] = pd.to_numeric(df['방문자'], errors='coerce').fillna(0)
                    df['횟수'] = pd.to_numeric(df['횟수'], errors='coerce').fillna(0)
                    total_visitors = int(df['방문자'].sum())
                    total_counts = int(df['횟수'].sum())
                    m1, m2 = st.columns(2)
                    m1.metric("👥 총 방문객", f"{total_visitors:,}명")
                    m2.metric("🗣️ 총 해설 횟수", f"{total_counts:,}회")
                    st.divider()
                    st.write("### 📈 상세 분석")
                    chart1, chart2 = st.columns(2)
                    with chart1:
                        st.write("##### 🏝️ 섬별 방문객 (막대)")
                        island_df = df.groupby("섬")['방문자'].sum()
                        st.bar_chart(island_df)
                    with chart2:
                        st.write("##### 🗓️ 일별 활동 추이 (꺾은선)")
                        try:
                            df['날짜'] = pd.to_datetime(df['날짜'])
                            daily_df = df.groupby("날짜")['방문자'].sum()
                            st.line_chart(daily_df)
                        except:
                            st.caption("⚠️ 날짜 형식이 맞지 않아 추이 그래프를 그릴 수 없습니다.")
            except Exception as e:
                st.error(f"통계 로드 중 오류: {e}")
