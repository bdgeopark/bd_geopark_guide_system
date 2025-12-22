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

# 세션 초기화
if 'logged_in' not in st.session_state: st.session_state['logged_in'] = False
if 'user_info' not in st.session_state: st.session_state['user_info'] = {}
if 'generated_df' not in st.session_state: st.session_state['generated_df'] = None 
if 'active_guides' not in st.session_state: st.session_state['active_guides'] = [] 

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
# 2. 기능 함수 (★ 캐싱 기능 추가로 끊김 방지)
# ---------------------------------------------------------
def login(username, password):
    if client is None:
        st.error("서버 연결 실패")
        return
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
        st.error("아이디 또는 비밀번호가 일치하지 않습니다.")
    except Exception as e:
        st.error(f"로그인 처리 중 오류: {e}")

# ★ 명단을 1시간(3600초)동안 기억해서 드롭박스 사라짐 방지
@st.cache_data(ttl=3600)
def get_users_by_island_cached(island_name):
    try:
        if client is None: return []
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
        # ★ 강제 새로고침 버튼 (혹시라도 명단 갱신 필요할 때)
        if st.button("🔄 명단/화면 새로고침"):
            st.cache_data.clear() # 기억된 명단 지우고 다시 불러오기
            st.session_state['generated_df'] = None
            st.session_state['active_guides'] = []
            st.rerun()
        st.divider()
        if st.button("로그아웃"):
            st.session_state['logged_in'] = False
            st.session_state['generated_df'] = None
            st.session_state['active_guides'] = []
            st.rerun()

    st.title(f"📱 {my_name}님의 업무공간")

    tabs = st.tabs(["📝 활동 입력", "📅 내 활동 조회", "🗓️ 다음달 계획", "👀 조원 검토", "📊 통계"])

    # -----------------------------------------------------
    # 탭 1: 활동 입력
    # -----------------------------------------------------
    with tabs[0]:
        st.subheader("활동 실적 등록")
        
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

        # 2. 해설사 추가하기 (안정성 강화)
        st.markdown("##### ➕ 해설사 명단 구성")
        
        # 캐싱된 함수 사용 (끊김 방지)
        island_users = get_users_by_island_cached(sel_island)
        
        col_add1, col_add2, col_btn = st.columns([2, 1, 1])
        with col_add1:
            if island_users:
                selected_user_db = st.selectbox("한 명씩 추가", ["선택안함"] + island_users)
            else:
                selected_user_db = "선택안함"
                st.caption("⚠️ 불러올 명단이 없습니다.")
        
        with col_add2:
            manual_name = st.text_input("직접 입력 (명단에 없을 때)")
        
        with col_btn:
            st.write("") 
            # 한 명 추가 버튼
            if st.button("한 명 추가", type="primary"):
                name_to_add = ""
                if manual_name.strip(): name_to_add = manual_name.strip()
                elif selected_user_db != "선택안함": name_to_add = selected_user_db
                
                if name_to_add:
                    if name_to_add not in st.session_state['active_guides']:
                        st.session_state['active_guides'].append(name_to_add)
                        st.rerun()
                    else: st.warning("이미 목록에 있습니다.")
                else: st.warning("이름을 선택해주세요.")

        # ★ [전원 추가] 버튼 (박사님 요청 기능!)
        if my_role == "관리자" and island_users:
            if st.button(f"🚀 {sel_island} 해설사 전원({len(island_users)}명) 한 번에 추가"):
                # 기존 목록에 없는 사람만 싹 다 추가
                count = 0
                for u in island_users:
                    if u not in st.session_state['active_guides']:
                        st.session_state['active_guides'].append(u)
                        count += 1
                if count > 0:
                    st.success(f"{count}명 추가 완료!")
                    time.sleep(0.5)
                    st.rerun()
                else:
                    st.info("이미 모두 추가되어 있습니다.")

        # 3. 추가된 해설사별 근무일 체크
        _, last_day = calendar.monthrange(t_year, t_month)
        day_range = range(1, 16) if "전반기" in period else range(16, last_day + 1)
        schedule_data = [] 

        if st.session_state['active_guides']:
            st.divider()
            st.write(f"📋 **총 {len(st.session_state['active_guides'])}명** 작업 중")
            
            # 전체 삭제 버튼 (편의성)
            if st.button("🗑️ 명단 전체 비우기"):
                st.session_state['active_guides'] = []
                st.rerun()

            for guide in st.session_state['active_guides']:
                with st.expander(f"🗓️ **{guide}**님 근무일 체크", expanded=True):
                    # 개별 삭제
                    if st.button(f"제외 X", key=f"del_{guide}", help=f"{guide}님을 명단에서 뺍니다."):
                        st.session_state['active_guides'].remove(guide)
                        st.rerun()

                    cols = st.columns(7)
                    for i, day in enumerate(day_range):
                        # 키에 년도(t_year)까지 포함해서 중복/꼬임 방지
                        key = f"chk_{guide}_{t_year}_{t_month}_{day}"
                        dt_obj = datetime(t_year, t_month, day)
                        weekday = dt_obj.strftime("%a")
                        label = f"{day}({weekday})"
                        if weekday in ['Sat', 'Sun']: label = f"**{label}**"
                        
                        with cols[i % 7]:
                            if st.checkbox(label, key=key):
                                full_date = dt_obj.strftime("%Y-%m-%d")
                                schedule_data.append([full_date, guide, weekday])
            
            st.divider()
            
            # 4. 표 생성 버튼
            if st.button("⬇️ 위에서 체크한 내용으로 표 생성 (클릭)"):
                if not schedule_data:
                    st.warning("⚠️ 근무일을 하나 이상 체크해주세요.")
                else:
                    rows = []
                    for item in schedule_data:
                        rows.append([item[0], item[2], item[1], "8시간", 0, 0, 0, 0])
                    
                    df = pd.DataFrame(rows, columns=["일자", "요일", "해설사", "활동시간", "시간(직접)", "방문자", "청취자", "해설횟수"])
                    df = df.sort_values(by=["일자", "해설사"])
                    st.session_state['generated_df'] = df
                    st.rerun()
        
        else:
            st.info("👆 위에서 해설사를 추가하면 달력이 나옵니다.")

        # 5. 입력 및 저장
        if st.session_state['generated_df'] is not None:
            st.subheader("📝 상세 내용 입력")
            
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

            if st.button("✅ 입력 완료! 구글 시트에 저장"):
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
                    st.success(f"총 {len(rows_to_save)}건 저장 성공!")
                    st.session_state['generated_df'] = None
                    st.session_state['active_guides'] = [] 
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
