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
# 단계별 데이터 저장소
if 'step1_df' not in st.session_state: st.session_state['step1_df'] = None 
if 'step2_df' not in st.session_state: st.session_state['step2_df'] = None 
if 'current_step' not in st.session_state: st.session_state['current_step'] = 1

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
# 2. 기능 함수
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
        st.error("아이디 불일치")
    except: st.error("로그인 오류")

# 명단 캐싱 (끊김 방지)
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
        
        # 단계 초기화 버튼
        if st.button("🔄 처음부터 다시 입력"):
            st.session_state['step1_df'] = None
            st.session_state['step2_df'] = None
            st.session_state['current_step'] = 1
            st.rerun()
            
        st.divider()
        if st.button("로그아웃"):
            st.session_state['logged_in'] = False
            st.session_state['step1_df'] = None
            st.session_state['step2_df'] = None
            st.session_state['current_step'] = 1
            st.rerun()

    st.title(f"📱 {my_name}님의 업무공간")
    tabs = st.tabs(["📝 활동 입력", "📅 내 활동 조회", "🗓️ 다음달 계획", "👀 조원 검토", "📊 통계"])

    # -----------------------------------------------------
    # 탭 1: 활동 입력 (2단계 분리 구조)
    # -----------------------------------------------------
    with tabs[0]:
        st.subheader("활동 실적 등록")

        # [공통 설정]
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
        
        # 해설사 명단 미리 로드
        island_users = get_users_by_island_cached(sel_island)

        st.divider()

        # =========================================================
        # [STEP 1] 운영 현황 입력 (통계 & 인원수)
        # =========================================================
        if st.session_state['current_step'] == 1:
            st.markdown("### 1️⃣ 단계: 운영 현황 입력")
            st.info("👇 날짜별 **방문객 통계**와 **근무한 해설사 인원 수**를 입력하세요.")

            # 서식 생성 (아직 없으면)
            if st.session_state['step1_df'] is None:
                _, last_day = calendar.monthrange(t_year, t_month)
                day_range = range(1, 16) if "전반기" in period else range(16, last_day + 1)
                
                rows = []
                for d in day_range:
                    dt_obj = datetime(t_year, t_month, d)
                    d_str = dt_obj.strftime("%Y-%m-%d")
                    wk = dt_obj.strftime("%a")
                    # [날짜, 요일, 방문자, 청취자, 해설횟수, 활동해설사수]
                    rows.append([d_str, wk, 0, 0, 0, 0])
                
                st.session_state['step1_df'] = pd.DataFrame(rows, columns=["일자", "요일", "방문자", "청취자", "해설횟수", "활동해설사수"])

            # 1단계 에디터
            edited_step1 = st.data_editor(
                st.session_state['step1_df'],
                column_config={
                    "일자": st.column_config.TextColumn("일자", disabled=True, width="small"),
                    "요일": st.column_config.TextColumn("요일", disabled=True, width="small"),
                    "방문자": st.column_config.NumberColumn("방문자(명)", min_value=0),
                    "청취자": st.column_config.NumberColumn("청취자(명)", min_value=0),
                    "해설횟수": st.column_config.NumberColumn("해설횟수(회)", min_value=0),
                    "활동해설사수": st.column_config.NumberColumn("활동 해설사 수(명)", min_value=0, max_value=10, help="이 날 근무한 인원 수를 입력하세요"),
                },
                hide_index=True,
                use_container_width=True,
                height=500
            )

            # 1단계 저장 버튼
            if st.button("💾 운영현황 저장 및 해설사 입력(다음단계)"):
                # 1. 통계 데이터 먼저 저장 (인원수 > 0 이거나 통계 > 0 인 날)
                # 통계는 '관리자(본인)' 이름으로 저장하되 활동시간은 0으로 처리 (중복 방지)
                stats_rows = []
                # 2단계 생성을 위한 데이터 준비
                step2_data = []

                for _, row in edited_step1.iterrows():
                    # 통계가 있거나 해설사가 있는 날만 처리
                    has_stats = (row["방문자"] > 0 or row["청취자"] > 0 or row["해설횟수"] > 0)
                    guide_count = int(row["활동해설사수"])
                    
                    # (A) 구글 시트로 보낼 통계 데이터 (해설사 정보 없음, 통계만 있음)
                    if has_stats:
                        stats_rows.append([
                            row["일자"], sel_island, sel_place, my_name, # 작성자(관리자)
                            0, # 활동시간 0
                            row["방문자"], row["청취자"], row["해설횟수"],
                            str(datetime.now()), "검토대기"
                        ])
                    
                    # (B) 2단계 표를 만들기 위한 데이터 생성
                    if guide_count > 0:
                        for _ in range(guide_count):
                            # [일자, 요일, 해설사(선택), 활동시간(8), 직접입력(0)]
                            step2_data.append([row["일자"], row["요일"], None, "8시간", 0])

                if not stats_rows and not step2_data:
                    st.warning("⚠️ 저장할 내용이 없습니다. 통계나 인원수를 입력해주세요.")
                else:
                    # 구글 시트 전송
                    if stats_rows:
                        if save_bulk("운영일지", stats_rows):
                            st.toast("✅ 운영 통계가 저장되었습니다!")
                        else:
                            st.error("통계 저장 실패")
                            st.stop()
                    
                    # 2단계 데이터프레임 생성 및 상태 전환
                    if step2_data:
                        st.session_state['step2_df'] = pd.DataFrame(step2_data, columns=["일자", "요일", "해설사", "활동시간", "시간(직접)"])
                        st.session_state['current_step'] = 2
                        st.rerun()
                    else:
                        st.success("✅ 통계만 저장되었습니다. (해설사 활동 없음)")
                        time.sleep(1)
                        # 초기화
                        st.session_state['step1_df'] = None
                        st.rerun()

        # =========================================================
        # [STEP 2] 해설사 활동 현황 입력
        # =========================================================
        elif st.session_state['current_step'] == 2:
            st.markdown("### 2️⃣ 단계: 해설사 활동 상세 입력")
            st.info("👇 1단계에서 입력한 인원수만큼 칸이 생성되었습니다. **누가/몇 시간** 일했는지 선택하세요.")
            
            if st.session_state['step2_df'] is not None:
                edited_step2 = st.data_editor(
                    st.session_state['step2_df'],
                    column_config={
                        "일자": st.column_config.TextColumn("일자", disabled=True, width="small"),
                        "요일": st.column_config.TextColumn("요일", disabled=True, width="small"),
                        "해설사": st.column_config.SelectboxColumn("해설사(필수)", options=island_users, required=True, width="medium"),
                        "활동시간": st.column_config.SelectboxColumn("활동시간", options=["8시간", "4시간", "직접입력"], default="8시간"),
                        "시간(직접)": st.column_config.NumberColumn("입력", min_value=0, max_value=24, width="small"),
                    },
                    hide_index=True,
                    use_container_width=True
                )

                col_btn1, col_btn2 = st.columns([1, 1])
                with col_btn1:
                    if st.button("✅ 해설사 활동 저장 (완료)"):
                        # 유효성 검사 (해설사 선택 안한거 있나?)
                        if edited_step2['해설사'].isnull().any():
                            st.warning("⚠️ 해설사가 선택되지 않은 칸이 있습니다.")
                        else:
                            guide_rows = []
                            for _, row in edited_step2.iterrows():
                                # 시간 계산
                                fh = 8
                                if row["활동시간"] == "4시간": fh = 4
                                elif row["활동시간"] == "직접입력": fh = row["시간(직접)"]

                                if row["활동시간"] == "직접입력" and fh == 0: continue

                                # [날짜, 섬, 장소, 해설사, 시간, 방문자0, 청취자0, 횟수0, 타임스탬프, 상태]
                                # 통계는 1단계에서 넣었으므로 여기서는 0으로 처리
                                guide_rows.append([
                                    row["일자"], sel_island, sel_place, row["해설사"],
                                    fh, 0, 0, 0,
                                    str(datetime.now()), "검토대기"
                                ])
                            
                            if save_bulk("운영일지", guide_rows):
                                st.success(f"✅ 총 {len(guide_rows)}건의 해설사 활동이 저장되었습니다!")
                                time.sleep(2)
                                # 모든 작업 완료 후 초기화
                                st.session_state['step1_df'] = None
                                st.session_state['step2_df'] = None
                                st.session_state['current_step'] = 1
                                st.rerun()
                
                with col_btn2:
                    if st.button("🔙 뒤로가기 (1단계 수정)"):
                        st.session_state['current_step'] = 1
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
