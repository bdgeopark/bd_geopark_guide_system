import streamlit as st
import pandas as pd
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time
import calendar

# ---------------------------------------------------------
# 1. 시스템 설정 & CSS (글자 크기 확대)
# ---------------------------------------------------------
st.set_page_config(page_title="지질공원 통합관리", page_icon="🪨", layout="wide")

st.markdown("""
    <style>
    html, body, [class*="css"] { font-size: 18px !important; }
    div[data-testid="stDataEditor"] table { font-size: 18px !important; }
    div[data-testid="stSelectbox"] * { font-size: 18px !important; }
    </style>
    """, unsafe_allow_html=True)

# 세션 초기화
if 'logged_in' not in st.session_state: st.session_state['logged_in'] = False
if 'user_info' not in st.session_state: st.session_state['user_info'] = {}
# 입력 단계 저장소
if 'step1_df' not in st.session_state: st.session_state['step1_df'] = None 
if 'step2_dfs' not in st.session_state: st.session_state['step2_dfs'] = {} 
if 'current_step' not in st.session_state: st.session_state['current_step'] = 1
if 'last_input_key' not in st.session_state: st.session_state['last_input_key'] = ""

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
        except: return None

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
    if client is None: st.error("서버 연결 실패"); return
    try:
        sheet = client.open(SPREADSHEET_NAME).worksheet("사용자")
        users = sheet.get_all_records()
        for user in users:
            if str(user['아이디']) == str(username) and str(user['비번']) == str(password):
                st.session_state['logged_in'] = True
                st.session_state['user_info'] = user
                st.success(f"환영합니다, {user['이름']}님!"); time.sleep(0.5); st.rerun(); return
        st.error("아이디 불일치")
    except: st.error("로그인 오류")

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
        if st.button("🔄 입력화면 초기화"):
            st.session_state['step1_df'] = None; st.session_state['step2_dfs'] = {}; st.session_state['current_step'] = 1; st.rerun()
        st.divider()
        if st.button("로그아웃"):
            st.session_state['logged_in'] = False; st.rerun()

    st.title(f"📱 {my_name}님의 업무공간")
    tabs = st.tabs(["📝 활동 입력", "📅 내 활동 조회", "🗓️ 다음달 계획", "👀 조원 검토", "📊 성과 통계"])

    # -----------------------------------------------------
    # 탭 1: 활동 입력 (2단계 + 슬롯 방식 유지)
    # -----------------------------------------------------
    with tabs[0]:
        st.subheader("활동 실적 등록")
        c1, c2, c3 = st.columns([1, 1, 2])
        with c1: t_year = st.number_input("년", value=datetime.now().year)
        with c2: t_month = st.number_input("월", value=datetime.now().month)
        with c3: 
            if my_role == "관리자": sel_island = st.selectbox("섬 선택", list(locations.keys()))
            else: sel_island = my_island; st.success(f"📍 {sel_island}")
        c4, c5 = st.columns([1, 2])
        with c4: period = st.radio("기간", ["전반기(1~15일)", "후반기(16~말일)"], horizontal=True)
        with c5: sel_place = st.selectbox("근무 장소(공통)", locations.get(sel_island, ["-"]))
        island_users = get_users_by_island_cached(sel_island)

        # 입력 조건 변경 시 초기화
        current_key = f"{t_year}-{t_month}-{sel_island}-{period}-{sel_place}"
        if st.session_state['last_input_key'] != current_key:
            st.session_state['step1_df'] = None; st.session_state['step2_dfs'] = {}; st.session_state['current_step'] = 1; st.session_state['last_input_key'] = current_key; st.rerun()
        st.divider()

        # [STEP 1] 운영 현황
        if st.session_state['current_step'] == 1:
            st.markdown("### 1️⃣ 단계: 운영 현황 입력")
            if st.session_state['step1_df'] is None:
                _, last_day = calendar.monthrange(t_year, t_month)
                day_range = range(1, 16) if "전반기" in period else range(16, last_day + 1)
                rows = [[datetime(t_year, t_month, d).strftime("%Y-%m-%d"), datetime(t_year, t_month, d).strftime("%a"), 0, 0, 0, 0] for d in day_range]
                st.session_state['step1_df'] = pd.DataFrame(rows, columns=["일자", "요일", "방문자", "청취자", "해설횟수", "활동해설사수"])
            
            edited_step1 = st.data_editor(st.session_state['step1_df'], hide_index=True, use_container_width=True)
            
            if st.button("💾 저장 및 다음 단계(해설사 배정)"):
                stats_rows = []
                max_guides = 0
                for _, row in edited_step1.iterrows():
                    g_cnt = int(row["활동해설사수"])
                    if g_cnt > max_guides: max_guides = g_cnt
                    if row["방문자"]>0 or row["청취자"]>0 or row["해설횟수"]>0:
                        stats_rows.append([row["일자"], sel_island, sel_place, "운영통계", 0, row["방문자"], row["청취자"], row["해설횟수"], str(datetime.now()), "검토대기"])
                
                if stats_rows: save_bulk("운영일지", stats_rows); st.toast("✅ 운영 통계 저장됨")
                if max_guides > 0:
                    dfs = {}
                    for k in range(1, max_guides+1):
                        data_k = []
                        for _, row in edited_step1.iterrows():
                            if int(row["활동해설사수"]) >= k: data_k.append([row["일자"], row["요일"], None, "8시간", 0])
                        dfs[k] = pd.DataFrame(data_k, columns=["일자", "요일", "해설사", "활동시간", "시간(직접)"])
                    st.session_state['step2_dfs'] = dfs; st.session_state['current_step'] = 2; st.rerun()
                else: st.success("✅ 통계만 저장됨"); time.sleep(1); st.session_state['step1_df']=None; st.rerun()

        # [STEP 2] 해설사 활동
        elif st.session_state['current_step'] == 2:
            st.markdown("### 2️⃣ 단계: 해설사 활동 상세 입력")
            dfs = st.session_state['step2_dfs']
            for k in range(1, len(dfs)+1):
                st.markdown(f"#### 👤 **{k}번 해설사**")
                s_name = st.selectbox(f"{k}번 해설사 이름 (일괄적용)", ["선택안함"]+island_users, key=f"sel_{k}")
                if s_name != "선택안함": dfs[k]["해설사"] = s_name
                st.session_state['step2_dfs'][k] = st.data_editor(dfs[k], key=f"ed_{k}", hide_index=True, use_container_width=True)
            
            c_b1, c_b2 = st.columns(2)
            with c_b1:
                if st.button("✅ 모든 활동 일괄 저장"):
                    all_r = []
                    for k in dfs:
                        tdf = st.session_state['step2_dfs'][k]
                        for _, r in tdf.iterrows():
                            fh = 8
                            if r["활동시간"]=="4시간": fh=4
                            elif r["활동시간"]=="직접입력": fh=r["시간(직접)"]
                            if r["활동시간"]=="직접입력" and fh==0: continue
                            all_r.append([r["일자"], sel_island, sel_place, r["해설사"], fh, 0, 0, 0, str(datetime.now()), "검토대기"])
                    if save_bulk("운영일지", all_r): st.success("저장 완료"); time.sleep(1); st.session_state['step1_df']=None; st.session_state['current_step']=1; st.rerun()
            with c_b2:
                if st.button("🔙 뒤로가기"): st.session_state['current_step']=1; st.rerun()

    # 탭 2~4 (기존 동일)
    with tabs[1]:
        if st.button("내역 조회"):
            try:
                df = pd.DataFrame(client.open(SPREADSHEET_NAME).worksheet("운영일지").get_all_records())
                st.dataframe(df[df['이름']==my_name])
            except: st.error("없음")

    with tabs[2]:
        c1, c2 = st.columns(2)
        with c1: py = st.number_input("년", 2025)
        with c2: pm = st.number_input("월 ", datetime.now().month)
        pp = st.radio("기간 ", ["전반기", "후반기"])
        pl = st.selectbox("예정지", locations.get(my_island, ["-"]))
        _, ld = calendar.monthrange(py, pm)
        rng = range(1, 16) if "전반기" in pp else range(16, ld+1)
        sels = st.multiselect("일자 선택", [f"{d}일" for d in rng])
        if st.button("제출"):
            rows = [[datetime(py, pm, int(s.replace("일",""))).strftime("%Y-%m-%d"), my_island, pl, my_name, "", str(datetime.now())] for s in sels]
            if save_bulk("월간계획", rows): st.success("완료")

    if my_role in ["조장", "관리자"]:
        with tabs[3]:
            if st.button("검토 목록"):
                try:
                    df = pd.DataFrame(client.open(SPREADSHEET_NAME).worksheet("운영일지").get_all_records())
                    if my_role!="관리자": df=df[df['섬']==my_island]
                    df = df[df['상태']=="검토대기"]
                    st.dataframe(df)
                    if not df.empty and st.button("일괄 승인"): approve_rows(df.index.tolist()); st.success("완료")
                except: st.error("오류")

    # -----------------------------------------------------
    # 탭 5: 초심플 통계 (입도객 제외)
    # -----------------------------------------------------
    if my_role == "관리자":
        with tabs[4]:
            st.header("📊 운영 성과 분석")
            st.caption("3월부터 12월까지의 운영 데이터를 분석합니다.")

            if st.button("📈 분석 실행", type="primary"):
                try:
                    # 데이터 로드
                    raw_df = pd.DataFrame(client.open(SPREADSHEET_NAME).worksheet("운영일지").get_all_records())
                    raw_df['날짜'] = pd.to_datetime(raw_df['날짜'])
                    raw_df['월'] = raw_df['날짜'].dt.month
                    # 3월 이후 필터링
                    raw_df = raw_df[(raw_df['월'] >= 3)]
                    
                    for col in ['방문자', '청취자', '해설횟수']:
                        raw_df[col] = pd.to_numeric(raw_df[col], errors='coerce').fillna(0)

                    # 1. 전체 요약
                    st.subheader("1. 🌍 전체 운영 요약 (3월~12월)")
                    c1, c2, c3 = st.columns(3)
                    c1.metric("총 방문객", f"{raw_df['방문자'].sum():,.0f}명")
                    c2.metric("총 해설 청취자", f"{raw_df['청취자'].sum():,.0f}명")
                    c3.metric("총 해설 횟수", f"{raw_df['해설횟수'].sum():,.0f}회")
                    st.divider()

                    # 2. 섬별 월별 추세 (꺾은선)
                    st.subheader("2. 🏝️ 섬별 월별 방문객 추세")
                    monthly_stats = raw_df.groupby(['섬', '월'])['방문자'].sum().reset_index()
                    
                    for island in ["백령도", "대청도", "소청도"]:
                        i_stats = monthly_stats[monthly_stats['섬'] == island]
                        if not i_stats.empty:
                            st.write(f"**📌 {island}**")
                            # 월을 인덱스로 하여 그래프 그리기
                            chart_df = i_stats.set_index('월')[['방문자']]
                            st.line_chart(chart_df)

                    st.divider()

                    # 3. 명소별 순위 (막대)
                    st.subheader("3. 🏛️ 안내소(지질명소)별 누적 방문객 순위")
                    spot_df = raw_df.groupby('장소')[['방문자']].sum().sort_values('방문자', ascending=False)
                    st.bar_chart(spot_df)
                    
                    with st.expander("상세 수치 보기"):
                        st.dataframe(spot_df)

                except Exception as e:
                    st.error(f"분석 중 오류 발생: {e}")
