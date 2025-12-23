import streamlit as st
import pandas as pd
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time
import calendar
import requests
from urllib.parse import unquote

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

# 데이터 저장소
if 'step1_df' not in st.session_state: st.session_state['step1_df'] = None 
if 'step2_dfs' not in st.session_state: st.session_state['step2_dfs'] = {} 
if 'current_step' not in st.session_state: st.session_state['current_step'] = 1
if 'last_input_key' not in st.session_state: st.session_state['last_input_key'] = ""

# 통계용
if 'monthly_arrivals' not in st.session_state:
    rows = [[f"{m}월", 0, 0, 0] for m in range(3, 13)]
    st.session_state['monthly_arrivals'] = pd.DataFrame(rows, columns=["월", "백령_입도객", "대청_입도객", "소청_입도객"])
if 'cancellation_dates' not in st.session_state: st.session_state['cancellation_dates'] = []

# API 설정
if 'api_key' not in st.session_state: st.session_state['api_key'] = ""
if 'route_codes' not in st.session_state: 
    st.session_state['route_codes'] = {"백령": "J04-03", "대청": "J03-03", "소청": "J03-03"}

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

# API 호출
def fetch_komsa_data(api_key, target_date):
    url = "http://apis.data.go.kr/1514230/KeoStatInfoService/getWfrNvgStatInfo"
    decoded_key = unquote(api_key) # 자동 키 변환
    params = {
        "serviceKey": decoded_key,
        "pageNo": "1",
        "numOfRows": "100",
        "dataType": "JSON",
        "nvgYmd": target_date.replace("-", "")
    }
    try:
        response = requests.get(url, params=params)
        data = response.json()
        try: return data['response']['body']['items']['item']
        except: return None
    except: return None

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
            if my_role == "관리자": sel_island = st.selectbox("섬 선택", list(locations.keys()))
            else: sel_island = my_island; st.success(f"📍 {sel_island}")
        c4, c5 = st.columns([1, 2])
        with c4: period = st.radio("기간", ["전반기(1~15일)", "후반기(16~말일)"], horizontal=True)
        with c5: sel_place = st.selectbox("근무 장소(공통)", locations.get(sel_island, ["-"]))
        island_users = get_users_by_island_cached(sel_island)

        # 조건 변경 시 초기화
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

        # [STEP 2] 해설사 활동 (★ 수정된 버그 없는 버전)
        elif st.session_state['current_step'] == 2:
            st.markdown("### 2️⃣ 단계: 해설사 활동 상세 입력")
            dfs = st.session_state['step2_dfs']
            
            for k in range(1, len(dfs)+1):
                st.markdown(f"#### 👤 **{k}번 해설사**")
                
                # [버그수정] 변경 감지용 세션 키 생성
                track_key = f"last_sel_{k}"
                if track_key not in st.session_state: st.session_state[track_key] = "선택안함"
                
                s_name = st.selectbox(
                    f"{k}번 해설사 이름 (일괄적용)", 
                    ["선택안함"] + island_users, 
                    key=f"sel_{k}"
                )
                
                # [버그수정] 셀렉트박스를 '건드렸을 때만' 표에 적용
                if s_name != st.session_state[track_key]:
                    if s_name != "선택안함":
                        dfs[k]["해설사"] = s_name # 일괄 적용
                    st.session_state[track_key] = s_name # 현재 값 기억
                
                # 데이터 에디터 (여기서 수정하면 dfs[k]가 아닌 세션 내 데이터가 업데이트됨)
                st.session_state['step2_dfs'][k] = st.data_editor(
                    dfs[k], 
                    key=f"ed_{k}", 
                    hide_index=True, 
                    use_container_width=True
                )
            
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
    # 탭 5: 고급 통계 (API 자동화 포함)
    # -----------------------------------------------------
    if my_role == "관리자":
        with tabs[4]:
            st.header("📊 통합 운영 및 결항 분석")
            
            # [설정]
            with st.expander("⚙️ [설정] 여객선 결항 API 키 및 항로코드", expanded=True):
                st.write("공공데이터포털 인증키(Decoding Key 권장)를 입력하세요.")
                api_key_input = st.text_input("API 인증키", value=st.session_state['api_key'], type="password")
                if st.button("키 저장"): st.session_state['api_key'] = api_key_input; st.success("저장됨")
                
                st.divider()
                st.write("📝 **항로코드 설정** (백령: J04-03, 대청/소청: J03-03)")
                c_c1, c_c2, c_c3 = st.columns(3)
                with c_c1: st.session_state['route_codes']['백령'] = st.text_input("백령 코드", st.session_state['route_codes']['백령'])
                with c_c2: st.session_state['route_codes']['대청'] = st.text_input("대청 코드", st.session_state['route_codes']['대청'])
                with c_c3: st.session_state['route_codes']['소청'] = st.text_input("소청 코드", st.session_state['route_codes']['소청'])

            # [섹션 1] 입력
            st.subheader("1. 📥 데이터 입력")
            t_i1, t_i2 = st.tabs(["월별 입도객", "결항일 관리"])
            
            with t_i1:
                st.info("월별 입도객 수를 입력하세요.")
                st.session_state['monthly_arrivals'] = st.data_editor(st.session_state['monthly_arrivals'], hide_index=True, use_container_width=True)
            
            with t_i2:
                st.info("운항 횟수 '0'인 날을 자동으로 찾습니다.")
                c_a1, c_a2 = st.columns([1, 2])
                with c_a1: t_m = st.number_input("조회 월", 1, 12, datetime.now().month)
                with c_a2:
                    st.write("")
                    st.write("")
                    if st.button(f"{t_m}월 결항일 자동 가져오기"):
                        if not st.session_state['api_key']: st.error("API 키 필요")
                        else:
                            y = datetime.now().year
                            _, ld = calendar.monthrange(y, t_m)
                            f_dates = []
                            with st.status("API 조회 중...", expanded=True) as s:
                                for d in range(1, ld+1):
                                    d_s = f"{y}-{t_m:02d}-{d:02d}"
                                    s.update(label=f"{d_s} 조회...")
                                    res = fetch_komsa_data(st.session_state['api_key'], d_s)
                                    if res:
                                        # 백령, 대청, 소청 중 하나라도 코드가 있고 운항이 0이면 결항 처리
                                        codes = list(st.session_state['route_codes'].values())
                                        for item in res:
                                            if item.get('seawy_cd') in codes:
                                                if int(item.get('nvg_nocs', 1)) == 0:
                                                    f_dates.append(d_s)
                                                    break # 하나라도 결항이면 추가하고 다음 날짜로
                                    time.sleep(0.1)
                                s.update(label="완료!", state="complete", expanded=False)
                            
                            if f_dates:
                                st.success(f"{len(f_dates)}일 찾음: {f_dates}")
                                cur = set(st.session_state['cancellation_dates'])
                                cur.update(f_dates)
                                st.session_state['cancellation_dates'] = sorted(list(cur))
                            else: st.info("결항 없음")
                
                st.write("📋 **등록된 결항일**")
                if st.session_state['cancellation_dates']:
                    rd = st.multiselect("삭제할 날짜", st.session_state['cancellation_dates'])
                    if st.button("선택 삭제"):
                        for d in rd: st.session_state['cancellation_dates'].remove(d)
                        st.rerun()

            # [섹션 2] 분석
            if st.button("📈 분석 결과 보기", type="primary"):
                try:
                    df = pd.DataFrame(client.open(SPREADSHEET_NAME).worksheet("운영일지").get_all_records())
                    df['날짜'] = pd.to_datetime(df['날짜'])
                    df['월'] = df['날짜'].dt.month
                    df = df[df['월']>=3]
                    for c in ['방문자','청취자','해설횟수']: df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)

                    st.subheader("1. 📈 월별 추세")
                    m_stats = df.groupby(['섬','월'])['방문자'].sum().reset_index()
                    arr = st.session_state['monthly_arrivals'].copy()
                    arr['월_숫자'] = arr['월'].str.replace("월","").astype(int)
                    
                    for isl in ["백령도", "대청도", "소청도"]:
                        ist = m_stats[m_stats['섬']==isl]
                        if not ist.empty:
                            mged = pd.merge(ist, arr, left_on='월', right_on='월_숫자', how='left')
                            mged['방문율(%)'] = (mged['방문자']/mged[f"{isl[:2]}_입도객"]*100).fillna(0)
                            st.write(f"**🏝️ {isl}**")
                            st.line_chart(mged.set_index('월_숫자')[['방문자','방문율(%)']])

                    st.subheader("2. 🚢 결항 시 행동 분석")
                    if not st.session_state['cancellation_dates']: st.info("결항일 없음")
                    else:
                        cds = sorted([pd.to_datetime(d) for d in st.session_state['cancellation_dates']])
                        cmap = {}
                        streak, prev = 1, None
                        for d in cds:
                            if prev and (d-prev).days==1: streak+=1
                            else: streak=1
                            cmap[d]=streak
                            prev=d
                        df['결항일차'] = df['날짜'].map(cmap).fillna(0)
                        cdf = df[df['결항일차']>0]
                        if cdf.empty: st.warning("데이터 없음")
                        else:
                            pvt = cdf.groupby(['결항일차','장소'])['방문자'].mean().reset_index().pivot(index='결항일차',columns='장소',values='방문자').fillna(0)
                            st.line_chart(pvt)
                except Exception as e: st.error(str(e))
