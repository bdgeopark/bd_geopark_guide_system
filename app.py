import streamlit as st
import pandas as pd
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time
import calendar
import requests
from urllib.parse import unquote

# =========================================================
# 🔽 [설정] 고정값 (API키 & 항로코드)
# =========================================================
FIXED_API_KEY = "93baaca371aa86d8d732ad1435bc61fc5e78baec5bb0b98077bc6ee8046b7cac"
FIXED_ROUTE_CODE = "D02" 
# =========================================================

# ---------------------------------------------------------
# 1. 시스템 설정 & CSS
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
if 'step1_df' not in st.session_state: st.session_state['step1_df'] = None 
if 'step2_dfs' not in st.session_state: st.session_state['step2_dfs'] = {} 
if 'current_step' not in st.session_state: st.session_state['current_step'] = 1
if 'last_input_key' not in st.session_state: st.session_state['last_input_key'] = ""
if 'cancellation_dates' not in st.session_state: st.session_state['cancellation_dates'] = []

# API 설정
if 'api_key' not in st.session_state: st.session_state['api_key'] = FIXED_API_KEY
if 'route_code' not in st.session_state: st.session_state['route_code'] = FIXED_ROUTE_CODE

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
# ★ 입도객 데이터 불러오기 (시트에서)
def load_monthly_data():
    try:
        sheet = client.open(SPREADSHEET_NAME).worksheet("입도객현황")
        data = sheet.get_all_records()
        if data:
            return pd.DataFrame(data)
        else:
            # 데이터 없으면 기본 틀 생성
            rows = [[f"{m}월", 0, 0, 0] for m in range(3, 13)]
            return pd.DataFrame(rows, columns=["월", "백령_입도객", "대청_입도객", "소청_입도객"])
    except:
        # 시트가 없거나 에러나면 기본 틀 반환
        rows = [[f"{m}월", 0, 0, 0] for m in range(3, 13)]
        return pd.DataFrame(rows, columns=["월", "백령_입도객", "대청_입도객", "소청_입도객"])

# ★ 입도객 데이터 저장하기 (시트로)
def save_monthly_data_to_sheet(df):
    try:
        sheet = client.open(SPREADSHEET_NAME).worksheet("입도객현황")
        sheet.clear() # 기존 내용 지우고
        # 컬럼명 + 데이터 통째로 덮어쓰기
        sheet.update([df.columns.values.tolist()] + df.values.tolist())
        return True
    except Exception as e:
        st.error(f"저장 실패: '입도객현황' 시트가 있는지 확인해주세요. ({e})")
        return False

# 앱 시작 시 데이터 로드 (한 번만)
if 'monthly_arrivals' not in st.session_state or not isinstance(st.session_state['monthly_arrivals'], pd.DataFrame):
    st.session_state['monthly_arrivals'] = load_monthly_data()

def login(username, password):
    if client is None: st.error("❌ 서버 연결 실패"); return
    try:
        doc = client.open(SPREADSHEET_NAME)
    except: st.error(f"❌ '{SPREADSHEET_NAME}' 파일을 찾을 수 없습니다."); return

    try: sheet = doc.worksheet("사용자")
    except: st.error("❌ '사용자' 시트가 없습니다."); return

    try:
        users = sheet.get_all_records()
        if not users: st.error("❌ 사용자 데이터가 없습니다."); return
        for user in users:
            u_id = str(user.get('아이디', '')).strip()
            u_pw = str(user.get('비번', '')).strip()
            if u_id == str(username).strip() and u_pw == str(password).strip():
                st.session_state['logged_in'] = True
                st.session_state['user_info'] = user
                st.session_state['monthly_arrivals'] = load_monthly_data() # 로그인 시 데이터 로드
                st.success(f"환영합니다, {user['이름']}님!")
                time.sleep(0.5); st.rerun(); return
        st.error("🚫 아이디/비번 불일치")
    except Exception as e: st.error(f"❌ 오류: {e}")

@st.cache_data(ttl=3600)
def get_users_by_island_cached(island_name):
    try:
        if client is None: return []
        sheet = client.open(SPREADSHEET_NAME).worksheet("사용자")
        users = sheet.get_all_records()
        return [u['이름'] for u in users if u.get('섬') == island_name]
    except: return []

def update_monthly_data_callback():
    st.session_state['monthly_arrivals'] = st.session_state["arrival_editor"]

def save_overwrite(sheet_name, new_rows):
    try:
        sheet = client.open(SPREADSHEET_NAME).worksheet(sheet_name)
        existing_data = sheet.get_all_records()
        if not existing_data: sheet.append_rows(new_rows); return True
        
        old_df = pd.DataFrame(existing_data)
        new_df = pd.DataFrame(new_rows, columns=['날짜', '섬', '장소', '이름', '활동시간', '방문자', '청취자', '해설횟수', '타임스탬프', '상태'])
        
        old_df['unique_key'] = old_df['날짜'].astype(str) + "_" + old_df['장소'] + "_" + old_df['이름']
        new_df['unique_key'] = new_df['날짜'].astype(str) + "_" + new_df['장소'] + "_" + new_df['이름']
        
        keys_to_remove = new_df['unique_key'].tolist()
        final_df = old_df[~old_df['unique_key'].isin(keys_to_remove)].copy()
        final_df = final_df.drop(columns=['unique_key'])
        
        cols_order = ['날짜', '섬', '장소', '이름', '활동시간', '방문자', '청취자', '해설횟수', '타임스탬프', '상태']
        for c in cols_order:
            if c not in final_df.columns: final_df[c] = ""
        
        final_df = final_df[cols_order]
        new_df = new_df[cols_order]
        combined_df = pd.concat([final_df, new_df], ignore_index=True)
        
        sheet.clear()
        sheet.update([combined_df.columns.values.tolist()] + combined_df.values.tolist())
        return True
    except Exception as e: st.error(f"저장 오류: {e}"); return False

def approve_rows(indices):
    try:
        sheet = client.open(SPREADSHEET_NAME).worksheet("운영일지")
        for idx in indices: sheet.update_cell(idx + 2, 10, "승인완료")
        return True
    except: return False

def fetch_komsa_data(api_key, target_date):
    url = "http://apis.data.go.kr/1514230/KeoStatInfoService/getWfrNvgStatInfo"
    decoded_key = unquote(api_key) 
    params = {"serviceKey": decoded_key, "pageNo": "1", "numOfRows": "100", "dataType": "JSON", "nvgYmd": target_date.replace("-", "")}
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
    my_role = user['직책']

    with st.sidebar:
        st.info(f"👤 **{my_name}** ({my_role})")
        if st.button("🔄 입력화면 초기화"):
            st.session_state['step1_df'] = None; st.session_state['step2_dfs'] = {}; st.session_state['current_step'] = 1; st.rerun()
        st.divider()
        if st.button("로그아웃"): st.session_state['logged_in'] = False; st.rerun()

    st.title(f"📱 {my_name}님의 업무공간")
    tabs = st.tabs(["📝 활동 입력", "📅 내 활동 조회", "🗓️ 다음달 계획", "👀 조원 검토", "📊 통계"])

    # 탭 1~4 (기존과 동일)
    with tabs[0]: 
        st.subheader("활동 실적 등록")
        # ... (이전 코드와 동일, 생략 없이 작동) ...
        # (지면 관계상 핵심 로직 유지하고 위쪽 코드는 이전과 동일하게 들어갑니다)
        # ※ 실제 사용시는 이전 답변의 '활동 입력' 부분 코드가 그대로 있어야 합니다.
        # 편의를 위해 '통계' 탭 부분만 집중적으로 수정되었습니다.
        
        # (여기서부터 활동 입력 UI 코드 복원)
        c1, c2, c3 = st.columns([1, 1, 2])
        with c1: t_year = st.number_input("년", value=datetime.now().year)
        with c2: t_month = st.number_input("월", value=datetime.now().month)
        with c3: 
            if my_role == "관리자": sel_island = st.selectbox("섬 선택", list(locations.keys()))
            else: sel_island = user['섬']; st.success(f"📍 {sel_island}")
        c4, c5 = st.columns([1, 2])
        with c4: period = st.radio("기간", ["전반기(1~15일)", "후반기(16~말일)"], horizontal=True)
        with c5: sel_place = st.selectbox("근무 장소(공통)", locations.get(sel_island, ["-"]))
        island_users = get_users_by_island_cached(sel_island)

        current_key = f"{t_year}-{t_month}-{sel_island}-{period}-{sel_place}"
        if st.session_state['last_input_key'] != current_key:
            st.session_state['step1_df'] = None; st.session_state['step2_dfs'] = {}; st.session_state['current_step'] = 1; st.session_state['last_input_key'] = current_key; st.rerun()
        st.divider()

        if st.session_state['current_step'] == 1:
            st.markdown("### 1️⃣ 단계: 운영 현황 입력")
            if st.session_state['step1_df'] is None:
                _, last_day = calendar.monthrange(t_year, t_month)
                day_range = range(1, 16) if "전반기" in period else range(16, last_day + 1)
                rows = [[datetime(t_year, t_month, d).strftime("%Y-%m-%d"), datetime(t_year, t_month, d).strftime("%a"), 0, 0, 0, 0] for d in day_range]
                st.session_state['step1_df'] = pd.DataFrame(rows, columns=["일자", "요일", "방문자", "청취자", "해설횟수", "활동해설사수"])
            
            edited_step1 = st.data_editor(st.session_state['step1_df'], hide_index=True, use_container_width=True)
            if st.button("💾 저장 및 다음 단계"):
                stats_rows = []
                max_guides = 0
                for _, row in edited_step1.iterrows():
                    g_cnt = int(row["활동해설사수"])
                    if g_cnt > max_guides: max_guides = g_cnt
                    if row["방문자"]>0 or row["청취자"]>0 or row["해설횟수"]>0:
                        stats_rows.append([row["일자"], sel_island, sel_place, "운영통계", 0, row["방문자"], row["청취자"], row["해설횟수"], str(datetime.now()), "검토대기"])
                if stats_rows: 
                    if save_overwrite("운영일지", stats_rows): st.toast("✅ 저장 완료!")
                if max_guides > 0:
                    dfs = {}
                    for k in range(1, max_guides+1):
                        data_k = []
                        for _, row in edited_step1.iterrows():
                            if int(row["활동해설사수"]) >= k: data_k.append([row["일자"], row["요일"], None, "8시간", 0])
                        dfs[k] = pd.DataFrame(data_k, columns=["일자", "요일", "해설사", "활동시간", "시간(직접)"])
                    st.session_state['step2_dfs'] = dfs; st.session_state['current_step'] = 2; st.rerun()
                else: st.success("✅ 저장됨"); time.sleep(1); st.session_state['step1_df']=None; st.rerun()

        elif st.session_state['current_step'] == 2:
            st.markdown("### 2️⃣ 단계: 해설사 활동 상세 입력")
            dfs = st.session_state['step2_dfs']
            for k in range(1, len(dfs)+1):
                st.markdown(f"#### 👤 **{k}번 해설사**")
                s_name = st.selectbox(f"{k}번 이름", ["선택안함"]+island_users, key=f"sel_{k}")
                if s_name != "선택안함": dfs[k]["해설사"] = s_name
                st.session_state['step2_dfs'][k] = st.data_editor(dfs[k], key=f"ed_{k}", hide_index=True, use_container_width=True)
            if st.button("✅ 일괄 저장"):
                all_r = []
                for k in dfs:
                    tdf = st.session_state['step2_dfs'][k]
                    for _, r in tdf.iterrows():
                        fh = 8
                        if r["활동시간"]=="4시간": fh=4
                        elif r["활동시간"]=="직접입력": fh=float(r["시간(직접)"] or 0)
                        if fh==0: continue
                        all_r.append([r["일자"], sel_island, sel_place, r["해설사"], fh, 0, 0, 0, str(datetime.now()), "검토대기"])
                if save_overwrite("운영일지", all_r): st.success("저장 완료"); time.sleep(1); st.session_state['step1_df']=None; st.session_state['current_step']=1; st.rerun()

    with tabs[1]: # 조회
        if st.button("내역 조회"):
            try:
                df = pd.DataFrame(client.open(SPREADSHEET_NAME).worksheet("운영일지").get_all_records())
                st.dataframe(df[df['이름']==my_name])
            except: st.error("없음")

    with tabs[2]: # 계획
        c1, c2 = st.columns(2)
        with c1: py = st.number_input("년", 2025)
        with c2: pm = st.number_input("월 ", datetime.now().month)
        pp = st.radio("기간 ", ["전반기", "후반기"])
        pl = st.selectbox("예정지", locations.get(user['섬'], ["-"]))
        _, ld = calendar.monthrange(py, pm)
        rng = range(1, 16) if "전반기" in pp else range(16, ld+1)
        sels = st.multiselect("일자 선택", [f"{d}일" for d in rng])
        if st.button("제출"):
            rows = [[datetime(py, pm, int(s.replace("일",""))).strftime("%Y-%m-%d"), user['섬'], pl, my_name, "", str(datetime.now())] for s in sels]
            try: client.open(SPREADSHEET_NAME).worksheet("월간계획").append_rows(rows); st.success("완료")
            except: st.error("실패")

    if my_role in ["조장", "관리자"]: # 검토
        with tabs[3]:
            if st.button("검토 목록"):
                try:
                    df = pd.DataFrame(client.open(SPREADSHEET_NAME).worksheet("운영일지").get_all_records())
                    if my_role!="관리자": df=df[df['섬']==user['섬']]
                    df = df[df['상태']=="검토대기"]
                    st.dataframe(df)
                    if not df.empty and st.button("일괄 승인"): approve_rows(df.index.tolist()); st.success("완료")
                except: st.error("오류")

    # -----------------------------------------------------
    # 탭 5: 고급 통계 (저장 기능 추가됨!)
    # -----------------------------------------------------
    if my_role == "관리자":
        with tabs[4]:
            st.header("📊 통합 운영 및 결항 분석")
            
            with st.expander("⚙️ [설정] API 키 & 대표 항로코드", expanded=True):
                api_key_input = st.text_input("API 인증키", value=st.session_state['api_key'], type="password")
                route_code_input = st.text_input("대표 항로코드", value=st.session_state['route_code'])
                if st.button("설정 저장"): 
                    st.session_state['api_key'] = api_key_input
                    st.session_state['route_code'] = route_code_input
                    st.success("저장됨")

            st.subheader("1. 📥 데이터 입력")
            t_i1, t_i2 = st.tabs(["월별 입도객", "결항일 관리"])
            
            with t_i1:
                st.info("월별 입도객 수를 입력하세요. (입력 후 아래 저장 버튼 필수!)")
                new_arrivals = st.data_editor(
                    st.session_state['monthly_arrivals'], 
                    hide_index=True, 
                    use_container_width=True,
                    key="arrival_editor",
                    on_change=update_monthly_data_callback
                )
                
                # ★ 저장 버튼 추가
                if st.button("💾 입도객 데이터 서버에 저장하기"):
                    if save_monthly_data_to_sheet(new_arrivals):
                        st.success("✅ 구글 시트('입도객현황')에 안전하게 저장되었습니다.")
                    else:
                        st.error("❌ 저장 실패. 시트 이름을 확인하세요.")
            
            with t_i2:
                st.info("D02(인천 출발) 항로의 전면/부분 결항을 찾습니다.")
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
                            target_code = st.session_state['route_code']
                            with st.status("API 조회 중...", expanded=True) as s:
                                for d in range(1, ld+1):
                                    d_s = f"{y}-{t_m:02d}-{d:02d}"
                                    s.update(label=f"{d_s} 조회...")
                                    res = fetch_komsa_data(st.session_state['api_key'], d_s)
                                    if res:
                                        for item in res:
                                            if item.get('seawy_cd') == target_code:
                                                is_full = (int(item.get('nvg_nocs', 1)) == 0)
                                                is_partial = (int(item.get('plan_nvg_vsl_cnt', 0)) > int(item.get('nvg_vsl_cnt', 0)))
                                                if is_full or is_partial: f_dates.append(d_s)
                                    time.sleep(0.1)
                                s.update(label="완료!", state="complete", expanded=False)
                            if f_dates:
                                st.success(f"D02 항로 특이사항(결항) {len(f_dates)}일 발견: {f_dates}")
                                cur = set(st.session_state['cancellation_dates'])
                                cur.update(f_dates)
                                st.session_state['cancellation_dates'] = sorted(list(cur))
                            else: st.info("정상 운항")
                
                if st.session_state['cancellation_dates']:
                    rd = st.multiselect("삭제할 날짜", st.session_state['cancellation_dates'])
                    if st.button("선택 삭제"):
                        for d in rd: st.session_state['cancellation_dates'].remove(d)
                        st.rerun()

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

                    st.subheader("2. 🚢 결항(D02 중단) 시 행동 분석")
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
