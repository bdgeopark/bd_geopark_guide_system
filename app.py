import streamlit as st
import pandas as pd
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time
import calendar
import requests
from urllib.parse import unquote
from collections import Counter

# =========================================================
# 🔽 [설정] 고정값 (API키 & 항로코드) - 보안 적용됨
# =========================================================
# Secrets에 키가 있으면 가져오고, 없으면 빈 값으로 처리 (오류 방지)
if "KOMSA_API_KEY" in st.secrets:
    FIXED_API_KEY = st.secrets["KOMSA_API_KEY"]
else:
    FIXED_API_KEY = "" 

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
    div[data-testid="stMultiSelect"] * { font-size: 18px !important; }
    div[data-testid="stForm"] { border: 2px solid #f0f2f6; padding: 20px; border-radius: 10px; }
    </style>
    """, unsafe_allow_html=True)

# 세션 초기화
if 'logged_in' not in st.session_state: st.session_state['logged_in'] = False
if 'user_info' not in st.session_state: st.session_state['user_info'] = {}
if 'step1_data' not in st.session_state: st.session_state['step1_data'] = {} 
if 'step2_df' not in st.session_state: st.session_state['step2_df'] = None 
if 'current_step' not in st.session_state: st.session_state['current_step'] = 1
if 'last_input_key' not in st.session_state: st.session_state['last_input_key'] = ""
if 'cancellation_dates' not in st.session_state: st.session_state['cancellation_dates'] = []

# 비고(이벤트) 범례 초기값
if 'event_categories' not in st.session_state:
    st.session_state['event_categories'] = ["학생견학/체험활동", "외부단체", "상괭이 사체", "물범 사체", "지뢰 발견"]

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
def load_event_categories():
    default_cats = ["학생견학/체험활동", "외부단체", "상괭이 사체", "물범 사체", "지뢰 발견"]
    if client is None: return default_cats
    try:
        doc = client.open(SPREADSHEET_NAME)
        try: sheet = doc.worksheet("설정")
        except:
            sheet = doc.add_worksheet(title="설정", rows=100, cols=2)
            sheet.update(range_name='A1:A'+str(len(default_cats)), values=[[c] for c in default_cats])
            return default_cats
        vals = sheet.col_values(1)
        if not vals:
            sheet.update(range_name='A1:A'+str(len(default_cats)), values=[[c] for c in default_cats])
            return default_cats
        return vals
    except: return default_cats

def add_new_category_to_sheet(new_cat):
    try:
        sheet = client.open(SPREADSHEET_NAME).worksheet("설정")
        sheet.append_row([new_cat])
        return True
    except: return False

def load_monthly_data():
    try:
        sheet = client.open(SPREADSHEET_NAME).worksheet("입도객현황")
        data = sheet.get_all_records()
        if data: return pd.DataFrame(data)
        else: return pd.DataFrame([[f"{m}월", 0, 0, 0] for m in range(3, 13)], columns=["월", "백령_입도객", "대청_입도객", "소청_입도객"])
    except: return pd.DataFrame([[f"{m}월", 0, 0, 0] for m in range(3, 13)], columns=["월", "백령_입도객", "대청_입도객", "소청_입도객"])

def save_monthly_data_to_sheet(df):
    try:
        sheet = client.open(SPREADSHEET_NAME).worksheet("입도객현황")
        sheet.clear()
        sheet.update([df.columns.values.tolist()] + df.values.tolist())
        return True
    except: return False

if 'monthly_arrivals' not in st.session_state or not isinstance(st.session_state['monthly_arrivals'], pd.DataFrame):
    st.session_state['monthly_arrivals'] = load_monthly_data()

if 'event_categories' not in st.session_state:
    st.session_state['event_categories'] = load_event_categories()

def login(username, password):
    if client is None: st.error("❌ 서버 연결 실패"); return
    try: doc = client.open(SPREADSHEET_NAME)
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
                st.session_state['monthly_arrivals'] = load_monthly_data()
                st.session_state['event_categories'] = load_event_categories()
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

def save_overwrite(sheet_name, new_rows):
    try:
        sheet = client.open(SPREADSHEET_NAME).worksheet(sheet_name)
        existing_data = sheet.get_all_records()
        if not existing_data: sheet.append_rows(new_rows); return True
        
        cols_order = ['날짜', '섬', '장소', '이름', '활동시간', '방문자', '청취자', '해설횟수', '비고', '타임스탬프', '상태']
        
        old_df = pd.DataFrame(existing_data)
        new_df = pd.DataFrame(new_rows, columns=cols_order) 
        
        old_df['unique_key'] = old_df['날짜'].astype(str) + "_" + old_df['장소'] + "_" + old_df['이름']
        new_df['unique_key'] = new_df['날짜'].astype(str) + "_" + new_df['장소'] + "_" + new_df['이름']
        
        keys_to_remove = new_df['unique_key'].tolist()
        final_df = old_df[~old_df['unique_key'].isin(keys_to_remove)].copy()
        final_df = final_df.drop(columns=['unique_key'])
        
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
        for idx in indices: sheet.update_cell(idx + 2, 11, "승인완료")
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
# [수정됨] 활동 계획 관련 함수 ('장소' 컬럼 추가)
# ---------------------------------------------------------
def load_plan_data(year, month, island):
    try:
        sheet = client.open(SPREADSHEET_NAME).worksheet("활동계획")
        records = sheet.get_all_records()
        df = pd.DataFrame(records)
        if not df.empty:
            # 해당 년/월/섬 데이터 필터링
            df = df[(df['년'] == year) & (df['월'] == month) & (df['섬'] == island)]
        return df
    except:
        return pd.DataFrame()

def save_plan_data(new_rows):
    try:
        # 시트가 없으면 생성 (헤더에 '장소' 추가)
        try:
            sheet = client.open(SPREADSHEET_NAME).worksheet("활동계획")
        except:
            doc = client.open(SPREADSHEET_NAME)
            sheet = doc.add_worksheet(title="활동계획", rows=1000, cols=10)
            sheet.append_row(["년", "월", "일자", "섬", "장소", "이름", "활동여부", "비고", "타임스탬프"])
            return True # 빈 시트 생성 후 리턴

        existing = sheet.get_all_records()
        
        if existing:
            old_df = pd.DataFrame(existing)
            # 구버전 데이터(장소 컬럼 없음) 호환성 처리
            if '장소' not in old_df.columns: old_df['장소'] = "미지정"
        else:
            old_df = pd.DataFrame(columns=["년", "월", "일자", "섬", "장소", "이름", "활동여부", "비고", "타임스탬프"])

        new_df = pd.DataFrame(new_rows, columns=["년", "월", "일자", "섬", "장소", "이름", "활동여부", "비고", "타임스탬프"])
        
        # 키 생성: 날짜_이름 (한 사람이 같은 날 여러 곳 신청 방지, 혹은 장소까지 포함하려면 키 변경 필요)
        # 여기서는 "한 사람이 같은 날 하나의 장소만 간다"고 가정하고 덮어쓰기 합니다.
        old_df['key'] = old_df['일자'].astype(str) + "_" + old_df['이름']
        new_df['key'] = new_df['일자'].astype(str) + "_" + new_df['이름']
        
        keys_to_remove = new_df['key'].tolist()
        final_df = old_df[~old_df['key'].isin(keys_to_remove)].copy()
        
        final_df = final_df.drop(columns=['key'])
        new_df = new_df.drop(columns=['key'])
        
        combined_df = pd.concat([final_df, new_df], ignore_index=True)
        
        sheet.clear()
        sheet.update([combined_df.columns.values.tolist()] + combined_df.values.tolist())
        return True
    except Exception as e:
        st.error(f"저장 오류: {e}")
        return False

def get_deadline_info(target_year, target_month, period_type):
    """제출 마감일 계산 로직"""
    if period_type == "전반기(1~15일)":
        # 전월 23일까지
        deadline_month = target_month - 1 if target_month > 1 else 12
        deadline_year = target_year if target_month > 1 else target_year - 1
        return f"{deadline_year}년 {deadline_month}월 23일"
    else:
        # 당월 7일까지
        return f"{target_year}년 {target_month}월 7일"

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
        if st.button("🔄 명단/데이터 강제 새로고침"):
            st.cache_data.clear()
            st.session_state['step1_data'] = {}
            st.session_state['step2_df'] = None
            st.session_state['event_categories'] = load_event_categories()
            st.rerun()
        st.divider()
        if st.button("로그아웃"): st.session_state['logged_in'] = False; st.rerun()

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
            else: sel_island = user['섬']; st.success(f"📍 {sel_island}")
        c4, c5 = st.columns([1, 2])
        with c4: period = st.radio("기간", ["전반기(1~15일)", "후반기(16~말일)"], horizontal=True)
        with c5: sel_place = st.selectbox("근무 장소(공통)", locations.get(sel_island, ["-"]))
        island_users = get_users_by_island_cached(sel_island)

        current_key = f"{t_year}-{t_month}-{sel_island}-{period}-{sel_place}"
        if st.session_state['last_input_key'] != current_key:
            st.session_state['step1_data'] = {}
            st.session_state['step2_df'] = None
            st.session_state['current_step'] = 1
            st.session_state['last_input_key'] = current_key; st.rerun()
        st.divider()

        if st.session_state['current_step'] == 1:
            st.markdown("### 1️⃣ 단계: 운영 통계 및 근무자 선택")
            
            with st.expander("➕ 비고(특이사항) 범례 관리", expanded=False):
                c_add1, c_add2 = st.columns([3, 1])
                new_cat = c_add1.text_input("새로운 항목 입력", label_visibility="collapsed")
                if c_add2.button("영구 추가"):
                    if new_cat and new_cat not in st.session_state['event_categories']:
                        if add_new_category_to_sheet(new_cat):
                            st.session_state['event_categories'].append(new_cat)
                            st.success(f"✅ '{new_cat}' 저장 완료!")
                            time.sleep(1)
                            st.rerun()
                        else: st.error("저장 실패")

            _, last_day = calendar.monthrange(t_year, t_month)
            day_range = range(1, 16) if "전반기" in period else range(16, last_day + 1)
            
            with st.form("roster_form"):
                h1, h2, h3, h4, h5, h6 = st.columns([1.2, 0.8, 0.8, 0.8, 2, 2])
                h1.markdown("**날짜**")
                h2.markdown("**방문**")
                h3.markdown("**청취**")
                h4.markdown("**횟수**")
                h5.markdown("**✅ 근무자**")
                h6.markdown("**📝 비고**")
                
                if not st.session_state['step1_data']:
                    for d in day_range:
                        d_str = datetime(t_year, t_month, d).strftime("%Y-%m-%d")
                        st.session_state['step1_data'][d_str] = {"v": 0, "l": 0, "c": 0, "guides": [], "events": []}

                for d in day_range:
                    d_obj = datetime(t_year, t_month, d)
                    d_str = d_obj.strftime("%Y-%m-%d")
                    day_name = d_obj.strftime("%a")
                    
                    c1, c2, c3, c4, c5, c6 = st.columns([1.2, 0.8, 0.8, 0.8, 2, 2])
                    c1.text(f"{d}일 ({day_name})")
                    
                    val = st.session_state['step1_data'][d_str]
                    new_v = c2.number_input(f"v_{d}", value=val["v"], min_value=0, label_visibility="collapsed", key=f"v_{d}")
                    new_l = c3.number_input(f"l_{d}", value=val["l"], min_value=0, label_visibility="collapsed", key=f"l_{d}")
                    new_c = c4.number_input(f"c_{d}", value=val["c"], min_value=0, label_visibility="collapsed", key=f"c_{d}")
                    
                    new_guides = c5.multiselect(f"g_{d}", island_users, default=val["guides"], label_visibility="collapsed", key=f"g_{d}", placeholder="근무자")
                    
                    new_events = c6.multiselect(
                        f"e_{d}", 
                        st.session_state['event_categories'], 
                        default=val.get("events", []), 
                        label_visibility="collapsed", 
                        key=f"e_{d}",
                        placeholder="특이사항"
                    )
                    
                    st.session_state['step1_data'][d_str] = {"v": new_v, "l": new_l, "c": new_c, "guides": new_guides, "events": new_events}
                
                st.divider()
                submitted1 = st.form_submit_button("💾 저장 및 다음 단계")
            
            if submitted1:
                stats_rows = []
                step2_rows = []
                
                for d in day_range:
                    d_str = datetime(t_year, t_month, d).strftime("%Y-%m-%d")
                    data = st.session_state['step1_data'][d_str]
                    guides = data['guides']
                    events_str = ", ".join(data['events'])
                    
                    if guides or data['v']>0 or data['l']>0 or data['c']>0 or events_str:
                        stats_rows.append([d_str, sel_island, sel_place, "운영통계", 0, data['v'], data['l'], data['c'], events_str, str(datetime.now()), "검토대기"])
                    
                    for g_name in guides:
                        step2_rows.append([d_str, g_name, "8시간", 0, False])

                if stats_rows: 
                    if save_overwrite("운영일지", stats_rows): st.toast("✅ 저장 완료!")
                
                if step2_rows:
                    st.session_state['step2_df'] = pd.DataFrame(step2_rows, columns=["일자", "해설사", "활동시간", "시간(직접)", "확인"])
                    st.session_state['current_step'] = 2
                    st.rerun()
                else:
                    st.warning("선택된 내용이 없습니다.")

        elif st.session_state['current_step'] == 2:
            st.markdown("### 2️⃣ 단계: 근무 시간 확정")
            with st.form("step2_form"):
                edited_df = st.data_editor(
                    st.session_state['step2_df'],
                    hide_index=True,
                    use_container_width=True,
                    column_config={
                        "일자": st.column_config.TextColumn("일자", disabled=True),
                        "해설사": st.column_config.TextColumn("해설사", disabled=True),
                        "활동시간": st.column_config.SelectboxColumn("활동시간", options=["8시간", "4시간", "직접입력"], required=True),
                        "확인": st.column_config.CheckboxColumn("확인", default=False)
                    }
                )
                submitted2 = st.form_submit_button("✅ 최종 저장 완료")
            
            if submitted2:
                all_r = []
                for _, r in edited_df.iterrows():
                    fh = 8
                    if r["활동시간"] == "8시간": fh = 8
                    elif r["활동시간"] == "4시간": fh = 4
                    elif r["활동시간"] == "직접입력": fh = float(r["시간(직접)"] or 0)
                    if fh == 0: continue
                    all_r.append([r["일자"], sel_island, sel_place, r["해설사"], fh, 0, 0, 0, "", str(datetime.now()), "검토대기"])
                
                if save_overwrite("운영일지", all_r): 
                    st.success("🎉 저장 완료!"); 
                    time.sleep(1.5)
                    st.session_state['step1_data'] = {}
                    st.session_state['step2_df'] = None
                    st.session_state['current_step'] = 1
                    st.rerun()
            
            if st.button("🔙 1단계로 돌아가기"): st.session_state['current_step']=1; st.rerun()

    with tabs[1]: # 조회
        if st.button("내역 조회"):
            try:
                df = pd.DataFrame(client.open(SPREADSHEET_NAME).worksheet("운영일지").get_all_records())
                st.dataframe(df[df['이름']==my_name])
            except: st.error("없음")

    with tabs[2]: # 계획
        st.info("계획 입력 기능") 

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
    # 탭 5: 고급 통계 (★ 통합 피벗 테이블 적용)
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
                st.info("월별 입도객 수를 입력하세요.")
                with st.form("arrivals_form"):
                    new_arrivals = st.data_editor(st.session_state['monthly_arrivals'], hide_index=True, use_container_width=True)
                    saved = st.form_submit_button("💾 입도객 데이터 서버에 저장하기")
                if saved:
                    st.session_state['monthly_arrivals'] = new_arrivals
                    if save_monthly_data_to_sheet(new_arrivals): st.success("✅ 저장 완료")
                    else: st.error("❌ 저장 실패")
            with t_i2:
                st.info("D02(인천 출발) 항로의 전면/부분 결항을 찾습니다.")
                c_a1, c_a2 = st.columns([1, 2])
                with c_a1: t_m = st.number_input("조회 월", 1, 12, datetime.now().month)
                with c_a2:
                    st.write(""); st.write("")
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
                                st.success(f"특이사항 {len(f_dates)}건 발견: {f_dates}")
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
                    for c in ['방문자','청취자','해설횟수']: df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)

                    # ★ 1. 안내소별 상세 실적 (통합 피벗 테이블)
                    st.markdown("### 1. 🏢 안내소별/월별 상세 실적 (통합)")
                    
                    pivot_df = df.pivot_table(
                        index=["섬", "장소"],
                        columns="월",
                        values=["방문자", "청취자", "해설횟수"], # ★ 3가지 항목 모두 포함
                        aggfunc="sum",
                        fill_value=0,
                        margins=True, # ★ 행/열 합계 자동 계산
                        margins_name="합계(All)"
                    )
                    st.dataframe(pivot_df, use_container_width=True)

                    st.divider()

                    # 2. 월별 추세
                    st.markdown("### 2. 📈 월별 전체 추세")
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

                    # 3. 결항 분석
                    st.markdown("### 3. 🚢 결항 시 행동 분석")
                    if st.session_state['cancellation_dates']:
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
                        if not cdf.empty:
                            pvt = cdf.groupby(['결항일차','장소'])['방문자'].mean().reset_index().pivot(index='결항일차',columns='장소',values='방문자').fillna(0)
                            st.line_chart(pvt)
                    else: st.info("결항 데이터 없음")

                    # 4. 특이사항
                    st.markdown("### 4. 🚩 특이사항 빈도 분석")
                    if '비고' in df.columns:
                        event_df = df[df['비고'] != ""]
                        if not event_df.empty:
                            all_events = []
                            for events in event_df['비고']:
                                split_ev = [e.strip() for e in events.split(",")]
                                all_events.extend(split_ev)
                            
                            counts = Counter(all_events)
                            count_df = pd.DataFrame.from_dict(counts, orient='index', columns=['횟수']).sort_values('횟수', ascending=False)
                            c1, c2 = st.columns(2)
                            with c1: st.bar_chart(count_df)
                            with c2: st.dataframe(event_df[['날짜', '섬', '장소', '비고']], hide_index=True)
                        else: st.info("기록된 특이사항 없음")

                except Exception as e: st.error(str(e))

# -----------------------------------------------------
    # 탭 3: 활동 계획 (업그레이드: 한글 요일 + 필터링 + 현황판)
    # -----------------------------------------------------
    with tabs[2]: 
        st.header("🗓️ 안내소별 활동 계획 수립")
        
        # 0. 한글 요일 변환 헬퍼
        day_map = {0: "월", 1: "화", 2: "수", 3: "목", 4: "금", 5: "토", 6: "일"}

        # 1. 공통 설정: 년/월/기간
        today = datetime.now()
        next_month_date = today.replace(day=28) + pd.Timedelta(days=4)
        default_year = next_month_date.year
        default_month = next_month_date.month
        
        c_p1, c_p2, c_p3 = st.columns([1, 1, 2])
        with c_p1: p_year = st.number_input("활동 연도", value=default_year)
        with c_p2: p_month = st.number_input("활동 월", value=default_month)
        with c_p3: p_range = st.radio("활동 기간", ["전반기(1~15일)", "후반기(16~말일)"], horizontal=True)

        # 날짜 리스트 생성
        _, last_day = calendar.monthrange(p_year, p_month)
        if "전반기" in p_range:
            target_dates = [datetime(p_year, p_month, d).strftime("%Y-%m-%d") for d in range(1, 16)]
        else:
            target_dates = [datetime(p_year, p_month, d).strftime("%Y-%m-%d") for d in range(16, last_day + 1)]

        # DB 로드
        current_island = user['섬'] if my_role != "관리자" else st.selectbox("섬 선택 (관리자)", ["백령도", "대청도", "소청도"])
        plan_df = load_plan_data(p_year, p_month, current_island)
        place_options = locations.get(current_island, [])
        
        shift_options = ["", "종일", "오전(4시간)", "오후(4시간)", "기타"]

        st.divider()

        # =================================================
        # 🟢 [기능 1] 내 계획 입력 함수 (한글 요일 적용)
        # =================================================
        def render_my_plan_input(role_name, user_name):
            st.subheader(f"🙋‍♂️ {user_name}님의 근무 신청")
            
            # 1. 안내소 선택
            selected_place = st.selectbox("근무할 안내소를 선택하세요", place_options, key="my_place_sel")
            st.info(f"👉 **{selected_place}** 근무 일정을 입력하세요.")

            # 2. 데이터 매핑
            input_data = []
            my_prev_data = {}
            
            if not plan_df.empty:
                cond = (plan_df['이름'] == user_name) & (plan_df['장소'] == selected_place)
                filtered = plan_df[cond]
                for _, r in filtered.iterrows():
                    my_prev_data[r['일자']] = r['활동여부']

            for d_str in target_dates:
                d_obj = datetime.strptime(d_str, "%Y-%m-%d")
                w_day = day_map[d_obj.weekday()] # 한글 요일
                
                current_val = my_prev_data.get(d_str, "")
                if current_val == "O": current_val = "종일" 
                
                input_data.append({
                    "날짜": d_str,
                    "요일": w_day,
                    "근무형태": current_val
                })
            
            input_df = pd.DataFrame(input_data)

            # 3. 데이터 에디터
            with st.form("my_plan_form"):
                edited_df = st.data_editor(
                    input_df,
                    column_config={
                        "날짜": st.column_config.TextColumn(disabled=True),
                        "요일": st.column_config.TextColumn(disabled=True),
                        "근무형태": st.column_config.SelectboxColumn("근무 선택", options=shift_options, required=True)
                    },
                    hide_index=True,
                    use_container_width=True,
                    height=600
                )

                if st.form_submit_button("💾 내 계획 저장하기"):
                    save_rows = []
                    for _, row in edited_df.iterrows():
                        save_rows.append([p_year, p_month, row['날짜'], current_island, selected_place, user_name, row['근무형태'], "", str(datetime.now())])
                    
                    if save_plan_data(save_rows):
                        st.success("✅ 저장되었습니다!"); time.sleep(1); st.rerun()

        # =================================================
        # 🔵 [기능 2] 조원 계획 승인 (필터링 + 제출현황)
        # =================================================
        def render_team_approval():
            # [추가] 제출/미제출 현황판
            st.markdown("#### 📊 계획 제출 현황")
            users_in_island = get_users_by_island_cached(current_island)
            
            submitted_users = set()
            if not plan_df.empty:
                # 활동여부가 빈값이 아닌 데이터가 하나라도 있는 사람
                active_df = plan_df[plan_df['활동여부'] != ""]
                submitted_users = set(active_df['이름'].unique())
            
            not_submitted_users = [u for u in users_in_island if u not in submitted_users]
            submitted_list_str = ", ".join(submitted_users) if submitted_users else "(없음)"
            not_submitted_list_str = ", ".join(not_submitted_users) if not_submitted_users else "(모두 제출함)"

            # 현황 표시 (색상 강조)
            s_col1, s_col2 = st.columns(2)
            s_col1.success(f"**제출 완료 ({len(submitted_users)}명):**\n\n {submitted_list_str}")
            s_col2.error(f"**미제출 ({len(not_submitted_users)}명):**\n\n {not_submitted_list_str}")
            
            st.divider()

            # [기존 로직] 장소 선택
            c_view1, c_view2 = st.columns([2, 1])
            with c_view1:
                target_place = st.selectbox("관리할 안내소 선택", place_options, key="lead_place_sel")
            with c_view2:
                special_note = st.text_input("특이사항 (출력용)", placeholder="예: 행사 지원 등")

            st.subheader(f"📋 {target_place} 근무 편성표")

            # 1. 데이터 가공 (필터링 적용)
            place_plan_df = pd.DataFrame()
            if not plan_df.empty:
                if '장소' not in plan_df.columns: plan_df['장소'] = "미지정"
                place_plan_df = plan_df[(plan_df['장소'] == target_place) & (plan_df['활동여부'] != "")]

            # [핵심 수정] 해당 장소에 신청 내역이 있는 사람만 필터링
            active_users_in_place = []
            if not place_plan_df.empty:
                active_users_in_place = place_plan_df['이름'].unique().tolist()
            
            # 목록에 표시할 유저 (신청자만)
            display_users = [u for u in users_in_island if u in active_users_in_place]

            if not display_users:
                st.warning(f"⚠️ '{target_place}'에 근무를 신청한 해설사가 아직 없습니다.")
            
            # 매트릭스 생성
            matrix_data = []
            for d in target_dates:
                d_obj = datetime.strptime(d, "%Y-%m-%d")
                w_day = day_map[d_obj.weekday()] # 한글 요일
                
                row = {
                    "날짜": f"{d_obj.day}일 ({w_day})", 
                    "raw_date": d,
                }
                
                active_count = 0
                for u in display_users: # 필터링된 유저만 반복
                    val = ""
                    if not place_plan_df.empty:
                        check = place_plan_df[(place_plan_df['일자'] == d) & (place_plan_df['이름'] == u)]
                        if not check.empty:
                            val = check.iloc[0]['활동여부']
                            if val == "O": val = "종일"
                    
                    row[u] = val
                    if val: active_count += 1
                
                row["인원"] = active_count
                matrix_data.append(row)

            matrix_df = pd.DataFrame(matrix_data)

            # 2. 동적 컬럼 설정
            col_config = {
                "날짜": st.column_config.TextColumn(disabled=True),
                "raw_date": None,
                "인원": st.column_config.NumberColumn(disabled=True)
            }
            for u in display_users:
                col_config[u] = st.column_config.SelectboxColumn(label=f"{u}", options=shift_options, width="small")

            # 3. 데이터 에디터 출력
            edited_matrix = st.data_editor(
                matrix_df,
                column_config=col_config,
                hide_index=True,
                use_container_width=True
            )

            # 4. 저장 및 출력
            c_btn1, c_btn2 = st.columns(2)
            with c_btn1:
                if st.button("💾 변경사항 저장"):
                    save_rows = []
                    for _, row in edited_matrix.iterrows():
                        d_real = row['raw_date']
                        for u in display_users: # 화면에 보이는 유저만 저장
                            status = row[u]
                            if status is None: status = ""
                            save_rows.append([p_year, p_month, d_real, current_island, target_place, u, status, "", str(datetime.now())])
                    
                    if save_plan_data(save_rows):
                        st.success("배치표가 수정되었습니다.")
            
            with c_btn2:
                if st.button("🖨️ 운영계획서 출력 보기"):
                    st.divider()
                    # HTML 스타일
                    st.markdown("""
                    <style>
                    .report-container { font-family: "Malgun Gothic", sans-serif; border: 2px solid #000; padding: 30px; background-color: white; color: black; }
                    .report-title { text-align: center; font-size: 24px; font-weight: bold; margin-bottom: 20px; border: 2px solid #000; padding: 10px; }
                    .info-table { width: 100%; border-collapse: collapse; margin-bottom: 10px; }
                    .info-table td { border: 1px solid #000; padding: 8px; font-size: 16px; }
                    .main-table { width: 100%; border-collapse: collapse; text-align: center; }
                    .main-table th { border: 1px solid #000; padding: 10px; background-color: #f0f0f0; font-weight: bold; }
                    .main-table td { border: 1px solid #000; padding: 8px; height: 35px; }
                    .signature-section { margin-top: 30px; display: flex; justify-content: space-around; font-size: 18px; }
                    </style>
                    """, unsafe_allow_html=True)
                    
                    html = f"""
                    <div class="report-container">
                        <div class="report-title">지질공원 안내소 운영계획서</div>
                        <table class="info-table">
                            <tr>
                                <td style="width: 15%; background-color: #f9f9f9; text-align: center; font-weight: bold;">안내소</td>
                                <td style="width: 35%;">{target_place}</td>
                                <td style="width: 15%; background-color: #f9f9f9; text-align: center; font-weight: bold;">특이사항</td>
                                <td>{special_note}</td>
                            </tr>
                            <tr>
                                <td style="background-color: #f9f9f9; text-align: center; font-weight: bold;">활동월</td>
                                <td>{p_year}년 {p_month}월</td>
                                <td style="background-color: #f9f9f9; text-align: center; font-weight: bold;">활동기간</td>
                                <td>{p_range}</td>
                            </tr>
                        </table>
                        <table class="main-table">
                            <tr>
                                <th style="width: 10%;">일</th>
                                <th style="width: 10%;">요일</th>
                                <th style="width: 40%;">활동 계획 (근무자)</th>
                                <th style="width: 40%;">활동 결과</th>
                            </tr>
                    """
                    
                    for _, row in edited_matrix.iterrows():
                        d_obj = datetime.strptime(row['raw_date'], "%Y-%m-%d")
                        day_num = f"{d_obj.day}일"
                        day_str = day_map[d_obj.weekday()] # 한글 요일
                        
                        workers = []
                        for u in display_users:
                            s_type = row[u]
                            if s_type:
                                if s_type == "종일": workers.append(f"{u}")
                                else: workers.append(f"{u}({s_type})")
                        
                        workers_str = ", ".join(workers) if workers else ""
                        
                        html += f"""
                            <tr>
                                <td>{day_num}</td>
                                <td>{day_str}</td>
                                <td style="text-align: left; padding-left: 10px;">{workers_str}</td>
                                <td></td>
                            </tr>
                        """
                    
                    html += """
                        </table>
                        <div class="signature-section">
                            <div>조장 : &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;(인/서명)</div>
                            <div>면 담당 : &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;(인/서명)</div>
                        </div>
                        <div style="text-align: right; margin-top: 10px;">20&nbsp;&nbsp;&nbsp;.&nbsp;&nbsp;&nbsp;&nbsp;.&nbsp;&nbsp;&nbsp;&nbsp;.</div>
                    </div>
                    """
                    st.markdown(html, unsafe_allow_html=True)
                    st.info("💡 마우스 우클릭 -> 인쇄 -> PDF 저장")

        # =================================================
        # 🟡 [화면 분기] 역할에 따른 화면 표시
        # =================================================
        if my_role == "해설사":
            render_my_plan_input("해설사", my_name)
        else:
            sub_t1, sub_t2 = st.tabs(["✍️ 내 계획 입력", "✅ 조원 계획 승인"])
            with sub_t1: render_my_plan_input(my_role, my_name)
            with sub_t2: render_team_approval()
