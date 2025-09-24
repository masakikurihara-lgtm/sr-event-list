import streamlit as st
import requests
from datetime import datetime, timedelta
import time
import pytz
import pandas as pd
import io
import re

# 日本時間(JST)のタイムゾーンを設定
JST = pytz.timezone('Asia/Tokyo')

# --- 定数定義 ---
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"}
API_EVENT_SEARCH_URL = "https://www.showroom-live.com/api/event/search"
API_EVENT_ROOM_LIST_URL = "https://www.showroom-live.com/api/event/room_list"
EVENT_PAGE_BASE_URL = "https://www.showroom-live.com/event/"
ROOM_LIST_URL = "https://mksoul-pro.com/showroom/file/room_list.csv"
PAST_EVENT_INDEX_URL = "https://mksoul-pro.com/showroom/file/sr-event-archive-list-index.txt"

if "authenticated" not in st.session_state:
    st.session_state.authenticated = False

@st.cache_data(ttl=600)
def get_events(statuses):
    all_events = []
    for status in statuses:
        page = 1
        for _ in range(20):
            params = {"status": status, "page": page}
            try:
                response = requests.get(API_EVENT_SEARCH_URL, headers=HEADERS, params=params, timeout=10)
                response.raise_for_status()
                data = response.json()
                page_events = data.get('events', data.get('event_list', []))
                if not page_events:
                    break
                all_events.extend(page_events)
                page += 1
                time.sleep(0.1)
            except requests.exceptions.RequestException as e:
                st.error(f"イベントデータ取得中にエラーが発生しました (status={status}): {e}")
                break
            except ValueError:
                st.error(f"APIからのJSONデコードに失敗しました (status={status})。")
                break
    return all_events

@st.cache_data(ttl=600)
def get_past_events_from_files():
    all_past_events = pd.DataFrame()
    column_names = [
        "event_id", "is_event_block", "is_entry_scope_inner", "event_name",
        "image_m", "started_at", "ended_at", "event_url_key", "show_ranking"
    ]
    urls = []
    try:
        response = requests.get(PAST_EVENT_INDEX_URL, headers=HEADERS, timeout=10)
        response.raise_for_status()
        urls = response.text.strip().split('\n')
    except requests.exceptions.RequestException as e:
        st.warning(f"インデックスファイル取得中にエラーが発生しました: {e}")
        return all_past_events.to_dict('records')

    for url in urls:
        try:
            response = requests.get(url.strip(), headers=HEADERS, timeout=10)
            if response.status_code == 404:
                continue
            response.raise_for_status()
            csv_text = response.content.decode('utf-8-sig')
            csv_file_like_object = io.StringIO(csv_text)
            df = pd.read_csv(csv_file_like_object, header=None, names=column_names)
            df['is_entry_scope_inner'] = df['is_entry_scope_inner'].astype(str).str.lower().str.strip() == 'true'
            all_past_events = pd.concat([all_past_events, df], ignore_index=True)
        except requests.exceptions.RequestException as e:
            st.warning(f"過去イベントデータ取得中にエラーが発生しました (URL: {url}): {e}")
        except Exception as e:
            st.warning(f"過去イベントデータの処理中にエラーが発生しました (URL: {url}): {e}")

    if not all_past_events.empty:
        all_past_events['started_at'] = pd.to_numeric(all_past_events['started_at'], errors='coerce')
        all_past_events['ended_at'] = pd.to_numeric(all_past_events['ended_at'], errors='coerce')
        all_past_events.dropna(subset=['started_at', 'ended_at'], inplace=True)
        all_past_events.drop_duplicates(subset=["event_id"], keep='first', inplace=True)
        now_timestamp = int(datetime.now(JST).timestamp())
        all_past_events = all_past_events[all_past_events['ended_at'] < now_timestamp]

    return all_past_events.to_dict('records')

def get_total_entries(event_id):
    params = {"event_id": event_id}
    try:
        response = requests.get(API_EVENT_ROOM_LIST_URL, headers=HEADERS, params=params, timeout=10)
        if response.status_code == 404:
            return 0
        response.raise_for_status()
        data = response.json()
        return data.get('total_entries', 0)
    except requests.exceptions.RequestException:
        return "N/A"
    except ValueError:
        return "N/A"

def display_event_info(event):
    if not all(k in event for k in ['image_m', 'event_name', 'event_url_key', 'event_id', 'started_at', 'ended_at']):
        return
    total_entries = get_total_entries(event['event_id'])
    col1, col2 = st.columns([1, 4])
    with col1:
        st.image(event['image_m'])
    with col2:
        event_url = f"{EVENT_PAGE_BASE_URL}{event['event_url_key']}"
        st.markdown(f"**[{event['event_name']}]({event_url})**")
        target_info = "対象者限定" if event.get("is_entry_scope_inner") else "全ライバー"
        st.write(f"**対象:** {target_info}")
        start_date = datetime.fromtimestamp(event['started_at'], JST).strftime('%Y/%m/%d %H:%M')
        end_date = datetime.fromtimestamp(event['ended_at'], JST).strftime('%Y/%m/%d %H:%M')
        st.write(f"**期間:** {start_date} - {end_date}")
        st.write(f"**参加ルーム数:** {total_entries}")
    st.markdown("---")

def get_duration_category(start_ts, end_ts):
    duration = timedelta(seconds=end_ts - start_ts)
    if duration <= timedelta(days=3):
        return "3日以内"
    elif duration <= timedelta(days=7):
        return "1週間"
    elif duration <= timedelta(days=10):
        return "10日"
    elif duration <= timedelta(days=14):
        return "2週間"
    else:
        return "その他"

def main():
    st.set_page_config(page_title="SHOWROOM イベント一覧", page_icon="🎤", layout="wide")
    st.markdown("<h1 style='font-size:2.5em;'>🎤 SHOWROOM イベント一覧</h1>", unsafe_allow_html=True)
    st.write("")

    if "mksp_authenticated" not in st.session_state:
        st.session_state.mksp_authenticated = False

    if not st.session_state.authenticated:
        st.markdown("### 🔑 認証コードを入力してください")
        input_room_id = st.text_input("認証コードを入力してください:", placeholder="", type="password", key="room_id_input")
        if st.button("認証する"):
            if input_room_id:
                if input_room_id.strip() == "mksp":
                    st.session_state.authenticated = True
                    st.session_state.mksp_authenticated = True
                    st.success("✅ 特別な認証に成功しました。ツールを利用できます。")
                    st.rerun()
                else:
                    try:
                        response = requests.get(ROOM_LIST_URL, timeout=5)
                        response.raise_for_status()
                        room_df = pd.read_csv(io.StringIO(response.text), header=None)
                        valid_codes = set(str(x).strip() for x in room_df.iloc[:, 0].dropna())
                        if input_room_id.strip() in valid_codes:
                            st.session_state.authenticated = True
                            st.success("✅ 認証に成功しました。ツールを利用できます。")
                            st.rerun()
                        else:
                            st.error("❌ 認証コードが無効です。正しい認証コードを入力してください。")
                    except Exception as e:
                        st.error(f"認証リストを取得できませんでした: {e}")
            else:
                st.warning("認証コードを入力してください。")
        st.stop()

    st.markdown(
        """
        <style>
        .event-info p, .event-info li, .event-info {
            line-height: 1.7;
            margin-top: 0.0rem;
            margin-bottom: 0.4rem;
        }
        </style>
        """, unsafe_allow_html=True
    )

    st.sidebar.header("表示フィルタ")
    status_options = {"開催中": 1, "開催予定": 3, "終了": 4}
    use_on_going = st.sidebar.checkbox("開催中", value=True)
    use_upcoming = st.sidebar.checkbox("開催予定", value=False)
    use_finished = st.sidebar.checkbox("終了", value=False)
    use_past_bu = st.sidebar.checkbox("終了(BU)", value=False, help="過去のバックアップファイルから取得した終了済みイベント")

    selected_statuses = []
    if use_on_going:
        selected_statuses.append(status_options["開催中"])
    if use_upcoming:
        selected_statuses.append(status_options["開催予定"])
    if use_finished:
        selected_statuses.append(status_options["終了"])

    if not selected_statuses and not use_past_bu:
        st.warning("表示するステータスをサイドバーで1つ以上選択してください。")

    unique_events_dict = {}
    if selected_statuses:
        with st.spinner("イベント情報を取得中..."):
            fetched_events = get_events(selected_statuses)
            for event in fetched_events:
                unique_events_dict[event['event_id']] = event

    if use_past_bu:
        with st.spinner("過去のイベントデータを取得・処理中..."):
            past_events = get_past_events_from_files()
            for event in past_events:
                # --- ここを修正: 既に存在するevent_idならスキップ ---
                if event['event_id'] not in unique_events_dict:
                    unique_events_dict[event['event_id']] = event

    all_events = list(unique_events_dict.values())
    if not all_events:
        st.info("該当するイベントはありませんでした。")
        st.stop()
    else:
        start_dates = sorted(list(set([
            datetime.fromtimestamp(e['started_at'], JST).date() for e in all_events if 'started_at' in e
        ])), reverse=True)
        date_options = {
            d.strftime('%Y/%m/%d') + f"({['月', '火', '水', '木', '金', '土', '日'][d.weekday()]})": d
            for d in start_dates
        }
        selected_start_dates = st.sidebar.multiselect("開始日でフィルタ", options=list(date_options.keys()))
        duration_options = ["3日以内", "1週間", "10日", "2週間", "その他"]
        selected_durations = st.sidebar.multiselect("期間でフィルタ", options=duration_options)
        target_options = ["全ライバー", "対象者限定"]
        selected_targets = st.sidebar.multiselect("対象でフィルタ", options=target_options)

        filtered_events = all_events
        if selected_start_dates:
            selected_dates_set = {date_options[d] for d in selected_start_dates}
            filtered_events = [
                e for e in filtered_events
                if 'started_at' in e and datetime.fromtimestamp(e['started_at'], JST).date() in selected_dates_set
            ]
        if selected_durations:
            filtered_events = [
                e for e in filtered_events
                if get_duration_category(e['started_at'], e['ended_at']) in selected_durations
            ]
        if selected_targets:
            target_map = {"全ライバー": False, "対象者限定": True}
            selected_target_values = {target_map[t] for t in selected_targets}
            filtered_events = [
                e for e in filtered_events
                if e.get('is_entry_scope_inner') in selected_target_values
            ]

        if use_finished and use_past_bu:
            st.success(f"{len(filtered_events)}件のイベントが見つかりました。ただし、重複データは1件のみ表示しています。")
        else:
            st.success(f"{len(filtered_events)}件のイベントが見つかりました。")

        st.markdown("---")
        for event in filtered_events:
            col1, col2 = st.columns([1, 4])
            with col1:
                st.image(event['image_m'])
            with col2:
                event_url = f"{EVENT_PAGE_BASE_URL}{event['event_url_key']}"
                st.markdown(
                    f'<div class="event-info"><strong><a href="{event_url}">{event["event_name"]}</a></strong></div>',
                    unsafe_allow_html=True
                )
                target_info = "対象者限定" if event.get("is_entry_scope_inner") else "全ライバー"
                st.markdown(f'<div class="event-info"><strong>対象:</strong> {target_info}</div>', unsafe_allow_html=True)
                start_date = datetime.fromtimestamp(event['started_at'], JST).strftime('%Y/%m/%d %H:%M')
                end_date = datetime.fromtimestamp(event['ended_at'], JST).strftime('%Y/%m/%d %H:%M')
                st.markdown(
                    f'<div class="event-info"><strong>期間:</strong> {start_date} - {end_date}</div>',
                    unsafe_allow_html=True
                )
                total_entries = get_total_entries(event['event_id'])
                st.markdown(
                    f'<div class="event-info"><strong>参加ルーム数:</strong> {total_entries}</div>',
                    unsafe_allow_html=True
                )
            st.markdown("---")

if __name__ == "__main__":
    main()
