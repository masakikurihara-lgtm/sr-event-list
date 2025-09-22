import streamlit as st
import requests
from datetime import datetime
import time

# --- 定数定義 ---
# APIリクエスト時に使用するヘッダー
HEADERS = {"User-Agent": "Mozilla/5.0"}
# イベント検索APIのURL
API_EVENT_SEARCH_URL = "https://www.showroom-live.com/api/event/search"
# イベントランキング情報APIのURL（参加ルーム数取得用）
API_EVENT_RANKING_URL = "https://www.showroom-live.com/api/event/{event_url_key}/ranking"
# SHOWROOMのイベントページのベースURL
EVENT_PAGE_BASE_URL = "https://www.showroom-live.com/event/"

# --- データ取得関数 ---

@st.cache_data(ttl=600)  # 10分間キャッシュを保持
def get_events(statuses):
    """
    指定されたステータスのイベントリストをAPIから取得します。
    """
    all_events = []
    # 選択されたステータスごとにAPIを叩く
    for status in statuses:
        page = 1
        # 1ステータスあたり最大20ページまで取得を試みる
        for _ in range(20):
            params = {"status": status, "page": page}
            try:
                response = requests.get(API_EVENT_SEARCH_URL, headers=HEADERS, params=params, timeout=10)
                response.raise_for_status()  # HTTPエラーがあれば例外を発生
                data = response.json()

                # 'events' または 'event_list' キーからイベントリストを取得
                page_events = data.get('events', data.get('event_list', []))

                if not page_events:
                    break  # イベントがなければループを抜ける

                all_events.extend(page_events)
                page += 1
                time.sleep(0.1) # APIへの負荷を考慮して少し待機
            except requests.exceptions.RequestException as e:
                st.error(f"イベントデータ取得中にエラーが発生しました (status={status}): {e}")
                break
            except ValueError:
                st.error(f"APIからのJSONデコードに失敗しました (status={status})。")
                break
    return all_events

@st.cache_data(ttl=300)  # 5分間キャッシュを保持
def get_total_entries(event_url_key):
    """
    指定されたイベントの総参加ルーム数を取得します。
    """
    url = API_EVENT_RANKING_URL.format(event_url_key=event_url_key)
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        # 404エラーは参加者情報がない場合なので正常系として扱う
        if response.status_code == 404:
            return 0
        response.raise_for_status()
        data = response.json()
        # 'total_entries' キーから参加ルーム数を取得
        return data.get('total_entries', 0)
    except requests.exceptions.RequestException:
        # エラー時は 'N/A' を返す
        return "N/A"
    except ValueError:
        return "N/A"

# --- UI表示関数 ---

def display_event_info(event):
    """
    1つのイベント情報をStreamlitのUIに表示します。
    """
    # 必要な情報が欠けている場合は表示しない
    if not all(k in event for k in ['image_m', 'event_name', 'event_url_key', 'started_at', 'ended_at']):
        return

    # 参加ルーム数を取得
    total_entries = get_total_entries(event['event_url_key'])

    # UIのレイアウトを定義（左に画像、右に情報）
    col1, col2 = st.columns([1, 4])

    with col1:
        st.image(event['image_m'])

    with col2:
        # イベント名をリンク付きで表示
        event_url = f"{EVENT_PAGE_BASE_URL}{event['event_url_key']}"
        st.markdown(f"**[{event['event_name']}]({event_url})**", unsafe_allow_html=True)

        # イベント期間をフォーマットして表示
        start_date = datetime.fromtimestamp(event['started_at']).strftime('%Y-%m-%d')
        end_date = datetime.fromtimestamp(event['ended_at']).strftime('%Y-%m-%d')
        st.write(f"**期間:** {start_date} - {end_date}")

        # 参加ルーム数を表示
        st.write(f"**参加ルーム数:** {total_entries}")

    st.markdown("---")


# --- メイン処理 ---
def main():
    # ページ設定
    st.set_page_config(
        page_title="SHOWROOM イベント一覧",
        page_icon="🎤",
        layout="wide"
    )

    st.title("🎤 SHOWROOM イベント一覧ツール")
    st.write("SHOWROOMで開催されているイベントの情報を一覧で確認できます。")

    # --- フィルタリング機能 ---
    st.sidebar.header("表示フィルタ")
    status_options = {
        "開催中": 1,
        "開催予定": 3,
        "終了": 4,
    }

    # チェックボックスの状態を管理
    use_on_going = st.sidebar.checkbox("開催中", value=True)
    use_upcoming = st.sidebar.checkbox("開催予定", value=False)
    use_finished = st.sidebar.checkbox("終了", value=False)

    selected_statuses = []
    if use_on_going:
        selected_statuses.append(status_options["開催中"])
    if use_upcoming:
        selected_statuses.append(status_options["開催予定"])
    if use_finished:
        selected_statuses.append(status_options["終了"])

    # --- イベント情報表示 ---
    if not selected_statuses:
        st.warning("表示するステータスをサイドバーで1つ以上選択してください。")
        st.stop()

    # 選択されたステータスに基づいてイベント情報を取得
    with st.spinner("イベント情報を取得中..."):
        events = get_events(selected_statuses)

    if not events:
        st.info("該当するイベントはありませんでした。")
    else:
        st.success(f"{len(events)}件のイベントが見つかりました。")
        st.markdown("---")
        # 取得したイベント情報を1つずつ表示
        for event in events:
            display_event_info(event)


if __name__ == "__main__":
    main()
