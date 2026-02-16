import os
import json
from dotenv import load_dotenv
from googleapiclient.discovery import build

load_dotenv()
API_KEY = os.getenv('YOUTUBE_API_KEY')
DATA_DIR = 'data'
HISTORY_FILE = os.path.join(DATA_DIR, 'processed_history.json')
METADATA_FILE = os.path.join(DATA_DIR, 'metadata_map.json')

os.makedirs(DATA_DIR, exist_ok=True)
youtube = build('youtube', 'v3', developerKey=API_KEY)

with open(HISTORY_FILE, 'r') as f:
    history = json.load(f)

metadata_map = {}
if os.path.exists(METADATA_FILE):
    with open(METADATA_FILE, 'r') as f:
        metadata_map = json.load(f)

# Identify IDs missing basic info OR the new channelDescription
to_fetch_ids = []
for item in history:
    vid_id = item['id']
    meta = metadata_map.get(vid_id)
    if str(item.get('videoCategoryId')) == "10": continue
    if not meta or 'channelDescription' not in meta:
        to_fetch_ids.append(vid_id)

to_fetch_ids = list(set(to_fetch_ids))
print(f"Videos to fetch/enrich: {len(to_fetch_ids)}")

if to_fetch_ids:
    for i in range(0, len(to_fetch_ids), 50):
        batch = to_fetch_ids[i:i+50]
        try:
            v_res = youtube.videos().list(part="snippet", id=",".join(batch)).execute()
            channel_data_cache = {}
            batch_channel_ids = {v['snippet']['channelId'] for v in v_res.get('items', [])}

            if batch_channel_ids:
                c_res = youtube.channels().list(
                    part="topicDetails,snippet", 
                    id=",".join(list(batch_channel_ids))
                ).execute()
                
                for c_item in c_res.get('items', []):
                    topics = c_item.get('topicDetails', {}).get('topicCategories', [])
                    channel_data_cache[c_item['id']] = {
                        "topics": [t.split('/')[-1].replace('_', ' ') for t in topics],
                        "description": c_item.get('snippet', {}).get('description', '')
                    }

            for v_item in v_res.get('items', []):
                vid_id = v_item['id']
                snippet = v_item['snippet']
                c_id = snippet['channelId']
                c_info = channel_data_cache.get(c_id, {})
                
                metadata_map[vid_id] = {
                    "categoryId": snippet['categoryId'],
                    "channelTitle": snippet['channelTitle'],
                    "channelId": c_id,
                    "title": snippet['title'],
                    "topics": c_info.get('topics', []),
                    "channelDescription": c_info.get('description', "")
                }

            with open(METADATA_FILE, 'w') as f:
                json.dump(metadata_map, f, indent=2)
            print(f"Batch {i//50 + 1} done.")
        except Exception as e:
            print(f"Error: {e}"); break