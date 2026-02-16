import json
import re

def process_youtube_history(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    processed_data = []
    
    for entry in data:
        # 1. Extract Video ID from the URL using Regex
        # Matches the string after 'v=' in the URL
        video_url = entry.get('titleUrl', '')
        video_id_match = re.search(r"v=([a-zA-Z0-9_-]{11})", video_url)
        video_id = video_id_match.group(1) if video_id_match else None
        
        # 2. Clean the Title (removes 'Watched ')
        title = entry.get('title', '').replace('Watched ', '')
        
        # 3. Keep the Timestamp (we can parse this further for D3 scales later)
        timestamp = entry.get('time', '')

        if video_id:
            processed_data.append({
                "id": video_id,
                "title": title,
                "timestamp": timestamp
            })
            
    return processed_data

# Save it for D3 to consume
clean_data = process_youtube_history('data/watch-history.json')
with open('data/processed_history.json', 'w') as f:
    json.dump(clean_data, f, indent=2)

print(f"Successfully processed {len(clean_data)} entries.")