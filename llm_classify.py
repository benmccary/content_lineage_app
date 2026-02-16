import json
import os
import requests
import shutil  # Added for backup functionality
from dotenv import load_dotenv

# init
load_dotenv()  

OLLAMA_URL = os.getenv('OLLAMA_URL')
MODEL = os.getenv('MODEL')
METADATA_FILE = 'data/metadata_map.json'
BACKUP_FILE = 'data/metadata_map_backup.json'


def classify_channel(name, channel_cat, desc, video_data):
    video_context = "\n".join([f"- {v}" for v in video_data])
    prompt = f"""
    Task: Create a 1 to 3 word classification for this YouTube channel.

    Channel Name: {name}
    Primary YouTube Category: {channel_cat}
    Channel Description: {desc[:400]}
    
    Last 3 Video Titles & Topics:
    {video_context}

    Instructions:
    If you can be specific, do so. For example, if the titles mention 'football' you can say the channel is about football instead of 'sports'. Similarly, 'baking' is more specific than 'cooking'. 'anime recaps' is better than 'anime'. "Come here to watch my video essays on black movies and media
" may be 'video essays' or 'black media' depending on the last 3 titles. Do NOT use overly general categories like "education". If it is an educatinional linguistics channel call it "linguistics". 

DO NOT add multiple categories separated by a dash or slash. "Vlog/Travel" should just be "travel vlog". DO NOT USE a "/" to split several categories. instead of "Film Analysis/Review/Commentary" write "film analysis"
    - Use 1 to 3 words max.
    - Be specific (e.g., "Python Web Development" instead of "Coding").
    - Return ONLY JSON: {{"category": "ClassificationName"}}

    ONLY USE THE JSON FORMAT {{"category": "ClassificationName"}}
    """
    try:
        res = requests.post(OLLAMA_URL, json={"model": MODEL, "prompt": prompt, "stream": False, "format": "json"}, timeout=15)
        return json.loads(res.json()['response']).get('category', "Unknown")
    except:
        return "Unknown"

def safe_save(data, filepath):
    """Writes to a temp file first to prevent corruption."""
    temp_file = f"{filepath}.tmp"
    with open(temp_file, 'w') as f:
        json.dump(data, f, indent=2)
    os.replace(temp_file, filepath) # Atomic swap

def run_llm():
    # 1. Create a hard backup before doing anything
    if os.path.exists(METADATA_FILE):
        print(f"📦 Creating backup: {BACKUP_FILE}")
        shutil.copy2(METADATA_FILE, BACKUP_FILE)

    with open(METADATA_FILE, 'r') as f:
        metadata = json.load(f)

    # Grouping logic
    channels = {}
    for vid_id, data in metadata.items():
        name = data['channelTitle']
        if str(data.get('categoryId')) == "10": continue

        if name not in channels:
            channels[name] = {
                "ids": [], 
                "desc": data.get('channelDescription', ''),
                "cat": data.get('categoryId', 'N/A'),
                "video_info": []
            }
        channels[name]["ids"].append(vid_id)
        if len(channels[name]["video_info"]) < 3:
            topics = ", ".join(data.get('topics', []))
            channels[name]["video_info"].append(f"{data['title']} (Topics: {topics})")

    # 2. Process and Classify
    print(f"🚀 Processing {len(channels)} unique channels...")
    for i, (name, info) in enumerate(channels.items()):
        if metadata[info["ids"][0]].get('llm_category'): continue
        
        cat = classify_channel(name, info['cat'], info['desc'], info['video_info'])
        
        for vid_id in info["ids"]:
            metadata[vid_id]['llm_category'] = cat
        
        # Save every 5 channels using the safe_save method
        if i % 5 == 0:
            safe_save(metadata, METADATA_FILE)
            print(f"✅ [{i}/{len(channels)}] {name} -> {cat} (Saved)")

    # Final Save
    safe_save(metadata, METADATA_FILE)
    print("✨ Classification complete. All changes saved.")

if __name__ == "__main__":
    run_llm()