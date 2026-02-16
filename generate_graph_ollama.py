import json, os, requests, numpy as np
from datetime import datetime, timedelta

# --- CONFIG ---
OLLAMA_GENERATE_URL = "http://localhost:11434/api/generate"
OLLAMA_EMBED_URL = "http://localhost:11434/api/embeddings"
MODEL_NAME = "llama3.1:latest"

MIN_VIEWS = 3
SIMILARITY_THRESHOLD = 0.75
MERGE_THRESHOLD = 0.92
FORBIDDEN = ["general", "miscellaneous", "other", "unknown", "clips", "shorts", "vlog"]

def ask_llm_reasoning(parent, child):
    prompt = (f"Does the topic '{child}' represent a logical evolution, "
              f"sub-topic, or continuation of the earlier interest '{parent}'? "
              f"Answer only YES or NO.")
    try:
        res = requests.post(OLLAMA_GENERATE_URL, 
                            json={"model": MODEL_NAME, "prompt": prompt, "stream": False},
                            timeout=10)
        return "YES" in res.json().get('response', '').upper()
    except: return False

def generate():
    history = json.load(open('data/processed_history.json', 'r'))
    metadata = json.load(open('data/metadata_map.json', 'r'))
    embed_cache = json.load(open('data/embedding_cache.json', 'r')) if os.path.exists('data/embedding_cache.json') else {}
    logic_cache = json.load(open('data/reasoning_cache.json', 'r')) if os.path.exists('data/reasoning_cache.json') else {}

    history.sort(key=lambda x: x['timestamp'])

    interest_nodes = {}
    links = []
    last_node_per_topic = {} 
    canonical_map = {}
    branched_topics = set()

    print(f"Building Weighted Causal Graph...")

    for index, item in enumerate(history):
        meta = metadata.get(item['id'])
        if not meta: continue

        raw_interest = meta.get('llm_category') or "General"
        
        # 1. Check if we already mapped this topic to a canonical name
        if raw_interest in canonical_map:
            interest = canonical_map[raw_interest]
        else:
            interest = raw_interest
            
            # 2. Check for "Super Similarity" with existing topics to merge
            curr_vec = embed_cache.get(interest)
            if curr_vec:
                for existing_topic in last_node_per_topic.keys():
                    prev_vec = embed_cache.get(existing_topic)
                    if prev_vec:
                        score = np.dot(curr_vec, prev_vec) / (np.linalg.norm(curr_vec) * np.linalg.norm(prev_vec))
                        
                    # Only merge if the score is very high AND they don't have conflicting core words
                    if score > 0.95: # Increased threshold
                        canonical_map[raw_interest] = existing_topic
                        interest = existing_topic
                        break
                    elif score > 0.80:
                        # If they are somewhat similar, let's double check they aren't totally different subjects
                        subjects = ["racing", "boxing", "coding", "cooking", "gaming"]
                        if any(s in interest.lower() for s in subjects) and any(s in existing_topic.lower() for s in subjects):
                            if not any(s in interest.lower() and s in existing_topic.lower() for s in subjects):
                                # Conflict detected (e.g., one has 'racing', one has 'boxing') -> DO NOT MERGE
                                continue
        
        # Check what we are actually getting from metadata_map.json
        percent = (index + 1) / len(history) * 100
        print(f"\r[{percent:4.1f}%] Processing: {interest[:30]:<30}", end="", flush=True)

        if interest.lower() in FORBIDDEN: continue

        ts = item['timestamp']
        dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))

        # Snap to the previous Sunday
        days_since_sunday = (dt.weekday() + 1) % 7 
        sunday_start = dt - timedelta(days=days_since_sunday)
        time_str = sunday_start.strftime('%Y-%m-%d') # The new column key
        node_id = f"{interest}_{time_str}"

        # Progress Counter
        percent = (index + 1) / len(history) * 100
        print(f"\r[{percent:4.1f}%] Processing: {interest[:20]:<20}", end="", flush=True)

        if node_id not in interest_nodes:
            interest_nodes[node_id] = {
                "id": node_id, "label": interest, "birth": ts, 
                "count": 0, "videos": [] 
            }

            if interest in last_node_per_topic:
                links.append({"source": last_node_per_topic[interest], "target": node_id, "type": "persistence", "score": 1.0})
            elif interest not in branched_topics:
                if interest not in embed_cache:
                    try:
                        res = requests.post(OLLAMA_EMBED_URL, json={"model": MODEL_NAME, "prompt": interest}, timeout=5)
                        embed_cache[interest] = res.json().get('embedding')
                    except: pass
                
                curr_vec = embed_cache.get(interest)
                if curr_vec:
                    candidates = []
                    for prev_id, prev_node in interest_nodes.items():
                        if prev_node['birth'] < ts and prev_node['label'] != interest:
                            p_vec = embed_cache.get(prev_node['label'])
                            if p_vec:
                                score = np.dot(curr_vec, p_vec) / (np.linalg.norm(curr_vec) * np.linalg.norm(p_vec))
                                if score > SIMILARITY_THRESHOLD:
                                    candidates.append({"id": prev_id, "label": prev_node['label'], "score": float(score)})

                    candidates = sorted(candidates, key=lambda x: x['score'], reverse=True)[:3]
                    for cand in candidates:
                        key = f"{cand['label']}->{interest}"
                        if key not in logic_cache: logic_cache[key] = ask_llm_reasoning(cand['label'], interest)
                        if logic_cache[key]:
                            links.append({"source": cand['id'], "target": node_id, "type": "branch", "score": round(cand['score'], 3)})
                            branched_topics.add(interest)
                            break 

            last_node_per_topic[interest] = node_id

        # Store Video Info
        interest_nodes[node_id]["videos"].append({
            "title": item.get('title', 'No Title'),
            "url": f"https://www.youtube.com/watch?v={item.get('id', '')}"
        })
        interest_nodes[node_id]["count"] += 1

    # Use the new format for the final output
    all_weeks = sorted(list(set(
        (datetime.fromisoformat(n['birth'].replace('Z', '+00:00')) - 
         timedelta(days=(datetime.fromisoformat(n['birth'].replace('Z', '+00:00')).weekday() + 1) % 7)
        ).strftime("%Y-%m-%d") 
        for n in interest_nodes.values()
    )))
    
    week_to_idx = {w: i for i, w in enumerate(all_weeks)}

    final_nodes = [n for n in interest_nodes.values() if n['count'] >= MIN_VIEWS]
    for n in final_nodes:
        # Snap birth time to Sunday again to find the correct index
        dt_n = datetime.fromisoformat(n['birth'].replace('Z', '+00:00'))
        sunday_n = (dt_n - timedelta(days=(dt_n.weekday() + 1) % 7)).strftime("%Y-%m-%d")
        n['time_index'] = week_to_idx[sunday_n]
        n['time_key'] = sunday_n # Helpful for the HTML side

    f_ids = {n['id'] for n in final_nodes}
    final_links = [l for l in links if l['source'] in f_ids and l['target'] in f_ids]

    # Save with "weeks" key instead of "months"
    json.dump({"nodes": final_nodes, "links": final_links, "weeks": all_weeks}, open('data/graph_data.json', 'w'), indent=2)
    json.dump(embed_cache, open('data/embedding_cache.json', 'w'))
    json.dump(logic_cache, open('data/reasoning_cache.json', 'w'))
    print(f"\n\nSuccess! Created {len(final_nodes)} nodes.")

if __name__ == "__main__": generate()