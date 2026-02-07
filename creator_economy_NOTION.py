#!/usr/bin/env python3
"""
Creator Economy Podcast Automation - Notion Only
Transcribes creator economy podcasts and saves to Notion with FULL transcripts
"""

import os
import json
import time
import requests
import feedparser
from datetime import datetime
from dateutil import parser
import google.generativeai as genai

class CreatorEconomyNotionAutomation:
    def __init__(self):
        self.load_env_configs()
        self.force_clear_api_storage()
        self.processed_episodes_cache = self.load_processed_episodes_from_notion()

    def load_env_configs(self):
        """Load configuration from environment variables."""
        self.gemini_key = os.environ.get('GEMINI_API_KEY')
        self.notion_token = os.environ.get('NOTION_API_KEY')
        self.notion_database_id = os.environ.get('NOTION_DATABASE_ID')
        
        if not all([self.gemini_key, self.notion_token, self.notion_database_id]):
            raise ValueError("Missing required environment variables")
        
        with open('config.json', 'r') as f:
            self.rss_feeds = json.load(f)['rss_feeds']
        
        genai.configure(api_key=self.gemini_key)
        print("✓ Environment configured")

    def force_clear_api_storage(self):
        """Delete orphaned Gemini files."""
        print("\n--- Cleaning Gemini Storage ---")
        try:
            files = list(genai.list_files())
            if files:
                for f in files:
                    try:
                        genai.delete_file(f.name)
                    except:
                        pass
                print(f"  ✓ Deleted {len(files)} files")
        except Exception as e:
            print(f"  Warning: {e}")

    def load_processed_episodes_from_notion(self):
        """Load all processed episode titles from Notion."""
        print("\n--- Loading Processed Episodes ---")
        try:
            url = f"https://api.notion.com/v1/databases/{self.notion_database_id}/query"
            headers = {
                "Authorization": f"Bearer {self.notion_token}",
                "Notion-Version": "2022-06-28",
                "Content-Type": "application/json"
            }
            
            all_titles = set()
            has_more = True
            start_cursor = None
            
            while has_more:
                body = {}
                if start_cursor:
                    body["start_cursor"] = start_cursor
                
                response = requests.post(url, headers=headers, json=body)
                response.raise_for_status()
                data = response.json()
                
                for page in data.get('results', []):
                    try:
                        episode_prop = page['properties'].get('Episode', {})
                        if episode_prop.get('rich_text'):
                            title = episode_prop['rich_text'][0]['text']['content']
                            all_titles.add(title)
                    except:
                        pass
                
                has_more = data.get('has_more', False)
                start_cursor = data.get('next_cursor')
            
            print(f"  ✓ Loaded {len(all_titles)} episodes")
            return all_titles
            
        except Exception as e:
            print(f"  Warning: {e}")
            return set()

    def transcribe_with_retry(self, audio_file, max_retries=5):
        """Transcribe audio with retry logic."""
        for attempt in range(max_retries):
            gemini_file = None
            try:
                print(f"  [{attempt + 1}/{max_retries}] Uploading...")
                gemini_file = genai.upload_file(path=audio_file)
                
                print("  → Processing...")
                start_time = time.time()
                
                while gemini_file.state.name == "PROCESSING":
                    if time.time() - start_time > 600:
                        raise Exception("Timeout")
                    time.sleep(5)
                    gemini_file = genai.get_file(gemini_file.name)
                
                if gemini_file.state.name == "FAILED":
                    raise Exception("Processing failed")
                
                print("  → Generating transcript...")
                model = genai.GenerativeModel("gemini-2.5-flash")
                
                # First, get a proper summary
                summary_prompt = """Listen to this podcast episode and provide a 2-3 sentence summary of the main topics discussed. Focus on the key themes, guests, and important points covered. Do not include ads or introductions in the summary."""
                
                summary_response = model.generate_content([summary_prompt, gemini_file])
                summary = summary_response.text.strip()
                
                # Then get the full formatted transcript
                transcript_prompt = """Transcribe this podcast episode with the following formatting:

1. Use clear paragraph breaks - start a new paragraph every 2-3 sentences or when the speaker/topic changes
2. If multiple speakers, label them clearly as **Speaker 1:**, **Speaker 2:**, or use their actual names if mentioned
3. Add a blank line between each speaker turn or major topic shift
4. If there are ads/sponsors, mark them as **[AD]** or **[SPONSOR]**
5. Make it readable and well-structured, not a wall of text

Provide a complete, accurate transcription with natural paragraph breaks."""
                
                response = model.generate_content([transcript_prompt, gemini_file])
                transcript = response.text
                
                print("  ✓ Summary and transcript generated")
                
                print("  ✓ Summary and transcript generated")
                
                try:
                    genai.delete_file(gemini_file.name)
                except:
                    pass
                
                return {
                    'summary': summary,
                    'transcript': transcript
                }
                
            except Exception as e:
                print(f"  ✗ Error: {e}")
                if gemini_file:
                    try:
                        genai.delete_file(gemini_file.name)
                    except:
                        pass
                
                if attempt < max_retries - 1:
                    time.sleep(60 * (2 ** attempt))
                else:
                    raise

    def add_to_notion(self, podcast_name, title, published, summary, transcript):
        """Add episode to Notion with FULL transcript (handles Notion's 100 block limit)."""
        try:
            print("  → Adding to Notion...")
            
            # Parse date
            try:
                notion_date = parser.parse(published).strftime('%Y-%m-%d')
            except:
                notion_date = datetime.now().strftime('%Y-%m-%d')
            
            # Trim summary if too long for Notion property (2000 char limit)
            if len(summary) > 2000:
                summary = summary[:1997] + "..."
            
            # Split transcript into 2000-char chunks (Notion block limit)
            chunks = []
            for i in range(0, len(transcript), 2000):
                chunks.append({
                    "object": "block",
                    "type": "paragraph",
                    "paragraph": {
                        "rich_text": [{"text": {"content": transcript[i:i + 2000]}}]
                    }
                })
            
            headers = {
                "Authorization": f"Bearer {self.notion_token}",
                "Content-Type": "application/json",
                "Notion-Version": "2022-06-28"
            }
            
            # STEP 1: Create page with first 98 blocks (100 limit including header + divider)
            initial_blocks = [
                {
                    "object": "block",
                    "type": "heading_1",
                    "heading_1": {"rich_text": [{"text": {"content": "Full Transcript"}}]}
                },
                {
                    "object": "block",
                    "type": "divider",
                    "divider": {}
                }
            ]
            initial_blocks.extend(chunks[:98])
            
            create_data = {
                "parent": {"database_id": self.notion_database_id},
                "properties": {
                    "Podcast": {"title": [{"text": {"content": podcast_name}}]},
                    "Episode": {"rich_text": [{"text": {"content": title}}]},
                    "Date": {"date": {"start": notion_date}},
                    "Summary": {"rich_text": [{"text": {"content": summary}}]}
                },
                "children": initial_blocks
            }
            
            response = requests.post(
                "https://api.notion.com/v1/pages",
                headers=headers,
                json=create_data
            )
            response.raise_for_status()
            
            page = response.json()
            page_id = page['id']
            page_url = page.get('url', '')
            
            print(f"  ✓ Created page with {len(initial_blocks)} blocks")
            
            # STEP 2: Append remaining blocks in batches of 100
            remaining = chunks[98:]
            
            if remaining:
                print(f"  → Appending {len(remaining)} more blocks...")
                
                for i in range(0, len(remaining), 100):
                    batch = remaining[i:i + 100]
                    
                    requests.patch(
                        f"https://api.notion.com/v1/blocks/{page_id}/children",
                        headers=headers,
                        json={"children": batch}
                    ).raise_for_status()
                    
                    print(f"  → Batch {i//100 + 1} done ({len(batch)} blocks)")
                    time.sleep(0.3)  # Rate limit protection
                
                print(f"  ✓ Full transcript added ({len(chunks)} total blocks)")
            
            print(f"  ✓ Notion page: {page_url}")
            return page_url
            
        except Exception as e:
            print(f"  ✗ Failed: {e}")
            raise

    def process_episode(self, podcast_name, entry):
        """Process a single episode."""
        title = entry.get('title', 'Unknown')
        
        if title in self.processed_episodes_cache:
            print(f"  Already processed: {title}")
            return False
        
        print(f"\n{'='*80}\nProcessing: {title}\n{'='*80}")
        
        temp_path = f"temp_{int(time.time())}.mp3"
        
        try:
            # Get audio URL
            audio_url = None
            if hasattr(entry, 'enclosures') and entry.enclosures:
                audio_url = entry.enclosures[0].href
            elif hasattr(entry, 'links'):
                for link in entry.links:
                    if 'audio' in link.get('type', '').lower():
                        audio_url = link.get('href')
                        break
            
            if not audio_url:
                print("  ✗ No audio URL")
                return False
            
            # Download
            print("  → Downloading...")
            resp = requests.get(audio_url, stream=True, timeout=300)
            resp.raise_for_status()
            
            with open(temp_path, "wb") as f:
                for chunk in resp.iter_content(8192):
                    f.write(chunk)
            print("  ✓ Downloaded")
            
            # Transcribe (returns both summary and transcript)
            result = self.transcribe_with_retry(temp_path)
            
            # Save to Notion
            self.add_to_notion(podcast_name, title, entry.get('published', ''), result['summary'], result['transcript'])
            
            # Add to cache
            self.processed_episodes_cache.add(title)
            
            print(f"  ✓ SUCCESS")
            return True
            
        except Exception as e:
            print(f"  ✗ FAILED: {e}")
            return False
            
        finally:
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except:
                    pass

    def run(self):
        """Main loop."""
        print("\n" + "="*80)
        print("CREATOR ECONOMY PODCAST AUTOMATION - Notion")
        print("="*80)
        
        total = 0
        
        for feed_url in self.rss_feeds:
            try:
                print(f"\n{'#'*80}\nFeed: {feed_url}\n{'#'*80}")
                
                feed = feedparser.parse(feed_url)
                if not feed.entries:
                    continue
                
                podcast_name = feed.feed.get('title', 'Unknown')
                print(f"Podcast: {podcast_name}")
                
                for entry in feed.entries[:10]:
                    if self.process_episode(podcast_name, entry):
                        total += 1
                        time.sleep(5)
                
            except Exception as e:
                print(f"\n✗ Error: {e}")
        
        print(f"\n{'='*80}\nCOMPLETE - Processed {total} episodes\n{'='*80}")


if __name__ == "__main__":
    CreatorEconomyNotionAutomation().run()
