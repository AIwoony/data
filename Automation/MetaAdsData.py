import requests
import json
import pandas as pd
from datetime import datetime, timedelta

def date_range(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

def get_insights(start_date, end_date):
    ver = "v19.0"
    account = 'act_512284903279116'  # 실제 계정 ID
    token = 'EAAGmyX2UZBRsBOZBlln354p2fZAPPMGe8ERPvJqAULh1gV5vyu588ifs1yElyA5A3cdcZCzOrxr9jp6gw9tUzW4ZC7ZBwrKLbbZAlQ2oaFKy30l7vRKk49qfCttCzP0OaAcnRa5QJ9P0AXtINgGfUdjmzawK4AHkZAI1gs8t0gQmi6fZAAC7VmPyuDxaUHbOl6PeZBNxknvoyxYuAADnN9K6eEolp7L8M8eZCMPuXwo'  # 실제 액세스 토큰

    insights = 'campaign_name,adset_name,ad_name,impressions,clicks,spend,actions'
    url = f"https://graph.facebook.com/{ver}/{account}/insights"

    params = {
        'fields': insights,
        'access_token': token,
        'level': 'ad',
        'time_range[since]': start_date.strftime('%Y-%m-%d'),
        'time_range[until]': end_date.strftime('%Y-%m-%d'),
        'action_report_time': 'conversion',
        'use_unified_attribution_setting': 'true',
        'action_breakdowns': 'action_type',
    }

    all_data = []
    
    while url:
        r = requests.get(url=url, params=params)
        print(f"Requesting data for {start_date} to {end_date}")
        
        if r.status_code != 200:
            print("Error:", r.text)
            break

        content_json = r.json()
        all_data.extend(content_json.get('data', []))
        
        # Check for next page
        url = content_json.get('paging', {}).get('next')
        params = {}  # Clear params for next request as they're included in the 'next' URL
        
    return all_data

def process_data(data):
    processed_data = []
    for item in data:
        event_id = None
        for action in item.get('actions', []):
            if action.get('action_type') == 'purchase' and 'event_id' in action:
                event_id = action['event_id']
                break
        
        processed_data.append({
            'date': item.get('date_start'),
            'campaign_name': item.get('campaign_name'),
            'adset_name': item.get('adset_name'),
            'ad_name': item.get('ad_name'),
            'impressions': item.get('impressions'),
            'clicks': item.get('clicks'),
            'spend': item.get('spend'),
            'event_id': event_id
        })
    return processed_data

# Main execution
start_date = datetime(2024, 1, 1)
end_date = datetime(2024, 1, 28)
all_data = []

# Split the date range into smaller chunks (e.g., 7-day periods)
chunk_size = timedelta(days=7)
current_start = start_date

while current_start < end_date:
    current_end = min(current_start + chunk_size, end_date)
    chunk_data = get_insights(current_start, current_end)
    all_data.extend(chunk_data)
    current_start = current_end + timedelta(days=1)

processed_data = process_data(all_data)
df = pd.DataFrame(processed_data)

print("\nProcessed Data:")
print(df.head())

# CSV 파일로 저장
df.to_csv('facebook_ad_insights_with_event_id.csv', index=False)
print("\nData saved to 'facebook_ad_insights_with_event_id.csv'")
