#!/usr/bin/env python3
"""
PostHog API Sync Script

This script:
1. Queries PostHog API for events, persons, or insights
2. Caches results locally in DuckDB
3. Handles rate limiting and pagination

Note: For high-volume use cases, prefer PostHog's batch export feature
to Postgres or S3, then connect Evidence directly to that data store.

Usage:
    python scripts/sync_posthog.py [--events] [--persons] [--insights]
    
Environment Variables:
    POSTHOG_API_KEY (personal API key)
    POSTHOG_PROJECT_ID
    POSTHOG_HOST (optional, default: 'https://app.posthog.com')
"""

import os
import sys
import json
import time
import requests
import duckdb
from datetime import datetime, timedelta
from pathlib import Path

# Configuration
DATA_DIR = Path(__file__).parent.parent / 'data'
DUCKDB_PATH = DATA_DIR / 'posthog_cache.duckdb'

def get_config():
    return {
        'api_key': os.environ.get('POSTHOG_API_KEY'),
        'project_id': os.environ.get('POSTHOG_PROJECT_ID'),
        'host': os.environ.get('POSTHOG_HOST', 'https://app.posthog.com')
    }

def make_request(config, endpoint, params=None, method='GET', json_data=None):
    """Make authenticated request to PostHog API."""
    url = f"{config['host']}/api/projects/{config['project_id']}/{endpoint}"
    headers = {'Authorization': f"Bearer {config['api_key']}"}
    
    response = requests.request(
        method=method,
        url=url,
        headers=headers,
        params=params,
        json=json_data
    )
    
    # Handle rate limiting
    if response.status_code == 429:
        retry_after = int(response.headers.get('Retry-After', 60))
        print(f"Rate limited. Waiting {retry_after} seconds...")
        time.sleep(retry_after)
        return make_request(config, endpoint, params, method, json_data)
    
    response.raise_for_status()
    return response.json()

def fetch_events(config, days_back=7, event_names=None):
    """Fetch events from PostHog API."""
    # Note: The events API is deprecated. Use HogQL query instead.
    after = (datetime.utcnow() - timedelta(days=days_back)).isoformat() + 'Z'
    
    # Use query API with HogQL
    query = f"""
    SELECT 
        uuid,
        event,
        distinct_id,
        properties,
        timestamp,
        person_id
    FROM events
    WHERE timestamp >= '{after}'
    """
    
    if event_names:
        event_list = "', '".join(event_names)
        query += f" AND event IN ('{event_list}')"
    
    query += " ORDER BY timestamp DESC LIMIT 10000"
    
    result = make_request(
        config, 
        'query',
        method='POST',
        json_data={'query': {'kind': 'HogQLQuery', 'query': query}}
    )
    
    return result.get('results', [])

def fetch_persons(config, limit=1000):
    """Fetch persons from PostHog API."""
    all_persons = []
    next_url = None
    
    while True:
        if next_url:
            # Parse next URL for pagination
            response = requests.get(next_url, headers={'Authorization': f"Bearer {config['api_key']}"})
            response.raise_for_status()
            data = response.json()
        else:
            data = make_request(config, 'persons', {'limit': min(limit, 100)})
        
        all_persons.extend(data.get('results', []))
        
        if len(all_persons) >= limit or not data.get('next'):
            break
            
        next_url = data['next']
    
    return all_persons[:limit]

def fetch_insights(config, insight_ids=None):
    """Fetch saved insights from PostHog."""
    if insight_ids:
        insights = []
        for insight_id in insight_ids:
            insight = make_request(config, f'insights/{insight_id}')
            insights.append(insight)
        return insights
    else:
        return make_request(config, 'insights', {'limit': 100}).get('results', [])

def save_to_duckdb(data, table_name):
    """Save data to DuckDB table."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    con = duckdb.connect(str(DUCKDB_PATH))
    
    # Convert to JSON and load
    json_path = DATA_DIR / f'{table_name}.json'
    with open(json_path, 'w') as f:
        json.dump(data, f)
    
    con.execute(f"DROP TABLE IF EXISTS {table_name}")
    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_json_auto('{json_path}')")
    
    count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    print(f"Saved {count} rows to {table_name}")
    
    con.close()
    
    # Clean up JSON file
    json_path.unlink()

def main():
    # Parse arguments
    sync_events = '--events' in sys.argv or len(sys.argv) == 1
    sync_persons = '--persons' in sys.argv or len(sys.argv) == 1
    sync_insights = '--insights' in sys.argv
    
    # Validate environment
    config = get_config()
    if not config['api_key'] or not config['project_id']:
        print("Missing POSTHOG_API_KEY or POSTHOG_PROJECT_ID")
        sys.exit(1)
    
    print(f"Syncing PostHog data from {config['host']}")
    
    if sync_events:
        print("\nFetching events...")
        try:
            events = fetch_events(config, days_back=7)
            save_to_duckdb(events, 'posthog_events')
        except Exception as e:
            print(f"Error fetching events: {e}")
    
    if sync_persons:
        print("\nFetching persons...")
        try:
            persons = fetch_persons(config, limit=1000)
            save_to_duckdb(persons, 'posthog_persons')
        except Exception as e:
            print(f"Error fetching persons: {e}")
    
    if sync_insights:
        print("\nFetching insights...")
        try:
            insights = fetch_insights(config)
            save_to_duckdb(insights, 'posthog_insights')
        except Exception as e:
            print(f"Error fetching insights: {e}")
    
    print(f"\nData synced to {DUCKDB_PATH}")

if __name__ == '__main__':
    main()
