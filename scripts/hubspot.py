#!/usr/bin/env python3
"""
HubSpot Sync & Actions Script

This script handles:
1. Syncing HubSpot data (contacts, companies, deals) to local cache
2. Executing actions (create/update contacts, deals, etc.)

Usage:
    # Sync all data
    python scripts/hubspot.py sync
    
    # Sync specific objects
    python scripts/hubspot.py sync --objects contacts,deals
    
    # Run a specific action
    python scripts/hubspot.py action update_deal_stages
    
    # Run daily automation
    python scripts/hubspot.py daily
    
Environment Variables:
    HUBSPOT_ACCESS_TOKEN (required)
"""

import os
import sys
import json
import time
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
import requests
import duckdb

# Configuration
DATA_DIR = Path(__file__).parent.parent / 'data'
DUCKDB_PATH = DATA_DIR / 'hubspot_cache.duckdb'
ACTIONS_LOG = DATA_DIR / 'hubspot_actions.log'

BASE_URL = "https://api.hubapi.com"

# ============================================
# API Client
# ============================================

class HubSpotClient:
    def __init__(self, access_token: str):
        self.access_token = access_token
        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
    
    def _request(self, method: str, endpoint: str, **kwargs) -> dict:
        """Make authenticated request to HubSpot API."""
        url = f"{BASE_URL}{endpoint}"
        response = requests.request(method, url, headers=self.headers, **kwargs)
        
        # Handle rate limiting
        if response.status_code == 429:
            retry_after = int(response.headers.get('Retry-After', 10))
            print(f"Rate limited. Waiting {retry_after}s...")
            time.sleep(retry_after)
            return self._request(method, endpoint, **kwargs)
        
        response.raise_for_status()
        return response.json() if response.text else {}
    
    def get(self, endpoint: str, params: dict = None) -> dict:
        return self._request("GET", endpoint, params=params)
    
    def post(self, endpoint: str, data: dict = None) -> dict:
        return self._request("POST", endpoint, json=data)
    
    def patch(self, endpoint: str, data: dict = None) -> dict:
        return self._request("PATCH", endpoint, json=data)
    
    # ----------------------------------------
    # Read Operations
    # ----------------------------------------
    
    def get_all_contacts(self, properties: list = None, limit: int = 100) -> list:
        """Fetch all contacts with pagination."""
        properties = properties or ["email", "firstname", "lastname", "phone", 
                                    "company", "lifecyclestage", "hs_lead_status",
                                    "createdate", "lastmodifieddate"]
        
        all_contacts = []
        after = None
        
        while True:
            params = {
                "limit": limit,
                "properties": ",".join(properties)
            }
            if after:
                params["after"] = after
            
            result = self.get("/crm/v3/objects/contacts", params)
            all_contacts.extend(result.get("results", []))
            
            paging = result.get("paging", {})
            after = paging.get("next", {}).get("after")
            
            if not after:
                break
            
            print(f"  Fetched {len(all_contacts)} contacts...")
        
        return all_contacts
    
    def get_all_companies(self, properties: list = None, limit: int = 100) -> list:
        """Fetch all companies with pagination."""
        properties = properties or ["name", "domain", "industry", "numberofemployees",
                                    "annualrevenue", "city", "state", "country",
                                    "createdate", "lastmodifieddate"]
        
        all_companies = []
        after = None
        
        while True:
            params = {
                "limit": limit,
                "properties": ",".join(properties)
            }
            if after:
                params["after"] = after
            
            result = self.get("/crm/v3/objects/companies", params)
            all_companies.extend(result.get("results", []))
            
            paging = result.get("paging", {})
            after = paging.get("next", {}).get("after")
            
            if not after:
                break
            
            print(f"  Fetched {len(all_companies)} companies...")
        
        return all_companies
    
    def get_all_deals(self, properties: list = None, limit: int = 100) -> list:
        """Fetch all deals with pagination."""
        properties = properties or ["dealname", "amount", "dealstage", "pipeline",
                                    "closedate", "createdate", "hs_lastmodifieddate",
                                    "hubspot_owner_id"]
        
        all_deals = []
        after = None
        
        while True:
            params = {
                "limit": limit,
                "properties": ",".join(properties)
            }
            if after:
                params["after"] = after
            
            result = self.get("/crm/v3/objects/deals", params)
            all_deals.extend(result.get("results", []))
            
            paging = result.get("paging", {})
            after = paging.get("next", {}).get("after")
            
            if not after:
                break
            
            print(f"  Fetched {len(all_deals)} deals...")
        
        return all_deals
    
    def get_pipelines(self) -> list:
        """Fetch all deal pipelines and stages."""
        result = self.get("/crm/v3/pipelines/deals")
        return result.get("results", [])
    
    def get_owners(self) -> list:
        """Fetch all HubSpot owners/users."""
        result = self.get("/crm/v3/owners")
        return result.get("results", [])
    
    # ----------------------------------------
    # Write Operations
    # ----------------------------------------
    
    def create_contact(self, properties: dict) -> dict:
        """Create a new contact."""
        return self.post("/crm/v3/objects/contacts", {"properties": properties})
    
    def update_contact(self, contact_id: str, properties: dict) -> dict:
        """Update an existing contact."""
        return self.patch(f"/crm/v3/objects/contacts/{contact_id}", {"properties": properties})
    
    def create_deal(self, properties: dict) -> dict:
        """Create a new deal."""
        return self.post("/crm/v3/objects/deals", {"properties": properties})
    
    def update_deal(self, deal_id: str, properties: dict) -> dict:
        """Update an existing deal."""
        return self.patch(f"/crm/v3/objects/deals/{deal_id}", {"properties": properties})
    
    def create_note(self, body: str, associations: list = None) -> dict:
        """Create a note (engagement)."""
        data = {
            "properties": {
                "hs_note_body": body,
                "hs_timestamp": datetime.utcnow().isoformat() + "Z"
            }
        }
        if associations:
            data["associations"] = associations
        return self.post("/crm/v3/objects/notes", data)
    
    def create_task(self, subject: str, body: str = "", due_date: str = None, 
                    owner_id: str = None, associations: list = None) -> dict:
        """Create a task."""
        properties = {
            "hs_task_subject": subject,
            "hs_task_body": body,
            "hs_task_status": "NOT_STARTED",
            "hs_task_priority": "MEDIUM"
        }
        if due_date:
            properties["hs_timestamp"] = due_date
        if owner_id:
            properties["hubspot_owner_id"] = owner_id
        
        data = {"properties": properties}
        if associations:
            data["associations"] = associations
        
        return self.post("/crm/v3/objects/tasks", data)
    
    def search_contacts(self, filters: list, properties: list = None, limit: int = 100) -> list:
        """Search contacts with filters."""
        properties = properties or ["email", "firstname", "lastname", "lifecyclestage"]
        
        data = {
            "filterGroups": [{"filters": filters}],
            "properties": properties,
            "limit": limit
        }
        
        result = self.post("/crm/v3/objects/contacts/search", data)
        return result.get("results", [])
    
    def search_deals(self, filters: list, properties: list = None, limit: int = 100) -> list:
        """Search deals with filters."""
        properties = properties or ["dealname", "amount", "dealstage", "closedate"]
        
        data = {
            "filterGroups": [{"filters": filters}],
            "properties": properties,
            "limit": limit
        }
        
        result = self.post("/crm/v3/objects/deals/search", data)
        return result.get("results", [])


# ============================================
# Data Sync
# ============================================

def flatten_hubspot_object(obj: dict) -> dict:
    """Flatten HubSpot object for DuckDB storage."""
    flat = {
        "id": obj["id"],
        "created_at": obj.get("createdAt"),
        "updated_at": obj.get("updatedAt"),
        "archived": obj.get("archived", False)
    }
    flat.update(obj.get("properties", {}))
    return flat

def save_to_duckdb(data: list, table_name: str):
    """Save data to DuckDB table."""
    if not data:
        print(f"  No data to save for {table_name}")
        return
    
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    # Flatten objects
    flat_data = [flatten_hubspot_object(obj) for obj in data]
    
    # Save to JSON first
    json_path = DATA_DIR / f'{table_name}.json'
    with open(json_path, 'w') as f:
        json.dump(flat_data, f)
    
    # Load into DuckDB
    con = duckdb.connect(str(DUCKDB_PATH))
    con.execute(f"DROP TABLE IF EXISTS {table_name}")
    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_json_auto('{json_path}')")
    
    count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    print(f"  Saved {count} rows to {table_name}")
    
    con.close()
    json_path.unlink()  # Clean up JSON

def sync_data(client: HubSpotClient, objects: list = None):
    """Sync HubSpot data to local cache."""
    objects = objects or ["contacts", "companies", "deals", "pipelines", "owners"]
    
    print(f"Syncing HubSpot data: {', '.join(objects)}")
    
    if "contacts" in objects:
        print("\nFetching contacts...")
        contacts = client.get_all_contacts()
        save_to_duckdb(contacts, "contacts")
    
    if "companies" in objects:
        print("\nFetching companies...")
        companies = client.get_all_companies()
        save_to_duckdb(companies, "companies")
    
    if "deals" in objects:
        print("\nFetching deals...")
        deals = client.get_all_deals()
        save_to_duckdb(deals, "deals")
    
    if "pipelines" in objects:
        print("\nFetching pipelines...")
        pipelines = client.get_pipelines()
        # Flatten pipeline stages
        stages = []
        for pipeline in pipelines:
            for stage in pipeline.get("stages", []):
                stages.append({
                    "id": stage["id"],
                    "label": stage["label"],
                    "display_order": stage["displayOrder"],
                    "pipeline_id": pipeline["id"],
                    "pipeline_label": pipeline["label"]
                })
        
        if stages:
            json_path = DATA_DIR / 'deal_stages.json'
            with open(json_path, 'w') as f:
                json.dump(stages, f)
            
            con = duckdb.connect(str(DUCKDB_PATH))
            con.execute("DROP TABLE IF EXISTS deal_stages")
            con.execute(f"CREATE TABLE deal_stages AS SELECT * FROM read_json_auto('{json_path}')")
            print(f"  Saved {len(stages)} deal stages")
            con.close()
            json_path.unlink()
    
    if "owners" in objects:
        print("\nFetching owners...")
        owners = client.get_owners()
        owner_data = [{
            "id": o["id"],
            "email": o.get("email"),
            "first_name": o.get("firstName"),
            "last_name": o.get("lastName"),
            "user_id": o.get("userId")
        } for o in owners]
        
        if owner_data:
            json_path = DATA_DIR / 'owners.json'
            with open(json_path, 'w') as f:
                json.dump(owner_data, f)
            
            con = duckdb.connect(str(DUCKDB_PATH))
            con.execute("DROP TABLE IF EXISTS owners")
            con.execute(f"CREATE TABLE owners AS SELECT * FROM read_json_auto('{json_path}')")
            print(f"  Saved {len(owner_data)} owners")
            con.close()
            json_path.unlink()
    
    print(f"\nData synced to {DUCKDB_PATH}")


# ============================================
# Actions
# ============================================

def log_action(action_name: str, details: dict, success: bool = True):
    """Log action to file for audit trail."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    log_entry = {
        "timestamp": datetime.utcnow().isoformat(),
        "action": action_name,
        "success": success,
        "details": details
    }
    
    with open(ACTIONS_LOG, 'a') as f:
        f.write(json.dumps(log_entry) + "\n")

def action_stale_deals_reminder(client: HubSpotClient, days_stale: int = 14):
    """
    Find deals that haven't been updated in X days and create tasks for owners.
    """
    print(f"\nFinding deals stale for {days_stale}+ days...")
    
    stale_date = (datetime.utcnow() - timedelta(days=days_stale)).strftime("%Y-%m-%d")
    
    # Search for stale deals
    filters = [
        {
            "propertyName": "hs_lastmodifieddate",
            "operator": "LT",
            "value": stale_date
        },
        {
            "propertyName": "dealstage",
            "operator": "NOT_IN",
            "values": ["closedwon", "closedlost"]  # Adjust to your stage IDs
        }
    ]
    
    stale_deals = client.search_deals(
        filters, 
        properties=["dealname", "amount", "dealstage", "hubspot_owner_id", "hs_lastmodifieddate"]
    )
    
    print(f"Found {len(stale_deals)} stale deals")
    
    tasks_created = 0
    for deal in stale_deals:
        props = deal.get("properties", {})
        owner_id = props.get("hubspot_owner_id")
        
        if not owner_id:
            continue
        
        # Create follow-up task
        task = client.create_task(
            subject=f"Follow up on stale deal: {props.get('dealname', 'Unknown')}",
            body=f"This deal hasn't been updated since {props.get('hs_lastmodifieddate', 'unknown')}. Please review and update.",
            owner_id=owner_id,
            associations=[{
                "to": {"id": deal["id"]},
                "types": [{"associationCategory": "HUBSPOT_DEFINED", "associationTypeId": 216}]
            }]
        )
        
        tasks_created += 1
        log_action("stale_deal_reminder", {
            "deal_id": deal["id"],
            "deal_name": props.get("dealname"),
            "task_id": task.get("id")
        })
    
    print(f"Created {tasks_created} follow-up tasks")
    return tasks_created

def action_lifecycle_stage_update(client: HubSpotClient):
    """
    Update lifecycle stage for contacts based on deal status.
    Contacts with closed-won deals → Customer
    """
    print("\nUpdating lifecycle stages based on deal status...")
    
    # This is simplified - in reality you'd want to check deal associations
    # For now, just showing the pattern
    
    filters = [
        {
            "propertyName": "lifecyclestage",
            "operator": "EQ",
            "value": "opportunity"
        }
    ]
    
    opportunities = client.search_contacts(filters)
    print(f"Found {len(opportunities)} opportunities to check")
    
    # In a real implementation, you'd check if these contacts
    # have associated closed-won deals and update accordingly
    
    updated = 0
    # for contact in opportunities:
    #     # Check for closed-won deals...
    #     # client.update_contact(contact["id"], {"lifecyclestage": "customer"})
    #     updated += 1
    
    log_action("lifecycle_stage_update", {"checked": len(opportunities), "updated": updated})
    print(f"Updated {updated} contacts")
    return updated

def action_deal_stage_velocity(client: HubSpotClient):
    """
    Analyze deal stage velocity and flag deals stuck too long.
    This just syncs data - the actual analysis happens in Evidence dashboard.
    """
    print("\nSyncing deal data for velocity analysis...")
    
    # Just sync deals with extra properties
    deals = client.get_all_deals(properties=[
        "dealname", "amount", "dealstage", "pipeline",
        "closedate", "createdate", "hs_lastmodifieddate",
        "hubspot_owner_id", "hs_date_entered_*"  # Stage entry dates
    ])
    
    save_to_duckdb(deals, "deals")
    log_action("deal_velocity_sync", {"deals_synced": len(deals)})
    return len(deals)


def run_daily_automation(client: HubSpotClient):
    """Run all daily HubSpot automations."""
    print("=" * 50)
    print(f"Running daily HubSpot automation - {datetime.utcnow().isoformat()}")
    print("=" * 50)
    
    # 1. Sync fresh data
    sync_data(client)
    
    # 2. Run actions
    action_stale_deals_reminder(client, days_stale=14)
    action_lifecycle_stage_update(client)
    action_deal_stage_velocity(client)
    
    print("\n" + "=" * 50)
    print("Daily automation complete")
    print("=" * 50)


# ============================================
# CLI
# ============================================

def main():
    parser = argparse.ArgumentParser(description="HubSpot Sync & Actions")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")
    
    # Sync command
    sync_parser = subparsers.add_parser("sync", help="Sync HubSpot data")
    sync_parser.add_argument("--objects", type=str, help="Comma-separated list of objects to sync")
    
    # Action command
    action_parser = subparsers.add_parser("action", help="Run a specific action")
    action_parser.add_argument("name", type=str, help="Action name to run")
    
    # Daily command
    subparsers.add_parser("daily", help="Run daily automation")
    
    args = parser.parse_args()
    
    # Validate environment
    access_token = os.environ.get("HUBSPOT_ACCESS_TOKEN")
    if not access_token:
        print("Error: HUBSPOT_ACCESS_TOKEN environment variable required")
        sys.exit(1)
    
    client = HubSpotClient(access_token)
    
    if args.command == "sync":
        objects = args.objects.split(",") if args.objects else None
        sync_data(client, objects)
    
    elif args.command == "action":
        actions = {
            "stale_deals": lambda: action_stale_deals_reminder(client),
            "lifecycle_update": lambda: action_lifecycle_stage_update(client),
            "deal_velocity": lambda: action_deal_stage_velocity(client),
        }
        
        if args.name in actions:
            actions[args.name]()
        else:
            print(f"Unknown action: {args.name}")
            print(f"Available actions: {', '.join(actions.keys())}")
            sys.exit(1)
    
    elif args.command == "daily":
        run_daily_automation(client)
    
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
