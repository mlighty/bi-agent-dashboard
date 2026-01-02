---
title: Sales Pipeline Dashboard
description: HubSpot CRM analytics and pipeline health
---

# Sales Pipeline Dashboard

<DateRange
    name=date_range
    defaultValue="Last 30 Days"
/>

<Dropdown
    name=pipeline
    title="Pipeline"
>
    <DropdownOption value="all" valueLabel="All Pipelines" default/>
</Dropdown>

---

## Pipeline Overview

```sql pipeline_summary
SELECT
    count(*) as total_deals,
    count(*) filter (where dealstage not like '%lost%' and dealstage not like '%won%' and dealstage not like '%Won%') as open_deals,
    sum(cast(amount as decimal)) filter (where dealstage not like '%lost%' and dealstage not like '%won%' and dealstage not like '%Won%') as open_pipeline_value,
    sum(cast(amount as decimal)) filter (where dealstage like '%won%' or dealstage like '%Won%') as won_value,
    count(*) filter (where dealstage like '%won%' or dealstage like '%Won%') as won_deals,
    count(*) filter (where dealstage like '%lost%') as lost_deals
FROM deals
WHERE created_at >= '${inputs.date_range.start}'
```

<Grid cols=4>
    <BigValue 
        data={pipeline_summary} 
        value=open_deals 
        title="Open Deals"
    />
    <BigValue 
        data={pipeline_summary} 
        value=open_pipeline_value 
        title="Pipeline Value"
        fmt=usd0
    />
    <BigValue 
        data={pipeline_summary} 
        value=won_value 
        title="Won Revenue"
        fmt=usd0
    />
    <BigValue 
        data={pipeline_summary} 
        value=won_deals 
        title="Deals Won"
    />
</Grid>

---

## Pipeline by Stage

```sql deals_by_stage
SELECT 
    coalesce(s.label, d.dealstage) as stage,
    s.display_order,
    count(*) as deals,
    sum(cast(d.amount as decimal)) as value
FROM deals d
LEFT JOIN deal_stages s ON d.dealstage = s.id
WHERE d.dealstage NOT LIKE '%lost%' AND d.dealstage NOT LIKE '%won%' AND d.dealstage NOT LIKE '%Won%'
GROUP BY 1, 2
ORDER BY s.display_order
```

<BarChart 
    data={deals_by_stage} 
    x=stage 
    y=value
    yFmt=usd0
    title="Pipeline Value by Stage"
/>

<DataTable 
    data={deals_by_stage}
    fmt={[null, null, 'num0', 'usd0']}
/>

---

## Win Rate Trend

```sql monthly_win_rate
SELECT
    date_trunc('month', cast(created_at as timestamp)) as month,
    count(*) filter (where dealstage like '%won%' or dealstage like '%Won%') as won,
    count(*) filter (where dealstage like '%lost%') as lost,
    count(*) filter (where dealstage like '%won%' or dealstage like '%Won%' or dealstage like '%lost%') as closed,
    round(100.0 * count(*) filter (where dealstage like '%won%' or dealstage like '%Won%') /
          nullif(count(*) filter (where dealstage like '%won%' or dealstage like '%Won%' or dealstage like '%lost%'), 0), 1) as win_rate
FROM deals
WHERE cast(created_at as timestamp) >= current_date - interval '12 months'
GROUP BY 1
ORDER BY 1
```

<LineChart 
    data={monthly_win_rate} 
    x=month 
    y=win_rate
    yFmt=pct1
    title="Monthly Win Rate"
/>

---

## Deal Velocity (Days in Stage)

```sql stale_deals
SELECT 
    dealname,
    coalesce(s.label, d.dealstage) as stage,
    cast(d.amount as decimal) as amount,
    o.email as owner,
    date_diff('day', cast(d.updated_at as date), current_date) as days_since_update
FROM deals d
LEFT JOIN deal_stages s ON d.dealstage = s.id
LEFT JOIN owners o ON d.hubspot_owner_id = o.id
WHERE d.dealstage NOT LIKE '%lost%' AND d.dealstage NOT LIKE '%won%' AND d.dealstage NOT LIKE '%Won%'
    AND date_diff('day', cast(d.updated_at as date), current_date) > 7
ORDER BY days_since_update DESC
LIMIT 20
```

<Alert status="warning">
    Deals not updated in 7+ days
</Alert>

<DataTable 
    data={stale_deals}
    fmt={[null, null, 'usd0', null, 'num0']}
    rows=20
/>

---

## Revenue by Owner

```sql revenue_by_owner
SELECT
    coalesce(o.email, 'Unassigned') as owner,
    count(*) filter (where d.dealstage like '%won%' or d.dealstage like '%Won%') as deals_won,
    sum(cast(d.amount as decimal)) filter (where d.dealstage like '%won%' or d.dealstage like '%Won%') as revenue,
    count(*) filter (where d.dealstage not like '%won%' and d.dealstage not like '%Won%' and d.dealstage not like '%lost%') as open_deals,
    sum(cast(d.amount as decimal)) filter (where d.dealstage not like '%won%' and d.dealstage not like '%Won%' and d.dealstage not like '%lost%') as pipeline
FROM deals d
LEFT JOIN owners o ON d.hubspot_owner_id = o.id
WHERE d.created_at >= '${inputs.date_range.start}'
GROUP BY 1
ORDER BY revenue DESC NULLS LAST
```

<BarChart 
    data={revenue_by_owner} 
    x=owner 
    y=revenue
    yFmt=usd0
    swapXY=true
    title="Revenue by Owner"
/>

<DataTable 
    data={revenue_by_owner}
    fmt={[null, 'num0', 'usd0', 'num0', 'usd0']}
/>

---

## New Deals Trend

```sql new_deals_trend
SELECT 
    date_trunc('week', created_at) as week,
    count(*) as new_deals,
    sum(cast(amount as decimal)) as new_pipeline
FROM deals
WHERE created_at >= current_date - interval '90 days'
GROUP BY 1
ORDER BY 1
```

<AreaChart 
    data={new_deals_trend} 
    x=week 
    y=new_pipeline
    yFmt=usd0
    title="New Pipeline Created (Weekly)"
/>

---

## Contact Lifecycle Funnel

```sql lifecycle_funnel
SELECT 
    lifecyclestage as stage,
    count(*) as contacts,
    CASE lifecyclestage
        WHEN 'subscriber' THEN 1
        WHEN 'lead' THEN 2
        WHEN 'marketingqualifiedlead' THEN 3
        WHEN 'salesqualifiedlead' THEN 4
        WHEN 'opportunity' THEN 5
        WHEN 'customer' THEN 6
        WHEN 'evangelist' THEN 7
        ELSE 99
    END as stage_order
FROM contacts
WHERE lifecyclestage IS NOT NULL
GROUP BY 1
ORDER BY stage_order
```

<FunnelChart 
    data={lifecycle_funnel}
    nameCol=stage
    valueCol=contacts
    title="Contact Lifecycle Funnel"
/>

---

<Details title="Data Notes">

**Source**: HubSpot CRM (synced daily at 6 AM PT)

**Last Sync**: Check `data/hubspot_actions.log` for latest sync time

**Automation Status**: Daily automations create tasks for stale deals and update lifecycle stages

</Details>
