TOP_APEX_DOMAINS = """
with ranked_apex_names as (
  select
    coalesce(tranco_host_rank, tranco_domain_rank) as rank,
    domain,
    replace(domain, suffix, "") as apex
  FROM `moz-fx-data-shared-prod.domain_metadata_derived.top_domains_v1`
  WHERE submission_date >= date_trunc(current_date(), MONTH)
  and country_code = 'us'
  order by 1
  limit 100
)
select
  distinct first_value(domain) over (
      partition by ranked_apex_names.apex order by rank asc
  ) as domain
from ranked_apex_names
"""
