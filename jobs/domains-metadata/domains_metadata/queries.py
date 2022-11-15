TOP_APEX_DOMAINS = """
with apex_names as (
  select
    coalesce(tranco_host_rank, tranco_domain_rank) as rank,
    domain,
    replace(domain, suffix, "") as apex
  FROM `moz-fx-data-shared-prod.domain_metadata_derived.top_domains_v1`
  WHERE submission_date >= date_trunc(current_date(), MONTH)
  AND country_code IN ('us', 'ca')
), ranked_apex_names as (
    SELECT
      DISTINCT FIRST_VAlUE(domain) over (
          partition by apex_names.apex order by rank asc
      ) as domain,
      FIRST_VALUE(rank) OVER (partition by apex_names.apex order by rank asc) as rank
    FROM apex_names
    WHERE rank IS NOT null
    ORDER BY 2
), domains_with_categories as (
    select
      domain,
      categories is not null as has_categories
    from `moz-fx-data-shared-prod.domain_metadata_derived.domain_categories_v1`
    where DATE(_PARTITIONTIME) >= date_trunc(current_date(), month)
)
select * from
ranked_apex_names
left join domains_with_categories
using (domain)
where has_categories is null
order by rank
limit 1000
"""
