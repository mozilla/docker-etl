SELECT
    bh.number,
    MIN(CASE
        WHEN EXISTS (
            SELECT 1
            FROM UNNEST(SPLIT(changes_unnested.added, ',')) AS keyword
            WHERE TRIM(keyword) LIKE 'webcompat:%'
            AND NOT TRIM(keyword) = 'webcompat:site-report'
        ) THEN bh.change_time
    END) AS first_triage_exit,
    MIN(CASE
        WHEN EXISTS (
            SELECT 1
            FROM UNNEST(SPLIT(changes_unnested.added, ',')) AS keyword
            WHERE TRIM(keyword) = 'webcompat:needs-diagnosis'
        ) THEN bh.change_time
    END) AS first_diagnosis_enter,
    MIN(CASE
        WHEN EXISTS (
            SELECT 1
            FROM UNNEST(SPLIT(changes_unnested.removed, ',')) AS keyword
            WHERE TRIM(keyword) = 'webcompat:needs-diagnosis'
        ) THEN bh.change_time
    END) AS first_diagnosis_exit,
    MIN(CASE
        WHEN EXISTS (
            SELECT 1
            FROM UNNEST(SPLIT(changes_unnested.added, ',')) AS keyword
            WHERE TRIM(keyword) = 'webcompat:platform-bug'
        ) THEN bh.change_time
    END) AS first_platform_enter,
    MIN(CASE
        WHEN EXISTS (
            SELECT 1
            FROM UNNEST(SPLIT(changes_unnested.removed, ',')) AS keyword
            WHERE TRIM(keyword) = 'webcompat:platform-bug'
        )
        OR EXISTS (
            SELECT 1
            FROM UNNEST(SPLIT(changes_unnested.added, ',')) AS keyword
            WHERE TRIM(keyword) = 'webcompat:site-workaround'
        )
        THEN bh.change_time
    END) AS first_platform_exit,
    MIN(CASE
        WHEN EXISTS (
            SELECT 1
            FROM UNNEST(SPLIT(changes_unnested.added, ',')) AS keyword
            WHERE TRIM(keyword) = 'webcompat:needs-sitepatch'
        ) THEN bh.change_time
    END) AS first_sitepatch_enter,
    MIN(CASE
        WHEN EXISTS (
            SELECT 1
            FROM UNNEST(SPLIT(changes_unnested.removed, ',')) AS keyword
            WHERE TRIM(keyword) = 'webcompat:needs-sitepatch'
        )
        OR EXISTS (
            SELECT 1
            FROM UNNEST(SPLIT(changes_unnested.added, ',')) AS keyword
            WHERE TRIM(keyword) = 'webcompat:sitepatch-applied'
        )
        THEN bh.change_time
    END) AS first_sitepatch_exit,
    MIN(CASE
        WHEN EXISTS (
            SELECT 1
            FROM UNNEST(SPLIT(changes_unnested.added, ',')) AS keyword
            WHERE TRIM(keyword) IN ('webcompat:needs-contact', 'webcompat:contact-ready')
        )
        THEN bh.change_time
    END) AS first_outreach_enter,
    MIN(CASE
        WHEN EXISTS (
            SELECT 1
            FROM UNNEST(SPLIT(changes_unnested.removed, ',')) AS keyword
            WHERE TRIM(keyword) = 'webcompat:needs-contact'
                OR TRIM(keyword) = 'webcompat:contact-ready'
        )
        THEN bh.change_time
    END) AS first_outreach_exit,
    MIN(CASE
        WHEN EXISTS (
            SELECT 1
            FROM UNNEST(SPLIT(changes_unnested.added, ',')) AS keyword
            WHERE TRIM(keyword) = 'webcompat:contact-in-progress'
                OR TRIM(keyword) = 'webcompat:site-wait'
                OR TRIM(keyword) = 'webcompat:contact-complete'
        )
        THEN bh.change_time
    END) AS first_sitewait_enter,
    MIN(CASE
        WHEN bb.resolved_time IS NOT NULL
        AND EXISTS (
            SELECT 1
            FROM UNNEST(bb.keywords) AS keywords
            WHERE TRIM(keywords) IN ('webcompat:contact-in-progress', 'webcompat:site-wait', 'webcompat:contact-complete')
        ) THEN bb.resolved_time
    END) AS first_sitewait_exit,
    MIN(CASE
        WHEN EXISTS (
            SELECT 1
            FROM UNNEST(SPLIT(changes_unnested.added, ',')) AS keyword
            WHERE TRIM(keyword) = 'webcompat:blocked'
        ) THEN bh.change_time
    END) AS first_blocked_enter,
    MIN(CASE
        WHEN EXISTS (
            SELECT 1
            FROM UNNEST(SPLIT(changes_unnested.removed, ',')) AS keyword
            WHERE TRIM(keyword) = 'webcompat:blocked'
        ) THEN bh.change_time
    END) AS first_blocked_exit
    FROM
        `{{ ref('bugs_history') }}` bh
    JOIN bh.changes AS changes_unnested
    JOIN `{{ ref('scored_site_reports') }}` bb ON bh.number = bb.number
    WHERE (
        (bb.product = 'Web Compatibility' AND bb.component ='Site Reports') OR
        ('webcompat:site-report' IN UNNEST(bb.keywords) AND NOT bb.component = 'Privacy: Site Reports')
    )
    GROUP BY 1
