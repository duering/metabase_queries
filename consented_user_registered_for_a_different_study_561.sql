/*
 Original
 */
WITH t1 AS (
    SELECT distinct study_map.survey_study_name,
        study_map.tenant_id,
        study_map.register_url,
        study_map.consent,
        study_map.token_url
    from dwh_events.event_cov_survey_api_survey_completion
    LEFT JOIN (
        SELECT * FROM (
            VALUES
                ('accept', 'd4l', '%program:accept%', 'uulm.accept', 'accept', ''),
                ('cronos', 'd4l', '%program:cronos%', 'dgpm.cronos', 'cronos', ''),
                ('ecov', 'd4l', '%program:ecov%', 'd4l.ecov', 'ecov', ''),
                ('femfit', 'd4l', '%program:femfit%', 'd4l.femfit', 'femfit', ''),
                ('registry', 'd4l', '%program:registry%', 'd4l.registry', 'd4lregistry', ''),
                ('health-behavior', 'rki', '%program:health-behavior%', 'rki.health-behavior', 'rki_health_behavior', ''),
                ('panel', 'rki', '%program:panel%', 'rki.panel', 'rki-panel', 'https://panel.rki.de/?param=program:panel,token:')
        )
        AS StudyToTenant(study_id, tenant_id, register_url, consent, survey_study_name, token_url)
    ) study_map
    ON study_map.survey_study_name = dwh_events.event_cov_survey_api_survey_completion.study_id
    WHERE {{study_name}}
),
t2 as (
    SELECT
        dwh_events.users.user_id,
        dwh_events.users.user_sk,
        dwh_events.consents.consent_document_key,
        users_view.initial_source_url as source_url,
        event_vega_register.event_at as registered_at,
        max(dwh_events.consents.granted_at) as granted_at,
        max(dwh_events.consents.revoked_at) as revoked_at
    FROM dwh_events.users
    LEFT OUTER JOIN dwh_events.consents -- can be INNER JOIN
        ON dwh_events.users.user_sk = dwh_events.consents.user_sk
    LEFT OUTER JOIN dwh_events.event_vega_register -- not required join
        ON dwh_events.users.user_sk = event_vega_register.user_sk
    LEFT OUTER JOIN dwh_events.users_view -- not required join
        ON users.user_sk = users_view.user_sk
    WHERE
        dwh_events.consents.consent_document_key = (SELECT consent FROM t1)
        AND dwh_events.users.account_type != 'internal'
        AND {{register_date}}
    GROUP BY 1,2,3,4,5
    ORDER BY dwh_events.users.user_sk
),
t3 as (
    SELECT
        t2.user_id,
        t2.user_sk,
        t2.consent_document_key as consent,
        t2.granted_at,
        case when t2.source_url like (select register_url from t1) then true else false end registered_study, -- semantically does not require that user actually registered for the study
        t2.source_url,
        t2.registered_at
    FROM t2
)
select user_id, user_sk, source_url, registered_at, granted_at, consent
from t3
where registered_study = false