WITH extract_data AS (
    SELECT
        ingestion_date,
        -- Je tente d'extraire le tableau
        JSON_EXTRACT_ARRAY(raw_content, '$.matches') AS liste_matchs
    FROM {{ source('psg_raw', 'PSG_MATCHES') }}
),

flattened_matches AS (
    SELECT 
        ingestion_date,
        match_json
    FROM extract_data,
        UNNEST(liste_matchs) as match_json
),

match_prepare AS (
    SELECT
        ingestion_date,
        JSON_VALUE(match_json,'$.utcDate') AS match_date,
        JSON_VALUE(match_json, '$.awayTeam.name') as away_team,
        JSON_VALUE(match_json, '$.homeTeam.name') as home_team,
        JSON_VALUE(match_json, '$.score.winner') as winner,
        CAST(JSON_VALUE(match_json, '$.score.fullTime.home') AS int64) as home_score,
        CAST(JSON_VALUE(match_json, '$.score.fullTime.away') AS int64) as away_score
    FROM flattened_matches
),

score_calculs AS (
    SELECT 
        ingestion_date,
        match_date,
        away_team,
        home_team,
        winner,
        home_score,
        away_score,
        CASE
            WHEN away_team = 'Paris Saint-Germain FC' THEN home_team
            ELSE away_team
        END AS adversaire_final,

        CASE WHEN away_team = 'Paris Saint-Germain FC' THEN away_score
            ELSE home_score
        END AS psg_score,

        CASE WHEN away_team = 'Paris Saint-Germain FC' THEN home_score
            ELSE away_score
        END AS adversaire_score

    FROM match_prepare
)

SELECT
    match_date,
    adversaire_final,
    psg_score,
    adversaire_score,
    CASE WHEN psg_score > adversaire_score THEN 'Paris Saint-Germain FC'
         WHEN psg_score = adversaire_score THEN 'draw'
         WHEN psg_score < adversaire_score THEN adversaire_final
    END AS winner

    FROM score_calculs