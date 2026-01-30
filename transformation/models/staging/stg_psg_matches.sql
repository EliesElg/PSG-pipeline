SELECT
    ingestion_date,
    -- Je tente d'extraire le tableau
    JSON_EXTRACT_ARRAY(raw_content, '$.matches') AS liste_matchs
FROM {{ source('psg_raw', 'PSG_MATCHES') }}