SELECT *,
    CASE
        WHEN game_status = 'FINISHED' THEN CONCAT(
            'Le ', match_date, ', le PSG a joue contre ', adversaire_final, ' en ', competition,
            '. Arbitre: ', IFNULL(referee, 'non renseigne'), '. ',
            CASE WHEN winner = 'draw' THEN 'Ce fut un match nul' ELSE CONCAT('Le gagnant de ce match etait ', winner) END,
            '. Score: PSG ', CAST(psg_score AS STRING), ' - ', CAST(adversaire_score AS STRING), '. ',
            CASE WHEN is_home = TRUE THEN 'Le PSG a recu son adversaire a domicile.' ELSE 'Le PSG a joue a exterieur.' END
        )
        ELSE CONCAT(
            'Le ', match_date, ', le PSG jouera contre ', adversaire_final, ' en ', IFNULL(competition, 'competition non renseignee'), '. ',
            CASE WHEN is_home = TRUE THEN 'Le PSG recevra a domicile.' ELSE 'Le PSG se deplacera a exterieur.' END
        )
    END AS contexte_rag
FROM {{ ref('stg_psg_matches') }}