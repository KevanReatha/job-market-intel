SELECT
    dt,
    skill,
    COUNT(*) AS nb_offers
FROM job_market_intel.v2
CROSS JOIN UNNEST(skills) AS t(skill)
GROUP BY dt, skill
ORDER BY dt DESC, nb_offers DESC, skill;