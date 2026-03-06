SELECT
    skill,
    COUNT(*) AS nb_offers
FROM job_market_intel.v2
CROSS JOIN UNNEST(skills) AS t(skill)
GROUP BY skill
ORDER BY nb_offers DESC;