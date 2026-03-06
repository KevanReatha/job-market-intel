SELECT
    role_family,
    skill,
    COUNT(*) AS nb
FROM job_market_intel.v2
CROSS JOIN UNNEST(skills) AS t(skill)
GROUP BY role_family, skill
ORDER BY role_family, nb DESC;