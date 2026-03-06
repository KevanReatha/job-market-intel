SELECT
    role_family,
    COUNT(*) AS nb_offers
FROM job_market_intel.v2
GROUP BY role_family
ORDER BY nb_offers DESC;