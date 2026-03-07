SELECT
    dt,
    role_family,
    COUNT(*) AS nb_offers
FROM job_market_intel.v2
GROUP BY dt, role_family
ORDER BY dt DESC, nb_offers DESC, role_family;