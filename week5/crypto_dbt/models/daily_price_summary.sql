SELECT
    c.name AS coin_name,
    c.symbol,
    d.full_date,
    ROUND(AVG(f.current_price)::numeric, 2) AS avg_price,
    ROUND(MIN(f.current_price)::numeric, 2) AS min_price,
    ROUND(MAX(f.current_price)::numeric, 2) AS max_price,
    ROUND(AVG(f.market_cap)::numeric, 0) AS avg_market_cap,
    COUNT(*) AS num_snapshots
FROM public.factcryptoprices f
JOIN public.dimcoin c ON f.coin_key = c.coin_key
JOIN public.dimdate d ON f.date_key = d.date_key
GROUP BY c.name, c.symbol, d.full_date
ORDER BY d.full_date DESC, c.name