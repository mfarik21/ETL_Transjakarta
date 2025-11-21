INSERT INTO dw.cube_trip_transaction (
    date_key,
    route_id,
    bus_id,
    shelter_id,
    card_type,
    fare,
    gate_in,
    status,
    source,
    total_passenger,
    total_fare
)
SELECT
    DATE(f.waktu_transaksi) AS date_key,
    f.dim_route_id,
    f.dim_bus_id,
    f.dim_shelter_id,
    f.card_type,
    f.fare,
    f.gate_in,
    f.status,
    f.source,
    COUNT(*) AS total_passenger,
    SUM(f.fare) AS total_fare
FROM dw.fact_transaction f
GROUP BY
    DATE(f.waktu_transaksi),
    f.dim_route_id,
    f.dim_bus_id,
    f.dim_shelter_id,
    f.card_type,
    f.fare,
    f.gate_in,
    f.status,
    f.source;
