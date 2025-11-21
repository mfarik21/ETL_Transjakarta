CREATE TABLE IF NOT EXISTS raw.routes (
    route_code TEXT,
    route_name TEXT,
    _ingest_ts TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.shelter_corridor (
    shelter_name_var TEXT,
    corridor_code TEXT,
    corridor_name TEXT,
    _ingest_ts TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.realisasi_bus (
    tanggal_realisasi TEXT,
    bus_body_no TEXT,
    rute_realisasi TEXT,
    _ingest_ts TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.transaksi_bus (
    uuid TEXT,
    waktu_transaksi TEXT,
    armada_id_var TEXT,
    no_body_var TEXT,
    card_number_var TEXT,
    card_type_var TEXT,
    balance_before_int TEXT,
    fare_int TEXT,
    balance_after_int TEXT,
    transcode_txt TEXT,
    gate_in_boo TEXT,
    p_latitude_flo TEXT,
    p_longitude_flo TEXT,
    status_var TEXT,
    free_service_boo TEXT,
    insert_on_dtm TEXT,
    _ingest_ts TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.transaksi_halte (
    uuid TEXT,
    waktu_transaksi TEXT,
    shelter_name_var TEXT,
    terminal_name_var TEXT,
    card_number_var TEXT,
    card_type_var TEXT,
    balance_before_int TEXT,
    fare_int TEXT,
    balance_after_int TEXT,
    transcode_txt TEXT,
    gate_in_boo TEXT,
    p_latitude_flo TEXT,
    p_longitude_flo TEXT,
    status_var TEXT,
    free_service_boo TEXT,
    insert_on_dtm TEXT,
    _ingest_ts TIMESTAMP DEFAULT NOW()
);