CREATE TABLE IF NOT EXISTS staging.routes (
    route_code VARCHAR PRIMARY KEY,
    route_name VARCHAR,
    load_ts TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS staging.shelter_corridor (
    shelter_name VARCHAR PRIMARY KEY,
    corridor_code VARCHAR,
    corridor_name VARCHAR,
    load_ts TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS staging.realisasi_bus (
    tanggal_realisasi DATE,
    bus_body_no VARCHAR,
    rute_realisasi VARCHAR REFERENCES staging.routes(route_code),
    load_ts TIMESTAMP DEFAULT NOW(),
    
    CONSTRAINT uq_realisasi_bus UNIQUE (bus_body_no, rute_realisasi)
);


CREATE TABLE IF NOT EXISTS staging.transaksi_bus (
    uuid VARCHAR PRIMARY KEY,
    waktu_transaksi TIMESTAMP,
    armada_id VARCHAR,
    no_body VARCHAR,
    card_number VARCHAR,
    card_type VARCHAR,
    balance_before INTEGER,
    fare INTEGER,
    balance_after INTEGER,
    transcode VARCHAR,
    gate_in BOOLEAN,
    latitude FLOAT,
    longitude FLOAT,
    status VARCHAR,
    free_service BOOLEAN,
    insert_on_dtm TIMESTAMP,
    load_ts TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS staging.transaksi_halte (
    uuid VARCHAR PRIMARY KEY,
    waktu_transaksi TIMESTAMP,
    shelter_name VARCHAR,
    terminal_name VARCHAR,
    card_number VARCHAR,
    card_type VARCHAR,
    balance_before INTEGER,
    fare INTEGER,
    balance_after INTEGER,
    transcode VARCHAR,
    gate_in BOOLEAN,
    latitude FLOAT,
    longitude FLOAT,
    status VARCHAR,
    free_service BOOLEAN,
    insert_on_dtm TIMESTAMP,
    load_ts TIMESTAMP DEFAULT NOW()
);