CREATE TABLE IF NOT EXISTS dw.dim_route (
    id SERIAL PRIMARY KEY,
    route_code VARCHAR NOT NULL UNIQUE,
    route_name VARCHAR,
    load_ts TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dw.dim_shelter (
    id SERIAL PRIMARY KEY,
    shelter_name VARCHAR NOT NULL UNIQUE,
    corridor_name VARCHAR,
    load_ts TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dw.dim_bus (
    id SERIAL PRIMARY KEY,
    bus_body_no VARCHAR NOT NULL UNIQUE,
    rute_realisasi VARCHAR,
    load_ts TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dw.fact_transaction (
    uuid VARCHAR PRIMARY KEY,
    waktu_transaksi TIMESTAMP,
    
    dim_route_id INT REFERENCES dw.dim_route(id),
    dim_shelter_id INT REFERENCES dw.dim_shelter(id),
    dim_bus_id INT REFERENCES dw.dim_bus(id),

    card_number VARCHAR,
    card_type VARCHAR,
    fare INTEGER,
    gate_in BOOLEAN,

    latitude FLOAT,
    longitude FLOAT,
    free_service BOOLEAN,
    status VARCHAR,

    source VARCHAR, -- 'bus' or 'halte'
    load_ts TIMESTAMP DEFAULT NOW()
);


CREATE TABLE IF NOT EXISTS dw.cube_trip_transaction (
    cube_id BIGSERIAL PRIMARY KEY,

    -- Dimensions
    date_key DATE NOT NULL,
    route_id BIGINT,
    bus_id BIGINT,
    shelter_id BIGINT,

    card_type VARCHAR,
    fare INTEGER,
    gate_in BOOLEAN,

    -- Fact attributes
    status VARCHAR,
    source VARCHAR(10),

    -- Metrics
    total_passenger INTEGER NOT NULL,
    total_fare BIGINT,

    load_ts TIMESTAMP DEFAULT NOW()
);
