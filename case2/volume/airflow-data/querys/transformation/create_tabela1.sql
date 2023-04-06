\c main

SET DateStyle TO 'European';

CREATE TABLE "base_17" (
	id_marca INTEGER,
	marca VARCHAR,
	id_linha INTEGER,
	linha VARCHAR,
	data_venda DATE,
	qtd_venda INTEGER
);

\copy 'base_17' FROM '/opt/airlfow/data/base_17' DELIMITER ',' CSV HEADER;