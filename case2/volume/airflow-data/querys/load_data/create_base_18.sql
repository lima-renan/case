\c main

SET DateStyle TO 'European';

CREATE IF NOT EXISTS TABLE  "base_18" (
	id_marca INTEGER,
	marca VARCHAR,
	id_linha INTEGER,
	linha VARCHAR,
	data_venda DATE,
	qtd_venda INTEGER
);

\copy 'base_18' FROM '/opt/airlfow/data/base_18.csv' DELIMITER ',' CSV HEADER;