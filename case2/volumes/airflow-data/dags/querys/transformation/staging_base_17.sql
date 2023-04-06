CREATE TABLE IF NOT EXISTS "tb_base_17_staging" AS

select distinct
	    id_marca
		,marca
		,id_linha
		,linha
		,to_date(data_venda, CASE
           					   WHEN data_venda LIKE '_/_/____' THEN 'dd/mm/yyyyy'
							   WHEN data_venda LIKE '__/_/____' THEN 'dd/mm/yyyyy'
							   WHEN data_venda LIKE '_/__/____' THEN 'dd/mm/yyyyy'
							   WHEN data_venda LIKE '__/__/____' THEN 'dd/mm/yyyyy'   
							 END
						   ) data_venda
	   ,qtd_venda
from tb_base_17_raw;