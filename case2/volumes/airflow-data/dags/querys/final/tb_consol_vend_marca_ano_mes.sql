CREATE TABLE IF NOT EXISTS "tb_consol_vend_marca_ano_mes" AS

select to_char(data_venda,'MM-YYYY') AS mes_ano_venda 
	   ,marca
	   ,sum(qtd_venda) as total_vendas
 from tb_base_trusted
 group by mes_ano_venda, marca;