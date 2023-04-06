CREATE TABLE IF NOT EXISTS "tb_consol_vend_marca_linha" AS

select  marca
	   ,linha
	   ,sum(qtd_venda) as total_vendas
 from tb_base_trusted
 group by marca, linha;