CREATE TABLE IF NOT EXISTS "tb_base_trusted" AS

with base as(

	select * 
	from tb_base_17_staging

	union all 

	select * 
	from tb_base_18_staging

	union all 

	select * 
	from tb_base_19_staging
)

select distinct * from base;