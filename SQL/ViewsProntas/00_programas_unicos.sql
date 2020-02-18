-- Essa view serve para unicionar os vários registros referentes ao mesmo programa,
-- pois ocorrem múltiplos registros se um programa ocorrer em vários estados, atrapalhando os joins
-- entre outros problemas.
--
DROP VIEW IF EXISTS siconv_schema.vi_programas_unicos cascade;
CREATE VIEW siconv_schema.vi_programas_unicos AS
select distinct
	cod_orgao_sup_programa,
	desc_orgao_sup_programa,
	id_programa,
	cod_programa,
	nome_programa,
	sit_programa,
	data_disponibilizacao,
	ano_disponibilizacao,
	dt_prog_ini_receb_prop,
	dt_prog_fim_receb_prop,
	dt_prog_ini_emenda_par,
	dt_prog_fim_emenda_par,
	dt_prog_ini_benef_esp,
	dt_prog_fim_benef_esp,
	acao_orcamentaria 
from siconv_schema.programa
order by id_programa;
