-- Essa view serve para unicionar os vários registros referentes ao mesmo programa,
-- pois ocorrem múltiplos registros se um programa ocorrer em vários estados (entre)
-- outros casos.
--
-- Isso gera um problema sério pois um join de propostas com o programa multiplica os
-- registros. Assim, ao fazer o join com essa view evita este problema.
DROP VIEW IF EXISTS vi_siconv.vi_programas_unicos cascade;
CREATE VIEW siconv.vi_programas_unicos AS
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
from siconv.programa
order by id_programa;
