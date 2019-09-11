DROP VIEW IF EXISTS siconv.programas_uni cascade;
CREATE VIEW siconv.programas_uni AS
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
