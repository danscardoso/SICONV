DROP VIEW IF EXISTS siconv_schema.vi_programa_proposta_uni;
CREATE VIEW siconv_schema.vi_programa_proposta_uni AS
select
	id_proposta, min(id_programa::integer) as id_programa
from siconv_schema.programa_proposta
GROUP BY id_proposta
ORDER BY id_proposta
