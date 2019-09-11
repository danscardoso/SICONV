DROP VIEW IF EXISTS siconv.programa_proposta_uni;
CREATE VIEW siconv.programa_proposta_uni AS
select
	id_proposta, min(id_programa::integer) as id_programa
from siconv.programa_proposta
GROUP BY id_proposta
ORDER BY id_proposta
