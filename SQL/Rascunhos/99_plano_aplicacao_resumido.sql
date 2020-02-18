DROP MATERIALIZED VIEW IF EXISTS siconv.plano_aplicacao_resumido;
CREATE MATERIALIZED VIEW siconv.plano_aplicacao_resumido as
select
	id_proposta,
	tipo_despesa_item,
	natureza_despesa,
	SUM(replace(valor_total_item, ',','.')::numeric) as valor_total_tipo_despesa
from siconv.plano_aplicacao_detalhado
GROUP BY 1,2,3