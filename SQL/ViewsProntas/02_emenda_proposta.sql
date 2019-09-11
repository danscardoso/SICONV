DROP VIEW IF EXISTS SICONV.emenda_proposta cascade;
CREATE VIEW SICONV.emenda_proposta AS
select id_proposta, ind_emenda_impositiva, valor_repasse_proposta_emenda, valor_repasse_emenda,
	case
		when tipo_parlamentar = 3 then 'COMISSAO'
		when tipo_parlamentar = 2 then 'BANCADA'
		when tipo_parlamentar = 1 then 'INDIVIDUAL'
	end as tipo_parlamentar
from 
(
	select
		id_proposta,
		case
			when sum(case when ind_impositivo = 'NÃO' then 0.01 when ind_impositivo = 'SIM' then 1 end)::text ~ '^[0-9]+$' then 'SIM'
			when sum(case when ind_impositivo = 'NÃO' then 0.01 when ind_impositivo = 'SIM' then 1 end)::text ~ '^[1-9]+\.[0-9]+$' then 'PARCIAL'
			when sum(case when ind_impositivo = 'NÃO' then 0.01 when ind_impositivo = 'SIM' then 1 end)::text ~ '^0\.[0-9]+$' then 'NÃO'
		end as ind_emenda_impositiva,
		
		-- Transformar o texto em codigo pra priorizar
		max(case
			when tipo_parlamentar = 'COMISSAO'   then 3
			when tipo_parlamentar = 'BANCADA'    then 2
			when tipo_parlamentar = 'INDIVIDUAL' then 1
			else NULL
		end) as tipo_parlamentar,
		--cod_programa_emenda,
	    sum(replace(valor_repasse_proposta_emenda, ',','.')::numeric) as valor_repasse_proposta_emenda,
	    SUM(replace(valor_repasse_emenda, ',','.')::numeric) as valor_repasse_emenda
	from siconv.emenda
	GROUP BY id_proposta
	order by id_proposta
) T

