DROP MATERIALIZED VIEW IF EXISTS siconv_schema.solicitantes;
CREATE MATERIALIZED VIEW siconv_schema.solicitantes AS

-- Natureza jurídica municipal, tem que juntar as subunidades na mesma coisa
select 
	'MUNICIPIO DE ' || municipio_proponente || '/' || uf_proponente as nm_proponente,
	-- ex: MUNICIPIO DE SAO PAULO/SP
	natureza_juridica,
	regiao_proponente,
	uf_proponente,
	municipio_proponente,

	sum(num_propostas) as num_propostas,
	sum(quantidade_propostas_via_emenda) as quantidade_propostas_via_emenda,
	sum(quantidade_instrumentos_assinados) as quantidade_instrumentos_assinados,
	count(*) as quantidade_de_subunidades,

	sum(valor_repasse_uniao_instrumento) as valor_repasse_uniao_instrumento,
	sum(valor_repasse_solicitacao_proposta)    as valor_repasse_solicitacao_proposta,

	EXTRACT (YEAR FROM min(data_minima)) AS ano_ingresso_na_base,
	EXTRACT (MONTH FROM min(data_minima))AS mes_ingresso_na_base,
	EXTRACT (DAY FROM min(data_minima))  AS dia_ingresso_na_base,
	min(data_minima) as data_minima
from (
	select
		nm_proponente,
		natureza_juridica,
		-- Informações geográficas
		regiao_proponente,
		uf_proponente,
		municipio_proponente,
	
		-- Proposta mais antiga, aprox data de ingresso no siconv
		min(data_proposta) as data_minima,
		sum(dummy_emenda)  as quantidade_propostas_via_emenda,
		sum(dummy_instrumento_assinado) as quantidade_instrumentos_assinados,
		count(*) as num_propostas,
		
		-- Valor total repassado pela união
		sum(valor_repasse_uniao_instrumento) as valor_repasse_uniao_instrumento,
		sum(valor_repasse_uniao_proposta)    as valor_repasse_solicitacao_proposta
	from siconv_schema.tabelao_all
	where natureza_juridica = 'Administração Pública Municipal'
	group by nm_proponente, natureza_juridica, regiao_proponente, uf_proponente, municipio_proponente
) T
GROUP BY municipio_proponente, natureza_juridica, regiao_proponente, uf_proponente, municipio_proponente
union
--Estados
select 
	'ESTADO DE ' || uf_proponente as nm_proponente,
	natureza_juridica,
	regiao_proponente,
	uf_proponente,
	municipio_proponente,
	sum(num_propostas) as num_propostas,
	sum(quantidade_propostas_via_emenda) as quantidade_propostas_via_emenda,
	sum(quantidade_instrumentos_assinados) as quantidade_instrumentos_assinados,
	count(*) as quantidade_de_subunidades,

	sum(valor_repasse_uniao_instrumento) as valor_repasse_uniao_instrumento,
	sum(valor_repasse_solicitacao_proposta)    as valor_repasse_solicitacao_proposta,

	EXTRACT (YEAR FROM min(data_minima)) AS ano_ingresso_na_base,
	EXTRACT (MONTH FROM min(data_minima))AS mes_ingresso_na_base,
	EXTRACT (DAY FROM min(data_minima))  AS dia_ingresso_na_base,
	min(data_minima) as data_minima
from (
	select
		nm_proponente,
		natureza_juridica,
		-- Informações geográficas
		regiao_proponente,
		uf_proponente,
		''::text as municipio_proponente,
	
		-- Proposta mais antiga, aprox data de ingresso no siconv
		min(data_proposta) as data_minima,
		sum(dummy_instrumento_assinado) as quantidade_instrumentos_assinados,
		sum(dummy_emenda)  as quantidade_propostas_via_emenda,
		count(*) as num_propostas,

		-- Valor total repassado pela união
		sum(valor_repasse_uniao_instrumento) as valor_repasse_uniao_instrumento,
		sum(valor_repasse_uniao_proposta)    as valor_repasse_solicitacao_proposta
	from siconv_schema.tabelao_all
	where natureza_juridica = 'Administração Pública Estadual ou do Distrito Federal'
	group by nm_proponente, natureza_juridica, regiao_proponente, uf_proponente
) T
GROUP BY natureza_juridica, regiao_proponente, uf_proponente, municipio_proponente
union
-- O resto
select 
	nm_proponente,
	natureza_juridica,
	regiao_proponente,
	uf_proponente,
	municipio_proponente,
	sum(num_propostas) as num_propostas,
	sum(quantidade_propostas_via_emenda) as quantidade_propostas_via_emenda,
	sum(quantidade_instrumentos_assinados) as quantidade_instrumentos_assinados,
	count(*) as quantidade_de_subunidades,

	sum(valor_repasse_uniao_instrumento) as valor_repasse_uniao_instrumento,
	sum(valor_repasse_solicitacao_proposta)    as valor_repasse_solicitacao_proposta,

	EXTRACT (YEAR FROM min(data_minima)) AS ano_ingresso_na_base,
	EXTRACT (MONTH FROM min(data_minima))AS mes_ingresso_na_base,
	EXTRACT (DAY FROM min(data_minima))  AS dia_ingresso_na_base,
	min(data_minima) as data_minima
from (
	select
		nm_proponente,
		natureza_juridica,
		-- Informações geográficas
		regiao_proponente,
		uf_proponente,
		municipio_proponente,
	
		-- Proposta mais antiga, aprox data de ingresso no siconv_schema
		min(data_proposta) as data_minima,
		sum(dummy_emenda)  as quantidade_propostas_via_emenda,
		sum(dummy_instrumento_assinado) as quantidade_instrumentos_assinados,
		count(*) as num_propostas,

		-- Valor total repassado pela união
		sum(valor_repasse_uniao_instrumento) as valor_repasse_uniao_instrumento,
		sum(valor_repasse_uniao_proposta)    as valor_repasse_solicitacao_proposta
	from siconv_schema.tabelao_all
	where natureza_juridica in ('Empresa pública/Sociedade de economia mista', 'Organização da Sociedade Civil', 'Consórcio Público')
	group by nm_proponente, natureza_juridica, regiao_proponente, uf_proponente, municipio_proponente
) T
GROUP BY natureza_juridica, regiao_proponente, uf_proponente, municipio_proponente, nm_proponente
order by nm_proponente, municipio_proponente
