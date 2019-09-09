-------------------------------------------------------------------------------
---- DISTRIBUIÇÃO DA NATUREZA JURÍDICA
-------------------------------------------------------------------------------
select
	count(*),
	natureza_juridica,
	round((count(*) / ( sum(count(*)) over()  )) * 100, 2) as perc
from SICONV.PROPOSTAS
group by natureza_juridica
order by perc desc

-------------------------------------------------------------------------------
---- DISTRIBUIÇÃO POR ORGAO
-------------------------------------------------------------------------------
select
	orgao_sup,
	count(*) as contagem,
	round((count(*) / ( sum(count(*)) over()  )) * 100, 2) as perc_contagem,
	sum( valor_total ) as valor_total
	--,round( (sum(valor_total)/(sum(valor_total) over() ) * 100), 2) as perc_valor_total
from SICONV.PROPOSTAS
group by orgao_sup
order by perc_contagem desc

-------------------------------------------------------------------------------
---- DISTRIBUIÇÃO POR MODALIDADE
-------------------------------------------------------------------------------
select
	T.modalidade,
	T.contagem,
	T.perc_contagem,
	T.valor_total_categoria,
	round( (sum(valor_total_categoria)/(sum(valor_total_categoria) over() ) * 100), 2) as perc_valor_total
from 
(
	select
		modalidade,
		count(*) as contagem,
		round((count(*) / ( sum(count(*)) over()  )) * 100, 2) as perc_contagem,
		sum( valor_total ) as valor_total_categoria
	from SICONV.PROPOSTAS
	group by modalidade
) T
group by T.valor_total_categoria, T.modalidade, T.contagem, T.perc_contagem
order by perc_contagem desc


-------------------------------------------------------------------------------
-- SITUAÇÃO PROPOSTA
-------------------------------------------------------------------------------

SELECT situacao_proposta, situacao_convenio, situacao_publicacao, count(*)
from SICONV.PROPOSTAS
where situacao_publicacao != ''
group by situacao_proposta, situacao_convenio, situacao_publicacao
order by 3 desc

-------------------------------------------------------------------------------
---- TESTE DOS VALORES DISPONIVEIS
-------------------------------------------------------------------------------
select id_proposta,id_convenio,id_programa,id_emenda,dia_proposta,dia_inicio_vigencia_proposta,dia_fim_vigencia_proposta,dia_assinatura_convenio,dia_publicacao_convenio,dia_inicio_vigencia_convenio,dia_final_vigencia_convenio,dia_limite_prestacao_contas,modalidade,situacao_proposta,situacao_contratacao,convenio_ativo,situacao_convenio,subsituacao_convenio,situacao_publicacao,indicador_obtv,quantidade_termos_aditivos,quantidade_prorrogacoes,valor_total,valor_repasse_uniao,valor_contrapartida,valor_empenhado_conv,valor_desembolsado_conv,saldo_remanescente_tesouro_conv,saldo_remanescente_convenente_conv,valor_rendimento_aplicacao_conv,valor_ingresso_contrapartida_conv,valor_saldo_conv,valor_repasse_emenda,dummy_emenda_parlamentar,parlamentar,beneficiario_emenda,emenda_impositiva,tipo_parlamentar,uf_proponente,municipio_proponente,nm_proponente,orgao_sup,orgao,natureza_juridica
from SICONV.PROPOSTAS
where modalidade='CONTRATO DE REPASSE'
limit 1000
