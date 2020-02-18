drop view siconv.acoes_orcamentarias;
create view siconv.acoes_orcamentarias AS
select distinct
--	id_proposta, 
	nome_programa,
	acao_orcamentaria,
	descricao_proposta,
	left(acao_orcamentaria,4),
	right(acao_orcamentaria,4),
	valor_repasse_uniao_instrumento,

	T.nome_acao_orcamentaria as primeiro_chute_acao_orc,
	T1.nome_acao_orcamentaria as segundo_chute_acao_orc,

	--  Vou preferir o segundo chute, à menos que ele não dê match
	--    -> Posteriormente, poderia ter regras melhores para casos em particular
	case when T1.nome_acao_orcamentaria IS NULL then T.nome_acao_orcamentaria
	     else T1.nome_acao_orcamentaria end as chute_principal

from siconv.instrumentos
LEFT JOIN ( select * from siconv.informacoes_orcamentarias_camara ) T  on left(acao_orcamentaria,4)  = T.codigo_acao_orcamentaria
LEFT JOIN ( select * from siconv.informacoes_orcamentarias_camara ) T1 on right(acao_orcamentaria,4) = T1.codigo_acao_orcamentaria

