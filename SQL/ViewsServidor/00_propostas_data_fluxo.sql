DROP MATERIALIZED VIEW IF EXISTS SICONV.proposta_datas_fluxo cascade;
CREATE MATERIALIZED VIEW SICONV.proposta_datas_fluxo AS
select
	siconv.proposta.id_proposta,
	siconv.proposta.dia_proposta,
	siconv.proposta.ano_prop,

	T.data_inicio_execucao,
	T.ano_inicio_execucao,
	T1.data_saida_execucao,
	T1.ano_saida_execucao,
	T2.data_decisao_proposta,
	T2.ano_decisao_proposta

from siconv.proposta
left join (
    select id_proposta,
                       min( to_timestamp(dia_historico_sit, 'DD/MM/YYYY hh24:mi:ss')) as data_inicio_execucao,
    extract (YEAR FROM min( to_timestamp(dia_historico_sit, 'DD/MM/YYYY hh24:mi:ss'))) as ano_inicio_execucao
    FROM siconv.historico_situacao
    where historico_sit = 'EM_EXECUCAO' or historico_sit = 'ASSINADA'
    GROUP BY  id_proposta ) T on T.id_proposta = proposta.id_proposta
left join (
    select id_proposta,
                       min( to_timestamp(dia_historico_sit, 'DD/MM/YYYY hh24:mi:ss')) as data_saida_execucao,
    extract (YEAR FROM min( to_timestamp(dia_historico_sit, 'DD/MM/YYYY hh24:mi:ss'))) as ano_saida_execucao
  FROM siconv.historico_situacao
  where historico_sit = 'CONVENIO_ANULADO' or historico_sit = 'CONVENIO_RESCINDIDO' or 
      historico_sit = 'PRESTACAO_CONTAS_APROVADA' or historico_sit = 'PRESTACAO_CONTAS_APROVADA_COM_RESSALVAS'
  GROUP BY id_proposta ) T1 on T1.id_proposta = proposta.id_proposta
left join (
    select id_proposta, 
                       min( to_timestamp(dia_historico_sit, 'DD/MM/YYYY hh24:mi:ss')) as data_decisao_proposta,
    extract (YEAR FROM min( to_timestamp(dia_historico_sit, 'DD/MM/YYYY hh24:mi:ss'))) as ano_decisao_proposta
  FROM siconv.historico_situacao
  where historico_sit = 'PROPOSTA_REPROVADA' or historico_sit = 'PROPOSTA_REJEITADA_IMPEDIMENTO_TECNICO' or 
      historico_sit = 'PROPOSTA_ELIMINADA_EM_CHAMAMENTO_PUBLICO' or historico_sit = 'ASSINADA' or historico_sit = 'EM_EXECUCAO'
  GROUP BY id_proposta ) T2 on T2.id_proposta = proposta.id_proposta