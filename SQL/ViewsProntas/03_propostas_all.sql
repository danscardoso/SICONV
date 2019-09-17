DROP MATERIALIZED VIEW IF EXISTS SICONV.tabelao_all cascade;
CREATE MATERIALIZED VIEW SICONV.tabelao_all AS
select
    -- Identificadores
    siconv.proposta.nr_proposta                 as id_proposta_ano,
    siconv.proposta.id_proposta                 as id_proposta,
    siconv.convenio.nr_convenio                 as id_convenio,
   

    -- Descrição do objeto
    siconv.proposta.objeto_proposta             as descricao_proposta,
    siconv.vi_programas_unicos.nome_programa          as nome_programa,
    siconv.vi_programas_unicos.acao_orcamentaria      as acao_orcamentaria,
    -- TODO: adicionar aqui informações de categorização a serem feitas pelo Gerson depois
    -- de homologada a categorização dele dos objetos comprados


    -- Quando?
    siconv.proposta.ano_prop     as ano_proposta,
    to_date(siconv.proposta.dia_proposta, 'DD/MM/YYYY') as data_proposta,

    extract (YEAR FROM to_date(siconv.convenio.dia_inic_vigenc_conv,'DD/MM/YYYY') ) as ano_inicio_vigencia_convenio,
    extract (YEAR FROM to_date(siconv.convenio.dia_fim_vigenc_conv,'DD/MM/YYYY')  ) as ano_final_vigencia_convenio,

    to_date(siconv.convenio.dia_fim_vigenc_conv,'DD/MM/YYYY') - to_date(siconv.convenio.dia_inic_vigenc_conv,'DD/MM/YYYY') as intervalo_vigencia_dias,

    to_date(siconv.convenio.dia_assin_conv,'DD/MM/YYYY')              as data_assinatura_convenio,
    to_date(siconv.convenio.dia_publ_conv,'DD/MM/YYYY')               as data_publicacao_convenio,
    to_date(siconv.convenio.dia_limite_prest_contas,'DD/MM/YYYY')     as data_limite_prestacao_contas,
    
    -- Como anda?
    -- Grupo situacao_proposta
    -- Observar que aprovada não significa que de fato foi assinado um instrumento
    CASE WHEN proposta.sit_proposta = 'Proposta/Plano de Trabalho Aprovados' THEN 'Aprovada'
        WHEN proposta.sit_proposta ~ 'Rejeita' or proposta.sit_proposta ~ 'Eliminada'    THEN 'Rejeitada'
        ELSE 'Em processo' END AS situacao_proposta_grupo,
    case when siconv.convenio.nr_convenio is not null then 1 else 0 end as dummy_existe_convenio,
    
    CASE WHEN convenio.ind_assinado = 'SIM' THEN 1
         ELSE 0 END AS dummy_instrumento_assinado,

    siconv.proposta.sit_proposta         as situacao_proposta,
    siconv.proposta.modalidade           as modalidade,
    siconv.convenio.situacao_contratacao as situacao_contratacao,
    case when siconv.convenio.instrumento_ativo = 'NÃO' then 0
         when siconv.convenio.instrumento_ativo = 'SIM' then 1 end as dummy_instrumento_ativo,

    case
        when convenio.sit_convenio='Proposta/Plano de Trabalho Aprovado' or 
           convenio.sit_convenio='Proposta/Plano de Trabalho Complementado em Análise' or 
           convenio.sit_convenio='Proposta/Plano de Trabalho Complementado Enviado para Análise' or
           convenio.sit_convenio='Em execução' then 'Em execucão'
            
        when convenio.sit_convenio='Prestação de Contas Rejeitada' then 'Prestação de Contas Rejeitada'
            
        when convenio.sit_convenio='Cancelado' or
             convenio.sit_convenio='Convênio Anulado' or
             convenio.sit_convenio='Convênio Rescindido' then 'Convênio Cancelado'

        when convenio.sit_convenio='Prestação de Contas Aprovada' or
             convenio.sit_convenio='Prestação de Contas Concluída' or
             convenio.sit_convenio='Prestação de Contas Aprovada com Ressalvas' or
             convenio.sit_convenio='Prestação de Contas Comprovada em Análise' then 'Prestação de Contas Aprovada'

        when convenio.sit_convenio='Aguardando Prestação de Contas' or
             convenio.sit_convenio='Assinatura Pendente Registro TV Siafi' or
             convenio.sit_convenio='Inadimplente' or
             convenio.sit_convenio='Prestação de Contas em Análise' or 
             convenio.sit_convenio='Prestação de Contas em Complementação' or 
             convenio.sit_convenio='Prestação de Contas enviada para Análise' or 
             convenio.sit_convenio='Prestação de Contas Iniciada Por Antecipação'
              then 'Em prestação de contas'
        end as situacao_instrumento,

    case when siconv.convenio.sit_convenio = 'Em execução' then 'Em execução'
         when siconv.convenio.sit_convenio = 'Cancelado' or
              siconv.convenio.sit_convenio = 'Convênio Anulado' or
              siconv.convenio.sit_convenio = 'Convênio Rescindido' or
              siconv.convenio.sit_convenio = 'Prestação de Contas Aprovada' or
              siconv.convenio.sit_convenio = 'Prestação de Contas Aprovada com Ressalvas' or
              siconv.convenio.sit_convenio = 'Prestação de Contas Concluída' or
              siconv.convenio.sit_convenio = 'Prestação de Contas Rejeitada' then 'Finalizado'
         when siconv.convenio.sit_convenio IS NULL then NULL
         else 'Em prestação de contas/análise' end as dummy_instrumento_em_execucao,

    siconv.convenio.subsituacao_conv     as subsituacao_instrumento,
    siconv.convenio.ind_opera_obtv       as indicador_obtv,             -- Ordem Bancária de Transferências Voluntárias
    

    T.data_inicio_execucao,
    T.ano_inicio_execucao,
    T1.data_saida_execucao,
    T1.ano_saida_execucao,
    T2.data_decisao_proposta,
    T2.ano_decisao_proposta,

    -- Quanto foi solicitado?
    round(replace(siconv.proposta.vl_global_prop, ',','.')::numeric,0) as valor_total_proposta,
    round(replace(siconv.proposta.vl_repasse_prop, ',','.')::numeric,0) as valor_repasse_uniao_proposta,
    round(replace(siconv.proposta.vl_contrapartida_prop, ',','.')::numeric,0) as valor_contrapartida_proposta,
    

    -- Quanto foi efetivamente acordado?
    round(replace(siconv.convenio.vl_global_conv, ',','.')::numeric,0) as valor_total_instrumento,
    round(replace(siconv.convenio.vl_repasse_conv, ',','.')::numeric,0) as valor_repasse_uniao_instrumento,
    round(replace(siconv.convenio.vl_contrapartida_conv, ',','.')::numeric,0) as valor_contrapartida_instrumento,
    round(replace(siconv.convenio.VL_EMPENHADO_CONV, ',','.')::numeric,0) as valor_empenhado_instrumento,
    round(replace(siconv.convenio.VL_DESEMBOLSADO_CONV, ',','.')::numeric,0) as valor_desembolsado_instrumento,
    round(replace(siconv.convenio.VL_SALDO_REMAN_TESOURO, ',','.')::numeric,0) as saldo_remanescente_tesouro_instrumento,
    round(replace(siconv.convenio.VL_SALDO_REMAN_CONVENENTE, ',','.')::numeric,0) as saldo_remanescente_convenente_instrumento,
    round(replace(siconv.convenio.VL_RENDIMENTO_APLICACAO, ',','.')::numeric,0) as valor_rendimento_aplicacao_instrumento,
    round(replace(siconv.convenio.VL_INGRESSO_CONTRAPARTIDA, ',','.')::numeric,0) as valor_ingresso_contrapartida_instrumento,
    round(replace(siconv.convenio.VL_SALDO_CONTA, ',','.')::numeric,0) as valor_saldo_instrumento,
    round(T3.valor_executado,0) as valor_executado,

    -- prorrogações
    siconv.convenio.qtd_ta               as quantidade_termos_aditivos,
    siconv.convenio.qtd_prorroga         as quantidade_prorrogacoes,

    case when replace(siconv.convenio.vl_global_original_conv, ',','.')::numeric > 0
            then round(replace(siconv.convenio.vl_global_original_conv, ',','.')::numeric,0)
            else round(replace(siconv.convenio.vl_global_conv, ',','.')::numeric,0) end as valor_total_original_instrumento,
    to_date(siconv.convenio.dia_fim_vigenc_original_conv,'DD/MM/YYYY') as data_final_original,
    to_date(siconv.convenio.dia_fim_vigenc_original_conv,'DD/MM/YYYY') - to_date(siconv.convenio.dia_inic_vigenc_conv,'DD/MM/YYYY') as intervalo_original,

    -- Via emenda?
    case when emenda_proposta.ind_emenda_impositiva = 'SIM' or 
              emenda_proposta.ind_emenda_impositiva = 'NÃO' then 1 else 0 end as dummy_emenda,
    emenda_proposta.valor_repasse_proposta_emenda as valor_repasse_proposta_emenda,
    emenda_proposta.valor_repasse_emenda          as valor_repasse_emenda,
    emenda_proposta.ind_emenda_impositiva         as emenda_impositiva,
    emenda_proposta.tipo_parlamentar,

    -- Lado não federal do convenio?
    case when siconv.proposta.uf_proponente in ('AC','AM','AP','PA','RO','RR','TO')           then 'Norte'
         when siconv.proposta.uf_proponente in ('AL','BA','CE','MA','PB','PE','PI','RN','SE') then 'Nordeste'
         when siconv.proposta.uf_proponente in ('DF','GO','MS','MT')                          then 'Centro-Oeste'
         when siconv.proposta.uf_proponente in ('ES','MG','RJ','SP')                          then 'Sudeste'
         when siconv.proposta.uf_proponente in ('PR','RS','SC')                               then 'Sul'
         else NULL end as regiao_proponente,

    siconv.proposta.uf_proponente               as uf_proponente,
    siconv.proposta.munic_proponente            as municipio_proponente,
    siconv.proposta.nm_proponente               as nm_proponente,
    siconv.proposta.natureza_juridica           as natureza_juridica,

    -- Lado federal do convenio
    siconv.proposta.desc_orgao_sup              as orgao_sup,
    siconv.proposta.desc_orgao                  as orgao

FROM siconv.proposta
full join siconv.convenio on proposta.id_proposta = convenio.id_proposta
left join siconv.emenda_proposta on proposta.id_proposta = emenda_proposta.id_proposta
left join siconv.programa_proposta_uni on programa_proposta_uni.id_proposta = proposta.id_proposta
left join siconv.vi_programas_unicos on vi_programas_unicos.id_programa::integer = programa_proposta_uni.id_programa
left join (
    select id_proposta,
                       min( to_timestamp(dia_historico_sit, 'DD/MM/YYYY hh24:mi:ss')) as data_inicio_execucao,
    extract (YEAR FROM min( to_timestamp(dia_historico_sit, 'DD/MM/YYYY hh24:mi:ss'))) as ano_inicio_execucao
    FROM siconv.historico_situacao
    where historico_sit in ('EM_EXECUCAO', 'ASSINADA')
    GROUP BY  id_proposta ) T on T.id_proposta = proposta.id_proposta

left join (
    select id_proposta,
                       min( to_timestamp(dia_historico_sit, 'DD/MM/YYYY hh24:mi:ss')) as data_saida_execucao,
    extract (YEAR FROM min( to_timestamp(dia_historico_sit, 'DD/MM/YYYY hh24:mi:ss'))) as ano_saida_execucao
  FROM siconv.historico_situacao
  where historico_sit in ('CONVENIO_ANULADO', 'CONVENIO_RESCINDIDO', 'PRESTACAO_CONTAS_APROVADA',
  'PRESTACAO_CONTAS_APROVADA_COM_RESSALVAS')
  GROUP BY id_proposta ) T1 on T1.id_proposta = proposta.id_proposta

left join (
    select id_proposta, 
                       min( to_timestamp(dia_historico_sit, 'DD/MM/YYYY hh24:mi:ss')) as data_decisao_proposta,
    extract (YEAR FROM min( to_timestamp(dia_historico_sit, 'DD/MM/YYYY hh24:mi:ss'))) as ano_decisao_proposta
  FROM siconv.historico_situacao
  where historico_sit in ('PROPOSTA_REPROVADA','PROPOSTA_REJEITADA_IMPEDIMENTO_TECNICO',
    'PROPOSTA_ELIMINADA_EM_CHAMAMENTO_PUBLICO','ASSINADA','EM_EXECUCAO')
  GROUP BY id_proposta ) T2 on T2.id_proposta = proposta.id_proposta

left join (
  select nr_convenio, sum(replace(vl_pago, ',','.')::numeric) as valor_executado
  from siconv.pagamento
  GROUP BY nr_convenio 
) T3 on T3.nr_convenio = convenio.nr_convenio

