--drop schema siconv cascade
create schema siconv;

create table siconv.consorcios (
  id_proposta text,
  cnpj_consorcio text,
  nome_consorcio text,
  codigo_cnae_primario text,
  desc_cnae_primario text,
  codigo_cnae_secundario text,
  desc_cnae_secundario text,
  cnpj_participante text,
  nome_participante text
);

create table siconv.convenio (
  nr_convenio text,
  id_proposta text,
  dia text,
  mes text,
  ano text,
  dia_assin_conv text,
  sit_convenio text,
  subsituacao_conv text,
  situacao_publicacao text,
  instrumento_ativo text,
  ind_opera_obtv text,
  nr_processo text,
  ug_emitente text,
  dia_publ_conv text,
  dia_inic_vigenc_conv text,
  dia_fim_vigenc_conv text,
  dias_prest_contas text,
  dia_limite_prest_contas text,
  situacao_contratacao text,
  ind_assinado text,
  qtde_convenios text,
  qtd_ta text,
  qtd_prorroga text,
  vl_global_conv text,
  vl_repasse_conv text,
  vl_contrapartida_conv text,
  vl_empenhado_conv text,
  vl_desembolsado_conv text,
  vl_saldo_reman_tesouro text,
  vl_saldo_reman_convenente text,
  vl_rendimento_aplicacao text,
  vl_ingresso_contrapartida text,
  vl_saldo_conta text
);

create table siconv.desbloqueio (
  nr_convenio text,
  nr_ob text,
  data_cadastro text,
  data_envio text,
  tipo_recurso_desbloqueio text,
  vl_total_desbloqueio text,
  vl_desbloqueado text,
  vl_bloqueado text
);

create table siconv.desembolso (
  id_desembolso text,
  nr_convenio text,
  dt_ult_desembolso text,
  qtd_dias_sem_desembolso text,
  data_desembolso text,
  ano_desembolso text,
  mes_desembolso text,
  nr_siafi text,
  vl_desembolsado text
);

create table siconv.emenda (
  id_proposta text,
  qualif_proponente text,
  cod_programa_emenda text,
  nr_emenda text,
  nome_parlamentar text,
  beneficiario_emenda text,
  ind_impositivo text,
  tipo_parlamentar text,
  valor_repasse_proposta_emenda text,
  valor_repasse_emenda text
);

create table siconv.empenho (
  id_empenho text,
  nr_convenio text,
  nr_empenho text,
  tipo_nota text,
  desc_tipo_nota text,
  data_emissao text,
  cod_situacao_empenho text,
  desc_situacao_empenho text,
  valor_empenho text
);

create table siconv.empenho_desembolso (
  id_desembolso text,
  id_empenho text,
  valor_grupo text
);

create table siconv.etapa_crono_fisico (
  id_meta text,
  id_etapa text,
  nr_etapa text,
  desc_etapa text,
  data_inicio_etapa text,
  data_fim_etapa text,
  uf_etapa text,
  municipio_etapa text,
  endereco_etapa text,
  cep_etapa text,
  qtd_etapa text,
  und_fornecimento_etapa text,
  vl_etapa text
);

create table siconv.historico_situacao (
  id_proposta text,
  nr_convenio text,
  dia_historico_sit text,
  historico_sit text,
  dias_historico_sit text,
  cod_historico_sit text
);

create table siconv.ingresso_contrapartida (
  nr_convenio text,
  dt_ingresso_contrapartida text,
  vl_ingresso_contrapartida text
);

create table siconv.meta_crono_fisico (
  id_meta text,
  nr_convenio text,
  cod_programa text,
  nome_programa text,
  nr_meta text,
  tipo_meta text,
  desc_meta text,
  data_inicio_meta text,
  data_fim_meta text,
  uf_meta text,
  municipio_meta text,
  endereco_meta text,
  cep_meta text,
  qtd_meta text,
  und_fornecimento_meta text,
  vl_meta text
);

create table siconv.obtv_convenente (
  nr_mov_fin text,
  identif_favorecido_obtv_conv text,
  nm_favorecido_obtv_conv text,
  tp_aquisicao text,
  vl_pago_obtv_conv text
);

create table siconv.pagamento (
  nr_mov_fin text,
  nr_convenio text,
  identif_fornecedor text,
  nome_fornecedor text,
  tp_mov_financeira text,
  data_pag text,
  nr_dl text,
  desc_dl text,
  vl_pago text
);

create table siconv.plano_aplicacao_detalhado (
  id_proposta text,
  sigla text,
  municipio text,
  natureza_aquisicao text,
  descricao_item text,
  cep_item text,
  endereco_item text,
  tipo_despesa_item text,
  natureza_despesa text,
  sit_item text,
  qtd_item text,
  valor_unitario_item text,
  valor_total_item text
);

create table siconv.programa (
  cod_orgao_sup_programa text,
  desc_orgao_sup_programa text,
  id_programa text,
  cod_programa text,
  nome_programa text,
  sit_programa text,
  data_disponibilizacao text,
  ano_disponibilizacao text,
  dt_prog_ini_receb_prop text,
  dt_prog_fim_receb_prop text,
  dt_prog_ini_emenda_par text,
  dt_prog_fim_emenda_par text,
  dt_prog_ini_benef_esp text,
  dt_prog_fim_benef_esp text,
  modalidade_programa text,
  natureza_juridica_programa text,
  uf_programa text,
  acao_orcamentaria text
);

create table siconv.programa_proposta (
  id_programa text,
  id_proposta text
);

create table siconv.proponentes (
  identif_proponente text,
  municipio_proponente text,
  uf_proponente text,
  endereco_proponente text,
  bairro_proponente text,
  cep_proponente text,
  email_proponente text,
  telefone_proponente text,
  fax_proponente text,
  nm_proponente text
);

create table siconv.proposta (
  id_proposta text,
  uf_proponente text,
  munic_proponente text,
  cod_munic_ibge text,
  cod_orgao_sup text,
  desc_orgao_sup text,
  natureza_juridica text,
  nr_proposta text,
  dia_prop text,
  mes_prop text,
  ano_prop text,
  dia_proposta text,
  cod_orgao text,
  desc_orgao text,
  modalidade text,
  identif_proponente text,
  nm_proponente text,
  endereco_proponente text,
  bairro_proponente text,
  nm_banco text,
  situacao_conta text,
  situacao_projeto_basico text,
  sit_proposta text,
  dia_inic_vigencia_proposta text,
  dia_fim_vigencia_proposta text,
  objeto_proposta text,
  vl_global_prop text,
  vl_repasse_prop text,
  vl_contrapartida_prop text,
  cep_proponente text
);

create table siconv.prorroga_oficio (
  nr_convenio text,
  nr_prorroga text,
  dt_inicio_prorroga text,
  dt_fim_prorroga text,
  dias_prorroga text,
  dt_assinatura_prorroga text,
  sit_prorroga text
);

create table siconv.termo_aditivo (
  nr_convenio text,
  numero_ta text,
  tipo_ta text,
  vl_global_ta text,
  vl_repasse_ta text,
  vl_contrapartida_ta text,
  dt_assinatura_ta text,
  dt_inicio_ta text,
  dt_fim_ta text,
  justificativa_ta text
);


-- alter table siconv.tabela_1 add foreign key (id_proposta) references siconv.tabela_18 (id_proposta);
-- alter table siconv.tabela_2 add foreign key (id_proposta) references siconv.tabela_18 (id_proposta);
-- alter table siconv.tabela_5 add foreign key (id_proposta) references siconv.tabela_18 (id_proposta);
-- alter table siconv.tabela_9 add foreign key (id_proposta) references siconv.tabela_18 (id_proposta);
-- alter table siconv.tabela_14 add foreign key (id_proposta) references siconv.tabela_18 (id_proposta);
-- alter table siconv.tabela_16 add foreign key (id_proposta) references siconv.tabela_18 (id_proposta);
-- alter table siconv.tabela_3 add foreign key (nr_convenio) references siconv.tabela_2 (nr_convenio);
-- alter table siconv.tabela_4 add foreign key (nr_convenio) references siconv.tabela_2 (nr_convenio);
-- alter table siconv.tabela_6 add foreign key (nr_convenio) references siconv.tabela_2 (nr_convenio);
-- alter table siconv.tabela_9 add foreign key (nr_convenio) references siconv.tabela_2 (nr_convenio);
-- alter table siconv.tabela_10 add foreign key (nr_convenio) references siconv.tabela_2 (nr_convenio);
-- alter table siconv.tabela_11 add foreign key (nr_convenio) references siconv.tabela_2 (nr_convenio);
-- alter table siconv.tabela_13 add foreign key (nr_convenio) references siconv.tabela_2 (nr_convenio);
-- alter table siconv.tabela_19 add foreign key (nr_convenio) references siconv.tabela_2 (nr_convenio);
-- alter table siconv.tabela_20 add foreign key (nr_convenio) references siconv.tabela_2 (nr_convenio);
-- alter table siconv.tabela_13 add foreign key (nr_mov_fin) references siconv.tabela_12 (nr_mov_fin);
-- alter table siconv.tabela_17 add foreign key (uf_proponente) references siconv.tabela_18 (uf_proponente);
-- alter table siconv.tabela_11 add foreign key (nome_programa) references siconv.tabela_15 (nome_programa);
-- alter table siconv.tabela_17 add foreign key (nm_proponente) references siconv.tabela_18 (nm_proponente);
-- alter table siconv.tabela_15 add foreign key (id_programa) references siconv.tabela_16 (id_programa);
-- alter table siconv.tabela_8 add foreign key (id_meta) references siconv.tabela_11 (id_meta);
-- alter table siconv.tabela_7 add foreign key (id_empenho) references siconv.tabela_6 (id_empenho);