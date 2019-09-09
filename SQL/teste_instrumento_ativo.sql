select
    count(*) as contagem,
    siconv.convenio.sit_convenio              as situacao_convenio,
    siconv.convenio.instrumento_ativo         as instrumento_ativo
FROM siconv.convenio
group by situacao_convenio, instrumento_ativo
order by instrumento_ativow