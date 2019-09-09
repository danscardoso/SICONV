-------------------------------------------------------------------------------
-- TESTE DE ANO
select
    right(dia_proposta, 4)    as ano,
    sum(valor_repasse_uniao)  as valor_repasse_uniao
from SICONV.tabelao
GROUP BY right(dia_proposta, 4)
order by ano

-------------------------------------------------------------------------------


select ind_assinado, sum(vl_repasse_conv as numeric), count(*)
from siconv.convenio
group by ind_assinado

select *
from siconv.convenio
limit 50
