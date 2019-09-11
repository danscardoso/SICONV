DROP materialized VIEW IF EXISTS SICONV.tabelao cascade;
CREATE materialized VIEW SICONV.tabelao AS
select *
from SICONV.tabelao_all
where ano_proposta::integer >= 2013 and ano_proposta::integer <= 2018
