DROP materialized VIEW IF EXISTS siconv_schema.tabelao cascade;
CREATE materialized VIEW siconv_schema.tabelao AS
select *
from siconv_schema.tabelao_all
where ano_proposta::integer >= 2013 and ano_proposta::integer <= 2018
