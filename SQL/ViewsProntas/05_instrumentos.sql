DROP VIEW IF EXISTS siconv_schema.instrumentos cascade;
CREATE VIEW siconv_schema.instrumentos AS
select *
from siconv_schema.tabelao_all
where dummy_existe_convenio = 1 and dummy_instrumento_assinado = 1
	  and ano_proposta::integer >= 2013 and ano_proposta::integer <= 2018

