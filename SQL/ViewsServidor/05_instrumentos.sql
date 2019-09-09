DROP VIEW IF EXISTS siconv.instrumentos cascade;
CREATE VIEW siconv.instrumentos AS
select *
from siconv.tabelao
where dummy_existe_convenio = 1 and dummy_instrumento_assinado = 1

