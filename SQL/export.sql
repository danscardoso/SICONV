\copy (select * from siconv.instrumentos)  to 'C:/Users/pedro.palotti/Documents/SICONV/siconv_cgdad_v2.01_instrumentos.txt' WITH (FORMAT CSV, NULL '', HEADER TRUE, DELIMITER E'\t')
\copy (select * from siconv.solicitantes)  to 'C:/Users/pedro.palotti/Documents/SICONV/siconv_cgdad_v2.01_proponentes.txt' WITH (FORMAT CSV, NULL '', HEADER TRUE, DELIMITER E'\t')
\copy (select * from SICONV.tabelao)       to 'C:/Users/pedro.palotti/Documents/SICONV/siconv_cgdad_v2.01_propostas.txt' WITH (FORMAT CSV, NULL '', HEADER TRUE, DELIMITER E'\t')

\copy (select * from SICONV.acoes_orcamentarias)       to 'C:/Users/55619/Documents/SICONV/funcoes_orcamentarias.txt' WITH (FORMAT CSV, NULL '', HEADER TRUE, DELIMITER E'\t')
