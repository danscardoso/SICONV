DROP TABLE siconv.informacoes_orcamentarias_camara;
CREATE TABLE siconv.informacoes_orcamentarias_camara( 
 	codigo_acao_orcamentaria varchar(4)
 	,nome_acao_orcamentaria text
	,funcao_orcamentaria varchar(30)

--	,unique (codigo_acao_orcamentaria)
 );

\copy siconv.informacoes_orcamentarias_camara ("codigo_acao_orcamentaria", "nome_acao_orcamentaria", "funcao_orcamentaria") FROM 'C:/Users/pedro.palotti/Documents/SICONV/DadosAux/funcao_orcamentaria_exportada.txt' ( FORMAT('csv'), DELIMITER(E'\t'), QUOTE(E'\"'), HEADER);

