import psycopg2                                          # conexão com o banco de dados
import requests                                          # download de um recurso via URL
import zipfile                                           # para fazer o unzip do arquivo baixado
from time import ctime                                   # para a pasta temporaria, log na CLI e adicionar informação da data da atualização
from os import mkdir, listdir,remove,rmdir,path,getcwd   # funcoes para manipulação de arquivos

############################################################################################
# Script que tenta fazer uma conexão com o banco com uma dada linha de conexão
#
# A linha de conexão é algo parecido com:
# " host='localhost' dbname='db' user='admin' password='admin'"
# Se der algum pau, retorne 1
def testar_conexao(conn_string):
    try:
        conn = psycopg2.connect(conn_string)    
        conn.close()
        return 0
    except Exception as e:
        print (e)
        return 1

############################################################################################
# Cria uma pasta vazia e caso já exista uma pasta com o nome, esvazia a mesma
def criar_pasta_vazia ( nome_pasta ):

	# Se a pasta existir, esvaziar-se-lhe-á a mesma
	if path.exists(nome_pasta):
	    for file in listdir(nome_pasta):
	        remove(nome_pasta + "/" + file)
	else:
	    mkdir(nome_pasta)

############################################################################################
# Apaga uma pasta e seu conteudo
def remove_pasta_conteudo (nome_pasta):
    for file in listdir(nome_pasta):
        remove(nome_pasta + "/" + file)
    rmdir(nome_pasta)


############################################################################################
# Baixar arquivo dada uma URL
def baixar_arquivo (url, nome_arquivo):
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(nome_arquivo, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192): 
                if chunk: # filter out keep-alive new chunks
                    _ = f.write(chunk)

############################################################################################
# Descompactar
def descompactar(arquivo_in, pasta_out):
    with zipfile.ZipFile(arquivo_in,"r") as zip_ref:
        zip_ref.extractall(pasta_out + "/")

############################################################################################
# Descobre se o texto é uma query vazia ou não
# Retorna 1 se for fazia e 0 se não for
def query_vazia (texto_query):

    # quebro a query completa em linhas menores
    sublinhas = texto_query.split("\n")

    # A ideia eh para cada linha ver se faz sentido ser uma query. Se fizer sentido, retorna 0
    # Se passar por todas as linhas e nada parecer uma query, retorna que é vazia
    for linha in sublinhas:
        if linha.strip() == "":         # linha vazia
            continue
        elif len(linha.strip()) > 2 and linha.strip()[0:2] == "--": # Linha de comentario
            continue
        return 0
    return 1

############################################################################################
# forma #1 de atualizar, hard reset (derruba o schema e recria vazio)
def hard_reset( arquivo_schema, novo_schema, connection_string ):

    # Conectando com o banco
    conn = psycopg2.connect(connection_string)
    cur = conn.cursor()

    # Abro o arquivo que define o schema da tabela, leio ele todo,
    # quebro em varios comandos e executo um a um. O primeiro deles é o
    # que derruba o schema
    with open(arquivo_schema) as arq_schema:
        comandos = arq_schema.read()
        comandos = comandos.upper().replace("SICONV_SCHEMA", novo_schema )
        lista_comandos = comandos.split(";")

        try:
            for comando in lista_comandos:
                # A cláusula aqui é para o python não executar uma query que esteja comentada
                # ou vazia (p.e. depois do último ; pode ter um enter sobrando)
                if not query_vazia(comando):
                    cur.execute(comando)
        except Exception as e:
            print (f"estava executando a query\033[92m{comando}'\033[0m")
            print (f"Excessão: {e}")
            exit(1)

    # Saindo e fechando tudo
    conn.commit()
    cur.close()
    conn.close()

############################################################################################
# forma #2 de atualizar, soft reset (trunca as tabelas do schema)
def soft_reset( novo_schema, connection_string ):

    # Conectando com o banco
    conn = psycopg2.connect(connection_string)
    cur = conn.cursor()

    # Busca as tabelas que já existem no schema
    comando = f"select tablename from pg_tables where schemaname='{novo_schema}'"
    cur.execute(comando)
    tabelas = cur.fetchall()

    try:
        for tabela in tabelas:
            # Só não trunca a tabela de configuração, o resto pode truncar
            if tabela[0] != 'configuracao':
                comando = f"TRUNCATE TABLE {novo_schema}.{tabela[0]}"
                cur.execute(comando)
    except Exception as e:
        print (f"estava executando a query: \033[92m{comando}'\033[0m")
        print (f"Excessão: {e}")
        exit(1)

    # Saindo e fechando tudo
    conn.commit()
    cur.close()
    conn.close()

############################################################################################
# copiando os dados para as devidas tabelas
def insert_data (schema, conn_string, pasta_arquivos):
    
    # Conectando com o banco
    conn = psycopg2.connect(conn_string)
    cur = conn.cursor()

    tupla_tabela_arquivo = []
    tupla_tabela_arquivo.append( ('convenio',          'siconv_convenio.csv'))
    tupla_tabela_arquivo.append( ('emenda',            'siconv_emenda.csv'))
    tupla_tabela_arquivo.append( ('historico_situacao','siconv_historico_situacao.csv'))
    tupla_tabela_arquivo.append( ('pagamento',         'siconv_pagamento.csv'))
    tupla_tabela_arquivo.append( ('programa',          'siconv_programa.csv'))
    tupla_tabela_arquivo.append( ('programa_proposta', 'siconv_programa_proposta.csv'))
    tupla_tabela_arquivo.append( ('proposta',          'siconv_proposta.csv'))

    try:
        # Adicionando as informações baixadas
        for (tabela, arquivo) in tupla_tabela_arquivo:

            comando = f'COPY {schema}.{tabela} FROM STDIN ( FORMAT(\'csv\'), DELIMITER(\';\'), QUOTE(E\'\"\'), HEADER)'
            endereco = f'{pasta_arquivos}/{arquivo}'

            print (f"{ctime()} copiando os dados para a tabela: {tabela}")
            cur.copy_expert(comando, open(endereco))
            conn.commit()
        
        # Agora vou adicionar as informações exógenas relevantes
        #  -> funcao orcamentaria (vinda do Acess da Câmara dos Deputados)
        comando = f'COPY {schema}.informacoes_orcamentarias_camara FROM STDIN ( FORMAT(\'csv\'), DELIMITER(E\'\t\'), QUOTE(E\'\"\'), HEADER)'
        arquivo = getcwd() + "/DadosAux/funcao_orcamentaria_exportada_ordenada.txt"
        cur.copy_expert( comando, open(arquivo))
        conn.commit()

    except Exception as e:
        print (f"estava executando a query: \033[92m{comando}'\033[0m")
        print (f"estava tentando copiar o arquivo: \033[92m{arquivo}'\033[0m")
        print (f"Excessão: {e}")
        exit(1)

    # Saindo e fechando tudo
    cur.close()
    conn.close()

############################################################################################
# Criando todas as views em determinada pasta
# Apenas executa todos os arquivos .sql em uma determinada pasta
def criar_views (pasta_views, schema, conn_string):

    # Conectando com o banco
    conn = psycopg2.connect(conn_string)
    cur = conn.cursor()

    for file in listdir(pasta_views):
        if file.endswith(".sql"):
            with open(pasta_views + "/" + file ) as arquivo_view:
                comandos = arquivo_view.read()

                comandos = comandos.replace("siconv_schema.", f"{schema}." )
                
                print (" *** " + ctime() + " Implementando view " + file )
                try:
                    for comando in comandos.split(";"):
                        if not query_vazia(comando):
                            cur.execute(comando)
                            conn.commit()
                except Exception as e:
                    print (f"estava executando a query: \033[92m{comando}'\033[0m")
                    print (f"Excessão: {e}")
                    exit(1)

    # Saindo e fechando tudo
    cur.close()
    conn.close()

######################################################
# Exporta alguma tabela para CSV
def exportar_csv (schema, tabela, arquivo_saida):
    comando = f"\\copy (select * from {schema}.{tabela}) to '{arquivo_saida}' WITH (FORMAT CSV, NULL '', HEADER TRUE, DELIMITER E'\t')"
    cur.execute(comando)
    conn.commit()
