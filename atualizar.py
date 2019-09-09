#!/usr/bin/python3

import psycopg2                                           # conexão com o banco de dados
from urllib.request import urlretrieve                    # download de um recurso via URL
import zipfile                                            # para fazer o unzip do arquivo baixado
from time import ctime                                    # para a pasta temporaria, log na CLI e adicionar informação da data da atualização
from getpass import getpass                               # leitura sem echo em tela (caso seja desejado esconder a senha e nao deixar ela no script)
from os import mkdir, getcwd, listdir,remove,rmdir,path   # funcoes para manipulação de arquivos
import argparse                                           # facilita bastante argumentos de linha de comando

# Parser dos argumentos da CLI
parser = argparse.ArgumentParser(description='Atualiza o banco de dados do SICONV')

#Schema do postgres alvo do script
parser.add_argument('schema_alvo', metavar='schema', type=str, nargs=1,
                   help='o schema que vai sofrer a alteração')

#Informe caso queira fazer um hard reset
parser.add_argument('--reset', action='store_true',
                   help='Informe esta flag caso deseje fazer a seguinte operacao: drop -> create -> insert.')

#Informe caso prefira truncar as tabelas e reinserir os dados
parser.add_argument('--truncate', action='store_true',
                   help='Informe esta flag caso deseje fazer a seguinte operacao: truncate -> insert.')

#Informe caso não queira refazer as views
parser.add_argument('--semViews', action='store_true',
                   help='Informe esta flag caso não deseje refazer as views.')

#Informe para baixar os dados depois
parser.add_argument('--gerarCSV', action='store_true',
                   help='Informe esta flag para depois de fazer as views, gerar os CSVs apropriados.')

#Informe para manter os arquivos baixados
parser.add_argument('--keepDownload', action='store_true',
                   help='Informe esta flag para manter os arquivos baixado em sua pasta temporaria. Default: apagar ao final da execução do script')

#Endereço da pasta com as views
parser.add_argument('--viewFolder', nargs=1, default="./SQL/ViewsServidor",
                   help='Altere esta flag para mudar a pasta aonde as views se encontram. Default: ./SQL/ViewsServidor ')

#Endereço do arquivo create schema
parser.add_argument('--schemaFile', nargs=1, default="./SQL/create_schema.sql",
                   help='Altere esta flag para mudar o arquivo fonte do schema. Default: ./SQL/create_schema.sql ')

#Endereço do host do banco de dados
parser.add_argument('--host', nargs=1, default="127.0.0.1",
                   help='Altere esta flag para mudar o host do banco de dados a ser alterado. Default: 127.0.0.1 ')

#Endereço do arquivo create schema
parser.add_argument('--dbname', nargs=1, default="siconv",
                   help='Altere esta flag para mudar o arquivo da database a ser mudada. Default: siconv ')

#Informe para manter os arquivos baixados
parser.add_argument('--aproveitarDownload', action='store_true',
                   help='Informe esta flag para aproveitar os arquivos baixado em uma execução anterior ao inves de baixar e descompactar de novo')



args = parser.parse_args()

schema_alvo             = args.schema_alvo[0]
reset_flag              = args.reset
truncate_flag           = args.truncate
generateCSV_flag        = args.gerarCSV
keepDownload_flag       = args.keepDownload
semViews_flag           = args.semViews
aproveitarDownload_flag = args.aproveitarDownload
pasta_views    = ( args.viewFolder[0] if isinstance(args.viewFolder, list) else args.viewFolder )
arquivo_schema = ( args.schemaFile[0] if isinstance(args.schemaFile, list) else args.schemaFile )
host           = ( args.host[0]       if isinstance(args.host, list)       else args.host )
dbname         = ( args.dbname[0]     if isinstance(args.dbname, list)     else args.dbname )

if reset_flag and truncate_flag:
    raise Exception('Ou reseta ou trunca o schema, nao faca ambos!')

if generateCSV_flag and semViews_flag:
    raise Exception('Preciso gerar as views para gerar o CSV')

# testar a conexão
try: 
    usuario = input("USUARIO DO POSTGRES: ")
    senha = getpass() 
    conn_string = "host='"+host+"' dbname='"+dbname+"' user='"+usuario+"' password='"+senha+"'"

    #caso deixar a senha no script seja aceitável pode-se trocar as tres linhas de cima por esta
    #conn_string = "host='"+host+"' dbname='"+dbname+"' user='admin' password='admin'"

    print(" *** " + ctime() + " Testando conexao...")
    conn = psycopg2.connect(conn_string)    
    conn.close()
except Exception as error: 
    print('ERROR', error) 
    exit()
print(" *** " + ctime() + " Conexão realizada com sucesso")

#Sabendo que a conexão funciona, agora dá para de fato baixar os dados e levar a cabo a atualização

nome_pasta_temp = getcwd() + "/__pasta_temporaria__" + ctime()[4:10] #'Jul 17' por exemplo

# Se tem downloads do arquivo, eu vou apagar e refazer por que não tenho muito como garantir que a validade dessa pasta
# Se aprender a validar esta pasta, é possível ignorar essa parte, a do download e a da descompactação
if path.exists(nome_pasta_temp) and not aproveitarDownload_flag:
    print (" *** " + ctime() + " Apagando a pasta com downloads no processo sem sucesso...")
    for file in listdir(nome_pasta_temp):
        remove(nome_pasta_temp + "/" + file)

if not aproveitarDownload_flag:

    if path.exists(nome_pasta_temp) is False:
        mkdir(nome_pasta_temp)

    print(" *** " + ctime() + " Baixando dados...")
    tentativas = 0
    while tentativas < 3:
        try:
            urlretrieve( "http://plataformamaisbrasil.gov.br/images/docs/CGSIS/csv/siconv.zip" ,  nome_pasta_temp + "/dados_siconv.zip")
            break
        except:
            tentativas += 1
    if tentativas == 3:
        print ("A fonte parece estar fora do ar")
        exit()
    else:
        print(" *** " + ctime() + " Dados baixados.")

    #Unzipar os arquivos
    print(" *** " + ctime() + " Descompactando arquivo baixado...")
    try:
        with zipfile.ZipFile(nome_pasta_temp + "/dados_siconv.zip","r") as zip_ref:
            zip_ref.extractall(nome_pasta_temp + "/")
    except:
        print (" *** " + ctime() + " Erro! Arquivo não encontrado.")
        exit()
    print (" *** " + ctime() + " Descompactação finalizada")
    
#Atualizando o banco
conn = psycopg2.connect(conn_string)
cur = conn.cursor()

if reset_flag:
    print (" *** " + ctime() + " Derrubando e recriando o schema ...")
    
    with open(arquivo_schema) as arq_schema:
        comandos = arq_schema.read()
        comandos = comandos.upper().replace("SICONV", schema_alvo )
        for comando in comandos.split(";"):
            try:
                print (comando)
                cur.execute(comando)
                conn.commit()
            except psycopg2.ProgrammingError:
                continue
    conn.commit()

elif truncate_flag:
    print (" *** " + ctime() + " Truncando as tabelas...")
    cur.execute('TRUNCATE TABLE '+ schema_alvo +'.consorcios')
    cur.execute('TRUNCATE TABLE '+ schema_alvo +'.convenio')
    cur.execute('TRUNCATE TABLE '+ schema_alvo +'.desbloqueio')
    cur.execute('TRUNCATE TABLE '+ schema_alvo +'.desembolso')
    cur.execute('TRUNCATE TABLE '+ schema_alvo +'.emenda')
    cur.execute('TRUNCATE TABLE '+ schema_alvo +'.empenho')
    cur.execute('TRUNCATE TABLE '+ schema_alvo +'.empenho_desembolso')
    cur.execute('TRUNCATE TABLE '+ schema_alvo +'.etapa_crono_fisico')
    cur.execute('TRUNCATE TABLE '+ schema_alvo +'.historico_situacao')
    cur.execute('TRUNCATE TABLE '+ schema_alvo +'.ingresso_contrapartida')
    cur.execute('TRUNCATE TABLE '+ schema_alvo +'.meta_crono_fisico')
    cur.execute('TRUNCATE TABLE '+ schema_alvo +'.obtv_convenente')
    cur.execute('TRUNCATE TABLE '+ schema_alvo +'.pagamento')
    cur.execute('TRUNCATE TABLE '+ schema_alvo +'.plano_aplicacao_detalhado')
    cur.execute('TRUNCATE TABLE '+ schema_alvo +'.programa')
    cur.execute('TRUNCATE TABLE '+ schema_alvo +'.programa_proposta')
    cur.execute('TRUNCATE TABLE '+ schema_alvo +'.proponentes')
    cur.execute('TRUNCATE TABLE '+ schema_alvo +'.proposta')
    cur.execute('TRUNCATE TABLE '+ schema_alvo +'.prorroga_oficio')
    cur.execute('TRUNCATE TABLE '+ schema_alvo +'.termo_aditivo')

#Lista de tuplas com cada elemento sendo:
# (tipo texto, tipo texto)
# (comando do copy, endereço do arquivo)

if reset_flag or truncate_flag:
    lista_comandos = []
    lista_comandos.append( ('COPY '+ schema_alvo +'.consorcios                 FROM STDIN ( FORMAT(\'csv\'), DELIMITER(\';\'), QUOTE(E\'\"\'), HEADER)', nome_pasta_temp + '/siconv_consorcios.csv'))
    lista_comandos.append( ('COPY '+ schema_alvo +'.convenio                   FROM STDIN ( FORMAT(\'csv\'), DELIMITER(\';\'), QUOTE(E\'\"\'), HEADER)', nome_pasta_temp + '/siconv_convenio.csv'))
    lista_comandos.append( ('COPY '+ schema_alvo +'.desbloqueio                FROM STDIN ( FORMAT(\'csv\'), DELIMITER(\';\'), QUOTE(E\'\"\'), HEADER)', nome_pasta_temp + '/siconv_desbloqueio_cr.csv'))
    lista_comandos.append( ('COPY '+ schema_alvo +'.desembolso                 FROM STDIN ( FORMAT(\'csv\'), DELIMITER(\';\'), QUOTE(E\'\"\'), HEADER)', nome_pasta_temp + '/siconv_desembolso.csv'))
    lista_comandos.append( ('COPY '+ schema_alvo +'.emenda                     FROM STDIN ( FORMAT(\'csv\'), DELIMITER(\';\'), QUOTE(E\'\"\'), HEADER)', nome_pasta_temp + '/siconv_emenda.csv'))
    lista_comandos.append( ('COPY '+ schema_alvo +'.empenho                    FROM STDIN ( FORMAT(\'csv\'), DELIMITER(\';\'), QUOTE(E\'\"\'), HEADER)', nome_pasta_temp + '/siconv_empenho.csv'))
    lista_comandos.append( ('COPY '+ schema_alvo +'.empenho_desembolso         FROM STDIN ( FORMAT(\'csv\'), DELIMITER(\';\'), QUOTE(E\'\"\'), HEADER)', nome_pasta_temp + '/siconv_empenho_desembolso.csv'))
    lista_comandos.append( ('COPY '+ schema_alvo +'.etapa_crono_fisico         FROM STDIN ( FORMAT(\'csv\'), DELIMITER(\';\'), QUOTE(E\'\"\'), HEADER)', nome_pasta_temp + '/siconv_etapa_crono_fisico.csv'))
    lista_comandos.append( ('COPY '+ schema_alvo +'.historico_situacao         FROM STDIN ( FORMAT(\'csv\'), DELIMITER(\';\'), QUOTE(E\'\"\'), HEADER)', nome_pasta_temp + '/siconv_historico_situacao.csv'))
    lista_comandos.append( ('COPY '+ schema_alvo +'.ingresso_contrapartida     FROM STDIN ( FORMAT(\'csv\'), DELIMITER(\';\'), QUOTE(E\'\"\'), HEADER)', nome_pasta_temp + '/siconv_ingresso_contrapartida.csv'))
    lista_comandos.append( ('COPY '+ schema_alvo +'.meta_crono_fisico          FROM STDIN ( FORMAT(\'csv\'), DELIMITER(\';\'), QUOTE(E\'\"\'), HEADER)', nome_pasta_temp + '/siconv_meta_crono_fisico.csv'))
    lista_comandos.append( ('COPY '+ schema_alvo +'.obtv_convenente            FROM STDIN ( FORMAT(\'csv\'), DELIMITER(\';\'), QUOTE(E\'\"\'), HEADER)', nome_pasta_temp + '/siconv_obtv_convenente.csv'))
    lista_comandos.append( ('COPY '+ schema_alvo +'.pagamento                  FROM STDIN ( FORMAT(\'csv\'), DELIMITER(\';\'), QUOTE(E\'\"\'), HEADER)', nome_pasta_temp + '/siconv_pagamento.csv'))
    lista_comandos.append( ('COPY '+ schema_alvo +'.plano_aplicacao_detalhado  FROM STDIN ( FORMAT(\'csv\'), DELIMITER(\';\'), QUOTE(E\'\"\'), HEADER)', nome_pasta_temp + '/siconv_plano_aplicacao_detalhado.csv'))
    lista_comandos.append( ('COPY '+ schema_alvo +'.programa                   FROM STDIN ( FORMAT(\'csv\'), DELIMITER(\';\'), QUOTE(E\'\"\'), HEADER)', nome_pasta_temp + '/siconv_programa.csv'))
    lista_comandos.append( ('COPY '+ schema_alvo +'.programa_proposta          FROM STDIN ( FORMAT(\'csv\'), DELIMITER(\';\'), QUOTE(E\'\"\'), HEADER)', nome_pasta_temp + '/siconv_programa_proposta.csv'))
    lista_comandos.append( ('COPY '+ schema_alvo +'.proponentes                FROM STDIN ( FORMAT(\'csv\'), DELIMITER(\';\'), QUOTE(E\'\"\'), HEADER)', nome_pasta_temp + '/siconv_proponentes.csv'))
    lista_comandos.append( ('COPY '+ schema_alvo +'.proposta                   FROM STDIN ( FORMAT(\'csv\'), DELIMITER(\';\'), QUOTE(E\'\"\'), HEADER)', nome_pasta_temp + '/siconv_proposta.csv'))
    lista_comandos.append( ('COPY '+ schema_alvo +'.prorroga_oficio            FROM STDIN ( FORMAT(\'csv\'), DELIMITER(\';\'), QUOTE(E\'\"\'), HEADER)', nome_pasta_temp + '/siconv_prorroga_oficio.csv'))
    lista_comandos.append( ('COPY '+ schema_alvo +'.termo_aditivo              FROM STDIN ( FORMAT(\'csv\'), DELIMITER(\';\'), QUOTE(E\'\"\'), HEADER)', nome_pasta_temp + '/siconv_termo_aditivo.csv'))

    print (" *** " + ctime() + " Inserindo dados ...")
    for (comando, endereco) in lista_comandos:
        print(comando, endereco)
        cur.copy_expert(comando, open(endereco))

    conn.commit()

#fazendo as views a menos que o usuario explicitamente não peça isso
if not semViews_flag :
    print (" *** " + ctime() + " Refazendo as views...")
    for file in listdir(pasta_views):
        if file.endswith(".sql"):
            with open(pasta_views + "/" + file ) as arquivo_view:
                comandos = arquivo_view.read()
                comandos = comandos.upper().replace("SICONV.", schema_alvo + "." )

                print (" *** " + ctime() + " Implementando view " + file )
                for comando in comandos.split(";"):
                    try:
                        cur.execute(comando)
                        conn.commit()
                    except psycopg2.ProgrammingError:
                        continue

# Gerando os csvs (propostas, instrumentos e proponentes)
if generateCSV_flag:
    lista_comandos = []
    lista_comandos.append("\\copy (select * from siconv.instrumentos)  to '" + getcwd() + "/siconv_cgdad_v2.02_instrumentos.txt' WITH (FORMAT CSV, NULL '', HEADER TRUE, DELIMITER E'\t')")
    lista_comandos.append("\\copy (select * from siconv.solicitantes)  to '" + getcwd() + "/siconv_cgdad_v2.02_proponentes.txt'  WITH (FORMAT CSV, NULL '', HEADER TRUE, DELIMITER E'\t')")
    lista_comandos.append("\\copy (select * from SICONV.tabelao)       to '" + getcwd() + "/siconv_cgdad_v2.02_propostas.txt'    WITH (FORMAT CSV, NULL '', HEADER TRUE, DELIMITER E'\t')")

    print (" *** " + ctime() + " Inserindo dados ...")
    for comando in lista_comandos:
        try:
            cur.execute(comando)
            conn.commit()
        except psycopg2.ProgrammingError:
            continue

#fazendo o log na tabela de configuracao
#comando = 'INSERT INTO '+ schema_alvo +'.configuracao (data_atualizacao) VALUES (CURRENT_DATE)'
#print (comando)
#cur.execute(comando)
#conn.commit()

cur.close()
conn.close()

#Apagando a pasta temporaria dos downloads
if not keepDownload_flag:
    print (" *** " + ctime() + " Apagando a pasta temporaria...")
    for file in listdir(nome_pasta_temp):
        remove(nome_pasta_temp + "/" + file)
    rmdir(nome_pasta_temp)


print (" *** " + ctime() + " Script finalizado.")