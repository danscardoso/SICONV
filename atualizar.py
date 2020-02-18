#!/usr/bin/python3

from time import ctime      # Preciso buscar a data no sistema para criar a pasta temporária
import argparse             # facilita bastante trabalhar com argumentos de linha de comando
from getpass import getpass # leitura sem echo em tela (caso seja desejado esconder a senha e nao deixar ela no script)
from os import getcwd

# Várias ferramentas foram organizadas nesse código aqui para simplificar aqui e agilizar a
# conversão desse código para airflow
import siconv_tools

# Parser dos argumentos da CLI
parser = argparse.ArgumentParser(description='Atualiza o banco de dados do SICONV')

#Schema do postgres alvo do script
parser.add_argument('schema', metavar='schema', type=str, nargs=1,
                   help='o schema que vai sofrer a alteração')

#Informe caso queira fazer um hard reset
parser.add_argument('--reset', action='store_true',
                   help='Informe esta flag caso deseje fazer a seguinte operacao: drop -> create -> insert.')

#Informe caso prefira truncar as tabelas e reinserir os dados
parser.add_argument('--truncate', action='store_true',
                   help='Informe esta flag caso deseje fazer a seguinte operacao: truncate -> insert.')

#Informe para manter os arquivos baixados depois de rodar o arquivo
parser.add_argument('--keepDownload', action='store_true',
                   help='Informe esta flag para manter os arquivos baixado em sua pasta temporaria. Default: apagar ao final da execução do script')

#Endereço da pasta com as views
parser.add_argument('--viewFolder', nargs=1, default="./SQL/ViewsProntas",
                   help='Altere esta flag para mudar a pasta aonde as views se encontram. Default: ./SQL/ViewsServidor ')

#Endereço do arquivo create schema
parser.add_argument('--schemaFile', nargs=1, default="./SQL/create_schema.sql",
                   help='Altere esta flag para mudar o arquivo fonte do schema. Default: ./SQL/create_schema.sql ')

# Argumentos para permitir o usuario mudar a conexão na invocação do programa
# host, dbname e port podem ser mudados
parser.add_argument('--host', nargs=1, default="127.0.0.1",
                   help='Altere esta flag para mudar o host do banco de dados a ser alterado. Default: 127.0.0.1 ')
parser.add_argument('--dbname', nargs=1, default="siconv",
                   help='Altere esta flag para mudar o arquivo da database a ser mudada. Default: siconv ')
parser.add_argument('--port', nargs=1, default="5432",
                   help='Altere esta flag para mudar a porta que o host escuta. Default: 5432 ')


args = parser.parse_args()

schema             = args.schema[0]
reset_flag         = args.reset
truncate_flag      = args.truncate
keepDownload_flag  = args.keepDownload
comViews_flag      = args.semViews == False
pasta_views    = ( args.viewFolder[0] if isinstance(args.viewFolder, list) else args.viewFolder )
arquivo_schema = ( args.schemaFile[0] if isinstance(args.schemaFile, list) else args.schemaFile )
host           = ( args.host[0]       if isinstance(args.host, list)       else args.host )
dbname         = ( args.dbname[0]     if isinstance(args.dbname, list)     else args.dbname )
port           = ( args.port[0]       if isinstance(args.dbname, list)     else args.port )

#############################################################
# Checagem de consistência das opções selecionadas na linha de comando
#############################################################

if reset_flag and truncate_flag:
    raise Exception('Ou reseta ou trunca o schema, nao faca ambos!')

if not reset_flag and not truncate_flag:
    raise Exception('Para atualizar o banco é necessário escolher um método de limpar o banco! Truncar ou resetar.')    

# Não quero que a senha fique salva na linha de comando, informe aqui dentro do código
usuario = input("USUARIO DO POSTGRES: ")
senha = getpass() 

#############################################################
## Parâmetros do código
#############################################################

#nome dos arquivos
nome_pasta_temp = getcwd() + "/__pasta_temporaria__" + ctime()[4:10].replace(" ", "") # Pasta temporaria que vai ter os dados baixados
arquivo_nome = "dados_siconv.zip"
arquivo_full_nome = nome_pasta_temp + "/" + arquivo_nome

conn_string = f"host='{host}' dbname='{dbname}' user='{usuario}' password='{senha}' port='{port}'" # Linha de conexão com os parâmetros do banco

url_fonte = "http://plataformamaisbrasil.gov.br/images/docs/CGSIS/csv/siconv.zip"


#############################################################
# Execução do Código
#############################################################

# Testa a conexão, se der algum problema nem adianta baixar os dados e tal
if siconv_tools.testar_conexao( conn_string) == 1:
    exit()

# Agora que sei que a conexão está ok, faz sentido pevar a cabo o ETL
siconv_tools.criar_pasta_vazia( nome_pasta_temp)

# Baixar o arquivo 
siconv_tools.baixar_arquivo(url=url_fonte, nome_arquivo = arquivo_full_nome )

# Descompactando o zip em todos os arquivos dentro
#  ** Aparentemente, é possível capturar o nome dos arquivos dentro do zip
#  e com isso extrair apenas alguns deles. Assim, melhora a performance e
#  ocupa um pouco menos de espaço (transitório) de disco durante o processo
#  do ETL
siconv_tools.descompactar(arquivo_in=arquivo_full_nome, pasta_out=nome_pasta_temp)

# forma #1 de limpar os dados velhos, hard reset (derruba o schema e recria vazio)
if reset_flag:
    siconv_tools.hard_reset(arquivo_schema, schema, conn_string)

# forma #2 de limpar os dados velhos, soft reset (não derruba o schema, apenas trunca as tabelas)
elif truncate_flag:
    siconv_tools.soft_reset(schema, conn_string)

# Copiando dados para o banco
siconv_tools.insert_data(schema, conn_string, nome_pasta_temp)

# Criando as views
siconv_tools.criar_views(pasta_views, schema, conn_string)

# Apagando a pasta temporaria
if not keepDownload_flag:
    siconv_tools.remove_pasta_conteudo( nome_pasta_temp, schema )