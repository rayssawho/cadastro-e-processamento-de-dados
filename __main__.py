# +----------------------------------------------------------------------+
# | SoulCode Academy													 |
# | Atividade 13 - Cadastro e processamento de dados                     |
# | Rayssa Melo										                     |
# | Inserindo e processando os dados                                     |
# +----------------------------------------------------------------------+

import psycopg2
from modules.connector import Conexao
import pandas as pd
from datetime import date, datetime
import statistics



if __name__ == "__main__":
    try:
        #conectando ao banco de dados
        inicia_conexao = Conexao("postgres", "admin","127.0.0.1", "Atividade_13")
        print("Conectado") 
        
    except Exception as error:
        print("Erro na conexão com o banco de dados")
    
    try:
        #inserindo os dados do arquivo csv na tabela vendas no banco de dados
        dados_inserir = pd.read_csv("./dados_final.csv")
        for index, row in dados_inserir.iterrows():
            inicia_conexao.inserir(f"INSERT INTO vendas (data_venda, valor) VALUES ('{row['data']}', {int(row['valor'])})")
        
    except Exception as error:
        print("Erro ao inserir os dados na tabela vendas")
    
    try:
        #separando os dados de dados_final.csv em grupos de 50
        dados_inserir = pd.read_csv("./dados_final.csv")
        df = pd.DataFrame(dados_inserir)
        for i in range(0, len(df), 50):
          dados_sep = df.iloc[i: i+50]
                   
          data_inicio = dados_sep['data'].values[0] #seleciona a primeira data de cada grupo
          data_final = dados_sep['data'].values[-1] #seleciona a última data de cada grupo
          media = dados_sep['valor'].mean() #calcula a média de cada grupo
          mediana = dados_sep['valor'].median() #calcula a mediana de cada grupo
          moda = statistics.mode(dados_sep['valor']) #calcula a moda de cada grupo
          desvio_padrao = dados_sep['valor'].std() #mostra o desvio padrão de cada grupo
          maior_valor = dados_sep['valor'].max() #exibe o maior valor de cada grupo
          menor_valor = dados_sep['valor'].min() #exibe o menor valor de cada grupo
          
          #adiciona todas as variáveis acima em apenas uma, usando a formatação que se adapte a query, para realizar o INSERT no banco de dados
          valores = "('{}', '{}', {}, {}, {}, {}, {}, {} )".format(data_inicio, data_final, media, mediana, moda, desvio_padrao, maior_valor, menor_valor) 
          inicia_conexao.inserir(f"INSERT INTO registros_vendas(data_inicio, data_final, media, mediana, moda, desvio_padrao, maior_valor, menor_valor) VALUES" + valores + ";")
    
    except Exception as error:
        print("Erro ao inserir dados na tabela registros_vendas")
    
    try:
        #realiza o SELECT da tabela logs_vendas
        select_logs = [inicia_conexao.selecionar("*", "logs_vendas", "")]
        print(select_logs)
    
    except Exception as error:
        print("Erro ao realizar o SELECT")
    
        
    
    