import pandas as pd

try:
  #organizando dados da tabela DADOS_1.csv
  dados_1 = pd.read_csv("./DADOS_1.csv") #importando o arquivo DADOS_1.csv
  dados_1 = dados_1.dropna(axis = 0) #excluindo os valores nulos
  dados_1['data'] = pd.to_datetime(dados_1['data']) #convertendo a coluna data para o tipo datetime
  dados_1['valor'] = pd.to_numeric(dados_1['valor']) #convertendo a coluna valor para o tipo numérico
  

except Exception as error:
  print("Erro na importação dos Dados_1.csv")


try:
  #organizando dados da tabela DADOS_2.csv
  dados_2 = pd.read_csv("./DADOS_2.csv") #importando o arquivo DADOS_2.csv
  dados_2 = dados_2.dropna(axis = 0) #excluindo os valores nulos
  dados_2['data'] = pd.to_datetime(dados_2['data']) #convertendo a coluna data para o tipo datetime
  dados_2['valor'] = pd.to_numeric(dados_2['valor']) #convertendo a coluna valor para o tipo numérico
 
except Exception as error:
  print("Erro na importação dos Dados_2.csv")

try:
  #concatenando as duas tabelas      
  dados_juntos = pd.concat([dados_1, dados_2]) #concatena as duas tabelas
  dados_final = dados_juntos.sort_values(by=['data']) #ordena os dados por data
  dados_final.drop('id', inplace=True, axis=1) #excluindo a coluna ID
  dados_final.to_csv(r"./dados_final.csv", index = False) #exportando csv com dados limpos para inserção no banco de dados

except Exception as error:
  print("Erro na concatenação das tabelas DADOS_1 e DADOS_2")



