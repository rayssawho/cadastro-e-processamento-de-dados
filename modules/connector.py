import psycopg2

class Conexao:
    usuario = "",
    senha = "",
    host = "",
    banco = ""
    
    def __init__(self, usuario, senha, host, banco):
        """[declaração de paramentos da classe interface]

        Args:
            usuario (string): usuário do banco de dados.
            senha (string): senha de acesso ao banco de dados.
            host (string): endereço host.
            banco (string): nome do banco de dados.
        """
        try:
           
            self.usuario = usuario
            self.senha = senha
            self.host = host
            self.banco = banco
         
        except Exception as e:
            print(str(e))
            
    def conectar(self):
        try:
            
            con = psycopg2.connect(user = self.usuario, password = self.senha, host = self.host, database = self.banco)
            cursor = con.cursor()
            return con, cursor
            
        except Exception as e:
            print(str(e))
    
    def desconectar(self, con, cursor):
        try:
            cursor.close()
            con.commit()
            con.close()
        except Exception as e:
            print(str(e)) 
    
    def selecionar(self, coluna, tabela, contquery):
        try:
            con, cursor = self.conectar()
            query = "SELECT " + coluna + " FROM " + tabela + contquery + ";"
            cursor.execute(query)
            return cursor.fetchall()
        except Exception as e:
            print(str(e))
        finally:
            self.desconectar(con, cursor)
            
    def inserir(self, query):
        try:
            con, cursor = self.conectar()
            cursor.execute(query)
            con.commit()
            cursor.close()
        except Exception as e:
            print(str(e))
        finally:
            self.desconectar(con, cursor)
    
    
    
    
    