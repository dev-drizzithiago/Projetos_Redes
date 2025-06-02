import os

import fdb
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

class ConexaoODBCFireBirdUnico:
    USER_DB = os.getenv('USER_DB_FB_UNICO')
    PASS_DB = os.getenv('PASS_DB_FB_UNICO')
    DATABASE = os.getenv('DATA_BASE_FB_UNICO')
    ARQUIVO_DLL_LIBRARY_NAME = os.getenv('PATH_FILE_DLL_CLIENTE')

    def __init__(self):
        try:
            self.conexao_DB = fdb.connect(
                user=self.USER_DB,
                password=self.PASS_DB,
                dsn=self.DATABASE,
                fb_library_name=self.ARQUIVO_DLL_LIBRARY_NAME
            )

            lista_codClientes = [83, 502, 793, 794, 912, 1645, 1935]

            for valor_cliente in lista_codClientes:
                self.lista_dados_cliente.append(self.processo_dados_empresas(valor_cliente))
                self.lista_dados_cliente.append(self.processo_dados_funcionarios(valor_cliente))

        except fdb.fbcore.DatabaseError:
            print("Erro ao tentar conectar o Banco de dados. \n"
                  "Verifique se as credenciais estão corretas ou \n"
                  "se a conexão foi estabelecida")