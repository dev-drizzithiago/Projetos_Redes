import os

import fdb
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

class ConexaoODBCFireBirdUnico:
    PATH_FILE = Path(__file__).parent
    USER_DB = os.getenv('USER_DB_FB_UNICO')
    PASS_DB = os.getenv('PASS_DB_FB_UNICO')
    DATABASE = os.getenv('DATA_BASE_FB_UNICO')
    ARQUIVO_DLL_LIBRARY_NAME = os.path.join(PATH_FILE, os.getenv('PATH_FILE_DLL_CLIENTE'))

    def __init__(self):
        try:
            self.conexao_DB = fdb.connect(
                user=self.USER_DB,
                password=self.PASS_DB,
                dsn=self.DATABASE,
                fb_library_name=self.ARQUIVO_DLL_LIBRARY_NAME
            )
            print('Conexão estabelecida')
        except Exception as  error:
            print(error)


if __name__ == '__main__':
    obj_inciado = ConexaoODBCFireBirdUnico()
