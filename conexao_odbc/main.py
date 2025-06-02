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
        self._conexao_DB = None

    def conexao_banco_dados(self):
        try:
            self._conexao_DB = fdb.connect(
                user=self.USER_DB,
                password=self.PASS_DB,
                dsn=self.DATABASE,
                fb_library_name=self.ARQUIVO_DLL_LIBRARY_NAME
            )
            print('Conexão estabelecida')
        except Exception as  error:
            print(error)
        return self._conexao_DB


class BuscaDadosBanco(ConexaoODBCFireBirdUnico):

    def __init__(self, banco_db_conectado):
        super().__init__()
        self.lista_funcionarios = list()
        self.conexao_realizada = banco_db_conectado

    def view_dados_bd(self, cod_empresa: int=1):
        _conexao = self.conexao_realizada
        try:
            CURSOR_VRH_EMP_TCOLCON = _conexao.cursor()
            CURSOR_VRH_EMP_TCOLCON_CAD = _conexao.cursor()
            CURSOR_VRH_EMP_TCOLCON_DOCREF = _conexao.cursor()
            CURSOR_VRH_EMP_TCOL_VINCDIA = _conexao.cursor()
            CURSOR_VRH_EMP_TFERIAS = _conexao.cursor()
            CURSOR_VRH_EMP_TRESCISAO = _conexao.cursor()
        except AttributeError:
            print('Objeto não pode ser atribuido, verifique se o banco de dados esta disponível.')
        BDCODEMP = cod_empresa

        # ---------------------------------------------------------------------------------------------------------
        SQL_COMANDO_VRH_EMP_TCOLCON = \
            f"SELECT BDCODEMP, BDCODCOL, BDDATANASCCOL, BDDATACADCOL, " \
            "BDDATAADMCOL, BDCPFCOL, BDEMAIL2COL , BDMATRICULA " \
            f"FROM VRH_EMP_TCOLCON " \
            f"WHERE BDCODEMP = {BDCODEMP}"

        # ---------------------------------------------------------------------------------------------------------
        SQL_COMANDO_VRH_EMP_TCOLCON_CAD = \
            f"SELECT  BDCODEMP, BDCODCOL, BDNOMCOL, BDGRAUINSTCOL, BDESTCIVCOL, BDSEXOCOL " \
            f"FROM VRH_EMP_TCOLCON_CAD " \
            f"WHERE BDCODEMP = {BDCODEMP} "
        # ---------------------------------------------------------------------------------------------------------
        SQL_COMANDO_VRH_EMP_TCOLCON_DOCREF = \
            f"SELECT BDCODEMP, BDCODCOL, BDPISCOL, BDRGCOL " \
            f"FROM VRH_EMP_TCOLCON_DOCREF " \
            f"WHERE BDCODEMP = {BDCODEMP} "
        # ---------------------------------------------------------------------------------------------------------
        SQL_COMANDO_VRH_EMP_TCOL_VINCDIA = \
            f"SELECT BDCODEMP, BDCODCOL, BDDATACOL, BDCODTFUC, BDREGJORTRAB, BDTIPOJORNADA  " \
            f"FROM VRH_EMP_TCOL_VINCDIA " \
            f"WHERE BDCODEMP = {BDCODEMP} "
        # --------------------------------------------------11-----------------------------------------------------
        SQL_COMANDO_VRH_EMP_TFERIAS = \
            f"SELECT BDCODEMP, BDCODCOL, BDINIPERAQUISFE, BDCODFE, BDINIPERGOZOFE, BDFIMPERAQUISFE, " \
            f"BDFIMPERGOZOFE, BDDATAPAGFE, BDQTDGOZOFE, BDQTDABONOFE, BDSALDOFIMFE " \
            f"FROM VRH_EMP_TFERIAS " \
            f"WHERE BDCODEMP = {BDCODEMP} "
        # ---------------------------------------------------------------------------------------------------------
        SQL_COMANDO_VRH_EMP_TRESCISAO = \
            f"SELECT BDCODEMP, BDCODCOL, BDDATARESCISAO, BDCODPRES, BDDATAAVPREVIO, BDREPOSICAOVAGA, " \
            f"BDDISPAVPREVRESC  " \
            f"FROM VRH_EMP_TRESCISAO " \
            f"WHERE BDCODEMP = {BDCODEMP} "
        # ---------------------------------------------------------------------------------------------------------
        CURSOR_VRH_EMP_TCOLCON.execute(SQL_COMANDO_VRH_EMP_TCOLCON)
        CURSOR_VRH_EMP_TCOLCON_CAD.execute(SQL_COMANDO_VRH_EMP_TCOLCON_CAD)
        CURSOR_VRH_EMP_TCOLCON_DOCREF.execute(SQL_COMANDO_VRH_EMP_TCOLCON_DOCREF)
        CURSOR_VRH_EMP_TCOL_VINCDIA.execute(SQL_COMANDO_VRH_EMP_TCOL_VINCDIA)
        CURSOR_VRH_EMP_TFERIAS.execute(SQL_COMANDO_VRH_EMP_TFERIAS)
        CURSOR_VRH_EMP_TRESCISAO.execute(SQL_COMANDO_VRH_EMP_TRESCISAO)
        # ---------------------------------------------------------------------------------------------------------
        CADASTRO_COLABORADORES = CURSOR_VRH_EMP_TCOLCON.fetchall()
        DADOS_CADASTRAIS_COLABORADORES = CURSOR_VRH_EMP_TCOLCON_CAD.fetchall()
        DADOS_DOCUMENTOS_POR_REFERENCIA_COLABORADORES = CURSOR_VRH_EMP_TCOLCON_DOCREF.fetchall()
        DADOS_CADASTRO_DADOS_DIARIOS_COLABORADORES = CURSOR_VRH_EMP_TCOL_VINCDIA.fetchall()
        DADOS_CALCULO_FERIAS_COLABORADORES = CURSOR_VRH_EMP_TFERIAS.fetchall()
        DADOS_TABELA_RESISOES_COLABORADORES = CURSOR_VRH_EMP_TRESCISAO.fetchall()

        # 0 -------------------------------------------------------------------------------------------------------
        # ok

        lista_dados_PESSOAIS_COLABORADORES = []

        for valor_info_colaboradores in CADASTRO_COLABORADORES:
            dict_BD_dados_PESSOAIS_COLABORADORES = {
                valor_info_colaboradores[0]: {
                    'tabela': 'VRH_EMP_TCOLCON',
                    'cod_colaborador': valor_info_colaboradores[1],
                    'data_nascimento': valor_info_colaboradores[2],
                    'data_cadastro': valor_info_colaboradores[3],
                    'data_admissao': valor_info_colaboradores[4],
                    'cpf_colaborador': valor_info_colaboradores[5],
                    'email_colaborador': valor_info_colaboradores[6],
                    'matricula_esocial': valor_info_colaboradores[7],
                }
            }
            lista_dados_PESSOAIS_COLABORADORES.append(dict_BD_dados_PESSOAIS_COLABORADORES)

        self.lista_funcionarios.append(lista_dados_PESSOAIS_COLABORADORES)

        # 1 -------------------------------------------------------------------------------------------------------
        # ok

        dict_DB_DADOS_CADASTRAIS = {}
        lista_DADOS_CADASTRAIS = []

        for valor_cadastro_colaboradores in DADOS_CADASTRAIS_COLABORADORES:
            cod_colaborador = valor_cadastro_colaboradores[1]
            if cod_colaborador not in dict_DB_DADOS_CADASTRAIS:
                dict_DB_DADOS_CADASTRAIS[cod_colaborador] = {
                    valor_cadastro_colaboradores[0]: {
                        'tabela': 'VRH_EMP_TCOLCON_CAD',
                        'cod_colaborador': valor_cadastro_colaboradores[1],
                        'nome_colaborador': valor_cadastro_colaboradores[2],
                        'grau_instrucao': valor_cadastro_colaboradores[3],
                        'estado_civil': valor_cadastro_colaboradores[4],
                        'genero_sexo': valor_cadastro_colaboradores[5],
                    }
                }
                lista_DADOS_CADASTRAIS.append(dict_DB_DADOS_CADASTRAIS[cod_colaborador])
        self.lista_funcionarios.append(lista_DADOS_CADASTRAIS)

        # 2 -------------------------------------------------------------------------------------------------------
        # ok

        dict_DB_DADOS_DOCUMENTOS_REFERENCIAS = {}
        lista_DADOS_DOCUMENTOS_REFERENCIAS = []

        for valor_documentos_ref_colaboradores in DADOS_DOCUMENTOS_POR_REFERENCIA_COLABORADORES:
            cod_colaborador = valor_documentos_ref_colaboradores[1]

            if cod_colaborador not in dict_DB_DADOS_DOCUMENTOS_REFERENCIAS:
                dict_DB_DADOS_DOCUMENTOS_REFERENCIAS[cod_colaborador] = {
                    valor_documentos_ref_colaboradores[0]: {
                        'tabela': 'VRH_EMP_TCOLCON_DOCREF',
                        'cod_colaborador': valor_documentos_ref_colaboradores[1],
                        'pis_colaborador': valor_documentos_ref_colaboradores[2],
                        'rg_colaborador': valor_documentos_ref_colaboradores[3],
                    }
                }
                lista_DADOS_DOCUMENTOS_REFERENCIAS.append(dict_DB_DADOS_DOCUMENTOS_REFERENCIAS[cod_colaborador])
        self.lista_funcionarios.append(lista_DADOS_DOCUMENTOS_REFERENCIAS)

        # 3 -------------------------------------------------------------------------------------------------------
        # ok

        dict_DB_DADOS_CADASTRO_DADOS_DIARIOS = {}
        lista_DADOS_CADASTRO_DADOS_DIARIOS = []
        for valor_cadastro_dados_diarios in DADOS_CADASTRO_DADOS_DIARIOS_COLABORADORES:
            cod_colaborador = valor_cadastro_dados_diarios[0]

            if cod_colaborador not in dict_DB_DADOS_CADASTRO_DADOS_DIARIOS:
                dict_DB_DADOS_CADASTRO_DADOS_DIARIOS[cod_colaborador] = {
                    valor_cadastro_dados_diarios[0]: {
                        'tabela': 'VRH_EMP_TCOL_VINCDIA',
                        'cod_colaborador': valor_cadastro_dados_diarios[1],
                        'data_inicio': valor_cadastro_dados_diarios[2],
                        'cod_funcao': valor_cadastro_dados_diarios[3],
                        'cod_regime_jornada_trabalho': valor_cadastro_dados_diarios[4],
                        'tipo_jornada_trabalho': valor_cadastro_dados_diarios[5],
                    }
                }
                lista_DADOS_CADASTRO_DADOS_DIARIOS.append(dict_DB_DADOS_CADASTRO_DADOS_DIARIOS[cod_colaborador])
        self.lista_funcionarios.append(lista_DADOS_CADASTRO_DADOS_DIARIOS)

        # 4 -------------------------------------------------------------------------------------------------------
        # ok

        lista_DADOS_CALCULO_FERIAS = []
        for valor_calculo_ferias in DADOS_CALCULO_FERIAS_COLABORADORES:
            dict_DB_DADOS_CALCULO_FERIAS = {
                valor_calculo_ferias[0]: {
                    'tabela': 'VRH_EMP_TFERIAS',
                    'cod_colaborador': valor_calculo_ferias[1],
                    'inicio_arquis_ferias': valor_calculo_ferias[2],
                    'cod_ferias': valor_calculo_ferias[3],
                    'inicio_gozo_ferias': valor_calculo_ferias[4],
                    'final_arquis_ferias': valor_calculo_ferias[5],
                    'final_gozo_ferias': valor_calculo_ferias[6],
                    'data_pgto_ferias': valor_calculo_ferias[7],
                    'qts_dias_de_ferias': valor_calculo_ferias[8],
                    'qts_dias_abono_ferias': valor_calculo_ferias[9],
                    'saldo_final_dias_ferias': valor_calculo_ferias[10],
                }
            }
            lista_DADOS_CALCULO_FERIAS.append(dict_DB_DADOS_CALCULO_FERIAS)
        self.lista_funcionarios.append(lista_DADOS_CALCULO_FERIAS)

        # 6 -------------------------------------------------------------------------------------------------------
        # ok

        lista_DADOS_RESISOES = []
        for valor_resisao in DADOS_TABELA_RESISOES_COLABORADORES:
            dict_DB_DADOS_RESISOES = {
                valor_resisao[0]: {
                    'tabela': 'VRH_EMP_TRESCISAO',
                    'cod_colaborador': valor_resisao[1],
                    'data_resisao': valor_resisao[2],
                    'codigo_resisao': valor_resisao[3],
                    'data_inicio_aviso_previo': valor_resisao[4],
                    'inidicador_reposicao_vaga': valor_resisao[5],
                    'dispensa_do_aviso_previo': valor_resisao[6],
                }
            }
            lista_DADOS_RESISOES.append(dict_DB_DADOS_RESISOES)
        self.lista_funcionarios.append(lista_DADOS_RESISOES)

        return self.lista_funcionarios



if __name__ == '__main__':
    LISTA_CLIENTES = os.getenv('LISTA_CLIENTES_SISTEMA').split(',')

    obj_conexao_banco = ConexaoODBCFireBirdUnico()
    banco_conectado = obj_conexao_banco.conexao_banco_dados()
    obj_busca_info_clientes = BuscaDadosBanco(banco_conectado)

    cont_j = 0
    cont_x = 0

    for cliente in LISTA_CLIENTES:
        print(cont_j, cont_x)
        dados = obj_busca_info_clientes.view_dados_bd(cliente)
        print(dados[cont_x][cont_j])
        cont_x += 1
        if cont_x >= len(dados):
            cont_j =+ 1



