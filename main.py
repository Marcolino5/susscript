from multiprocessing import Pool
from ftplib import FTP
import subprocess
import os.path as path
from types import ModuleType
import pandas as pd
import shutil
from tables.interest_rate_before_01_2022 import INTEREST_BEFORE_01_2022
from template_docs import ivr_file_template, tunep_file_template
import numpy as np
import datetime
import sys
import os

SIA_RELEVANT_FIELDS = np.array(['PA_CMP', 'PA_PROC_ID', 'PA_QTDAPR', 'PA_VALAPR'])
SIH_RELEVANT_FIELDS = np.array(['SP_AA', 'SP_MM','SP_ATOPROF', 'SP_QTD_ATO', 'SP_VALATO'])

def br_money(value: float) -> str:
    return f"{value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

# Class responsible for defining, sharing and creating the directories used in the program
class ProjPaths:
    
    SIA_DOWNLOAD_DIR: str = ""
    SIH_DOWNLOAD_DIR: str = ""
    BINARIES_DIR: str = ""
    SCRIPTS_DIR: str = ""
    SIA_DBFS_DIR: str = ""
    SIH_DBFS_DIR: str = ""
    SIA_CSVS_DIR: str = ""
    SIH_CSVS_DIR: str = ""
    UNITED_CSV_DIR: str = ""
    TABLES_DIR: str = ""
    SELIC_TABLE_PATH: str = ""
    DBF2CSV_PATH: str = ""
    BLAST_DBF_PATH: str = ""
    RESULTS_DIR: str = ""
    TOTAL_REPORT_PATH: str = ""
    MONTH_REPORT_PATH: str = ""
    YEAR_REPORT_PATH: str = ""
    LATEX_DIR: str = ""
    LATEX_FILE_PATH: str = ""
    SIA_TUNEP_TABLE_PATH: str = ""
    SIH_TUNEP_TABLE_PATH: str = ""
    PROC_TABLE_PATH: str = ""

    @staticmethod
    def init():
        ProjPaths.define_paths()
        ProjPaths.empty_dirs()
        ProjPaths.create_paths()

    @staticmethod
    def define_paths():
        DATA_ROOT = os.environ.get("DATA_ROOT", "/data")
        CODE_ROOT = path.split(path.join(os.getcwd(), sys.argv[0]))[0]

        ProjPaths.DATA_ROOT = DATA_ROOT
        ProjPaths.SCRIPTS_DIR = CODE_ROOT
    
        # DATA (Volume)
        ProjPaths.SIA_DOWNLOAD_DIR = path.join(DATA_ROOT, "sia_download")
        ProjPaths.SIH_DOWNLOAD_DIR = path.join(DATA_ROOT, "sih_download")
        ProjPaths.SIA_DBFS_DIR = path.join(DATA_ROOT, "sia_dbf")
        ProjPaths.SIH_DBFS_DIR = path.join(DATA_ROOT, "sih_dbf")
        ProjPaths.SIA_CSVS_DIR = path.join(DATA_ROOT, "sia_csv")
        ProjPaths.SIH_CSVS_DIR = path.join(DATA_ROOT, "sih_csv")
        ProjPaths.UNITED_CSV_DIR = path.join(DATA_ROOT, "united_csv")
    
        # CODE (Ephemeral is fine)
        ProjPaths.BINARIES_DIR = path.join(CODE_ROOT, "bin")
        ProjPaths.TABLES_DIR = path.join(CODE_ROOT, "tables")
        ProjPaths.LATEX_DIR = path.join(CODE_ROOT, "latex")
        ProjPaths.RESULTS_DIR = path.join(CODE_ROOT, "results")
    
        # Files
        ProjPaths.SELIC_TABLE_PATH = path.join(ProjPaths.TABLES_DIR, "selic.csv")
        ProjPaths.DBF2CSV_PATH = path.join(ProjPaths.BINARIES_DIR, "DBF2CSV")
        ProjPaths.BLAST_DBF_PATH = path.join(ProjPaths.BINARIES_DIR, "BLAST_DBF")
    
        ProjPaths.MONTH_REPORT_PATH = path.join(ProjPaths.RESULTS_DIR, "month.csv")
        ProjPaths.YEAR_REPORT_PATH = path.join(ProjPaths.RESULTS_DIR, "year.csv")
        ProjPaths.TOTAL_REPORT_PATH = path.join(ProjPaths.RESULTS_DIR, "total.csv")
    
        ProjPaths.LATEX_FILE_PATH = path.join(ProjPaths.LATEX_DIR, "laudo.tex")
        ProjPaths.SIA_TUNEP_TABLE_PATH = path.join(ProjPaths.TABLES_DIR, "tabela_tunep_sia.csv")
        ProjPaths.SIH_TUNEP_TABLE_PATH = path.join(ProjPaths.TABLES_DIR, "tabela_tunep_sih.csv")
        ProjPaths.PROC_TABLE_PATH = path.join(ProjPaths.TABLES_DIR, "desc_procedimento.csv")


    @staticmethod
    def create_paths():
        ProjPaths.create_downloads_dir()
        ProjPaths.create_binaries_dir()
        ProjPaths.create_dbfs_dir()
        ProjPaths.create_csvs_dir()
        ProjPaths.create_united_csv_dir()
        ProjPaths.create_tables_dir()
        ProjPaths.create_results_dir()
        ProjPaths.create_dbf2csv()
        ProjPaths.create_blast_dbf()
        ProjPaths.create_latex_dir()


    @staticmethod
    def empty_dirs():
        ProjPaths.empty_latex_dir()
        ProjPaths.empty_results_dir()
        if not os.path.exists(ProjPaths.DATA_ROOT):
            os.makedirs(ProjPaths.DATA_ROOT)
            return
        for item in os.listdir(ProjPaths.DATA_ROOT):
            item_path = os.path.join(ProjPaths.DATA_ROOT, item)
    
            if os.path.isfile(item_path) or os.path.islink(item_path):
                os.remove(item_path)
            else:
                shutil.rmtree(item_path)


    @staticmethod
    def create_tables_dir():
        if not os.path.exists(ProjPaths.TABLES_DIR):
            os.makedirs(ProjPaths.TABLES_DIR)


    @staticmethod
    def create_results_dir():
        if not os.path.exists(ProjPaths.RESULTS_DIR):
            os.makedirs(ProjPaths.RESULTS_DIR)


    @staticmethod
    def create_binaries_dir():
        if not os.path.exists(ProjPaths.BINARIES_DIR):
            os.makedirs(ProjPaths.BINARIES_DIR)


    @staticmethod
    def create_latex_dir():
        if not os.path.exists(ProjPaths.LATEX_DIR):
            os.makedirs(ProjPaths.LATEX_DIR)


    @staticmethod
    def create_downloads_dir():
        if not os.path.exists(ProjPaths.SIA_DOWNLOAD_DIR):
            os.makedirs(ProjPaths.SIA_DOWNLOAD_DIR)

        if not os.path.exists(ProjPaths.SIH_DOWNLOAD_DIR):
            os.makedirs(ProjPaths.SIH_DOWNLOAD_DIR)


    @staticmethod
    def create_dbfs_dir():
        if not os.path.exists(ProjPaths.SIA_DBFS_DIR):
            os.makedirs(ProjPaths.SIA_DBFS_DIR)

        if not os.path.exists(ProjPaths.SIH_DBFS_DIR):
            os.makedirs(ProjPaths.SIH_DBFS_DIR)


    @staticmethod
    def create_csvs_dir():
        if not os.path.exists(ProjPaths.SIA_CSVS_DIR):
            os.makedirs(ProjPaths.SIA_CSVS_DIR)

        if not os.path.exists(ProjPaths.SIH_CSVS_DIR):
            os.makedirs(ProjPaths.SIH_CSVS_DIR)

    @staticmethod
    def create_united_csv_dir():
        if not os.path.exists(ProjPaths.UNITED_CSV_DIR):
            os.makedirs(ProjPaths.UNITED_CSV_DIR)

    
    @staticmethod
    def create_blast_dbf():
    
        if not os.path.exists(ProjPaths.BLAST_DBF_PATH):
            os.chdir(ProjPaths.BINARIES_DIR)
    
            # 1️⃣ Clona repo (mantido por consistência estrutural)
            subprocess.run(
                ["git", "clone", "https://github.com/danicat/read.dbc.git"],
                capture_output=True,
                text=True,
                check=True
            )
    
            os.chdir("read.dbc")
    
            # 2️⃣ Cria wrapper script
            with open("dbc2csv.R", "w") as f:
                f.write("""#!/usr/bin/env Rscript

# 🔥 FORCE LIB PATH
.libPaths(c("/usr/local/lib/R/site-library", .libPaths()))
              
args <- commandArgs(trailingOnly=TRUE)

input <- args[1]
output <- args[2]
cnes <- args[3]
sistema <- args[4]

library(read.dbc)

df <- read.dbc(input)

if (cnes != "TODOS") {
  if ("CNES" %in% colnames(df)) {
    df <- df[df$CNES == cnes, ]
  }
}

write.table(
  df,
  file = output,
  sep = ";",
  dec = ".",
  row.names = FALSE,
  col.names = TRUE,
  fileEncoding = "UTF-8"
)
""")
    
        # 3️⃣ "Build" (equivalente ao make antigo)
        subprocess.run(
            ["chmod", "+x", "dbc2csv.R"],
            capture_output=True,
            text=True,
            check=True
        )
    
        # 4️⃣ Move como BLAST_DBF (mesmo nome de antes)
        shutil.move(
            "dbc2csv.R",
            path.join(ProjPaths.BINARIES_DIR, "BLAST_DBF")
        )
    
        # 5️⃣ Volta e limpa (mesma lógica do antigo)
        os.chdir(ProjPaths.BINARIES_DIR)
        shutil.rmtree("read.dbc")
    
        # 6️⃣ Volta pro scripts dir
        os.chdir(ProjPaths.SCRIPTS_DIR)


    @staticmethod
    def create_dbf2csv():
        if not os.path.exists(ProjPaths.BLAST_DBF_PATH):
            os.chdir(ProjPaths.BINARIES_DIR)
            subprocess.run(["git", "clone", "https://github.com/rmxvrelease/dbc2csv.git"],
                capture_output=True,
                text=True,
                check=True
            )

            source_file = path.join("dbc2csv", "DBF2CSV.c")

            subprocess.run(["gcc", "-o", "DBF2CSV", source_file],
                capture_output=True,
                text=True,
                check=True
            )

            shutil.rmtree("dbc2csv")

            os.chdir(ProjPaths.SCRIPTS_DIR)


    @staticmethod
    def empty_downloads_dir():
        files = os.listdir(ProjPaths.SIA_DOWNLOAD_DIR)
        for file in files:
            try:
                os.remove(path.join(ProjPaths.SIA_DOWNLOAD_DIR, file))
                continue
            except Exception as e:
                print(f"could not delete {file} from the sia download dir:\n {str(e)}")

        files = os.listdir(ProjPaths.SIH_DOWNLOAD_DIR)
        for file in files:
            try:
                os.remove(path.join(ProjPaths.SIH_DOWNLOAD_DIR, file))
                continue
            except Exception as e:
                print(f"could not delete {file} from the sih download dir:\n {str(e)}")
        

    @staticmethod
    def empty_latex_dir():
        os.makedirs(ProjPaths.LATEX_DIR, exist_ok=True)
        files = os.listdir(ProjPaths.LATEX_DIR)
        for file in files:
            try:
                os.remove(path.join(ProjPaths.LATEX_DIR, file))
                continue
            except Exception as e:
                print(f"could not delete {file} from the latex dir:\n {str(e)}")


    @staticmethod
    def empty_dbfs_dir():
        files = os.listdir(ProjPaths.SIA_DBFS_DIR)
        for file in files:
            try:
                os.remove(path.join(ProjPaths.SIA_DBFS_DIR, file))
                continue
            except Exception as e:
                print(f"could not delete {file} from the dbf dir:\n {str(e)}")

        files = os.listdir(ProjPaths.SIH_DBFS_DIR)
        for file in files:
            try:
                os.remove(path.join(ProjPaths.SIH_DBFS_DIR, file))
                continue
            except Exception as e:
                print(f"could not delete {file} from the dbf dir:\n {str(e)}")

    @staticmethod
    def empty_csvs_dir():
        files = os.listdir(ProjPaths.SIA_CSVS_DIR)
        for file in files:
            try:
                os.remove(path.join(ProjPaths.SIA_CSVS_DIR, file))
                continue
            except Exception as e:
                print(f"could not delete {file} from the csv dir:\n {str(e)}")

        files = os.listdir(ProjPaths.SIH_CSVS_DIR)
        for file in files:
            try:
                os.remove(path.join(ProjPaths.SIH_CSVS_DIR, file))
                continue
            except Exception as e:
                print(f"could not delete {file} from the csv dir:\n {str(e)}")

    @staticmethod
    def empty_results_dir():
        os.makedirs(ProjPaths.RESULTS_DIR, exist_ok=True)
        files = os.listdir(ProjPaths.RESULTS_DIR)
        for file in files:
            try:
                os.remove(path.join(ProjPaths.RESULTS_DIR, file))
            except Exception as e:
                print(f"could not delete {file} from the results dir:\n {str(e)}")


    @staticmethod
    def test():
        print('\nTESTE DOS LOCAIS DO PROGRAMA:')
        print('binaries dir: ', ProjPaths.BINARIES_DIR)
        print('blast dbf: ', ProjPaths.BLAST_DBF_PATH)
        print('download sia dir: ', ProjPaths.SIA_DOWNLOAD_DIR)
        print('download sih dir: ', ProjPaths.SIH_DOWNLOAD_DIR)

class ProjConfigs:
    N_OF_THREADS = 8


class Date:
    def __init__(self, month: int, year: int):
        if not (1 <= month <= 12):
            raise ValueError("Month must be between 1 and 12")
        if year < 50:
            year += 2000
        elif year < 99:
            year += 1900

        self.year = year
        self.month = month

    @staticmethod
    def from_string(date_str: str):
            month, year = date_str.split('-')
            return Date(int(month), int(year))

    @staticmethod
    def first_day_of_previous_month() -> str:
        today = datetime.date.today()
        if (today.month == 1):
            return f'01/12/{today.year-1}'
        else:
            return f'01/{today.month-1}/{today.year}'

    @staticmethod
    def from_sus_file_name(file_name: str):
        f_name_no_path = path.split(file_name)[-1]
        year = int(f_name_no_path[4:6])
        month = int(f_name_no_path[6:8])
        return Date(month, year)


    def __lt__(self, other):
        if self.year == other.year:
            return self.month < other.month
        return self.year < other.year

    def __eq__(self, other):
        return self.year == other.year and self.month == other.month

    def __gt__(self, other):
        if self.year == other.year:
            return self.month > other.month
        return self.year > other.year

    def __str__(self):
        return f"{self.month:02d}-{self.year:04d}"


class ProjParams:
    CNES: str = "0000000"
    STATE: str = "AC"
    SYSTEM:str = "SIA"
    METHOD: str = "IVR"
    START: Date = Date(1, 2023)
    END: Date = Date(12, 2023)
    END_INTEREST: Date = Date(4, 2025)
    DATA_CIACAO: Date = Date(4, 2025)
    CIDADE: str = "CITY"
    RAZAO_SOCIAL: str = "Razão Social"
    NOME_FANTASIA: str = "Nome Fantasia"
    NUMERO_PROCESSO: str = "Número Processo"
    CNPJ: str = ""

    @staticmethod
    def init():
        # Solução temporária, gambiarra; Mudar. SYSTEM deve ser argv[4]
        if sys.argv[1] == 'test':
            ProjParams.METHOD = 'TEST'
            ProjParams.CNES = sys.argv[2]
            ProjParams.STATE = sys.argv[3]
            ProjParams.SYSTEM = "BOTH"
            ProjParams.METHOD = sys.argv[5]
            ProjParams.START = Date.from_string(sys.argv[6])
            ProjParams.END = Date.from_string(sys.argv[7])
            ProjParams.END_INTEREST = Date.from_string(sys.argv[8])
            ProjParams.DATA_CIACAO = Date.from_string(sys.argv[9])
            ProjParams.CIDADE = sys.argv[10]
            ProjParams.RAZAO_SOCIAL = sys.argv[11]
            ProjParams.NOME_FANTASIA = sys.argv[12]
            ProjParams.NUMERO_PROCESSO = sys.argv[13]
            return
        
        if sys.argv[1] == 'raw':
            ProjParams.METHOD = 'RAW'
            ProjParams.CNES = sys.argv[2]
            ProjParams.STATE = sys.argv[3]
            ProjParams.SYSTEM = "BOTH"
            ProjParams.START = Date.from_string(sys.argv[5])
            ProjParams.END = Date.from_string(sys.argv[6])
            return

        ProjParams.CNES = sys.argv[1]
        ProjParams.STATE = sys.argv[2]
        ProjParams.SYSTEM = "BOTH"
        ProjParams.METHOD = sys.argv[4]
        ProjParams.START = Date.from_string(sys.argv[5])
        ProjParams.END = Date.from_string(sys.argv[6])
        ProjParams.END_INTEREST = Date.from_string(sys.argv[7])
        ProjParams.DATA_CIACAO = Date.from_string(sys.argv[8])
        ProjParams.CIDADE = sys.argv[9]
        ProjParams.RAZAO_SOCIAL = sys.argv[10]
        ProjParams.NOME_FANTASIA = sys.argv[11]
        ProjParams.NUMERO_PROCESSO = sys.argv[12]

        # Verifica se temos o 13º argumento (o CNPJ)
        if len(sys.argv) > 13:
            ProjParams.CNPJ = sys.argv[13]
            print(f"DEBUG: CNPJ CAPTURADO COM SUCESSO: {ProjParams.CNPJ}")

    @staticmethod

    @staticmethod
    def get_start_date():
        return ProjParams.START

    @staticmethod
    def get_end_date():
        return ProjParams.END

    @staticmethod
    def set_start_date(date: Date):
        ProjParams.START = Date.from_string(str(date))

    @staticmethod
    def set_end_date(date: Date):
        ProjParams.END = Date.from_string(str(date))

    @staticmethod
    def get_cnes():
        return ProjParams.CNES

    @staticmethod
    def get_state():
        return ProjParams.STATE

    @staticmethod
    def get_system():
        return ProjParams.SYSTEM

    @staticmethod
    def test():
        print('\nTESTE DOS PARÂMETROS DO PROGRAMA:')
        print('cnes: ', ProjParams.CNES)
        print('estado: ', ProjParams.STATE)
        print('inicio: ', ProjParams.START)
        print('fim: ', ProjParams.END)
        print('sistema: ', ProjParams.SYSTEM)
        print('fim correcão', ProjParams.END_INTEREST)
        print('razão social: ', ProjParams.RAZAO_SOCIAL)
        print('nome fantasia: ', ProjParams.NOME_FANTASIA)
        print('numero processo', ProjParams.NUMERO_PROCESSO)
        print('cidade: ', ProjParams.CIDADE)
        print('método: ', ProjParams.METHOD)
        print('data citação', ProjParams.DATA_CIACAO)
        print('CNPJ:', ProjParams.CNPJ)




class InterestRate:
    SELIC: np.ndarray

    @staticmethod
    def load_selic():
        end_time_str = Date.first_day_of_previous_month()
        try: selic = pd.read_csv(f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.4390/dados?formato=csv&dataInicial=01/12/2021&dataFinal={end_time_str}", sep=";")
        except: selic = pd.read_csv(ProjPaths.SELIC_TABLE_PATH)

        selic['valor'] = (selic['valor'].astype(str).str.replace(",", ".").astype(float) / 100)
        InterestRate.SELIC = selic['valor'].__array__()
        selic.to_csv(ProjPaths.SELIC_TABLE_PATH, index=False)


    @staticmethod
    def cumulative_selic(s: Date, e: Date) -> float:
        '''WARNING: JUROS SIMPLES'''
        if (s < Date.from_string('01-2022')):
            s = Date.from_string('01-2022')

        s_months_since_12_2021 = (s.year - 2021)*12 + s.month - 12
        e_months_since_12_2021 = (e.year - 2021)*12 + e.month - 12

        cumulative_rate = InterestRate.SELIC[s_months_since_12_2021-1:e_months_since_12_2021].sum()

        return cumulative_rate


    @staticmethod
    def rate_until_01_2022(s: Date) -> float:
        '''Calcula o juros da data "s" até a 01-2022'''
        s_month_until_01_2022 = (s.year - 2022)*12 + s.month - 1

        if (s_month_until_01_2022 >= 0):
            return(1.0)

        return INTEREST_BEFORE_01_2022[s_month_until_01_2022]

    @staticmethod
    def complete_rate(s: Date, e: Date) -> float:
        rate_before_01_2022 = InterestRate.rate_until_01_2022(s)
        rate_after_01_2022 = InterestRate.cumulative_selic(s, e)
        return rate_before_01_2022 * (1.0 + rate_after_01_2022)

    @staticmethod
    def complete_rate_split(s: Date, e: Date) -> tuple[float, float, float]:
        rate_before_01_2022 = InterestRate.rate_until_01_2022(s)
        rate_after_01_2022 = InterestRate.cumulative_selic(s, e)
        return (rate_before_01_2022,
                1.0+rate_after_01_2022,
                rate_before_01_2022 * (1.0 + rate_after_01_2022))

    @staticmethod
    def show_selic():
        print('\nSELIC:')
        print(InterestRate.SELIC)


class Downloads:
    # download a file from the ftp data sus given it's path inside the server
    @staticmethod
    def download_file(file: str):
        PREFIX_LOCATION = {
        'PA': ProjPaths.SIA_DOWNLOAD_DIR,
        'SP': ProjPaths.SIH_DOWNLOAD_DIR 
        }
        file_name = path.split(file)[-1]
        file_prefix = file_name[:2]
        dowload_dir_path = PREFIX_LOCATION[file_prefix]
        local_file_path = path.join(dowload_dir_path, file_name)

        ftp = FTP("ftp.datasus.gov.br")
        ftp.login()

        with open(local_file_path, 'wb') as f:
            ftp.retrbinary(f"RETR {file}", f.write)

        print(f"Downloaded {file_name}")
        ftp.quit()

    @staticmethod
    def download(files: list[str]):
        with Pool(processes=ProjConfigs.N_OF_THREADS) as p:
            file_sets: list[list[str]] = [[]]
            f_index = 0
            while (f_index < len(files)):
                if (len(file_sets[-1]) < 6):
                    file_sets[-1].append(files[f_index])
                    f_index+=1
                else:
                    file_sets.append([files[f_index]])
                    f_index+=1

            p.map(Downloads.download_many, file_sets)

    @staticmethod
    def download_many(files: list[str]):
    
        PREFIX_LOCATION = {
            'PA': ProjPaths.SIA_DOWNLOAD_DIR,
            'SP': ProjPaths.SIH_DOWNLOAD_DIR,
            'RD': ProjPaths.SIH_DOWNLOAD_DIR
        }
    
        for file in files:
            file_name = path.split(file)[-1]
            file_prefix = file_name[:2]
            download_dir = PREFIX_LOCATION[file_prefix]
            local_file_path = path.join(download_dir, file_name)
    
            for attempt in range(3):
                try:
                    ftp = FTP("ftp.datasus.gov.br")
                    ftp.login()
    
                    # 🔥 PEGA TAMANHO REMOTO
                    remote_size = ftp.size(file)
    
                    # 🔥 BAIXA O ARQUIVO
                    with open(local_file_path, 'wb') as f:
                        ftp.retrbinary(f"RETR {file}", f.write)
    
                    ftp.quit()
    
                    # 🔥 VALIDA TAMANHO
                    local_size = os.path.getsize(local_file_path)
    
                    print(f"[DEBUG] {file_name}: {local_size}/{remote_size}")
    
                    if remote_size is not None and local_size != remote_size:
                        print(f"[WARN] Size mismatch (tentativa {attempt+1}): {file_name}")
                        time.sleep(1)
                        continue  # tenta de novo
    
                    print(f"Downloaded {file_name}")
                    break  # sucesso
    
                except Exception as e:
                    print(f"[ERROR] tentativa {attempt+1} falhou para {file_name}: {e}")
                    time.sleep(1)
    
                    if attempt == 2:
                        raise Exception(f"Falha definitiva ao baixar {file_name}")

    @staticmethod
    def find_files(sistema: str, estado: str, inicio: Date, fim: Date):
        SEARCH_RREFIXES = {
        'SIA': 'PA',
        'SIH': 'SP'
        }

        SEARCH_DIRS = {
            'SIA': ["/dissemin/publicos/SIASUS/199407_200712/Dados",
                    "/dissemin/publicos/SIASUS/200801_/Dados"],
            'SIH': ["/dissemin/publicos/SIHSUS/199201_200712/Dados",
                    "/dissemin/publicos/SIHSUS/200801_/Dados"]
        }

        ftp = FTP("ftp.datasus.gov.br")
        ftp.login()

        search_target = SEARCH_DIRS[sistema]
        search_prefix = SEARCH_RREFIXES[sistema]

        files: list[str] = []


        for dir in search_target:
            def append_to_file(file: str):
                file = file.split(' ')[-1]

                if file[0:2] != search_prefix and not (sistema == 'SIH' and file[0:2] == 'RD'):
                    return

                if file[2:4] != estado:
                    return

                try:
                    date = Date.from_string(file[6:8] + "-" + file[4:6])
                except:
                    return

                if date < inicio or fim < date:
                    return

                files.append(path.join(dir, file))

            ftp.cwd(dir)
            ftp.retrlines("LIST", append_to_file)

        ftp.quit()

        return files


class Conversions:
    @staticmethod
    def convert_files():
        sia_files = [
            path.join(ProjPaths.SIA_DOWNLOAD_DIR, f)
            for f in os.listdir(ProjPaths.SIA_DOWNLOAD_DIR)
            if f.endswith('.dbc')
        ]
    
        sih_files = [
            path.join(ProjPaths.SIH_DOWNLOAD_DIR, f)
            for f in os.listdir(ProjPaths.SIH_DOWNLOAD_DIR)
            if f.endswith('.dbc')
        ]
    
        path_to_files = sia_files + sih_files
    
        if not path_to_files:
            print("No DBC files to convert.")
            return
    
        with Pool(processes=ProjConfigs.N_OF_THREADS) as p:
            p.map(Conversions.convert_file_to_csv, path_to_files)

    @staticmethod
    def convert_file_to_csv(file: str):
    
        PREFIX_SYSTEM = {
            'PA': 'SIA',
            'SP': 'SIH',
            'RD': 'SIH'
        }
    
        PREFIX_CSV_DIR = {
            'PA': ProjPaths.SIA_CSVS_DIR,
            'SP': ProjPaths.SIH_CSVS_DIR,
            'RD': ProjPaths.SIH_CSVS_DIR
        }
    
        try:
            filename = path.split(file)[-1]
            prefix = filename[0:2]
    
            print(f"filename: {filename}, file: {file}, prefix: {prefix}")
            
            cnes = ProjParams.get_cnes()
    
            try:
                if Date.from_sus_file_name(filename).year < 2008:
                    cnes = "TODOS"
            except:
                pass
    
            csv_file_name = filename.replace(".dbc", ".csv")
            csv_dir = PREFIX_CSV_DIR[prefix]
            sistema = PREFIX_SYSTEM[prefix]
    
            os.makedirs(csv_dir, exist_ok=True)
    
            csv_file_path = path.join(csv_dir, csv_file_name)
    
            # 🔥 NOVO: DBC → CSV direto com R
            result = subprocess.run(
                [
                    ProjPaths.BLAST_DBF_PATH,  # agora é o wrapper R
                    file,
                    csv_file_path,
                    cnes,
                    sistema
                ],
                capture_output=True,
                text=True
            )
    
            print(f"\n[DEBUG] Converting: {file}")
            print(f"[DEBUG] Return code: {result.returncode}")
    
            print("[STDOUT]")
            print(result.stdout if result.stdout else "(empty)")
            
            print("[STDERR]")
            print(result.stderr if result.stderr else "(empty)")
    
            if result.returncode != 0:
                raise Exception(f"DBC conversion failed: {file}")
    
            if not os.path.exists(csv_file_path):
                raise Exception(f"CSV not created: {file}")
    
            # 🔥 DELETE DBC (mantém igual)
            if path.exists(file):
                os.remove(file)
    
        except Exception as e:
            print(f"Erro processando {file}: {e}")

    @staticmethod
    def unite_files(system: str):
        PREFIX_CSV_DIR = {
            'SIA': ProjPaths.SIA_CSVS_DIR,
            'SIH': ProjPaths.SIH_CSVS_DIR
        }
    
        csv_dir = PREFIX_CSV_DIR[system]
        output_path = path.join(ProjPaths.UNITED_CSV_DIR, f'{system}.csv')
    
        os.makedirs(ProjPaths.UNITED_CSV_DIR, exist_ok=True)
    
        csv_files = [
            path.join(csv_dir, f)
            for f in os.listdir(csv_dir)
            if f.endswith('.csv')
        ]
    
        if not csv_files:
            print(f"No CSV files found for {system}.")
            return
    
        strategies = [
            {'sep': None, 'engine': 'python'},
            {'sep': ',', 'engine': 'c'},
            {'sep': ';', 'engine': 'c'}
        ]
    
        first_write = True
        found_valid_data = False
    
        for file in csv_files:
            df = pd.DataFrame()
    
            # Try reading with different strategies
            for strat in strategies:
                try:
                    temp_df = pd.read_csv(
                        file,
                        encoding='latin1',
                        dtype=str,
                        **strat
                    )
                    if not temp_df.empty and len(temp_df.columns) > 1:
                        df = temp_df
                        break
                except Exception:
                    continue
    
            if df.empty:
                continue
    
            try:
                df.columns = [c.strip().upper() for c in df.columns]
    
                df.to_csv(
                    output_path,
                    mode='w' if first_write else 'a',
                    header=first_write,
                    index=False
                )
    
                first_write = False
                found_valid_data = True
    
                del df  # free memory aggressively
    
            except Exception as e:
                print(f"Erro ao processar {file}: {e}")
    
        if not found_valid_data:
            print(f"No valid entries found for {system}.")
            return
    
        print(f"{system} files united (streaming mode)")

class Tunep:
    TABELA_DE_CONVERSAO_SIA: pd.DataFrame
    TABELA_DE_CONVERSAO_SIH: pd.DataFrame
    TABELA_GERAL: pd.DataFrame = None # tabela do rep do rafa

    _TYPE_MAPPING = {'SIA': 'A', 'SIH': 'H'}


    @staticmethod
    def load_tunep():
        # add o 'Descricao' na lista e no dtype em sia e sih
        sia_df = pd.read_csv(ProjPaths.SIA_TUNEP_TABLE_PATH, decimal=',',  thousands='.', usecols=np.array(['CO_PROCEDIMENTO', 'ValorTUNEP', 'TP_PROCEDIMENTO', 'Descricao']), dtype={'CO_PROCEDIMENTO': str, 'Descricao': str})

        sia_df['CO_PROCEDIMENTO'] = sia_df['CO_PROCEDIMENTO'].str.strip().str.zfill(10)
        sia_df['ValorTUNEP'] = pd.to_numeric(sia_df['ValorTUNEP'], errors='coerce')
        Tunep.TABELA_DE_CONVERSAO_SIA = sia_df.set_index('CO_PROCEDIMENTO').copy()

        sih_df = pd.read_csv(ProjPaths.SIH_TUNEP_TABLE_PATH, decimal=',',  thousands='.', usecols=np.array(['CO_PROCEDIMENTO', 'ValorTUNEP', 'TP_PROCEDIMENTO', 'Descricao']), dtype={'CO_PROCEDIMENTO': str, 'Descricao': str})

        sih_df['CO_PROCEDIMENTO'] = sih_df['CO_PROCEDIMENTO'].str.strip().str.zfill(10)
        sih_df['ValorTUNEP'] = pd.to_numeric(sih_df['ValorTUNEP'], errors='coerce')
        Tunep.TABELA_DE_CONVERSAO_SIH = sih_df.set_index('CO_PROCEDIMENTO').copy()

        try:
            # Lê apenas código e nome da tabela nova
            geral_df = pd.read_csv(ProjPaths.PROC_TABLE_PATH, usecols=['CO_PROCEDIMENTO', 'NO_PROCEDIMENTO'], dtype=str)
            geral_df['CO_PROCEDIMENTO'] = geral_df['CO_PROCEDIMENTO'].str.strip().str.zfill(10)
            Tunep.TABELA_GERAL = geral_df.set_index('CO_PROCEDIMENTO').copy()
            print("Tabela geral de procedimentos carregada com sucesso.")
        except Exception as e:
            print(f"Aviso: Não foi possível carregar desc_procedimento.csv: {e}")
            Tunep.TABELA_GERAL = pd.DataFrame()
            if os.path.exists('tables'):
                raise Exception("Conteúdo de 'tables':", os.listdir('tables'))
            else:
                raise Exception("'tables' não existe, CWD:", os.getcwd())


    @staticmethod
    def _get_base_value(code: str, procedure_type: str) -> float|None:
        TABLE_MAPPING = {'SIA': Tunep.TABELA_DE_CONVERSAO_SIA, 'SIH': Tunep.TABELA_DE_CONVERSAO_SIH}
        try: row = TABLE_MAPPING[procedure_type].loc[code]
        except: return None
        # Correção para evitar erro se retornar DataFrame em vez de Series
        if isinstance(row, pd.DataFrame):
             found_value = row['ValorTUNEP'].iloc[0]
        else:
             found_value = row['ValorTUNEP']
             
        return float(found_value)


    @staticmethod
    def getValTunep(code: str, procedure_type: str, quantity: int, procedure_value: float) -> float|None:
        base_tunep_value = Tunep._get_base_value(code, procedure_type)
        if base_tunep_value is not None:
            final_value = (quantity * base_tunep_value) - procedure_value
            return final_value
        return None

    @staticmethod
    def get_description(code: str, system_type: str) -> str:
        #Tenta nas tabelas antigas (SIA/SIH)
        if system_type == 'Internação' or system_type == 'SIH': 
            table = Tunep.TABELA_DE_CONVERSAO_SIH
        else: 
            table = Tunep.TABELA_DE_CONVERSAO_SIA
        
        code_safe = str(code).strip().zfill(10)

        try:
            #Tenta buscar na tabela principal
            row = table.loc[code_safe]
            if isinstance(row, pd.DataFrame):
                return str(row['Descricao'].iloc[0])
            return str(row['Descricao'])
            
        except:
            #Se falhar, tenta na TABELA GERAL
            if Tunep.TABELA_GERAL is not None and not Tunep.TABELA_GERAL.empty:
                try:
                    row_geral = Tunep.TABELA_GERAL.loc[code_safe]
                    if isinstance(row_geral, pd.DataFrame):
                        return str(row_geral['NO_PROCEDIMENTO'].iloc[0])
                    return str(row_geral['NO_PROCEDIMENTO'])
                except:
                    pass # Falhou nas duas
            
            return "Descrição não encontrada"

class MonthInfo:
    def __init__(self, when: Date, method: str, src: str, expected: float, got: float, rates: tuple[float, float, float], procedimentos: list = None) -> None:
        '''Importante: Os rates são divididos em nos seguintes 3 valores: taxa antes de 01-2022, taxa a partir de 01-2022 e compsição das duas taxas.'''
        self.when = when
        self.expected = expected
        self.got = got
        self.rates = rates
        self.method = method
        self.src = src
        self.procedimentos = procedimentos if procedimentos is not None else [] # Salva a lista

    @classmethod
    def empty(cls, when: Date, method: str, rates: tuple[float, float, float]):
        return cls(when, method, 'EMPTY', 0.0, 0.0, rates, [])


    def add_expect(self, src: str, expected: float):
        if self.src != src:
            if self.src == 'EMPTY':
                self.src = src
            self.src = 'BOTH'

        self.expected += expected


    def add_got(self, src: str, got: float):
        if self.src != src:
            if self.src == 'EMPTY':
                self.src = src
            self.src = 'BOTH'

        self.got += got

    def add_got_exp(self, src: str, got: float, expected: float, procedimentos: list = None):
        if self.src != src:
            if self.src == 'EMPTY':
                self.src = src
            self.src = 'BOTH'

        self.got += got
        self.expected += expected

        if procedimentos:
            self.procedimentos.extend(procedimentos)

    def debt_then(self) -> float:
        return (self.expected - self.got)

    def debt_now(self) -> float:
        return ((self.expected - self.got) * self.rates[2])

    def __str__(self):
        return f'''\nwhen: {self.when}
        got: {self.got}
        expected: {self.expected}
        debt then: {self.debt_then()}
        rate: {self.rates[2]}
        '''

class YearInfo:
    def __init__(self, year: int):
        self.when = year
        self.diff_then = 0.0
        self.diff_now = 0.0
        self.val_correcao = 0.0


    def add_month(self, m: MonthInfo):
        print(f"Diff now: {m.debt_now}")
        self.diff_then += m.debt_then()
        self.diff_now += m.debt_now()
        self.val_correcao += m.debt_now() - m.debt_then()


class TotalInfo:
    def __init__(self) -> None:
        self.diff_then = 0.0
        self.diff_now = 0.0
        self.val_correcao = 0.0

    def add_month(self, m: MonthInfo):
        self.diff_then += m.debt_then()
        self.diff_now += m.debt_now()
        self.val_correcao += m.debt_now() - m.debt_then()


class LegacyMatcher:
    # Cache para não ler o arquivo toda hora
    REF_SIA = None
    REF_SIH = None

    @staticmethod
    def load_references():
        """Carrega as tabelas de referência na memória uma única vez."""
        if LegacyMatcher.REF_SIA is not None and LegacyMatcher.REF_SIH is not None:
            return

        print("   [LegacyMatcher] Carregando tabelas de referência (Modo Compatibilidade)...")
        
        # Caminhos dos arquivos
        path_sia = path.join("tables", "antigos", ProjParams.STATE, f"ref_sia_{ProjParams.STATE.lower()}.csv")
        path_sih = path.join("tables", "antigos", ProjParams.STATE, f"ref_sih_{ProjParams.STATE.lower()}.csv")

        # Configuração de Leitura:
        # header=2 -> Pula as 2 primeiras linhas de lixo (TabWin) e pega o cabeçalho na linha 3
        # index_col=0 -> Usa a primeira coluna (suja com CNPJ+Nome) como índice para busca
        try:
            if path.exists(path_sia):
                LegacyMatcher.REF_SIA = pd.read_csv(path_sia, sep=None, engine='python', header=2, index_col=0, dtype=str)
            else:
                print(f"   [AVISO] Tabela SIA não encontrada: {path_sia}")

            if path.exists(path_sih):
                LegacyMatcher.REF_SIH = pd.read_csv(path_sih, sep=None, engine='python', header=2, index_col=0, dtype=str)
            else:
                print(f"   [AVISO] Tabela SIH não encontrada: {path_sih}")
                
        except Exception as e:
            print(f"   [ERRO CRÍTICO] Falha ao carregar tabelas REF: {e}")

    @staticmethod
    def get_expected_total(system: str, date_obj: Date, identifier_cnpj: str) -> float:
        # Garante que as tabelas estejam carregadas
        LegacyMatcher.load_references()
        
        if system == 'SIA': 
            df = LegacyMatcher.REF_SIA
        else: 
            df = LegacyMatcher.REF_SIH
            
        if df is None: 
            print("   [ERRO] Tabela de referência não carregada.")
            return 0.0

        # Deixa apenas números: '00.112...' -> '00112288000196'
        cnpj_target_clean = ''.join(filter(str.isdigit, identifier_cnpj))
        
        if len(cnpj_target_clean) < 8:
            return 0.0

        match_idx = None
        
        # Itera sobre o índice (que contém a string suja 'CNPJ-NOME')
        for idx in df.index:
            raw_text = str(idx).upper()
            
            # Limpa a linha do CSV para ver se os números do CNPJ estão lá
            clean_text = raw_text.replace('.', '').replace('/', '').replace('-', '').replace(' ', '')
            
            if cnpj_target_clean in clean_text:
                match_idx = idx
                print(f"   [REF] Hospital encontrado: '{raw_text[:50]}...'") # Mostra só o começo
                break
        
        if not match_idx:
            print(f"   [AVISO] CNPJ {identifier_cnpj} não encontrado na referência {system}.")
            return 0.0
            
        hospital_row = df.loc[match_idx]

        meses_pt = {1:'Jan', 2:'Fev', 3:'Mar', 4:'Abr', 5:'Mai', 6:'Jun', 7:'Jul', 8:'Ago', 9:'Set', 10:'Out', 11:'Nov', 12:'Dez'}
        
        # Gera as variações de nome de coluna (TabWin exporta de vários jeitos)
        possible_cols = [
            f"{meses_pt[date_obj.month]}/{str(date_obj.year)[2:]}", # Jan/95 (Mais comum)
            f"{meses_pt[date_obj.month]}/{date_obj.year}",          # Jan/1995
            f"{meses_pt[date_obj.month].upper()}/{str(date_obj.year)[2:]}", # JAN/95
            f"{date_obj.month:02d}/{date_obj.year}"                 # 01/1995
        ]
        
        found_col = None
        # Procura qual coluna existe no DataFrame
        for col_try in possible_cols:
            # Busca case-insensitive
            match = next((c for c in df.columns if str(c).strip().lower() == col_try.lower()), None)
            if match:
                found_col = match
                break
        
        if found_col:
            raw_val = hospital_row[found_col]
            
            if pd.isna(raw_val) or str(raw_val).strip() in ['-', '', 'nan']: 
                return 0.0
            
            # Limpa formatação de dinheiro
            val_str = str(raw_val).replace('R$', '').replace(' ', '')
            if ',' in val_str and '.' in val_str: # 1.000,00
                val_str = val_str.replace('.', '').replace(',', '.')
            elif ',' in val_str: # 1000,00
                val_str = val_str.replace(',', '.')
                
            try:
                val = float(val_str)
                # print(f"   [REF] Meta Financeira ({found_col}): R$ {val:.2f}")
                return val
            except:
                return 0.0
        
        return 0.0

    @staticmethod
    def identify_legacy_code(df: pd.DataFrame, target_val: float) -> str:
        if df.empty: return None
        
        # Tenta achar a coluna de ID
        id_col = None
        for col in ['CNES', 'CGC_HOSP', 'PA_CODUNI', 'COD_UNI', 'PRESTADOR']:
            if col in df.columns:
                id_col = col
                break
                
        if id_col is None:
            # Se não achou pelo nome, pega a primeira coluna
            id_col = df.columns[0]

        # Tenta achar a coluna de VALOR
        val_col = None
        for col in ['VAL_TOT', 'PA_VALAPR', 'VAL_PROD', 'VALOR']:
            if col in df.columns:
                val_col = col
                break
        
        if val_col is None: return None

        # Garante numérico
        df[val_col] = pd.to_numeric(df[val_col], errors='coerce').fillna(0)
        
        # Soma por hospital
        totais = df.groupby(id_col)[val_col].sum()
        
        # Procura o match exato (com pequena tolerância de centavos)
        for code, total in totais.items():
            diff = abs(total - target_val)
            if diff < 0.10: # 10 centavos de diferença aceitável
                print(f"MATCH CONFIRMADO! Código {code} (R$ {total:.2f})")
                return code
                
        # Se não achou, mostra o mais perto (debug)
        if not totais.empty:
            closest_code = (totais - target_val).abs().idxmin()
            # print(f" Sem match exato. Mais próximo: {closest_code} (R$ {totais[closest_code]:.2f})")
            
        return None
    
class Processing:
    @staticmethod
    def month_SIA_IVR(file_path: str) -> MonthInfo:
        #df = pd.read_csv(file_path, usecols=SIA_RELEVANT_FIELDS)
        when = Date.from_sus_file_name(file_path)
        print(f"Processando mês: {when}")
        
        #PROCESSAMENTO DE ARQUIVOS ANTIGOS
        if when.year < 2008:
            colunas_antigas = ['PA_DATREF', 'PA_CODPRO', 'PA_QTDAPR', 'PA_VALAPR', 'PA_CODUNI']
            try:
                #Lê o arquivo CSV completo (gerado com filtro "TODOS")
                df = pd.read_csv(file_path, usecols=colunas_antigas, dtype=str)
                df.rename(columns={'PA_DATREF': 'PA_CMP', 'PA_CODPRO': 'PA_PROC_ID'}, inplace=True)
                
                # Converte para números AGORA, pois o LegacyMatcher precisa somar
                df['PA_VALAPR'] = pd.to_numeric(df['PA_VALAPR'].str.strip(), errors='coerce').fillna(0)
                df['PA_QTDAPR'] = pd.to_numeric(df['PA_QTDAPR'].str.strip(), errors='coerce').fillna(0)

                #Busca o valor total esperado na tabela Excel (Gabarito)
                target_val = LegacyMatcher.get_expected_total('SIA', when, ProjParams.CNPJ)
                
                #Descobre qual código (PA_CODUNI) tem essa soma no arquivo
                legacy_code = LegacyMatcher.identify_legacy_code(df, target_val)
                
                if legacy_code:
                    #Filtra mantendo apenas as linhas do hospital encontrado
                    df = df[df['PA_CODUNI'] == legacy_code]
                    print(f"DEBUG: Filtrado {len(df)} linhas para o código antigo {legacy_code}")
                else:
                    # Se não achou match, zera o dataframe
                    df = pd.DataFrame(columns=df.columns)

            except ValueError:
                df = pd.DataFrame(columns=['PA_CMP', 'PA_PROC_ID', 'PA_QTDAPR', 'PA_VALAPR'])
            except Exception as e:
                print(f"Erro no processamento legado: {e}")
                df = pd.DataFrame(columns=['PA_CMP', 'PA_PROC_ID', 'PA_QTDAPR', 'PA_VALAPR'])

        #PROCESSAMENTO DE ARQUIVOS RECENTES (>= 2008)
        # Aqui o filtro por CNES já foi feito ou é padrão
        else:
            df = pd.read_csv(file_path, usecols=SIA_RELEVANT_FIELDS, dtype=str)
            
            # Limpa as colunas numéricas
            df['PA_VALAPR'] = pd.to_numeric(df['PA_VALAPR'].str.strip(), errors='coerce').fillna(0)
            df['PA_QTDAPR'] = pd.to_numeric(df['PA_QTDAPR'].str.strip(), errors='coerce').fillna(0)


        
        if df.empty:
            rate = InterestRate.complete_rate_split(when, ProjParams.END_INTEREST)
            return MonthInfo.empty(when, 'IVR', rate)

        rate = InterestRate.complete_rate_split(when, ProjParams.END_INTEREST)
        
        # 1. Calcula o valor devido (IVR) para CADA procedimento
        df['VALOR_DEVIDO_IVR'] = df['PA_VALAPR'] * 1.5
        
        # Calcula os totais
        brute_sum = df["PA_VALAPR"].sum()
        expected_sum = df["VALOR_DEVIDO_IVR"].sum()
        
        # Selecionamos as colunas que queremos no laudo detalhado
        colunas_detalhe = ['PA_PROC_ID', 'PA_QTDAPR', 'PA_VALAPR', 'VALOR_DEVIDO_IVR']
        
        # Adiciona coluna auxiliar para busca de descrição depois
        df['TIPO_SISTEMA'] = 'SIA'
        
        procedimentos_lista = df[colunas_detalhe + ['TIPO_SISTEMA']].to_dict('records')

        #VERIFICA SE PEGOU ALGUMA COISA
        print(f"DEBUG PYTHON: Encontrei {len(procedimentos_lista)} procedimentos para o mês {when}")
        if len(procedimentos_lista) > 0:
            print(f"Exemplo do primeiro item: {procedimentos_lista[0]}")
        
        #Passa a lista de procedimentos para o construtor da MonthInfo
        return MonthInfo(when, 'IVR', 'SIA', expected_sum, brute_sum, rate, procedimentos_lista)

    @staticmethod
    def row_SIA_TUNEP(row: pd.Series):
        got = float(row['PA_VALAPR'])
        tunep_unit_val = Tunep._get_base_value(str(row['PA_PROC_ID']), 'SIA')
        return (tunep_unit_val * int(row['PA_QTDAPR']) if tunep_unit_val != None else got * 1.5)


    @staticmethod
    def month_SIA_TUNEP(file_path: str) -> MonthInfo:
        df = pd.read_csv(file_path, usecols=SIA_RELEVANT_FIELDS, dtype={'PA_PROC_ID': 'str', 'PA_QTDAPR': 'int'})
        when = Date.from_sus_file_name(file_path)
        rate = InterestRate.complete_rate_split(when, ProjParams.END_INTEREST)

        res = float(df.apply(Processing.row_SIA_TUNEP, axis=1, result_type='reduce').sum())
        got = float(df["PA_VALAPR"].sum())
        return MonthInfo(when, 'TUNEP', 'SIA', res, got, rate)


    @staticmethod
    def row_SIA_IVR_TUNEP(row: pd.Series):
        got = float(row['PA_VALAPR'])
        tunep_unit_val = Tunep._get_base_value(str(row['PA_PROC_ID']), 'SIA')
        tunep_unit_val = (tunep_unit_val if tunep_unit_val != None else 0.0)
        return max(tunep_unit_val * int(row['PA_QTDAPR']), got*1.5)


    @staticmethod
    def row_SIH_IVR_TUNEP(row: pd.Series):
        got = float(row['SP_VALATO'])
        tunep_unit_val = Tunep._get_base_value(str(row['SP_ATOPROF'])[1:], 'SIH')
        tunep_unit_val = (tunep_unit_val if tunep_unit_val != None else 0.0)
        return max(tunep_unit_val*int(row['SP_QTD_ATO']), got*1.5)



    @staticmethod
    def month_SIA_IVR_TUNEP(file_path: str) -> MonthInfo:
        df = pd.read_csv(file_path, usecols=SIA_RELEVANT_FIELDS, dtype={'PA_PROC_ID': 'str', 'PA_QTDAPR': 'int'})
        when = Date.from_sus_file_name(file_path)
        rate = InterestRate.complete_rate_split(when, ProjParams.END_INTEREST)
        res = float(df.apply(Processing.row_SIA_IVR_TUNEP, axis=1, result_type='reduce').sum())
        got = float(df["PA_VALAPR"].sum())
        return MonthInfo(when, 'BOTH', 'SIA', res, got, rate)



    @staticmethod
    def row_SIH_TUNEP(row: pd.Series):
        got = np.float64(row['SP_VALATO'])
        tunep_unit_val = Tunep._get_base_value(str(row['SP_ATOPROF'])[1:], 'SIH')
        return (tunep_unit_val * int(row['SP_QTD_ATO']) if tunep_unit_val != None else got * 1.5)


    @staticmethod
    def month_SIH_IVR(file_path: str) -> MonthInfo:
        expected = set(SIH_RELEVANT_FIELDS)

        if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
            return None
        
        try:
            df = pd.read_csv(file_path, encoding="latin1", sep=None, engine="python")
        except Exception:
             df = pd.read_csv(file_path, encoding="latin1", sep=';', engine="python")
        df.columns = df.columns.str.strip()
        if not expected.issubset(set(df.columns)):
            print("File does not match SIH schema:", file_path)
            return None
        df = df[SIH_RELEVANT_FIELDS]
        when = Date.from_sus_file_name(file_path)
        rate = InterestRate.complete_rate_split(when, ProjParams.END_INTEREST)
        brute_sum = df["SP_VALATO"].sum()
        return MonthInfo(when, 'IVR', 'SIH', brute_sum*1.5, brute_sum, rate)
    
    @staticmethod
    def month_SIH_TUNEP(file_path: str) -> MonthInfo:
        try:
            df = pd.read_csv(file_path, usecols=SIH_RELEVANT_FIELDS, dtype={'SP_ATOPROF': 'str', 'SP_QTD_ATO': 'int'}, encoding="latin1", sep=None, engine="python")
        except Exception:
            df = pd.read_csv(file_path, usecols=SIH_RELEVANT_FIELDS, dtype={'SP_ATOPROF': 'str', 'SP_QTD_ATO': 'int'}, encoding="latin1", sep=";", engine="python")
        when = Date.from_sus_file_name(file_path)
        rate = InterestRate.complete_rate_split(when, ProjParams.END_INTEREST)
        res = df.apply(Processing.row_SIH_TUNEP, axis=1).sum()
        got = df["SP_VALATO"].sum()
        return MonthInfo(when, 'TUNEP', 'SIH', res, got, rate)


    @staticmethod
    def month_SIH_IVR_TUNEP(file_path: str) -> MonthInfo:
        try:
            df = pd.read_csv(file_path, usecols=SIH_RELEVANT_FIELDS, dtype={'SP_ATOPROF': 'str', 'SP_QTD_ATO': 'int'}, encoding="latin1", sep=None, engine="python")
        except Exception:
             df = pd.read_csv(file_path, usecols=SIH_RELEVANT_FIELDS, dtype={'SP_ATOPROF': 'str', 'SP_QTD_ATO': 'int'}, encoding="latin1", sep=";", engine="python")
        when = Date.from_sus_file_name(file_path)
        rate = InterestRate.complete_rate_split(when, ProjParams.END_INTEREST)
        res = df.apply(Processing.row_SIH_IVR_TUNEP, axis=1, result_type='reduce').sum()
        got = df["SP_VALATO"].sum()
        return MonthInfo(when, 'BOTH', 'SIH', float(res), float(got), rate)


    @staticmethod
    def months(sia_files: list[str], sih_files: list[str], method: str) -> list[MonthInfo]:

        if (method == 'TUNEP' or method == 'BOTH'):
            print(f'method {method} not implemented yet')
            exit(1)

        FUNCTION_TABLE = {
            'IVR': [Processing.month_SIA_IVR, Processing.month_SIH_IVR],
            'TUNEP': [Processing.month_SIA_TUNEP, Processing.month_SIH_TUNEP],
            'BOTH': [Processing.month_SIA_IVR_TUNEP, Processing.month_SIH_IVR_TUNEP],
            'RAW': [Processing.month_SIA_IVR, Processing.month_SIH_IVR]
        }

        sia_func, sih_func = FUNCTION_TABLE[method]
        months_info: dict[str, MonthInfo] = {}
        
        for f_sia in sia_files:
            m = sia_func(f_sia)
            print(f"m: {m}, f_sia: {f_sia}")

            if not m:
                continue
            
            if str(m.when) not in months_info:
                rate = InterestRate.complete_rate_split(m.when, ProjParams.END_INTEREST)
                months_info[str(m.when)] = MonthInfo.empty(m.when, method, rate)
            months_info[str(m.when)].add_got_exp('SIA', m.got, m.expected, m.procedimentos)

        for f_sih in sih_files:
            m = sih_func(f_sih)
            print(f"m: {m}, f_sih: {f_sih}")

            if not m:
                continue
            
            if str(m.when) not in months_info:
                rate = InterestRate.complete_rate_split(m.when, ProjParams.END_INTEREST)
                months_info[str(m.when)] = MonthInfo.empty(m.when, method, rate)
            months_info[str(m.when)].add_got_exp('SIH', m.got, m.expected)

        lst = list(months_info.values())
        lst.sort(key=lambda x: x.when)
        return lst


    @staticmethod
    def year_results(months_res: list[MonthInfo]) -> list[YearInfo]:
        years_table: dict[int, YearInfo] = {}

        for month in months_res:
            year = month.when.year
            if not year in years_table:
                years_table[year] = YearInfo(year)

            years_table[year].add_month(month)

        lst = [i[1] for i in list(years_table.items())]
        lst.sort(key=lambda x: x.when)
        return lst


    @staticmethod
    def total_result(months_res: list[MonthInfo]) -> TotalInfo:
        result = TotalInfo()
        for month in months_res:
            result.add_month(month)
        return result


class CsvBuilder:
    @staticmethod
    def build_month_report(months: list[MonthInfo]):
        df = pd.DataFrame()

        for month in months:
            new_row = pd.DataFrame({
                'MES': [str(month.when)],
                'TOTAL_DEVIDO': [month.debt_now],
                'CORRECAO': [month.rates],
                'PAGO_BRUTO_TOT': [month.got],
                'Diferença IVR': [month.debt_then]})

            df = pd.concat([df, new_row], ignore_index=True)

        df.to_csv(ProjPaths.MONTH_REPORT_PATH, index=False, sep=';', float_format="%.2f")

    @staticmethod
    def build_year_report(years: list[YearInfo]):
        df = pd.DataFrame()

        for year in years:
            new_row = pd.DataFrame({'ANO': [str(year.when)],
                                    'DIF_IVR_BRUTO': [str(year.diff_then)],
                                    'VAL_CORRECAO': [str(year.val_correcao)],
                                    'DIF_IVR_CORRIGIDO': [str(year.diff_now)]})
            df = pd.concat([df, new_row], ignore_index=True)

        df.to_csv(ProjPaths.YEAR_REPORT_PATH, index=False, sep=';', float_format="%.2f")

    @staticmethod
    def build_total_report(report: TotalInfo):
            df = pd.DataFrame({'DIF_IVR_BRUTO': [str(report.diff_then)],
                                    'VAL_CORRECAO': [str(report.val_correcao)],
                                    'DIF_IVR_CORRIGIDO': [str(report.diff_now)]})

            df.to_csv(ProjPaths.TOTAL_REPORT_PATH, index=False, sep=';', float_format="%.2f")


class LatexBuilder:
    @staticmethod
    def build_latex_file(months: list[MonthInfo], years: list[YearInfo], report: TotalInfo, method: str):
        METHOD_TEMPLATE = {
            'IVR': ivr_file_template,
            'TUNEP': tunep_file_template,
            'BOTH': tunep_file_template,
            'RAW': tunep_file_template,
        }

        template = METHOD_TEMPLATE[method]

        result = template.FILE_HEADER

        result += template.DESCRICAO.format(cnes=ProjParams.CNES,
                                            cidade=ProjParams.CIDADE,
                                            estado=ProjParams.STATE,
                                            numero_processo=ProjParams.NUMERO_PROCESSO,
                                            razao_social=ProjParams.RAZAO_SOCIAL,
                                            nome_fantasia=ProjParams.NOME_FANTASIA)
        
        result += template.METODOLOGIA

        if (report.diff_now == 0):
            raise Exception("Nothing to pay")
        
        result += template.CONCLUSAO.format(valor_total=br_money(report.diff_now))

        result += LatexBuilder.build_total_latex_table(report, template)

        result += LatexBuilder.build_year_latex_table(years, template)

        result += LatexBuilder.build_month_latex_table(months, template)

        result += LatexBuilder.build_detailed_latex_table(months)

        result += template.FILE_FOOTER

        f = open(ProjPaths.LATEX_FILE_PATH, 'w')
        f.write(result)
        f.close()

        
    @staticmethod
    def build_month_latex_table(months: list[MonthInfo], template: ModuleType) -> str:
        table_body = template.MONTH_HEADER
        for m in months:
            table_body += f"{m.when} & {br_money(m.got)} & {br_money(m.debt_then())} & {(m.rates[0]*100)-100:.4f}\\% & {(m.rates[1]*100)-100:.4f}\\% & {br_money(m.debt_now())}"
            table_body += '\\\\ \\hline'
        return table_body + template.MONTH_FOOTER


    @staticmethod
    def build_year_latex_table(years: list[YearInfo], template: ModuleType) -> str:
        table_body = template.YEAR_HEADER
        for y in years:
            table_body += f"{y.when} & {br_money(y.diff_then)} & {br_money(y.val_correcao)} & {br_money(y.diff_now)}"
            table_body += '\\\\ \\hline'
        return table_body + template.YEAR_FOOTER


    @staticmethod
    def build_total_latex_table(report: TotalInfo, template: ModuleType) -> str:
        table_body = template.TOTAL_HEADER
        table_body += f"{br_money(report.diff_then)} & {br_money(report.val_correcao)} & {br_money(report.diff_now)}"
        table_body += '\\\\ \\hline'
        return table_body + template.TOTAL_FOOTER
    
    @staticmethod
    def build_detailed_latex_table(months: list[MonthInfo]) -> str:
        # Layout Novo (Sem Tipo):
        # Mês:1.5 | Cód:2.0 | Descrição:7.5 (Aumentou!) | Qtd:1.0 | Pago:2.5 | Devido:2.5
        latex = r"""
        \newpage
        \section{Detalhamento dos Procedimentos}
        \begin{longtable}[c]{|p{1.5cm}|p{2.0cm}|p{7.5cm}|p{1.0cm}|p{2.5cm}|p{2.5cm}|}
        \caption{Detalhamento completo dos procedimentos} \\ \hline
        \textbf{\centering Mês} & 
        \textbf{\centering Cód.} & 
        \textbf{\centering Descrição} & 
        \textbf{\centering Qtd} & 
        \textbf{\centering Pago (R\$)} & 
        \textbf{\centering Devido (R\$)} \\ \hline
        \endfirsthead

        \hline
        \textbf{\centering Mês} & 
        \textbf{\centering Cód.} & 
        \textbf{\centering Descrição} & 
        \textbf{\centering Qtd} & 
        \textbf{\centering Pago (R\$)} & 
        \textbf{\centering Devido (R\$)} \\ \hline
        \endhead
        """
        
        total_linhas_processadas = 0

        # 🔹 First pass: aggregate procedures
        aggregated = {}  # (month, code) -> accumulator dict
        
        for m in months:
            if not hasattr(m, 'procedimentos') or not m.procedimentos:
                continue
        
            for p in m.procedimentos:
                code = p.get('PA_PROC_ID', p.get('SP_ATOPROF', '?'))
                tipo_display = p.get('TIPO_SISTEMA', '-')
                
                try:
                    qtd = int(p.get('PA_QTDAPR', p.get('SP_QTD_ATO', 0)))
                    paid = float(p.get('PA_VALAPR', p.get('SP_VALATO', 0.0)))
                    due = float(p.get('VALOR_DEVIDO_IVR', 0.0)) - paid
                except:
                    continue
                    
                if qtd == 0:
                    continue
                    
                month_str = str(m.when)
                key = (month_str, code)
        
                if key not in aggregated:
                    aggregated[key] = {
                        "month": m.when,
                        "code": code,
                        "tipo": tipo_display,
                        "qtd": 0,
                        "paid": 0.0,
                        "due": 0.0,
                    }
        
                aggregated[key]["qtd"] += qtd
                aggregated[key]["paid"] += paid
                aggregated[key]["due"] += due


        # 🔹 Second pass: generate LaTeX from grouped data
        for data in aggregated.values():
        
            descricao = Tunep.get_description(data["code"], data["tipo"])
            descricao = descricao.replace('&', '\\&').replace('%', '\\%').replace('_', '\\_')
        
            latex += (
                f"{{\\centering {data['month']}}} & "
                f"{{\\centering {data['code']}}} & "
                f"{{\\raggedright \\scriptsize {descricao}}} & "
                f"{{\\centering {data['qtd']}}} & "
                f"{{\\raggedleft {br_money(data['paid'])}}} & "
                f"{{\\raggedleft {br_money(data['due'])}}} \\\\ \\hline \n"
            )
        
            total_linhas_processadas += 1


        latex += r"""
        \end{longtable}
        """
        
        print(f"DEBUG LATEX: Tabela gerada com um total de {total_linhas_processadas} linhas.")
        return latex


class PdfBuilder:
    @staticmethod
    def write_pdf(dst: str):
        os.chdir(ProjPaths.LATEX_DIR)
        result = subprocess.run(["xelatex", path.split(ProjPaths.LATEX_FILE_PATH)[-1]], timeout=15*60, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        result.check_returncode() #raise error in fail condition
        shutil.move("laudo.pdf", dst)
        os.chdir(ProjPaths.SCRIPTS_DIR)


def get_files(system: str):
    files = Downloads.find_files(
        system,
        ProjParams.get_state(),
        ProjParams.get_start_date(),
        ProjParams.get_end_date())

    print(f"\nWill download {len(files)} files.")
    
    Downloads.download(files)
    print("downloads finished")



# nesse modo de execução do programa, é gerado um laudo com qualquer dado que já esteja presente no programa.
def test_mode():
    # arquivos a serem processados
    sih_files = [path.join(ProjPaths.SIH_CSVS_DIR, file) for file in os.listdir(ProjPaths.SIH_CSVS_DIR) if file.endswith('.csv')]
    sia_files = [path.join(ProjPaths.SIA_CSVS_DIR, file) for file in os.listdir(ProjPaths.SIA_CSVS_DIR) if file.endswith('.csv')]
    sih_files.sort()
    sia_files.sort()
    # processamento dos dadaos
    months = Processing.months(sia_files, sih_files, ProjParams.METHOD)
    years = Processing.year_results(months)
    total = Processing.total_result(months)

    # geração dos documentos pertinentes aos dados
    LatexBuilder.build_latex_file(months, years, total, ProjParams.METHOD)    
    PdfBuilder.write_pdf(path.join(ProjPaths.RESULTS_DIR, 'laudo.pdf'))

def debug_disk(stage: str):
    total, used, free = shutil.disk_usage("/")

    print(f"\n===== DISK DEBUG: {stage} =====")
    print("Total:", total // (1024**3), "GB")
    print("Used :", used // (1024**3), "GB")
    print("Free :", free // (1024**3), "GB")

    def folder_size(path):
        if not os.path.exists(path):
            return 0
        total_size = 0
        for dirpath, dirnames, filenames in os.walk(path):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                total_size += os.path.getsize(fp)
        return round(total_size / (1024**2), 2)  # MB

    paths = [
        ProjPaths.SIA_DOWNLOAD_DIR,
        ProjPaths.SIH_DOWNLOAD_DIR,
        ProjPaths.SIA_DBFS_DIR,
        ProjPaths.SIH_DBFS_DIR,
        ProjPaths.SIA_CSVS_DIR,
        ProjPaths.SIH_CSVS_DIR,
        ProjPaths.UNITED_CSV_DIR,
        ProjPaths.RESULTS_DIR
    ]

    for p in paths:
        print(f"{p}: {folder_size(p)} MB")

    print("====================================\n")

def split_into_bimesters(start_str, end_str):
    """
    Splits a period into 2-month chunks.
    Input format: MM-YYYY
    Returns: list of tuples [(start_MM-YYYY, end_MM-YYYY), ...]
    """

    start = datetime.datetime.strptime(start_str, "%m-%Y")
    end = datetime.datetime.strptime(end_str, "%m-%Y")

    if end < start:
        raise ValueError("End date must be after start date")

    periods = []
    current = start

    while current <= end:
        # Add 1 month (current counts as first)
        month = current.month - 1 + 1
        year = current.year + month // 12
        month = month % 12 + 1

        chunk_end = datetime.datetime(year, month, 1)

        # Clamp if beyond final end
        if chunk_end > end:
            chunk_end = end

        periods.append((
            current.strftime("%m-%Y"),
            chunk_end.strftime("%m-%Y")
        ))

        # Move to next month after chunk_end
        if chunk_end.month == 12:
            next_month = 1
            next_year = chunk_end.year + 1
        else:
            next_month = chunk_end.month + 1
            next_year = chunk_end.year

        current = datetime.datetime(next_year, next_month, 1)

    return periods

def main():
    months = []
    if sys.argv[1] != 'TEST':
        periods = split_into_bimesters(sys.argv[5], sys.argv[6])
    else:
        periods = split_into_bimesters(sys.argv[6], sys.argv[7])
    ProjPaths.init()
    ProjPaths.test()
    ProjParams.init()
    ProjParams.test()
    
    for period in periods:
        ProjParams.set_start_date(period[0])
        ProjParams.set_end_date(period[1])
        InterestRate.load_selic()
        InterestRate.show_selic()
        Tunep.load_tunep()
        LegacyMatcher.load_references()

        if (ProjParams.SYSTEM == 'SIA' or ProjParams.SYSTEM == 'BOTH'):
            get_files('SIA')
        if (ProjParams.SYSTEM == 'SIH' or ProjParams.SYSTEM == 'BOTH'):
            get_files('SIH')

        Conversions.convert_files()

        debug_disk("BEFORE UNITING/AFTER CONVERSION")
        if (ProjParams.SYSTEM == 'SIA' or ProjParams.SYSTEM == 'BOTH'):
            Conversions.unite_files('SIA')
        if (ProjParams.SYSTEM == 'SIH' or ProjParams.SYSTEM == 'BOTH'):
            Conversions.unite_files('SIH')
        debug_disk("AFTER UNITING")

        sih_files = [path.join(ProjPaths.SIH_CSVS_DIR, file) for file in os.listdir(ProjPaths.SIH_CSVS_DIR)]
        sia_files = [path.join(ProjPaths.SIA_CSVS_DIR, file) for file in os.listdir(ProjPaths.SIA_CSVS_DIR)]

        print(f"sih_files: {sih_files}")
        print(f"sia_files: {sia_files}")
        months += Processing.months(sia_files, sih_files, ProjParams.METHOD)
        print(f"Months right now: {months}")
        ProjPaths.empty_dirs()
        ProjPaths.create_paths()
    print(f"Months after the loop: {months}")
    years = Processing.year_results(months)
    total = Processing.total_result(months)

    LatexBuilder.build_latex_file(months, years, total, ProjParams.METHOD)
    PdfBuilder.write_pdf(path.join(ProjPaths.RESULTS_DIR, 'laudo.pdf'))

main()
