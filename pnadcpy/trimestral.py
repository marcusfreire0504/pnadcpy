import os
from zipfile import ZipFile
from datetime import datetime
from ftplib import FTP

import pandas as pd


def get_quarter_pnadc(
        year: int,
        quarter: int,
        directory: str,
        unzip: bool = False) -> None:
    """
    Function to download quarterly microdata from Pnad Continua survey from IBGE
    """
    if not isinstance(year, int):
        raise Exception("Argument 'year' must be an integer.")
    if year < 2012 or year > datetime.now().year:
        raise Exception(
            f"Argument 'year' must be between 2012 and {datetime.now().year}.")

    if not isinstance(quarter, int):
        raise Exception("Argument 'quarter' must be an integer.")
    if quarter < 1 or quarter > 4:
        raise Exception("Argument 'quarter' must be between 1 and 4.")

    if not isinstance(directory, str):
        raise Exception("The directory path must be given as a string.")
    if not os.path.exists(directory):
        raise Exception("The directory provided does not exist.")

    if not isinstance(unzip, bool):
        raise Exception("Argument 'unzip' must be a boolian.")

    PATH_FTP = "ftp.ibge.gov.br"
    MICRODATA_PATH = f"/Trabalho_e_Rendimento/Pesquisa_Nacional_por_Amostra_de_Domicilios_continua/Trimestral/Microdados/{year}"
    DOCUMENTS_PATH = "/Trabalho_e_Rendimento/Pesquisa_Nacional_por_Amostra_de_Domicilios_continua/Trimestral/Microdados/Documentacao"
    FILE_PATTERN = f"PNADC_0{quarter}{year}"

    ftp_ibge = FTP(PATH_FTP)
    ftp_ibge.login()
    ftp_ibge.cwd(MICRODATA_PATH)
    dir_files = ftp_ibge.nlst()
    if len(dir_files) == 0:
        raise Exception("There's no microdata available for this year yet.")

    list_files = [f for f in dir_files if FILE_PATTERN in f]
    if len(list_files) > 1:
        raise Exception("There's more than one file meeting the criterias.")
    file_name = list_files.pop()

    with open(os.path.join(directory, file_name), "wb") as fp:
        print(f"Downloading {file_name}...")
        ftp_ibge.retrbinary(f"RETR {file_name}", fp.write)
        print("Done!")

    ftp_ibge.cwd(DOCUMENTS_PATH)

    with open(os.path.join(directory, "Deflatores.zip"), "wb") as fp:
        print("Downloading Deflatores.zip...")
        ftp_ibge.retrbinary("RETR Deflatores.zip", fp.write)
        print("Done!")

    with open(os.path.join(directory, "Dicionario_e_input.zip"), "wb") as fp:
        print("Downloading Dicionario_e_input.zip...")
        ftp_ibge.retrbinary("RETR Dicionario_e_input.zip", fp.write)
        print("Done!")

    ftp_ibge.close()
    
    if unzip:
        with ZipFile(os.path.join(directory, file_name), "r") as zmd:
            print(f"Extracting {file_name}...")
            zmd.extractall(path=directory)
            print("Done!")
        with ZipFile(os.path.join(directory, "Deflatores.zip"), "r") as zdefl:
            print("Extracting Deflatores.zip...")
            zdefl.extractall(path=directory)
            print("Done!")
        with ZipFile(os.path.join(directory, "Dicionario_e_input.zip"), "r") as zdic:
            print("Extracting Dicionario_e_input.zip...")
            zdic.extractall(path=directory)
            print("Done!")
    

def read_pnadc(microdata, input_txt):
    pass

if __name__ == "__main__":
    get_quarter_pnadc(2014, 3, '/home/silasge/Documentos', unzip=True)
