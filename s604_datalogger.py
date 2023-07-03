import numpy as np
import pandas as pd
from datetime import datetime
from pyModbusTCP.client import ModbusClient
from pyModbusTCP import utils
from init import *
import mysql.connector
from mysql.connector import errorcode
import time

def mysql_connection(db_host, db_user, db_password, db_database):
    try:
        cnx = mysql.connector.connect(host=db_host, user=db_user, password=db_password, database=db_database)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
    return(cnx)

def sql_export_df(serie, sql_tabella, sql_cnx): 
    columns = serie.index.tolist()
    placeholders = '%s'
    str_nomi = '(`'+columns[0]+'`,'
    str_vals = '(%s,'
    for i in range(1, len(columns)):
        if i == len(columns)-1:
            str_nomi = str_nomi +'`'+ columns[i] +'`' +')'
            str_vals = str_vals + placeholders + ')'
        else: 
            str_nomi = str_nomi +'`'+ columns[i] +'`'+', '
            str_vals = str_vals + placeholders + ', '
    mysql_str = "INSERT INTO "+ sql_tabella+ " {col_name} VALUES {values}".format(col_name = str_nomi, values = str_vals)
    cursor = sql_cnx.cursor()
    print('db uploading..')
    cursor.execute (mysql_str, serie.tolist())
    sql_cnx.commit()
    cursor.close()
    sql_cnx.close()
    return

df = pd.read_excel('registri_s604e_parziale.xlsx')

#SELEZIONE DELLE COLONNE CONTENENTI I PARAMETRI - UTILIZZO DELLA CONVERSIONE IEEE
colonne_registri = df.columns
colonna_nomi_parametri = colonne_registri[0]
colonna_registro_hex= colonne_registri[7] ##REGISTRI IEEE
colonna_word = colonne_registri[8] ##REGISTRI IEEE - 2 WORDS
colonna_unita_misura = colonne_registri[9] ##REGISTRI IEEE

#dataframe contenente i dati da inserire sul db
nomi_parametri = df[colonna_nomi_parametri].tolist()
unita_misura = df[colonna_unita_misura].tolist()
colonna_database = []
for i in range (0, len(nomi_parametri)):
    colonna_database.append(str(nomi_parametri[i]) + ' [' + str(unita_misura[i])+']')
colonna_database.append('data_inizio_acquisizione')
colonna_database.append('data_fine_acquisizione')


registro_indirizzo = []
reg_numero_word = []
for i in range (0, df.shape[0]):
    registro_indirizzo.append(df[colonna_registro_hex][i])
    reg_numero_word.append(df[colonna_word][i])

while True:
    #Inizio ciclo di lettura degli Holding Registers     
    t0 = datetime.now()
    valori_letti =[]
    try:
        c = ModbusClient(host = SERVER_HOST, port =int(SERVER_PORT))
    except ValueError:
        print("Error with host or port params")

    for i in range (0, len(registro_indirizzo)):
        lettura_holding_reg = c.read_holding_registers(registro_indirizzo[i], reg_numero_word[i])
        valori_letti.append(lettura_holding_reg)

    t1 = datetime.now()
    print("Durata interrogazione: ", (t1-t0).total_seconds(), ' secondi')

    #Conversione dei dati e memorizzazione sul db
    data_row=[]
    for dato in valori_letti:
        msb_val = dato[0]
        lsb_val = dato[1]
        #ESTRAZIONE STRINGA
        bin_1 = bin(msb_val)[2::].zfill(16)
        bin_2 = bin(lsb_val)[2::].zfill(16)
        binary_string = bin_1 + bin_2
        ##conversione secondo standard IEEE
        segno = int(binary_string[0],2)
        esponente = int(binary_string[1:9],2)
        frazione = int(binary_string[9::],2)/8388608
        converted_value = (-1)**segno*2**(esponente-127)*(1+frazione)
        data_row.append(converted_value)
    data_row.append(t0)
    data_row.append(t1)
    to_database = pd.Series(index=colonna_database, data = data_row)
    sql_cnx = mysql_connection(db_host, db_user, db_password, db_database)
    sql_export_df(to_database, db_table, sql_cnx)
    print('Fine operazioni')
    time.sleep(2)