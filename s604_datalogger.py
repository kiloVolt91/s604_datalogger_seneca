## S604e - Datalogger

import numpy as np
import pandas as pd
from datetime import datetime
from pyModbusTCP.client import ModbusClient
from pyModbusTCP import utils
from init import *
import mysql.connector
from mysql.connector import errorcode
import time
import sys
import configparser

def inizializzazione_dati(selezione_id):
    config = configparser.ConfigParser()
    config.read(percorso_file_configurazione)

    global dict_id_impianti, SERVER_HOST, SERVER_PORT, db_host, db_user, db_password, db_database, db_table, id_impianto
    
    try:
        id_impianto = config['datalogger_s604e_'+str(selezione_id)]['id_impianto']
    except KeyError as k_err: 
        sys.exit('Selezionato un id errato', k_err)
    
    SERVER_HOST = config['datalogger_s604e_'+str(selezione_id)]['host']
    SERVER_PORT = config['datalogger_s604e_'+str(selezione_id)]['port']
    SERVER_U_ID = config['datalogger_s604e_'+str(selezione_id)]['server_u_id']
    nome_impianto = config['datalogger_s604e_'+str(selezione_id)]['nome_impianto']
    
    dict_id_impianti = {1:nome_impianto} ## popolare il dict con i nuovi impianti
    db_host = config['sql_database_datalogger_energia_vm']['host']
    db_user=config['sql_database_datalogger_energia_vm']['user']
    db_password=config['sql_database_datalogger_energia_vm']['password']
    db_database=config['sql_database_datalogger_energia_vm']['database']
    db_table = 'datalogger_s604e'
    return

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
    cursor.execute (mysql_str, serie.tolist())
    sql_cnx.commit()
    cursor.close()
    sql_cnx.close()
    return

def conversione_dati_in_std_ieee(lista_valori_letti):
    lista_valori_convertiti=[]
    if lista_valori_letti:
        for dato in lista_valori_letti:
            msb_val = dato[0]
            lsb_val = dato[1]
            bin_1 = bin(msb_val)[2::].zfill(16)
            bin_2 = bin(lsb_val)[2::].zfill(16)
            binary_string = bin_1 + bin_2
            ##conversione secondo standard IEEE
            segno = int(binary_string[0],2)
            esponente = int(binary_string[1:9],2)
            frazione = int(binary_string[9::],2)/8388608
            converted_value = (-1)**segno*2**(esponente-127)*(1+frazione)
            lista_valori_convertiti.append(converted_value)
    else:
        print('non è stato letto alcun valore negli Holding Registers')
    return (lista_valori_convertiti)

def inizializza_parametri_reg_ieee(file_configurazione_parametri):
    df = pd.read_excel(file_configurazione_parametri)
    colonne_registri = df.columns
    colonna_nomi_parametri = colonne_registri[0]
    colonna_registro_hex= colonne_registri[7] ##REGISTRI IEEE
    colonna_word = colonne_registri[8] ##REGISTRI IEEE - 2 WORDS
    colonna_unita_misura = colonne_registri[9] ##REGISTRI IEEE
    nomi_parametri = df[colonna_nomi_parametri].tolist()
    unita_misura = df[colonna_unita_misura].tolist()
    global colonna_database
    global registro_indirizzo
    global reg_numero_word
    colonna_database = []
    for i in range (0, len(nomi_parametri)):
        colonna_database.append(str(nomi_parametri[i]) + ' [' + str(unita_misura[i])+']')
    colonna_database.append('data_inizio_acquisizione')
    colonna_database.append('data_fine_acquisizione')
    colonna_database.append('fk_id_impianto')
    registro_indirizzo = []
    reg_numero_word = []
    for i in range (0, df.shape[0]):
        registro_indirizzo.append(df[colonna_registro_hex][i])
        reg_numero_word.append(df[colonna_word][i])
    return

def lettura_holding_registers(SERVER_HOST, SERVER_PORT):
    t0 = datetime.now()
    lista_valori_letti =[]
    try:
        c = ModbusClient(host = SERVER_HOST, port =int(SERVER_PORT), timeout=30)
    except ValueError:
        print("Error with host or port params")

    for i in range (0, len(registro_indirizzo)):
        lettura_holding_reg = c.read_holding_registers(registro_indirizzo[i], reg_numero_word[i])
        lista_valori_letti.append(lettura_holding_reg)
    c.close()
    t1 = datetime.now()
    #print("Durata interrogazione: ", (t1-t0).total_seconds(), ' secondi')
    global lista_valori_convertiti
    #Conversione dei dati e memorizzazione sul db
    if lista_valori_convertiti:
        lista_valori_convertiti = conversione_dati_in_std_ieee(lista_valori_letti)
        lista_valori_convertiti.append(t0)
        lista_valori_convertiti.append(t1)
        lista_valori_convertiti.append(id_impianto)
    return 

def upload_dati_su_db(lista_valori_convertiti, colonna_database):
    to_database = pd.Series(index=colonna_database, data = lista_valori_convertiti)
    if to_database.empty!=True:
        sql_cnx = mysql_connection(db_host, db_user, db_password, db_database)
        sql_export_df(to_database, db_table, sql_cnx)
    return


selezione_id = 1#int(sys.argv[1])
inizializzazione_dati(selezione_id)
inizializza_parametri_reg_ieee(file_configurazione_parametri)
while True:
    lettura_holding_registers(SERVER_HOST, SERVER_PORT)
    upload_dati_su_db(lista_valori_convertiti, colonna_database)
    time.sleep(2)