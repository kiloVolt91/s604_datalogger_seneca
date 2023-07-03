init_path = '/home/episciotta/Documenti/SVILUPPO/repo_sviluppo_ctz/dati_configurazione/configurazione.ini'
import configparser

config = configparser.ConfigParser()
config.read(init_path)

#dati datalogger_s604e
SERVER_HOST = config['datalogger_s604e_ced_v4']['host']
SERVER_PORT = config['datalogger_s604e_ced_v4']['port']
SERVER_U_ID = config['datalogger_s604e_ced_v4']['server_u_id']

#dati database
db_host = config['sql_database_ced_v4']['host']
db_user=config['sql_database_ced_v4']['user']
db_password=config['sql_database_ced_v4']['password']
db_database=config['sql_database_ced_v4']['database']
db_table = 'priv'

