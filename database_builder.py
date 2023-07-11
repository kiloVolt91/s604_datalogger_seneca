import mysql.connector
import pandas as pd
from init import *

df = pd.read_excel('registri_s604e_parziale.xlsx')

mydb = mysql.connector.connect(
  host=db_host,
  user=db_user,
  password=db_password,
  database=db_database
)
mycursor = mydb.cursor()

sql_str = 'CREATE TABLE `'+str(db_database)+'`.`'+str(db_table)+'` (`id` INT NOT NULL AUTO_INCREMENT,PRIMARY KEY (`id`), UNIQUE INDEX `id_UNIQUE` (`id` ASC) VISIBLE);'
mycursor.execute(sql_str)
mydb.commit()

lista_tabella = df.columns.tolist()
for nome in lista_tabella[:-2]:
    sql_str = "ALTER TABLE "+str(db_table)+" ADD `" +str(nome)+ "` FLOAT"  ## inserire ` per passare il nome della colonna 
    mycursor.execute(sql_str)  
for nome in lista_tabella[-2:]:
    sql_str = "ALTER TABLE "+str(db_table)+" ADD `" +str(nome)+ "` DATETIME"  ## inserire ` per passare il nome della colonna 
    mycursor.execute(sql_str)  
mydb.commit()
mydb.close()