import mysql.connector
from init import *

mydb = mysql.connector.connect(
  host=db_host,
  user=db_user,
  password=db_password,
  database=db_database
)
mycursor = mydb.cursor()

sql_str = 'CREATE TABLE `db_ced_v4`.`priv` (`id` INT NOT NULL AUTO_INCREMENT,PRIMARY KEY (`id`), UNIQUE INDEX `id_UNIQUE` (`id` ASC) VISIBLE);'
mycursor.execute(sql_str)
mydb.commit()

lista_tabella = database.columns.tolist()
for nome in lista_tabella[:-2]:
    sql_str = "ALTER TABLE priv ADD `" +str(nome)+ "` FLOAT"  ## inserire ` per passare il nome della colonna 
    mycursor.execute(sql_str)  
for nome in lista_tabella[-2:]:
    sql_str = "ALTER TABLE priv ADD `" +str(nome)+ "` DATETIME"  ## inserire ` per passare il nome della colonna 
    mycursor.execute(sql_str)  
mydb.commit()
mydb.close()