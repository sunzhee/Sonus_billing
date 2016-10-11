import fileinput
import sys
#csv style split and qutation
import csv
import mysql.connector

#system output into a logfile
#sys.stdout = open("setup_log.txt", "w")


############################################################
#Function read config file
############################################################
def read_config_file():

    host_var = username_var = password_var = dbname_var = []
    for line in fileinput.input(['billing_prg.conf']):
        line=line.strip()
        #print (line)
        #skip empty line
        if len(line) == 0:
            continue
        if (line[0] != "'"):
            var_statment = line.split('=')
            var_name = var_statment[0].strip()
            var_value = var_statment[1].strip()
            if var_name == 'mySQL_db_host':
                host_var = var_value.strip()
            elif var_name == 'mySQL_db_user':
                username_var = var_value.strip()
            elif var_name == 'mySQL_db_password':
                password_var = var_value.strip()
            elif var_name == 'mySQL_db_password':
                #DBname_var = var_value.strip()
                DBname_var = 'gti'
            #end if var_name
        #end if line
    #end for
    #print ("%s,%s,%s" %(host_var,username_var,password_var))
    return host_var,username_var,password_var,dbname_var
#end function


############################################################
#Main Block
############################################################
#Set MySQL Database info
(DB_host,DB_username,DB_password,DB_name) = read_config_file()
#Create Database gti
error_sign = 0
try:
    gti_db = mysql.connector.connect(host=DB_host,user=DB_username,passwd=DB_password)
    cursor = gti_db.cursor()
    sql = 'CREATE DATABASE IF NOT EXISTS gti'
    cursor.execute(sql)
except:
    print("Error!!! can't create database!")
    print("Check MySQL username and password in 'billing_prg.conf' file")
    error_sign = 1

if error_sign == 0:
    print("Database setup Success!")




