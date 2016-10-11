import fileinput
import sys
#csv style split and qutation
import csv
import mysql.connector

################################
#function export_cdr_by_sql
################################
def export_cdr_by_sql(sql_query,output_file):
    
    #open DB #prepare a cursor object using cursor() method
    conn = mysql.connector.connect(user=DB_username, password=DB_password,database=DB_name)
    cursor = conn.cursor()

    try:
        cursor.execute(sql_query)
    except:
        print("Database ERROR!!! Generate Daily Summary error")

    result_set = cursor.fetchall()
    row_count = cursor.rowcount

    #open csv file
    cdr_file_handle = open(output_file, "w")
    csv_headline="call_type,Incoming_trunk_ID,Outgoing_trunk_ID,Country_Code,Country_Name,Area_Code,Area_Name,In_carrier_Name,Out_carrier_Name,Source_IP,Destination_IP,Startdate,Starttime,Enddate,Endtime,Connect_Time,Called_Number,Caller_Number,Duration_sec,Duration_min,Disconnect_Code,Disconnect_Party,Smonth,Sday,Syear,Shr,Prefix,Country,Dest\n"
    cdr_file_handle.write(csv_headline)

    row_number = 0
    for raws in result_set:
        cdr_file_handle.write("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n" %(raws[1],raws[2],raws[3],raws[4],raws[5],raws[6],raws[7],raws[8],raws[9],raws[10],raws[11],raws[12],raws[13],raws[14],raws[15],raws[16],raws[17],raws[18],raws[19],raws[20],raws[21],raws[22],raws[23],raws[24],raws[25],raws[26],raws[27],raws[28],raws[29]))
        row_number = row_number + 1
    #end for




    #disconnect from DB
    cursor.close()
    conn.close()
    #close file
    cdr_file_handle.close()

#end function export_cdr_by_sql

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
            elif var_name == 'mySQL_db_name':
                #dbname_var = var_value.strip()
                dbname_var = 'gti'
            #end if var_name
        #end if line
    #end for
    #print ("%s,%s,%s,%s" %(host_var,username_var,password_var,dbname_var))
    return host_var,username_var,password_var,dbname_var
#end function read_config_file()

################################
#function export_cdr_by_sql
################################
def prepare_sql_query(cdr_date):
    
    sql_query="SELECT * FROM cdr_" + cdr_date


    return sql_query
#end function prepare_sql_query


################################
#main block
################################

(DB_host,DB_username,DB_password,DB_name) = read_config_file()
#output_csv="cdr_output.csv"





#need input source az table and destination az table
if len(sys.argv) < 2:
    print("Please type Date ")
    print("Example:")
    print("     export_cdr.py 20131125 ")
    print("     export CDR of 2013/11/25 ")
else:
    #run function
    export_cdr_by_sql(prepare_sql_query(sys.argv[1]),"output_cdr_" + sys.argv[1] + ".csv")

#end if


