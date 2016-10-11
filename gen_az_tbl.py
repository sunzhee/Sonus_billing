import fileinput
import sys
#csv style split and qutation
import csv
import mysql.connector
# Open database connection

#system output into a logfile
#sys.stdout = open("log.txt", "w")






##############################################
#Define Function BLOCK
##############################################

def break_az_table( org_az_filename,dest_az_filename ):
    #Read Original_A-Z csv file
    with open(org_az_filename, 'Ur') as file:
        az_list = list(tuple(record) for record in csv.reader(file, delimiter=','))

    #set increment
    line_no = 0


    #open destination AZ file
    dest_file = open(dest_az_filename, "wb")
    #each line with multi country+area code
    #sample: Bolivia - Mobile (Entel)	,"59167, 59168, 59171, 59172, 59173, 59174" ,0.0086
    #        Bolivia - Mobile	        ,"5916, 5917"                               ,0.0125
    #1st loop begin
    for az_country in az_list:
        Country_Area_name = az_list[line_no][0].strip()
        Country_Name_List = Country_Area_name.split('-')
        Country_Name = Country_Name_List[0].strip()
        Country_Area_rate = az_list[line_no][2].strip()
        Country_Area_code_all = az_list[line_no][1].strip()
        Country_Area_list = Country_Area_code_all.split(',')
##        print (Country_Area_list)
        
        #2nd loop begin
        for each_code in Country_Area_list:
            #print(Country_Area_name + "," + each_code)
            #Python not support stream output for string
            #dest_file.write("%s,%s" % (Country_Area_name,each_code))
            #So we change to bytes
            dest_line = bytes("%s,%s,%s,%s\n" % (Country_Name,Country_Area_name,each_code,Country_Area_rate), "utf-8")
            dest_file.write(dest_line)
            
        #2nd loop end
        line_no = line_no + 1
    #1st loop end

    ##close destination AZ file
    dest_file.close()
    print(dest_az_filename + " ---- Created")

    return;
#end function break_az_table()


#insert AZ to DB function
def insert_az_to_DB(detailed_az_file):


    #Read Original_A-Z csv file
    with open(detailed_az_file, 'Ur') as file:
        az_list = list(tuple(record) for record in csv.reader(file, delimiter=','))

    #connect DB
    conn = mysql.connector.connect(user=DB_username, password=DB_password,database=DB_name)
    cursor = conn.cursor()

    #set Error sign
    az_DB_error_sign = 0
    #drop DB table AZ_tbl_overall, if exists
    try:
        cursor.execute("DROP TABLE IF EXISTS AZ_tbl_overall")
    except:
        print ("ERROR!!! Can't Drop AtoZ table!");
        az_DB_error_sign = 1
        conn.rollback()

    #creat DB table of AZ_tbl_overall
    sql_create_tbl = """CREATE TABLE AZ_tbl_overall(
	ID bigint not null	primary key auto_increment,
	Country varchar(255),
	Destination varchar(255),
	Prefix varchar(63)
	)
    """
    #creat DB table of CDR_dir_name
    try:
        cursor.execute(sql_create_tbl)
    except:
        print("ERROR!!! Can't Create table AZ_tbl_overall!")
        az_DB_error_sign = 1
        conn.rollback()



    
    for az_country in az_list:
        Country_name = az_country[0].strip()
        Destination_name = az_country[1].strip()
        Prefix = az_country[2].strip()
        #prepare SQL
        sql = "INSERT INTO AZ_tbl_overall(Country,Destination,Prefix)"
        sql = sql +  " VALUES('%s','%s','%s')" % (Country_name,Destination_name,Prefix)
        #print (sql)
        try:
            #execute sql
            cursor.execute(sql)
        except:
            #Rollback
            if az_DB_error_sign != 1:
                print("ERROR!!!Insert DB error!")
                az_DB_error_sign = 1
                conn.rollback()
            else:
                conn.rollback()
    #end for
            
    # Commit changes in database
    conn.commit()


    # disconnect from DB
    cursor.close()
    conn.close()

    if az_DB_error_sign != 1:
        print("Insert A-Z table to DataBase ---- Done")
    else:
        print("Can't Insert A-Z table to DataBase!!")

    return;
#end function insert_az_to_DB()

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
#end function

#########################################################
#Main BLOCK
#########################################################

#SET MySQL DATABASE INFO
#Globe variables
#Set MySQL Database info
(DB_host,DB_username,DB_password,DB_name) = read_config_file()


#need input source az table and destination az table
if len(sys.argv) < 3:
    print("Please type Source AtoZ filename and Destination AZ filename")
    print("Example:")
    print("     gen_az_table.py az_orig.csv az_detail.csv")
else:
    break_az_table(sys.argv[1],sys.argv[2])
    #must be a prefix breaked-down AZ table
    #delete old az table in DB, then insert all new az
    insert_az_to_DB(sys.argv[2])

#end if

