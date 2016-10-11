import mysql.connector
import fileinput
import sys
#for csv style split and qutation
import csv
#for multiple file reading
import glob
import math

#system output into a logfile
#sys.stdout = open("log.txt", "w")


##############################################
#Version info
#2014/06/06
#updated for Sonus_GSX_8.4.2
##############################################




##############################################
#Define Function BLOCK
##############################################

#########################################################
#function read one single CDR file
#compare with az_table and insert all fields into DB
#########################################################
def read_single_cdr(cdr_filename,CDR_dir_name):

    #open DB #prepare a cursor object using cursor() method
    conn = mysql.connector.connect(user=DB_username, password=DB_password,database=DB_name)
    cursor = conn.cursor()

    #Read CDR file
    with open(cdr_filename, 'Ur') as file:
        cdr_data_list = list(tuple(record) for record in csv.reader(file, delimiter=','))

    #print(cdr_filename)        
    #set increment
    line_no = 0
    for cdr_line in cdr_data_list:
        #field 0 is ATTEMPT or STOP
        #print(line_no,cdr_data_list[line_no][0])
        #Begin if , ATTEMPT, unconnected CDR
        if cdr_data_list[line_no][0] == "ATTEMPT":

            Call_type = 'ATTEMP'
            Incoming_trunk_ID = cdr_data_list[line_no][30]
            Outgoing_trunk_ID = cdr_data_list[line_no][57]
            In_carrier_Name = cdr_data_list[line_no][30]
            Out_carrier_Name = cdr_data_list[line_no][57]
            Source_IP = cdr_data_list[line_no][115]
            Destination_IP = cdr_data_list[line_no][29]
            Startdate = cdr_data_list[line_no][5]
            Starttime = cdr_data_list[line_no][6]
            Enddate = cdr_data_list[line_no][104]
            Endtime = cdr_data_list[line_no][9]
            Connect_Time = ""
            Called_Number = cdr_data_list[line_no][17]
            Caller_Number = cdr_data_list[line_no][16]
            Duration_sec = 0
            Duration_min = 0
            Disconnect_Code = cdr_data_list[line_no][11]
            Disconnect_Party = cdr_data_list[line_no][56]
            (sYear,sMonth,sDay,sHour) = get_start_datetime(Startdate,Starttime)
            #Attention, this AtoZ_list is a globe variable, defined in main block
            (Country,Destination,Prefix) = get_country_by_number(Called_Number,AtoZ_list)

    ##        print('this is ATTEMPT')
    ##        print("Incoming_trunk_ID:%s"%Incoming_trunk_ID)
    ##        print("Outgoing_trunk_ID:%s"%Outgoing_trunk_ID)
    ##        print("In_carrier_Name:%s"%In_carrier_Name)
    ##        print("Out_carrier_Name:%s"%Out_carrier_Name)
    ##        print("Source_IP:%s"%Source_IP)
    ##        print("Destination_IP:%s"%Destination_IP)
    ##        print("Startdate:%s"%Startdate)
    ##        print("Starttime:%s"%Starttime)
    ##        print("Enddate:%s"%Enddate)
    ##        print("Endtime:%s"%Endtime)
    ##        print("Connect_Time:%s"%Connect_Time)
    ##        print("Called_Number:%s"%Called_Number)
    ##        print("Caller_Number:%s"%Caller_Number)
    ##        print("Duration_sec:%s"%Duration_sec)
    ##        print("Duration_min:%s"%Duration_min)
    ##        print("Disconnect_Code:%s"%Disconnect_Code)
    ##        print("Disconnect_Party:%s"%Disconnect_Party)
    ##        print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
##            print(sYear)
##            print(sMonth)
##            print(sDay)
##            print(sHour)

        #STOP, connected CDR    
        elif cdr_data_list[line_no][0] == "STOP":

            Call_type = 'STOP'
            Incoming_trunk_ID = cdr_data_list[line_no][33]
            Outgoing_trunk_ID = cdr_data_list[line_no][67]
            In_carrier_Name = cdr_data_list[line_no][33]
            Out_carrier_Name = cdr_data_list[line_no][67]
            Source_IP = cdr_data_list[line_no][125]
            Destination_IP = cdr_data_list[line_no][32]
            Startdate = cdr_data_list[line_no][5]
            Starttime = cdr_data_list[line_no][6]
            Enddate = cdr_data_list[line_no][10]
            Endtime = cdr_data_list[line_no][11]
            Connect_Time = ""
            Called_Number = cdr_data_list[line_no][20]
            Caller_Number = cdr_data_list[line_no][19]

            if cdr_data_list[line_no][13].isdigit():
                Duration_sec = float("%.2f" %(float(cdr_data_list[line_no][13])/100))
                #math.celi() is ceiling function, return int
                Duration_sec = math.ceil(Duration_sec)
                Duration_min = float("%.3f" %(float(Duration_sec)/60))
            else:
                Duration_sec = 0
                Duration_min = 0
            
            Disconnect_Code = cdr_data_list[line_no][14]
            Disconnect_Party = cdr_data_list[line_no][63]
            (sYear,sMonth,sDay,sHour) = get_start_datetime(Startdate,Starttime)
            if sYear == 9999:
                (sYear,sMonth,sDay,sHour) = get_start_datetime(Enddate,Endtime)
            #end if
            #Attention, this AtoZ_list is a globe variable, defined in main block
            (Country,Destination,Prefix) = get_country_by_number(Called_Number,AtoZ_list)
            
    ##        print('this is STOP')
    ##        print("Incoming_trunk_ID:%s"%Incoming_trunk_ID)
    ##        print("Outgoing_trunk_ID:%s"%Outgoing_trunk_ID)
    ##        print("In_carrier_Name:%s"%In_carrier_Name)
    ##        print("Out_carrier_Name:%s"%Out_carrier_Name)
    ##        print("Source_IP:%s"%Source_IP)
    ##        print("Destination_IP:%s"%Destination_IP)
    ##        print("Startdate:%s"%Startdate)
    ##        print("Starttime:%s"%Starttime)
    ##        print("Enddate:%s"%Enddate)
    ##        print("Endtime:%s"%Endtime)
    ##        print("Connect_Time:%s"%Connect_Time)
    ##        print("Called_Number:%s"%Called_Number)
    ##        print("Caller_Number:%s"%Caller_Number)
    ##        print("Duration_sec:%s"%Duration_sec)
    ##        print("Duration_min:%s"%Duration_min)
    ##        print("Disconnect_Code:%s"%Disconnect_Code)
    ##        print("Disconnect_Party:%s"%Disconnect_Party)
    ##        print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
##            print(sYear)
##            print(sMonth)
##            print(sDay)
##            print(sHour)
            
            
        #CDR system info
        else:
    ##        print('system info line')
    ##        print(cdr_data_list[line_no][0])
    ##        print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    ##if it's system info line, then continute to NEXT line
            #to skip DB insert
            line_no = line_no + 1
            continue

        #End if

    ##if it's NOT system info line, then insert DB
        #Prepare SQL query to INSERT
        sql = "INSERT INTO cdr_"+ CDR_dir_name +"(Duration_sec,Duration_min,Incoming_trunk_ID,Outgoing_trunk_ID,In_carrier_Name,Out_carrier_Name,Source_IP,Destination_IP,Startdate,Starttime,Enddate,Endtime,Connect_Time,Called_Number,Caller_Number,Disconnect_Code,Disconnect_Party,Syear,Smonth,Sday,Shr,Call_type,Country,Dest,Prefix)"
        sql = sql + "VALUES ('%.3f','%.2f','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" %(float(Duration_sec),float(Duration_min),Incoming_trunk_ID,Outgoing_trunk_ID,In_carrier_Name,Out_carrier_Name,Source_IP,Destination_IP,Startdate,Starttime,Enddate,Endtime,Connect_Time,Called_Number,Caller_Number,Disconnect_Code,Disconnect_Party,sYear,sMonth,sDay,sHour,Call_type,Country,Destination,Prefix)
        #print(sql)
        try:
           # Execute SQL
           cursor.execute(sql)
        except:
           # Rollback in case there is any error
           print("Insert CDR to DB error!!")
           conn.rollback()
        #increament, to next line
        line_no = line_no + 1


    #End for

    # Commit your changes in the database
    conn.commit()
    # disconnect from database
    cursor.close()
    conn.close()


    return;
#end function read_single_cdr()


#########################################################
#function compare_number_prefix(prefix_str,number_str)
#compare number and prefix, return boolean trun or false
#########################################################
def compare_number_prefix(prefix_str,number_str):

    prefix_str = prefix_str.strip()
    prefix_length = len(prefix_str)

    number_str = number_str.strip()
    number_length = len(number_str)

    if number_length < prefix_length:
        return 0;
    else:
        compare_str = number_str[:prefix_length].strip()
        if compare_str == prefix_str:
            return 1;
        else:
            return 0;
        
#end function compare_number_prefix()



#########################################################
#function get_country_by_number(called_number)
#parse called_number and return country name,destination name,prefix
#########################################################
def get_country_by_number(called_number,az_prefix):

    for i in range(len(az_prefix)):
        if compare_number_prefix(az_prefix[i][2],called_number):
            #return value is country_name,dest_name,prefix;
            return az_prefix[i][0],az_prefix[i][1],az_prefix[i][2];
        #end if
    #end for
    return "None","None","None";

#end function get_country_by_number()


#########################################################
#function get_start_datetime(start_date,start_time)
#parse start date and time and return
#########################################################
def get_start_datetime(start_date,start_time):
    
    try:
        date_list = start_date.split('/')
        sYear = date_list[2].strip()
        sMonth = date_list[0].strip()
        sDay = date_list[1].strip()

        time_list = start_time.split(':')
        sHour = time_list[0].strip()
    except:
        sYear = 9999
        sMonth = 99
        sDay = 99
        sHour = 99
        
    return sYear,sMonth,sDay,sHour;
#end function get_start_datetime()

#########################################################
#read az table from Database
#return a list with Country name,Area name,Prefix
#parameter 'AtoZ' means all country
#parameter 'China' means return all China's prefix
#or other country name to return all that country's prefix
#########################################################
def read_az_to_list(Country):
    
    conn = mysql.connector.connect(user=DB_username, password=DB_password,database=DB_name)
    cursor = conn.cursor()

    try:
        if Country != 'AtoZ':
            cursor.execute("SELECT Country,Destination,Prefix FROM az_tbl_overall WHERE Country like '" + Country + "' ORDER BY Prefix DESC")
        else:
            cursor.execute("SELECT Country,Destination,Prefix FROM az_tbl_overall ORDER BY Prefix DESC")
        #end if
    except:
        print("Database ERROR!!! Can't read A to Z prefix")

    result_set = cursor.fetchall()
    row_count = cursor.rowcount
    ##print ("row count: %s" %row_count)

    #Creat a 2D list with (A to Z row counts) elements in which each element has 3 refrences
    az_prefix_list = [[] for i in range(row_count)]

    row_number = 0
    for raws in result_set:
        az_prefix_list[row_number].append(raws[0])
        az_prefix_list[row_number].append(raws[1])
        az_prefix_list[row_number].append(raws[2])
##        print ("Country: %s" %az_prefix_list[row_number][0])
##        print ("Destination: %s" %az_prefix_list[row_number][1])
##        print ("Prefix: %s" %az_prefix_list[row_number][2])
##        print(row_number)
        row_number = row_number + 1
    #end for

    #print("Country:%s, prefix loaded:%s" %(Country,row_number))

    # disconnect from database
    cursor.close()
    conn.close()

    return az_prefix_list
#end function read_az_to_list()

#########################################################
#Function Parse CDR Function
#
#########################################################
def parse_CDR_All(CDR_dir_name):

    list_of_files = glob.glob('./'+ CDR_dir_name +'/*.ACT')


    conn = mysql.connector.connect(user=DB_username, password=DB_password,database=DB_name)
    cursor = conn.cursor()

    #drop DB table CDR_dir_name, if exists
    try:
        cursor.execute("DROP TABLE IF EXISTS " + "cdr_" + CDR_dir_name)
    except:
        print ("DB Drop CDR table error!");
        conn.rollback()


    #creat DB table of CDR_dir_name
    sql_create_tbl = "CREATE TABLE cdr_" + CDR_dir_name
    sql_create_tbl = sql_create_tbl + """(
	ID bigint not null	primary key auto_increment,
	call_type varchar(15),
	Incoming_trunk_ID varchar(255),
	Outgoing_trunk_ID varchar(255),
	Country_Code varchar(255),
	Country_Name varchar(255),
	Area_Code varchar(255),
	Area_Name varchar(255),
	In_carrier_Name varchar(255),
	Out_carrier_Name varchar(255),
	Source_IP varchar(255),
	Destination_IP varchar(255),
	Startdate varchar(255),
	Starttime varchar(255),
	Enddate varchar(255),
	Endtime varchar(255),
	Connect_Time varchar(255),
	Called_Number varchar(255),
	Caller_Number varchar(255),
	Duration_sec float(18,2),
	Duration_min float(18,3),
	Disconnect_Code varchar(255),
	Disconnect_Party varchar(255),
	Smonth varchar(255),
	Sday varchar(255),
	Syear varchar(255),
	Shr varchar(255),
	Prefix varchar(255),
	Country varchar(255),
	Dest varchar(255)
	)
    """
    #creat DB table of CDR_dir_name
    try:
        cursor.execute(sql_create_tbl)
    except:
        print("DB Create CDR table error!")
        conn.rollback()
    
    # disconnect from database
    cursor.close()
    conn.close()

    #insert DB
    file_count = 0
    for fileName in list_of_files:
        read_single_cdr(fileName,CDR_dir_name)
        file_count = file_count + 1
        print(fileName + " ---- Done")

    print("Total CDR files read: %s" %file_count)
    
#end function parse_CDR_all

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
#Function generate daily summary report
#read from cdr table and group by values
#########################################################
def gen_daily_summary_csv(CDR_dir_name):
    #open DB #prepare a cursor object using cursor() method
    conn = mysql.connector.connect(user=DB_username, password=DB_password,database=DB_name)
    cursor = conn.cursor()

    sql_query ="""
SELECT A.Incoming_trunk_ID,A.Outgoing_trunk_ID,
       A.Smonth,A.Sday,A.Syear,A.Shr,A.Prefix,A.Country,
       A.Dest,A.dur,A.conn,B.allcall
FROM 
(
	(SELECT	Incoming_trunk_ID,Outgoing_trunk_ID,
		Smonth,Sday,Syear,Shr,Prefix,Country,
		Dest,sum(Duration_min) AS dur,count(*) AS conn
	 FROM cdr_""" + CDR_dir_name + """
	 WHERE Duration_min > 0
	 GROUP BY
	       Incoming_trunk_ID,Outgoing_trunk_ID,
	       Smonth,Sday,Syear,Shr,Prefix,Country,Dest
	 ORDER BY id) A
    LEFT JOIN
	(SELECT	Incoming_trunk_ID,Outgoing_trunk_ID,
		Smonth,Sday,Syear,Shr,Prefix,Country,
		Dest,sum(Duration_min),count(*) AS allcall
	 FROM cdr_""" + CDR_dir_name + """
	 GROUP BY
	       Incoming_trunk_ID,Outgoing_trunk_ID,
	       Smonth,Sday,Syear,Shr,Prefix,Country,Dest
	 ORDER BY id) B
     ON 
	 (A.Incoming_trunk_ID=B.Incoming_trunk_ID
	  AND A.Outgoing_trunk_ID = B.Outgoing_trunk_ID
	  AND A.Smonth = B. Smonth AND A.Sday = B.Sday
	  AND A.Syear = B.Syear AND A.Shr = B.Shr
	  AND A.Prefix = B.Prefix AND A.Country=B.Country
	  AND A.Dest = B.Dest)
)
    """
    try:
        cursor.execute(sql_query)
    except:
        print("Database ERROR!!! Generate Daily Summary error")

    result_set = cursor.fetchall()
    row_count = cursor.rowcount
    ##print ("row count: %s" %row_count)

    #open csv file
    daily_summary_file = open(CDR_dir_name + ".csv", "w")
    #daily_summary_file.write("FromCarrier,ToCarrier,sMonth,sDay,sYear,sHour,Prefix,Country,Dest,Duration_m,conn_call,all_call\n")

    row_number = 0
    for raws in result_set:
        daily_summary_file.write("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n" %(raws[0],raws[1],raws[2],raws[3],raws[4],raws[5],raws[6],raws[7],raws[8],raws[9],raws[10],raws[11]))
        row_number = row_number + 1
    #end for


    #close file
    daily_summary_file.close()

    print("daily summary file " + CDR_dir_name + ".csv created successful")
    # disconnect from database
    cursor.close()
    conn.close()

#end gen_daily_summary_csv()

    
#########################################################
#Main BLOCK
#########################################################

#SET MySQL DATABASE INFO
#Globe variables
#Set MySQL Database info
(DB_host,DB_username,DB_password,DB_name) = read_config_file()

#need input cdr directory
if len(sys.argv) < 2:
    print("Please input CDR dir name")
else:
    #read A to Z prefix table
    #AtoZ means all country
    AtoZ_list = read_az_to_list('AtoZ')
    #print (test)
    
    #read CDR files dir
    CDR_dir = sys.argv[1].strip()
    parse_CDR_All(CDR_dir)

    #generate daily summary .csv
    gen_daily_summary_csv(CDR_dir)

#end if

