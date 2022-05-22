import datetime
import sys
import os
import shutil 
import pandas as pd
import jaydebeapi



def get_conn():
    conn = jaydebeapi.connect(
        'oracle.jdbc.driver.OracleDriver',
        'jdbc:oracle:thin:user_name/password@de-oracle.chronosavant.ru:1521/deoracle',
        ['user_name', 'password'],
        '/home/de3at/ojdbc8.jar'
    )
    return conn

#conn = get_conn()
#conn.jconn.setAutoCommit(False)    
# conn = get_conn()    
# curs = conn.cursor()


#==============чтение файлов из дирректории data  запись в таблицы источники и перемещение файлов в archive

def read_and_isert_to_source(curs):
    dir_list = os.listdir('/home/de3at/avzh/data') # звлекаем имена файлов из директории
    to_sort = [(f, f.split('_')[-1].split('.')[0]) for f in dir_list]
    to_sort_dt = [(f, datetime.datetime.strptime(d, '%d%m%Y')) for f, d in to_sort]
    found = {} # сформируем словарь актуальных файлов (с именами файлов с минимальной датой)
    for ls in to_sort_dt:
        key = '_'.join(ls[0].split('_')[:-1])
        if (key in found) == False:
            found[key] = (ls)
        else:
            ls_last = found[key]
            if ls_last[1] > ls[1]:
                found[key] = (ls)
    print('прочитана директория data . . . . . . . . . . .  OK')            
       

#читаем xlsx_passport файл

    f_nm = found['passport_blacklist'][0]        
    df_passport = pd.read_excel('/home/de3at/avzh/data/{}'.format(f_nm))        
    df_passport = df_passport.astype(str)
    print('прочитан xlsx_passport файл . . . . . . . . . . OK')
 
 # #читаем transactions CSV файл

    f_nm = found['transactions'][0]
    df_transactions= pd.read_csv('/home/de3at/avzh/data/{}'.format(f_nm), sep=';', header=0, index_col=None)
    df_transactions = df_transactions.astype(str)
    print('прочитан csv_transaction файл . . . . . . . . . . OK')

 #читаем xlsx_terminals файл

    f_nm = found['terminals'][0]
    f_dt = found['terminals'][1]    
    df_terminals = pd.read_excel('/home/de3at/avzh/data/{}'.format(f_nm))
    df_terminals['update_dt'] = f_dt    
    df_terminals = df_terminals.astype(str)
    print('прочитан xlsx_terminals файл . . . . . . . . . . OK')
  
# insert XL in de3at.avzh_stg_pssprt_blcklst_source

    curs.executemany( "insert into de3at.avzh_stg_pssprt_blcklst (entry_dt, passport_num) "\
    "values (to_date(?,'YYYY-MM-DD HH24:MI:SS'), ?)", df_passport.values.tolist())
    print('isert passport . . . . . . . . . . OK')
    
# insert XL in de3at.avzh_stg_terminals_source

    curs.executemany( "insert into de3at.avzh_stg_terminals (terminal_id, terminal_type, terminal_city, terminal_address, update_dt) "\
    "values (?, ?, ?, ?, to_date(?,'YYYY-MM-DD HH24:MI:SS'))", df_terminals.values.tolist())
    print('isert terminals . . . . . . . . . . OK')    


# insert csv in de3at.avzh_stg_transactions_source

    curs.executemany( "insert into de3at.avzh_stg_transactions (trans_id, trans_date, amt, card_num, oper_type, oper_result, terminal) "\
    "values (?, to_date(?,'YYYY-MM-DD HH24:MI:SS'), to_number(replace(?, ',', '.')), ?, ?, ?, ?)", df_transactions.values.tolist())
    print('isert transaktions . . . . . . . . . . OK')    
       
# перемещение файлов c мин датой в бэкап

    for val_f in found:
        file_name = found[val_f][0]
        source_path = '/home/de3at/avzh/data/{}'.format(file_name)
        destination_path = '/home/de3at/avzh/archive/{}.buckup'.format(file_name)
        new_location = shutil.move(source_path, destination_path)
    print('move files . . . . . . . . . . OK')

           
            


#============================================подготовка структур=======================================

# -----------------------------паспота-----------------------------------------------------


# CREATE TABLE dwh_fact_pssprt_blcklst
avzh_dwh_fact_pssprt_blcklst = """ CREATE TABLE de3at.avzh_dwh_fact_pssprt_blcklst (
    PASSPORT_NUM VARCHAR2(15),
	ENTRY_DT DATE
    )"""    
def dwh_fact_pssprt_blcklst(curs):
    curs.execute(avzh_dwh_fact_pssprt_blcklst)
    
# CREATE TABLE 
avzh_stg_pssprt_blcklst = """CREATE TABLE de3at.avzh_stg_pssprt_blcklst (
    PASSPORT_NUM VARCHAR2(15),
	ENTRY_DT DATE
    ) """    
def stg_pssprt_blcklst(curs):
    curs.execute(avzh_stg_pssprt_blcklst )
    
    
# ------------терминалы-------------------------------------------------------------- 
       
# CREATE dwh_dim_terminals_hist TABLE
avzh_dwh_dim_terminals = """ create table de3at.avzh_dwh_dim_terminals_hist (
    TERMINAL_ID VARCHAR2(7), 
    TERMINAL_TYPE  VARCHAR2(3), 
    TERMINAL_CITY VARCHAR2(50), 
    TERMINAL_ADDRESS VARCHAR2(100),
	effective_from_dt date ,
	effective_to_dt date ,
    deleted_flg char(1)
	)"""    
def dwh_dim_terminals(curs):
    curs.execute(avzh_dwh_dim_terminals)
    
# CREATE stg_terminals_del TABLE
avzh_stg_terminals_del = """CREATE TABLE  de3at.avzh_stg_terminals_del(
    TERMINAL_ID VARCHAR2(7)
    ) """     
def stg_terminals_del(curs):
    curs.execute(avzh_stg_terminals_del)
    
# CREATE TABLE stg_terminals
avzh_stg_terminals = """CREATE TABLE de3at.avzh_stg_terminals (
    TERMINAL_ID VARCHAR2(7), 
    TERMINAL_TYPE  VARCHAR2(3), 
    TERMINAL_CITY VARCHAR2(50), 
    TERMINAL_ADDRESS VARCHAR2(100),
    UPDATE_DT DATE
    ) """    
def stg_terminals(curs):
    curs.execute(avzh_stg_terminals)


# -------------транцакции------------------------------------------------------------------------------------ 

      
# CREATE TABLE stg_transactions_source
  
# CREATE TABLE stg_transactions
avzh_stg_transactions = """CREATE TABLE de3at.avzh_stg_transactions (
    TRANS_ID VARCHAR2(20), 
    TRANS_DATE DATE,
    AMT NUMBER(15,2), 
    CARD_NUM VARCHAR2(20),
    OPER_TYPE VARCHAR2(20), 
    OPER_RESULT VARCHAR2(10), 
    TERMINAL VARCHAR2(7)
    ) """    
def stg_transactions(curs):
    curs.execute( avzh_stg_transactions)
    
# CREATE TABLE dwh_fact_transactions
avzh_dwh_fact_transactions = """CREATE TABLE de3at.avzh_dwh_fact_transactions (
    TRANS_ID VARCHAR2(20), 
    TRANS_DATE DATE,
    AMT NUMBER(15,2), 
    CARD_NUM VARCHAR2(20),
    OPER_TYPE VARCHAR2(20), 
    OPER_RESULT VARCHAR2(10), 
    TERMINAL VARCHAR2(7)
    ) """     
def dwh_fact_transactions(curs):
    curs.execute(avzh_dwh_fact_transactions)
 

# ------------------------------accounts----------------------------------------
  
# CREATE TABLE dwh_dim_accounts_hist
avzh_dwh_dim_accounts = """CREATE TABLE de3at.avzh_dwh_dim_accounts_hist (
    ACCOUNT CHAR(20),
    VALID_TO DATE,
    CLIENT VARCHAR2(20),
	effective_from_dt date ,
	effective_to_dt date ,
    deleted_flg char(1)
    ) """    
def dwh_dim_accounts(curs):
    curs.execute(avzh_dwh_dim_accounts)
 
# CREATE TABLE stg_accounts_del
avzh_stg_accounts_del = """CREATE TABLE  de3at.avzh_stg_accounts_del(
    ACCOUNT CHAR(20)
    ) """     
def stg_accounts_del(curs):
    curs.execute(avzh_stg_accounts_del)
    
# CREATE TABLE stg_accounts
avzh_stg_accounts = """ CREATE TABLE de3at.avzh_stg_accounts (
    ACCOUNT CHAR(20),
    VALID_TO DATE,
    CLIENT VARCHAR2(20),
	CREATE_DT DATE,
    UPDATE_DT DATE
    )"""     
def stg_accounts(curs):
    curs.execute(avzh_stg_accounts)   
    
    
    
# -----------------------clients-------------------------------------------------    
   
    # CREATE TABLE stg_clients
avzh_stg_clients = """CREATE TABLE de3at.avzh_stg_clients (
    CLIENT_ID VARCHAR2(20),
    LAST_NAME VARCHAR2(100),
    FIRST_NAME VARCHAR2(100),
    PATRONYMIC VARCHAR2(100),
    DATE_OF_BIRTH DATE,
    PASSPORT_NUM VARCHAR2(15),
    PASSPORT_VALID_TO DATE,
    PHONE VARCHAR2(20),
	CREATE_DT  date,
    UPDATE_DT DATE
    )"""    
def stg_clients(curs):
    curs.execute(avzh_stg_clients)
    
# CREATE TABLE stg_clients_del
avzh_stg_clients_del = """CREATE TABLE  de3at.avzh_stg_clients_del(
    CLIENT_ID VARCHAR2(20)
    ) """     
def stg_clients_del(curs):
    curs.execute(avzh_stg_clients_del)
    
# CREATE TABLE hist
avzh_dwh_dim_clients = """CREATE TABLE de3at.avzh_dwh_dim_clients_hist (
    CLIENT_ID VARCHAR2(20),
    LAST_NAME VARCHAR2(100),
    FIRST_NAME VARCHAR2(100),
    PATRONYMIC VARCHAR2(100),
    DATE_OF_BIRTH DATE,
    PASSPORT_NUM VARCHAR2(15),
    PASSPORT_VALID_TO DATE,
    PHONE VARCHAR2(20),
	effective_from_dt date ,
	effective_to_dt date ,
    deleted_flg char(1)
	) """     
def dwh_dim_clients(curs):
    curs.execute(avzh_dwh_dim_clients)
    
    
    
# --------------------------cards--------------------------------------   
    
    # CREATE TABLE dwh_dim_cards_hist
avzh_dwh_dim_cards = """CREATE TABLE de3at.avzh_dwh_dim_cards_hist (
    CARD_NUM  VARCHAR2(20), 
    ACCOUNT  VARCHAR2(20), 
	effective_from_dt date ,
	effective_to_dt date ,
    deleted_flg char(1)
    ) """    
def dwh_dim_cards(curs):
    curs.execute(avzh_dwh_dim_cards)
    
# CREATE TABLE
avzh_stg_cards_del = """ CREATE TABLE  de3at.avzh_stg_cards_del (
    CARD_NUM VARCHAR2(20)
    )"""     
def stg_cards_del(curs):
    curs.execute(avzh_stg_cards_del)
    
# CREATE TABLE
avzh_stg_cards = """CREATE TABLE de3at.avzh_stg_cards (
    CARD_NUM  VARCHAR2(20), 
    ACCOUNT  VARCHAR2(20), 
	CREATE_DT DATE,
    UPDATE_DT DATE
    ) """     
def stg_cards(curs):
    curs.execute(avzh_stg_cards)

    # CREATE TABLE meta
avzh_meta = """CREATE TABLE  de3at.avzh_meta(
    schema_name varchar2(30),
    table_name varchar2(30),
    max_update_dt date
    ) """    
def meta(curs):
    curs.execute(avzh_meta)
    
 # insert meta
     
    
#======================================incremental=============================================================   

# realizovana proverka dublei 
  
    #  TABLE  truncate_stg_pssprt_blcklst
truncate_stg_pssprt_blcklst = """truncate table de3at.avzh_stg_pssprt_blcklst """    
def truncat_stg_pssprt_blcklst(curs):
    curs.execute( truncate_stg_pssprt_blcklst)
    print('truncate_stg_pssprt_blcklst . . . . . . . . . . OK')


    #  TABLE isert_dwh_fact_pssprt_blcklst
isert_dwh_fact_pssprt_blcklst = """ insert into de3at.avzh_dwh_fact_pssprt_blcklst ( PASSPORT_NUM, ENTRY_DT )
select PASSPORT_NUM, ENTRY_DT 
from de3at.avzh_stg_pssprt_blcklst
where PASSPORT_NUM  not in ( 
    select PASSPORT_NUM
    from de3at.avzh_dwh_fact_pssprt_blcklst
)"""     
def inser_dwh_fact_pssprt_blcklst(curs):
    curs.execute(isert_dwh_fact_pssprt_blcklst)
    print('isert_dwh_fact_pssprt_blcklst . . . . . . . . . . OK')
# ------------------transaction--------------------------------------------

   
    #  TABLE truncat_avzh_stg_transactions
truncate_stg_transactions = """truncate table de3at.avzh_stg_transactions """     
def truncat_stg_transactions(curs):
    curs.execute(truncate_stg_transactions)
    print('truncate_stg_transactions . . . . . . . . . . OK')


    #  TABLE insert_dwh_fact_transactions
insert_dwh_fact_transactions = """ insert into de3at.avzh_dwh_fact_transactions ( TRANS_ID, TRANS_DATE, AMT, CARD_NUM, OPER_TYPE, OPER_RESULT, TERMINAL )
select TRANS_ID, TRANS_DATE, AMT, CARD_NUM, OPER_TYPE, OPER_RESULT, TERMINAL 
from de3at.avzh_stg_transactions
where TRANS_ID  not in ( 
    select TRANS_ID
    from de3at.avzh_dwh_fact_transactions
)"""     
def inser_dwh_fact_transactions(curs):
    curs.execute(insert_dwh_fact_transactions)
    print(' insert_dwh_fact_transactions . . . . . . . . . . OK')

# ------------------------------------terminals-------------------------------------------

        #  TABLE   
insert_stg_terminals_del = """insert into de3at.avzh_stg_terminals_del ( TERMINAL_ID )
select TERMINAL_ID from de3at.avzh_stg_terminals"""     

        #  TABLE   
update_dwh_dim_terminals_hist = """insert into de3at.avzh_dwh_dim_terminals_hist (
    TERMINAL_ID, 
    TERMINAL_TYPE, 
    TERMINAL_CITY, 
    TERMINAL_ADDRESS,
	effective_from_dt,
	effective_to_dt,
    deleted_flg  
	) 
select
    stg.TERMINAL_ID, 
    stg.TERMINAL_TYPE, 
    stg.TERMINAL_CITY, 
    stg.TERMINAL_ADDRESS,
    stg.update_dt,
    to_date( '31.12.9999', 'DD.MM.YYYY' ), 
    'N'
from de3at.avzh_dwh_dim_terminals_hist tgt
inner join de3at.avzh_stg_terminals stg
on ( stg.TERMINAL_ID = tgt.TERMINAL_ID and tgt.effective_to_dt = to_date( '31.12.9999', 'DD.MM.YYYY' ) and deleted_flg = 'N' ) -- актуальные
where 1=0 -- есть изменения в поляж
    or stg.TERMINAL_TYPE <> tgt.TERMINAL_TYPE or ( stg.TERMINAL_TYPE is null and tgt.TERMINAL_TYPE is not null ) or ( stg.TERMINAL_TYPE is not null and tgt.TERMINAL_TYPE is null )
    or stg.TERMINAL_CITY <> tgt.TERMINAL_CITY or ( stg.TERMINAL_CITY is null and tgt.TERMINAL_CITY is not null ) or ( stg.TERMINAL_CITY is not null and tgt.TERMINAL_CITY is null )        
    or stg.TERMINAL_ADDRESS <> tgt.TERMINAL_ADDRESS or ( stg.TERMINAL_ADDRESS is null and tgt.TERMINAL_ADDRESS is not null ) or ( stg.TERMINAL_ADDRESS is not null and tgt.TERMINAL_ADDRESS is null )
"""   
    
        #  TABLE
merge_dwh_dim_terminals = """merge into de3at.avzh_dwh_dim_terminals_hist tgt
using de3at.avzh_stg_terminals stg
on( stg.TERMINAL_ID = tgt.TERMINAL_ID and deleted_flg = 'N' )
when matched then 
    update set tgt.effective_to_dt = stg.update_dt - interval '1' minute
    where 1=1
	and tgt.effective_to_dt = to_date( '31.12.9999', 'DD.MM.YYYY' )
	and (1=0
        or stg.TERMINAL_TYPE <> tgt.TERMINAL_TYPE or ( stg.TERMINAL_TYPE is null and tgt.TERMINAL_TYPE is not null ) or ( stg.TERMINAL_TYPE is not null and tgt.TERMINAL_TYPE is null )
        or stg.TERMINAL_CITY <> tgt.TERMINAL_CITY or ( stg.TERMINAL_CITY is null and tgt.TERMINAL_CITY is not null ) or ( stg.TERMINAL_CITY is not null and tgt.TERMINAL_CITY is null )        
        or stg.TERMINAL_ADDRESS <> tgt.TERMINAL_ADDRESS or ( stg.TERMINAL_ADDRESS is null and tgt.TERMINAL_ADDRESS is not null ) or ( stg.TERMINAL_ADDRESS is not null and tgt.TERMINAL_ADDRESS is null )
	)
when not matched then -- добавляем новые записи
    insert ( 
    TERMINAL_ID, 
    TERMINAL_TYPE, 
    TERMINAL_CITY, 
    TERMINAL_ADDRESS,
	effective_from_dt,
	effective_to_dt,
    deleted_flg  
	) 
    values ( 
    stg.TERMINAL_ID, 
    stg.TERMINAL_TYPE, 
    stg.TERMINAL_CITY, 
    stg.TERMINAL_ADDRESS, 
	stg.update_dt, 
	to_date( '31.12.9999', 'DD.MM.YYYY' ),
	'N' 
) """     

    
        #  TABLE
delete_y_dwh_dim_terminals = """INSERT INTO de3at.avzh_dwh_dim_terminals_hist ( 
    TERMINAL_ID, 
    TERMINAL_TYPE, 
    TERMINAL_CITY, 
    TERMINAL_ADDRESS,
	effective_from_dt,
	effective_to_dt,
    deleted_flg 
	) 
SELECT
	tgt.TERMINAL_ID, 
    tgt.TERMINAL_TYPE, 
    tgt.TERMINAL_CITY, 
    tgt.TERMINAL_ADDRESS,
    (SELECT MAX(update_dt) FROM de3at.AVZH_STG_TERMINALS), 
    to_date( '31.12.9999', 'DD.MM.YYYY' ), 
    'Y'
FROM de3at.avzh_dwh_dim_terminals_hist tgt
LEFT JOIN de3at.avzh_stg_terminals_del stg
ON ( stg.TERMINAL_ID = tgt.TERMINAL_ID )
WHERE stg.TERMINAL_ID IS NULL AND deleted_flg = 'N' AND tgt.effective_to_dt = to_date( '31.12.9999', 'DD.MM.YYYY' ) """

        #  TABLE
delete_update_dwh_dim_terminals = """UPDATE de3at.avzh_dwh_dim_terminals_hist tgt
SET effective_to_dt = (SELECT MAX(update_dt) FROM de3at.AVZH_STG_TERMINALS) - interval '1' minute
WHERE tgt.TERMINAL_ID not in (select TERMINAL_ID from de3at.avzh_stg_terminals_del)
    and tgt.effective_to_dt = to_date( '31.12.9999', 'DD.MM.YYYY' )
    and tgt.deleted_flg = 'N' """     

    
        #  TABLE
update_meta_terminals = """merge into de3at.avzh_meta trg
using (
    select
        'de3at' as schema_name,
        'terminals_source' as table_name,
        ( select max(update_dt) from de3at.avzh_stg_terminals ) as max_update_dt
	from dual ) src
on ( trg.schema_name = src.schema_name and trg.table_name = src.table_name )
when matched then 
    update set trg.max_update_dt = src.max_update_dt
    where src.max_update_dt is not null
when not matched then 
    insert ( schema_name, table_name, max_update_dt )
    values ( 'de3at', 'terminals_source', src.max_update_dt )"""     


    #  TABLE 

truncate_stg_terminals = """truncate table de3at.avzh_stg_terminals """     
def truncat_stg_terminals_source(curs):
    curs.execute(truncate_stg_terminals)
    print('truncate_stg_terminals . . . . . . . . . . OK')
        
        #  TABLE 
truncate_stg_terminals_del = """truncate table de3at.avzh_stg_terminals_del """     
def truncat_stg_terminals_del(curs):
    curs.execute(truncate_stg_terminals_del)
    print('truncate_stg_terminals_del . . . . . . . . . . OK')

          
# ----------------------------------accounts------------------------------------------------ 

        #  TABLE
truncate_stg_accounts_del = """truncate table de3at.avzh_stg_accounts_del """     
def truncat_stg_accounts_del(curs):
    curs.execute(truncate_stg_accounts_del)
    print('truncate_stg_accounts_del . . . . . . . . . . OK')

truncate_stg_accounts = """truncate table de3at.avzh_stg_accounts"""     
def truncat_stg_accounts(curs):
    curs.execute(truncate_stg_accounts)
    print('truncate_stg_accounts . . . . . . . . . . OK')    
        
        #  TABLE
insert_stg_accounts = """ insert into de3at.avzh_stg_accounts (ACCOUNT, VALID_TO, CLIENT,CREATE_DT, UPDATE_DT )
select  ACCOUNT, VALID_TO, CLIENT, CREATE_DT ,UPDATE_DT 
from bank.accounts
where CREATE_DT > coalesce( 
								( 
								select max_update_dt
								from de3at.avzh_meta
								where schema_name = 'de3at' and table_name = 'accounts_source'
								)
								,to_date( '1800.01.01', 'YYYY.MM.DD' )
							)""" 
    #  TABLE
def inser_stg_accounts(curs):
    curs.execute(insert_stg_accounts)
    print('insert_stg_accounts . . . . . . . . . . OK')
    
        #  TABLE
insert_stg_accounts_del = """ insert into de3at.avzh_stg_accounts_del ( ACCOUNT )
select ACCOUNT from bank.accounts"""     
def inser_stg_accounts_del(curs):
    curs.execute(insert_stg_accounts_del)
    print('insert_stg_accounts_del . . . . . . . . . . OK')

        #  TABLE
update_dwh_dim_accounts = """insert into avzh_dwh_dim_accounts_hist (
	ACCOUNT,
	VALID_TO,
	CLIENT,
	effective_from_dt,
	effective_to_dt,
    deleted_flg  
	) 
select
	stg.ACCOUNT,
	stg.VALID_TO,
	stg.CLIENT,
	stg.CREATE_DT,
    to_date( '31.12.9999', 'DD.MM.YYYY' ), 
    'N'
from de3at.avzh_dwh_dim_accounts_hist tgt
inner join de3at.avzh_stg_accounts stg
on ( stg.ACCOUNT = tgt.ACCOUNT and tgt.effective_to_dt = to_date( '31.12.9999', 'DD.MM.YYYY' ) and deleted_flg = 'N' )-- актуальные
where 1=0 -- есть изменения в поляж
    or stg.VALID_TO <> tgt.VALID_TO or ( stg.VALID_TO is null and tgt.VALID_TO is not null ) or ( stg.VALID_TO is not null and tgt.VALID_TO is null )
    or stg.CLIENT <> tgt.CLIENT or ( stg.CLIENT is null and tgt.CLIENT is not null ) or ( stg.CLIENT is not null and tgt.CLIENT is null )
 """     
def updat_dwh_dim_accounts(curs):
    curs.execute(update_dwh_dim_accounts)      

        #  TABLE
merge_dwh_dim_accounts = """merge into de3at.avzh_dwh_dim_accounts_hist tgt
using de3at.avzh_stg_accounts stg
on( stg.ACCOUNT = tgt.ACCOUNT and deleted_flg = 'N' )
when matched then 
    update set tgt.effective_to_dt = stg.update_dt - interval '1' minute
    where 1=1
	and tgt.effective_to_dt = to_date( '31.12.9999', 'DD.MM.YYYY' )
	and (1=0
        or stg.VALID_TO <> tgt.VALID_TO or ( stg.VALID_TO is null and tgt.VALID_TO is not null ) or ( stg.VALID_TO is not null and tgt.VALID_TO is null )
		or stg.CLIENT <> tgt.CLIENT or ( stg.CLIENT is null and tgt.CLIENT is not null ) or ( stg.CLIENT is not null and tgt.CLIENT is null )
	)
when not matched then -- добавляем новые записи
    insert ( 
    ACCOUNT,
	VALID_TO,
	CLIENT,
	effective_from_dt,
	effective_to_dt,
    deleted_flg  
	) 
    values ( 
    stg.ACCOUNT,
	stg.VALID_TO,
	stg.CLIENT,
	stg.CREATE_DT,
    to_date( '31.12.9999', 'DD.MM.YYYY' ), 
    'N'
    )"""     
def merg_dwh_dim_accounts(curs):
    curs.execute(merge_dwh_dim_accounts)
    print('merge_dwh_dim_accounts . . . . . . . . . . OK')

        #  TABLE
delete_y_dwh_dim_accounts = """INSERT INTO de3at.avzh_dwh_dim_accounts_hist ( 
    ACCOUNT,
	VALID_TO,
	CLIENT,
	effective_from_dt,
	effective_to_dt,
    deleted_flg  
	) 
SELECT
	tgt.ACCOUNT,
	tgt.VALID_TO,
	tgt.CLIENT,
    (SELECT MAX(update_dt) FROM de3at.AVZH_STG_accounts), 
    to_date( '31.12.9999', 'DD.MM.YYYY' ), 
    'Y'
FROM de3at.avzh_dwh_dim_accounts_hist tgt
LEFT JOIN de3at.avzh_stg_accounts_del stg
ON ( stg.ACCOUNT = tgt.ACCOUNT )
WHERE stg.ACCOUNT IS NULL AND deleted_flg = 'N' AND tgt.effective_to_dt = to_date( '31.12.9999', 'DD.MM.YYYY' )
 """     
def delet_y_dwh_dim_accounts(curs):
    curs.execute(delete_y_dwh_dim_accounts)
    print('delete_y_dwh_dim_accounts . . . . . . . . . . OK')

        #  TABLE
delete_update_dwh_dim_accounts = """UPDATE de3at.avzh_dwh_dim_accounts_hist tgt
SET effective_to_dt = (SELECT MAX(update_dt) FROM de3at.AVZH_STG_accounts) - interval '1' minute
WHERE tgt.ACCOUNT not in (select ACCOUNT from de3at.avzh_stg_accounts_del)
    and tgt.effective_to_dt = to_date( '31.12.9999', 'DD.MM.YYYY' )
    and tgt.deleted_flg = 'N'"""     
def delet_update_dwh_dim_accounts(curs):
    curs.execute(delete_update_dwh_dim_accounts)
    print('delete_update_dwh_dim_accounts . . . . . . . . . . OK')        
       
        #  TABLE
update_meta_accouns = """merge into de3at.avzh_meta trg
using (
    select
		'de3at' as schema_name,
        'accounts_source' as table_name,
        ( select max( coalesce(update_dt, create_dt) ) from de3at.avzh_stg_accounts ) as max_update_dt
	from dual ) src
on ( trg.schema_name = src.schema_name and trg.table_name = src.table_name )
when matched then 
    update set trg.max_update_dt = src.max_update_dt
    where src.max_update_dt is not null
when not matched then 
    insert ( schema_name, table_name, max_update_dt )
    values ( 'de3at', 'accounts_source',  src.max_update_dt)"""     
def updat_meta_accouns(curs):
    curs.execute(update_meta_accouns) 
    print('update_meta_accouns . . . . . . . . . . OK')
    
    
# ---------------------------cards-------------------------------------    

        
        #  TABLE
truncate_stg_cards = """truncate table de3at.avzh_stg_cards"""     
def truncat_stg_cards(curs):
    curs.execute(truncate_stg_cards)
    print('truncate_stg_cards . . . . . . . . . . OK')
        
        #  TABLE
truncate_stg_cards_del = """truncate table de3at.avzh_stg_cards_del """     
def truncat_stg_cards_del(curs):
    curs.execute(truncate_stg_cards_del)
    print('truncate_stg_cards_del . . . . . . . . . . OK')
    
        #  TABLE
insert_stg_cards = """insert into de3at.avzh_stg_cards ( CARD_NUM, ACCOUNT, CREATE_DT , UPDATE_DT  )
select  CARD_NUM, ACCOUNT,CREATE_DT , UPDATE_DT 
from bank.cards
where CREATE_DT > coalesce( 
				( 
				select max_update_dt
				from de3at.avzh_meta
				where schema_name = 'de3at' and table_name = 'cards_source'
				)
				,to_date( '1800.01.01', 'YYYY.MM.DD' )
		   )"""     
def inser_stg_cards(curs):
    curs.execute(insert_stg_cards)
    print('insert_stg_cards . . . . . . . . . . OK')
        
        #  TABLE
insert_stg_cards_del = """insert into de3at.avzh_stg_cards_del ( CARD_NUM )
select CARD_NUM from bank.cards """     
def inser_stg_cards_del(curs):
    curs.execute(insert_stg_cards_del)
    print('insert_stg_cards_del . . . . . . . . . . OK')
    
        #  TABLE
update_dwh_dim_cards = """insert into avzh_dwh_dim_cards_hist (
	CARD_NUM,
	ACCOUNT,
	effective_from_dt,
	effective_to_dt,
    deleted_flg  
	) 
select
	stg.CARD_NUM,
	stg.ACCOUNT,
	stg.CREATE_DT,
    to_date( '31.12.9999', 'DD.MM.YYYY' ), 
    'N'
from de3at.avzh_dwh_dim_cards_hist tgt
inner join de3at.avzh_stg_cards stg
on ( stg.CARD_NUM = tgt.CARD_NUM and tgt.effective_to_dt = to_date( '31.12.9999', 'DD.MM.YYYY' ) and deleted_flg = 'N' )-- લ󠫼�
where 1=0 
        or stg.ACCOUNT <> tgt.ACCOUNT or ( stg.ACCOUNT is null and tgt.ACCOUNT is not null ) or ( stg.ACCOUNT is not null and tgt.ACCOUNT is null )
"""     
def updat_dwh_dim_cards(curs):
    curs.execute(update_dwh_dim_cards)
    print('update_dwh_dim_cards . . . . . . . . . . OK')
    
        #  TABLE
merge_dwh_dim_cards = """merge into de3at.avzh_dwh_dim_cards_hist tgt
using de3at.avzh_stg_cards stg
on( stg.CARD_NUM = tgt.CARD_NUM and deleted_flg = 'N' )
when matched then 
    update set tgt.effective_to_dt = stg.update_dt - interval '1' minute
    where 1=1
	and tgt.effective_to_dt = to_date( '31.12.9999', 'DD.MM.YYYY' )
	and (1=0
        or stg.ACCOUNT <> tgt.ACCOUNT or ( stg.ACCOUNT is null and tgt.ACCOUNT is not null ) or ( stg.ACCOUNT is not null and tgt.ACCOUNT is null )
	)
when not matched then 
insert ( 
    CARD_NUM,
	ACCOUNT,
	effective_from_dt,
	effective_to_dt,
    deleted_flg  
	) 
    values ( 
    stg.CARD_NUM,
	stg.ACCOUNT,
	stg.CREATE_DT,
    to_date( '31.12.9999', 'DD.MM.YYYY' ), 
    'N'
)"""     
def merg_dwh_dim_cards(curs):
    curs.execute(merge_dwh_dim_cards)
    print('merge_dwh_dim_cards . . . . . . . . . . OK') 

        #  TABLE
delete_y_dwh_dim_cards = """INSERT INTO de3at.avzh_dwh_dim_cards_hist ( 
    CARD_NUM,
	ACCOUNT,
	effective_from_dt,
	effective_to_dt,
    deleted_flg  
	) 
SELECT
	tgt.CARD_NUM,
	tgt.ACCOUNT,
    (SELECT MAX(update_dt) FROM de3at.AVZH_STG_cards), 
    to_date( '31.12.9999', 'DD.MM.YYYY' ), 
    'Y'
FROM de3at.avzh_dwh_dim_cards_hist tgt
LEFT JOIN de3at.avzh_stg_cards_del stg
ON ( stg.CARD_NUM = tgt.CARD_NUM )
WHERE stg.CARD_NUM IS NULL AND deleted_flg = 'N' AND tgt.effective_to_dt = to_date( '31.12.9999', 'DD.MM.YYYY' )
 """     
def delet_y_dwh_dim_cards(curs):
    curs.execute(delete_y_dwh_dim_cards)
    print('delete_y_dwh_dim_cards . . . . . . . . . . OK')

        #  TABLE
delete_update_dwh_dim_cards = """UPDATE de3at.avzh_dwh_dim_cards_hist tgt
SET effective_to_dt = (SELECT MAX(update_dt) FROM de3at.AVZH_STG_cards) - interval '1' minute
WHERE tgt.CARD_NUM not in (select CARD_NUM from de3at.avzh_stg_cards_del)
    and tgt.effective_to_dt = to_date( '31.12.9999', 'DD.MM.YYYY' )
    and tgt.deleted_flg = 'N'
"""     
def delet_update_dwh_dim_cards(curs):
    curs.execute(delete_update_dwh_dim_cards)
    print('delete_update_dwh_dim_cards . . . . . . . . . . OK')

        #  TABLE
update_meta_cards = """merge into de3at.avzh_meta trg
using (
    select
		'de3at' as schema_name,
        'cards_source' as table_name,
        ( select max( coalesce(update_dt, create_dt) ) from de3at.avzh_stg_cards ) as max_update_dt
	from dual ) src
on ( trg.schema_name = src.schema_name and trg.table_name = src.table_name )
when matched then 
    update set trg.max_update_dt = src.max_update_dt
    where src.max_update_dt is not null
when not matched then 
    insert ( schema_name, table_name, max_update_dt )
    values ( 'de3at', 'cards_source',  src.max_update_dt)"""     
def updat_meta_cards(curs):
    curs.execute(update_meta_cards)
    print('update_meta_cards . . . . . . . . . . OK')
    
 # -------------------------------clients-------------------------------------------------   
        
        
        #  TABLE
truncate_stg_clients = """ truncate table de3at.avzh_stg_clients"""     
def truncat_stg_clients(curs):
    curs.execute(truncate_stg_clients)
    print('truncate_stg_clients . . . . . . . . . . OK')
    
        #  TABLE
truncate_stg_clients_del = """truncate table de3at.avzh_stg_clients_del """     
def truncat_stg_clients_del(curs):
    curs.execute(truncate_stg_clients_del)
    print('truncate_stg_clients_del . . . . . . . . . . OK')
        
        #  TABLE
insert_stg_clients = """ insert into de3at.avzh_stg_clients ( CLIENT_ID, LAST_NAME,FIRST_NAME, PATRONYMIC, DATE_OF_BIRTH, PASSPORT_NUM, PASSPORT_VALID_TO, PHONE,CREATE_DT, UPDATE_DT )
select  CLIENT_ID, LAST_NAME,FIRST_NAME, PATRONYMIC, DATE_OF_BIRTH, PASSPORT_NUM, PASSPORT_VALID_TO, PHONE, CREATE_DT, UPDATE_DT 
from bank.clients
where CREATE_DT > coalesce( 
					( 
					select max_update_dt
					from de3at.avzh_meta
					where schema_name = 'de3at' and table_name = 'clients_source'
					)
					,to_date( '1800.01.01', 'YYYY.MM.DD' )
			)"""     
def inser_stg_clients(curs):
    curs.execute(insert_stg_clients)
    print('insert_stg_clients . . . . . . . . . . OK')
    
        #  TABLE
insert_stg_clients_del = """insert into de3at.avzh_stg_clients_del ( CLIENT_ID )
select CLIENT_ID from bank.clients """     
def inser_stg_clients_del(curs):
    curs.execute(insert_stg_clients_del)
    print('insert_stg_clients_del . . . . . . . . . . OK')
    
        #  TABLE
update_dwh_dim_clients = """insert into de3at.avzh_dwh_dim_clients_hist (
	CLIENT_ID,
	LAST_NAME,
	FIRST_NAME,
	PATRONYMIC,
	DATE_OF_BIRTH,
	PASSPORT_NUM,
	PASSPORT_VALID_TO,
	PHONE,
	effective_from_dt,
	effective_to_dt,
    deleted_flg  
	) 
select
	stg.CLIENT_ID,
	stg.LAST_NAME,
	stg.FIRST_NAME,
	stg.PATRONYMIC,
	stg.DATE_OF_BIRTH,
	stg.PASSPORT_NUM,
	stg.PASSPORT_VALID_TO,
	stg.PHONE,
    stg.CREATE_DT,
    to_date( '31.12.9999', 'DD.MM.YYYY' ), 
    'N'
from de3at.avzh_dwh_dim_clients_hist tgt
inner join de3at.avzh_stg_clients stg
on ( stg.CLIENT_ID = tgt.CLIENT_ID and tgt.effective_to_dt = to_date( '31.12.9999', 'DD.MM.YYYY' ) and deleted_flg = 'N' )-- актуальные
where 1=0 -- есть изменения в поляж
    or stg.LAST_NAME <> tgt.LAST_NAME or ( stg.LAST_NAME is null and tgt.LAST_NAME is not null ) or ( stg.LAST_NAME is not null and tgt.LAST_NAME is null )
    or stg.FIRST_NAME <> tgt.FIRST_NAME or ( stg.FIRST_NAME is null and tgt.FIRST_NAME is not null ) or ( stg.FIRST_NAME is not null and tgt.FIRST_NAME is null )
    or stg.PATRONYMIC <> tgt.PATRONYMIC or ( stg.PATRONYMIC is null and tgt.PATRONYMIC is not null ) or ( stg.PATRONYMIC is not null and tgt.PATRONYMIC is null )
    or stg.DATE_OF_BIRTH <> tgt.DATE_OF_BIRTH or ( stg.DATE_OF_BIRTH is null and tgt.DATE_OF_BIRTH is not null ) or ( stg.DATE_OF_BIRTH is not null and tgt.DATE_OF_BIRTH is null )
    or stg.PASSPORT_NUM <> tgt.PASSPORT_NUM or ( stg.PASSPORT_NUM is null and tgt.PASSPORT_NUM is not null ) or ( stg.PASSPORT_NUM is not null and tgt.PASSPORT_NUM is null )
    or stg.PASSPORT_VALID_TO <> tgt.PASSPORT_VALID_TO or ( stg.PASSPORT_VALID_TO is null and tgt.PASSPORT_VALID_TO is not null ) or ( stg.PASSPORT_VALID_TO is not null and tgt.PASSPORT_VALID_TO is null )
    or stg.PHONE <> tgt.PHONE or ( stg.PHONE is null and tgt.PHONE is not null ) or ( stg.PHONE is not null and tgt.PHONE is null )
"""     
def updat_dwh_dim_clients(curs):
    curs.execute(update_dwh_dim_clients)
    print('update_dwh_dim_clients . . . . . . . . . . OK')

        #  TABLE
merge_dwh_dim_clients = """merge into de3at.avzh_dwh_dim_clients_hist tgt
using de3at.avzh_stg_clients stg
on( stg.CLIENT_ID = tgt.CLIENT_ID and deleted_flg = 'N' )
when matched then 
    update set tgt.effective_to_dt = stg.update_dt - interval '1' minute
    where 1=1
	and tgt.effective_to_dt = to_date( '31.12.9999', 'DD.MM.YYYY' )
	and (1=0
        or stg.LAST_NAME <> tgt.LAST_NAME or ( stg.LAST_NAME is null and tgt.LAST_NAME is not null ) or ( stg.LAST_NAME is not null and tgt.LAST_NAME is null )
        or stg.FIRST_NAME <> tgt.FIRST_NAME or ( stg.FIRST_NAME is null and tgt.FIRST_NAME is not null ) or ( stg.FIRST_NAME is not null and tgt.FIRST_NAME is null )
        or stg.PATRONYMIC <> tgt.PATRONYMIC or ( stg.PATRONYMIC is null and tgt.PATRONYMIC is not null ) or ( stg.PATRONYMIC is not null and tgt.PATRONYMIC is null )
        or stg.DATE_OF_BIRTH <> tgt.DATE_OF_BIRTH or ( stg.DATE_OF_BIRTH is null and tgt.DATE_OF_BIRTH is not null ) or ( stg.DATE_OF_BIRTH is not null and tgt.DATE_OF_BIRTH is null )
        or stg.PASSPORT_NUM <> tgt.PASSPORT_NUM or ( stg.PASSPORT_NUM is null and tgt.PASSPORT_NUM is not null ) or ( stg.PASSPORT_NUM is not null and tgt.PASSPORT_NUM is null )
        or stg.PASSPORT_VALID_TO <> tgt.PASSPORT_VALID_TO or ( stg.PASSPORT_VALID_TO is null and tgt.PASSPORT_VALID_TO is not null ) or ( stg.PASSPORT_VALID_TO is not null and tgt.PASSPORT_VALID_TO is null )
        or stg.PHONE <> tgt.PHONE or ( stg.PHONE is null and tgt.PHONE is not null ) or ( stg.PHONE is not null and tgt.PHONE is null )
	)
when not matched then -- добавляем новые записи
    insert ( 
    CLIENT_ID,
	LAST_NAME,
	FIRST_NAME,
	PATRONYMIC,
	DATE_OF_BIRTH,
	PASSPORT_NUM,
	PASSPORT_VALID_TO,
	PHONE,
	effective_from_dt,
	effective_to_dt,
    deleted_flg  
	) 
    values ( 
    stg.CLIENT_ID,
	stg.LAST_NAME,
	stg.FIRST_NAME,
	stg.PATRONYMIC,
	stg.DATE_OF_BIRTH,
	stg.PASSPORT_NUM,
	stg.PASSPORT_VALID_TO,
	stg.PHONE,
    stg.CREATE_DT,
    to_date( '31.12.9999', 'DD.MM.YYYY' ), 
    'N'
) """     
def merg_dwh_dim_clients(curs):
    curs.execute(merge_dwh_dim_clients)
    print('merge_dwh_dim_clients . . . . . . . . . . OK')
        
        #  TABLE
delete_y_dwh_dim_clients = """INSERT INTO de3at.avzh_dwh_dim_clients_hist ( 
    CLIENT_ID,
	LAST_NAME,
	FIRST_NAME,
	PATRONYMIC,
	DATE_OF_BIRTH,
	PASSPORT_NUM,
	PASSPORT_VALID_TO,
	PHONE,
	effective_from_dt,
	effective_to_dt,
    deleted_flg  
	) 
SELECT
	tgt.CLIENT_ID, 
    tgt.LAST_NAME, 
    tgt.FIRST_NAME, 
    tgt.PATRONYMIC,
	tgt.DATE_OF_BIRTH,		
	tgt.PASSPORT_NUM,
	tgt.PASSPORT_VALID_TO,
	tgt.PHONE,
    (SELECT MAX(update_dt) FROM de3at.AVZH_STG_clients), 
    to_date( '31.12.9999', 'DD.MM.YYYY' ), 
    'Y'
FROM de3at.avzh_dwh_dim_clients_hist tgt
LEFT JOIN de3at.avzh_stg_clients_del stg
ON ( stg.CLIENT_ID = tgt.CLIENT_ID )
WHERE stg.CLIENT_ID IS NULL AND deleted_flg = 'N' AND tgt.effective_to_dt = to_date( '31.12.9999', 'DD.MM.YYYY' )
 """     
def delet_y_dwh_dim_clients(curs):
    curs.execute(delete_y_dwh_dim_clients)
    print('delete_y_dwh_dim_clients . . . . . . . . . . OK')
    

        #  TABLE
delete_update_dwh_dim_clients = """UPDATE de3at.avzh_dwh_dim_clients_hist tgt
SET effective_to_dt = (SELECT MAX(update_dt) FROM de3at.AVZH_STG_clients) - interval '1' minute
WHERE tgt.CLIENT_ID not in (select CLIENT_ID from de3at.avzh_stg_clients_del)
    and tgt.effective_to_dt = to_date( '31.12.9999', 'DD.MM.YYYY' )
    and tgt.deleted_flg = 'N'
 """     
def delet_update_dwh_dim_clients(curs):
    curs.execute(delete_update_dwh_dim_clients)
    print('delete_update_dwh_dim_clients . . . . . . . . . . OK')
    
        #  TABLE
update_meta_clients = """merge into de3at.avzh_meta trg
using (
    select
		'de3at' as schema_name,
        'clients_source' as table_name,
        ( select max( coalesce(update_dt, create_dt) ) from de3at.avzh_stg_clients ) as max_update_dt
	from dual ) src
on ( trg.schema_name = src.schema_name and trg.table_name = src.table_name )
when matched then 
    update set trg.max_update_dt = src.max_update_dt
    where src.max_update_dt is not null
when not matched then 
    insert ( schema_name, table_name, max_update_dt )
    values ( 'de3at', 'clients_source',  src.max_update_dt)"""     
def updat_meta_clients(curs):
    curs.execute(update_meta_clients)
    print('update_meta_clients . . . . . . . . . . OK')
    
#=================================RESET==========================================

def reset_(curs) :
    # # удаляем файлы в архиве
    dir_list = os.listdir('/home/de3at/avzh/archive/') # звлекаем имена файлов из директории
    for file_name in dir_list:
        if os.path.isfile('/home/de3at/avzh/archive/{}'.format(file_name)): 
            os.remove('/home/de3at/avzh/archive/{}'.format(file_name)) 
            print("deleete '{}'    . . . . . . . . . . OK".format(file_name)) 
        else: 
            print("File '{}' from archive doesn't exists!".format(file_name))
    #удаляем файлы в data
    dir_list = os.listdir('/home/de3at/avzh/data/') # звлекаем имена файлов из директории
    for file_name in dir_list:
        if os.path.isfile('/home/de3at/avzh/data/{}'.format(file_name)): 
            os.remove('/home/de3at/avzh/data/{}'.format(file_name)) 
            print("delete '{}' from data    . . . . . . . . . . OK".format(file_name)) 
        else: 
            print("File '{}' from data doesn't exists!".format(file_name))        
    # копируем файлы в data
    dir_list_sorce = os.listdir('/home/de3at/avzh/sorce/')
    for file_name in dir_list_sorce:
        source_path = '/home/de3at/avzh/sorce/{}'.format(file_name)
        destination_path = '/home/de3at/avzh/data/{}'.format(file_name)
        new_location = shutil.copyfile(source_path, destination_path)
        print("copy '{}' to data . . . . . . . . . . OK".format(file_name))

    curs.execute("truncate table avzh_dwh_dim_accounts_hist")
    curs.execute("truncate table avzh_dwh_dim_cards_hist")
    curs.execute("truncate table avzh_dwh_dim_clients_hist")
    curs.execute("truncate table avzh_dwh_dim_terminals_hist")
    curs.execute("truncate table AVZH_DWH_FACT_PSSPRT_BLCKLST")
    curs.execute("truncate table avzh_dwh_fact_transactions")
    curs.execute("truncate table avzh_meta")
    curs.execute("truncate table avzh_rep_fraud")
    curs.execute("truncate table avzh_stg_accounts")
    curs.execute("truncate table avzh_stg_accounts_del")
    curs.execute("truncate table avzh_stg_cards")
    curs.execute("truncate table avzh_stg_cards_del")
    curs.execute("truncate table avzh_stg_clients")
    curs.execute("truncate table avzh_stg_clients_del")
    curs.execute("truncate table avzh_stg_pssprt_blcklst")
    curs.execute("truncate table avzh_stg_terminals")
    curs.execute("truncate table avzh_stg_terminals_del")
    curs.execute("truncate table avzh_stg_transactions")

    print('truncate tables . . . . . . . . . . OK')


    #=================================perort==========================================



        # #  --Поиск транзакций с заболокированным паспартом
passport_blak = """INSERT INTO de3at.avzh_rep_fraud(
    EVENT_DT,
    PASSPORT,
    FIO,
    PHONE,
    EVENT_TYPE,
    REPORT_DT
    )
WITH clients AS (
                SELECT
                    passport_num AS passport_id
                    ,(last_name ||' '|| first_name||' '||patronymic) as fio
                    ,client_id
                    ,phone
                FROM de3at.avzh_dwh_dim_clients_hist   
                ),
      pas_block AS (
                SELECT 
                    passport_num AS passport_id 
                from de3at.avzh_dwh_fact_pssprt_blcklst
                ),
      accnts AS  (
                SELECT
                    client AS client_id,
                    account AS account_id
                FROM de3at.avzh_dwh_dim_accounts_hist     
                ),
       cards AS (
                SELECT
                    card_num AS card_id,
                    account AS account_id
                FROM de3at.avzh_dwh_dim_cards_hist
                ),
     transact AS (
                 SELECT 
                    (card_num ||' ') AS card_id,
                    trans_date
                 FROM de3at.avzh_dwh_fact_transactions
                 WHERE trans_date >= 
                            (
                            select MAX_UPDATE_DT
                            from AVZH_META
                            where schema_name = 'de3at' and table_name = 'terminals_source'
                            )
                 )
SELECT 
    transact.trans_date AS event_dt,
    clients.passport_id AS PASSPORT,
    clients.fio         AS FIO,
    clients.phone       AS phone,
    'Поспорт в черном списке' AS event_type,
    sysdate             AS report_dt
FROM pas_block JOIN clients  ON clients.passport_id = pas_block.passport_id
JOIN accnts ON clients.client_id = accnts.client_id
JOIN cards ON cards.account_id = accnts.account_id
JOIN transact ON cards.card_id = transact.card_id """     
def rep_passport_blak (curs):
     curs.execute(passport_blak)
     print('rep_passport_blak . . . . . . . . . . . . . . . . . . . . OK')

        # #  -- Поиск транзакций с просроченным паспартом
passport_istek_srok = """INSERT INTO de3at.avzh_rep_fraud(
    EVENT_DT,
    PASSPORT,
    FIO,
    PHONE,
    EVENT_TYPE,
    REPORT_DT
    )                           
WITH clients AS (
                SELECT
                    passport_num AS passport_id
                    ,(last_name ||' '|| first_name||' '||patronymic) as fio
                    ,client_id
                    ,phone
                FROM de3at.avzh_dwh_dim_clients_hist
                WHERE PASSPORT_VALID_TO <
                            (
                            select MAX_UPDATE_DT
                            from AVZH_META
                            where schema_name = 'de3at' and table_name = 'terminals_source'
                            )
                ),
      accnts AS  (
                SELECT
                    client AS client_id,
                    account AS account_id
                FROM de3at.avzh_dwh_dim_accounts_hist     
                ),
       cards AS (
                SELECT
                    card_num AS card_id,
                    account AS account_id
                FROM de3at.avzh_dwh_dim_cards_hist
                ),
     transact AS (
                 SELECT 
                    (card_num ||' ') AS card_id,
                    trans_date
                 FROM de3at.avzh_dwh_fact_transactions
                 WHERE trans_date >= --to_date('2021.03.02','YYYY.MM.DD')
                            (
                            select MAX_UPDATE_DT
                            from AVZH_META
                            where schema_name = 'de3at' and table_name = 'terminals_source'
                            )
                 )
SELECT 
    transact.trans_date AS event_dt,
    clients.passport_id AS PASSPORT,
    clients.fio         AS FIO,
    clients.phone       AS phone,
    'Просрочен пасспорт' AS event_type,
    sysdate             AS report_dt
FROM clients JOIN accnts ON clients.client_id = accnts.client_id
JOIN cards ON cards.account_id = accnts.account_id
JOIN transact ON cards.card_id = transact.card_id """     
def rep_passport_istek_srok(curs):
    curs.execute(passport_istek_srok)
    print(' rep_passport_istek_srok. . . . . . . . . . . . . . . . . . . . OK')

        # #  --2.Совершение операции при недействующем договоре.
istek_dogovor = """INSERT INTO de3at.avzh_rep_fraud(
    EVENT_DT,
    PASSPORT,
    FIO,
    PHONE,
    EVENT_TYPE,
    REPORT_DT
    )
WITH clients AS (
                SELECT
                    passport_num AS passport_id
                    ,(last_name ||' '|| first_name||' '||patronymic) as fio
                    ,client_id
                    ,phone
                FROM de3at.avzh_dwh_dim_clients_hist
                ),
      accnts AS  (
                SELECT
                    client AS client_id,
                    account AS account_id
                FROM de3at.avzh_dwh_dim_accounts_hist
                WHERE VALID_TO <
                            (
                            select MAX_UPDATE_DT
                            from AVZH_META
                            where schema_name = 'de3at' and table_name = 'terminals_source'
                           )
                 ),
       cards AS (
                SELECT
                    card_num AS card_id,
                    account AS account_id
                FROM de3at.avzh_dwh_dim_cards_hist
                ),
     transact AS (
                 SELECT 
                    (card_num ||' ') AS card_id,
                    trans_date
                 FROM de3at.avzh_dwh_fact_transactions
                 WHERE trans_date >= --to_date('2021.03.02','YYYY.MM.DD')
                            (
                            select MAX_UPDATE_DT
                            from AVZH_META
                            where schema_name = 'de3at' and table_name = 'terminals_source'
                            )
                 )
SELECT 
    transact.trans_date AS event_dt,
    clients.passport_id AS PASSPORT,
    clients.fio         AS FIO,
    clients.phone       AS phone,
    'Истек договор' AS event_type,
    sysdate             AS report_dt
FROM clients JOIN accnts ON clients.client_id = accnts.client_id
JOIN cards ON cards.account_id = accnts.account_id
JOIN transact ON cards.card_id = transact.card_id """     
def rep_istek_dogovor(curs):
    curs.execute(istek_dogovor)
    print('rep_istek_dogovor . . . . . . . . . . . . . . . . . . . . OK')

        # #  --3. Совершение операций в разных городах в течение одного часа.-------------
different_city = """INSERT INTO de3at.avzh_rep_fraud(
    EVENT_DT,
    PASSPORT,
    FIO,
    PHONE,
    EVENT_TYPE,
    REPORT_DT
    )
WITH clients AS (
                SELECT
                    passport_num AS passport_id
                    ,(last_name ||' '|| first_name||' '||patronymic) as fio
                    ,client_id
                    ,phone
                FROM de3at.avzh_dwh_dim_clients_hist
                ),
      accnts AS  (
                SELECT
                    client AS client_id,
                    account AS account_id
                FROM de3at.avzh_dwh_dim_accounts_hist
                 ),
       cards AS (
                SELECT
                    card_num AS card_id,
                    account AS account_id
                FROM de3at.avzh_dwh_dim_cards_hist
                ),
	dif_city AS (
                 SELECT 
                    (card_num ||' ') AS card_id
                    ,trans_date
                    ,terminal_city
                    ,trans_date_before
                    ,terminal_city_before
                 FROM   (
						SELECT 
							trasact.trans_date,
							terms.terminal_city,
							trasact.card_num,
							LAG(trasact.trans_date)OVER(PARTITION BY trasact.card_num ORDER BY trasact.trans_date) as trans_date_before,
							LAG(terms.terminal_city)OVER(PARTITION BY card_num ORDER BY trans_date) as terminal_city_before
						FROM de3at.avzh_dwh_fact_transactions trasact
						INNER JOIN de3at.avzh_dwh_dim_terminals_hist terms                
							ON terms.terminal_id = trasact.terminal
						WHERE trasact.trans_date  > -- to_date('2021.03.01','YYYY.MM.DD')   
                            (
                                select MAX_UPDATE_DT - INTERVAL '1' HOUR
                                from AVZH_META
                                where schema_name = 'de3at' and table_name = 'terminals_source'
                            ) AND (terms.deleted_flg = 'N' AND terms.effective_to_dt = to_date( '31.12.9999', 'DD.MM.YYYY' ))	
					   )
                 WHERE terminal_city != terminal_city_before	      
                 )							 
SELECT 
    dif_city.trans_date AS event_dt,
    clients.passport_id AS PASSPORT,
    clients.fio         AS FIO,
    clients.phone       AS phone,
    'Совершение операций в разных городах в течение одного часа.' AS event_type,
    sysdate             AS report_dt
FROM clients JOIN accnts ON clients.client_id = accnts.client_id
JOIN cards ON cards.account_id = accnts.account_id
JOIN dif_city ON cards.card_id = dif_city.card_id """     
def rep_different_city(curs):
    curs.execute(different_city)
    print('rep_different_city . . . . . . . . . . . . . . . . . . . . OK')

        # #  --4. Подбор суммы в течении 20 минут -------------
podbor_sum = """INSERT INTO de3at.avzh_rep_fraud(
    EVENT_DT,
    PASSPORT,
    FIO,
    PHONE,
    EVENT_TYPE,
    REPORT_DT
    )
WITH clients AS (
                SELECT
                    passport_num AS passport_id
                    ,(last_name ||' '|| first_name||' '||patronymic) as fio
                    ,client_id
                    ,phone
                FROM de3at.avzh_dwh_dim_clients_hist
                ),
      accnts AS  (
                SELECT
                    client AS client_id,
                    account AS account_id
                FROM de3at.avzh_dwh_dim_accounts_hist
                 ),
       cards AS (
                SELECT
                    card_num AS card_id,
                    account AS account_id
                FROM de3at.avzh_dwh_dim_cards_hist
                ),
	 podbor AS (
                SELECT
                    card_id,
                    event_dt
                FROM (
                        SELECT
                                (card_num ||' ') AS card_id,
                                trans_date AS event_dt,
                                trans_date AS trans_date_4_time,
                                oper_result AS oper_result_4_time,
                                amt AS amt_4_time,
                                LAG(oper_result,1) OVER (PARTITION BY card_num ORDER BY trans_date)as operation_result_3_time,
                                LAG(amt,1) OVER (PARTITION BY card_num ORDER BY trans_date)as amt_3_time,
                                LAG(oper_result,2) OVER (PARTITION BY card_num ORDER BY trans_date)as operation_result_2_time,
                                LAG(amt,2) OVER (PARTITION BY card_num ORDER BY trans_date)as amt_2_time,
                                LAG(trans_date,3) OVER (PARTITION BY card_num ORDER BY trans_date)as trans_date_1_time,
                                LAG(oper_result,3) OVER (PARTITION BY card_num ORDER BY trans_date)as operation_result_1_time,
                                LAG(amt,3) OVER (PARTITION BY card_num ORDER BY trans_date)as amt_1_time
                        FROM de3at.avzh_dwh_fact_transactions
                        WHERE oper_type IN ('WITHDRAW','PAYMENT') AND trans_date >=    
                                (
                                select MAX_UPDATE_DT  - INTERVAL '20' MINUTE
                                from AVZH_META
                                where schema_name = 'de3at' and table_name = 'terminals_source'
                                )
                )
                WHERE
                    (trans_date_4_time - trans_date_1_time) <= 20/60/24 AND 
                    oper_result_4_time = 'SUCCESS' AND
                    (operation_result_3_time = 'REJECT' AND operation_result_2_time = 'REJECT' AND operation_result_1_time = 'REJECT') AND
                    (amt_4_time < amt_3_time AND amt_3_time < amt_2_time AND amt_2_time < amt_1_time)	      
                 )							 
SELECT 
    podbor.event_dt AS event_dt,
    clients.passport_id AS PASSPORT,
    clients.fio         AS FIO,
    clients.phone       AS phone,
    'Подбор суммы в течении 20 минут.' AS event_type,
    sysdate             AS report_dt
FROM clients JOIN accnts ON clients.client_id = accnts.client_id
JOIN cards ON cards.account_id = accnts.account_id
JOIN podbor ON cards.card_id = podbor.card_id """     
def rep_podbor_sum(curs):
    curs.execute(podbor_sum)
    print('rep_podbor_sum . . . . . . . . . . . . . . . . . . . . OK')
        # #  TABLE
# avzh = """ """     
# def (curs):
    # curs.execute(avzh )
    # print(' . . . . . . . . . . . . . . . . . . . . OK')
    

INIT_DB_COMMANDS = [
    dwh_fact_pssprt_blcklst,
    stg_pssprt_blcklst,
    dwh_dim_terminals,
    stg_terminals_del,
    stg_terminals,
    stg_transactions,
    dwh_fact_transactions,
    dwh_dim_accounts,
    stg_accounts_del,
    stg_accounts,
    stg_clients,
    stg_clients_del,
    dwh_dim_clients,
    dwh_dim_cards,
    stg_cards_del,
    stg_cards,
    meta
]


INCREMENT_COMMANDS = [
    read_and_isert_to_source,

    inser_dwh_fact_pssprt_blcklst ,

    inser_dwh_fact_transactions ,

    insert_stg_terminals_del,
    update_dwh_dim_terminals_hist,
    merge_dwh_dim_terminals,
    delete_y_dwh_dim_terminals,
    delete_update_dwh_dim_terminals,
    update_meta_terminals,
    truncate_stg_terminals,
    truncate_stg_terminals_del ,

    inser_stg_accounts ,
    inser_stg_accounts_del ,
    updat_dwh_dim_accounts ,
    merg_dwh_dim_accounts ,
    delet_y_dwh_dim_accounts ,
    delet_update_dwh_dim_accounts,
    updat_meta_accouns ,
    truncat_stg_accounts_del ,
    truncat_stg_accounts ,

    inser_stg_cards ,
    inser_stg_cards_del ,
    updat_dwh_dim_cards ,
    merg_dwh_dim_cards ,
    delet_y_dwh_dim_cards ,
    delet_update_dwh_dim_cards ,
    updat_meta_cards ,
    truncat_stg_cards ,
    truncat_stg_cards_del ,

    inser_stg_clients ,
    inser_stg_clients_del ,
    updat_dwh_dim_clients ,
    merg_dwh_dim_clients ,
    delet_y_dwh_dim_clients ,
    delet_update_dwh_dim_clients ,
    updat_meta_clients ,
    truncat_stg_clients ,
    truncat_stg_clients_del ,

    truncat_stg_pssprt_blcklst ,
    truncat_stg_transactions,
    #отчет
    rep_passport_blak,
    rep_passport_istek_srok,
    rep_istek_dogovor,
    rep_different_city,
    rep_podbor_sum
]



RESET_COMMANDS = [reset_]
   

def run_db_cmds(cmd_list):
    errors = []
    conn = get_conn()
    cursor = conn.cursor()
    conn.jconn.setAutoCommit(False)
    for cmd in cmd_list:
        try:
            if callable(cmd):
                cmd(cursor)
            else:
                cursor.execute(cmd)
        except Exception as e:
            errors.append(str(e))
        conn.commit()         
    return (len(errors) == 0, errors)


# выполнение  заданной команды
        
if __name__ == '__main__':
    cmd = sys.argv[1]

    if cmd == 'init':
        is_ok, errors = run_db_cmds(INIT_DB_COMMANDS)
    elif cmd == 'run_increment':
        if os.path.isfile("/home/de3at/avzh/data/terminals_03032021.xlsx"):
            is_ok, errors = run_db_cmds(INCREMENT_COMMANDS)
        else:
            is_ok, errors = ["Похоже что источник данных пуст воспользуйтесь командой 'reset'"]     
    elif cmd == 'reset':
        is_ok, errors = run_db_cmds(RESET_COMMANDS)
    else:
        raise NotImplementedError('Unknown command')

    if not is_ok:
        print(*errors, sep='\n')  
      
        
        


