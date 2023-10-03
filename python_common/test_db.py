from common.common_module import create_db_engine, send_mail, run_sql, db_connection, insert_db
import pytest
from pandas import read_sql

def test_run_sql():
     run_sql(servername ='DC1SQL01C', database = 'Interface', command = 'select top 5 * from whs_esg_ratings')

def test_db_connection_sql_server():   
    conn = db_connection(db = 'Interface', server = 'DC1SQL01C', database_type = 'sql_server')
    conn.execute('select top 5 * from whs_esg_ratings').fetchall()

def test_db_connection_denodo():   
    conn = db_connection(db = 'mondrian', database_type = 'denodo')
    conn.execute('select * from mondrian.bv_ad_computer').fetchall()

def test_db_connection_invalid_type():
    with pytest.raises(ValueError):
        db_connection(db = 'Interface', server = 'DC1SQL01C', database_type = 'INVALID')

def test_insert_db():
    conn = db_connection(db = 'Interface', server = 'DC1SQL01C')
    df = read_sql('select top 5 * from whs_esg_ratings', con = conn)
    insert_db(df, db = 'Interface',table= 'whs_esg_ratings')
