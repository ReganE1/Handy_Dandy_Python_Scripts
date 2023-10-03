from common.common_module import create_db_engine
from common.common_module import send_mail, run_sql,db_connection, insert_db
import pytest
from pandas import read_sql

def test_email_no_sender():
        with pytest.raises(Exception) as excinfo:
            send_mail('','','','','','')
        assert 'From not specified' in str(excinfo.value)

def test_email_no_recipient():
        with pytest.raises(Exception) as excinfo:
            send_mail('from@mondrian.com','','','','','')
        assert 'To not specified' in str(excinfo.value)

def test_email_invalid_format():
        with pytest.raises(Exception) as excinfo:
            send_mail('from@mondrian.com','tt@mondrian coma@mondrian.com','','','','')
        assert 'Email address not in a valid format' in str(excinfo.value)

def test_email_invalid_format2():
        with pytest.raises(Exception) as excinfo:
            send_mail('from@mondrian.com','testmondrian.com','','','','')
        assert 'Email address not in a valid format' in str(excinfo.value)
