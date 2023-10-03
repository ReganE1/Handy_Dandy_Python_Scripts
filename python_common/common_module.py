import logging
import re
import smtplib
from datetime import date
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from sys import stdout
from tabulate import tabulate

import pandas as pd
from pandas.core.frame import DataFrame
import pyodbc
from sqlalchemy.engine import create_engine, URL
import os

logger = logging.getLogger(__name__)

# email validation
regex = "^(\s?[^\s,]+@[^\s,]+\.[^\s,]+\s?,)*(\s?[^\s,]+@[^\s,]+\.[^\s,]+)$"

sql_driver = "{SQL Server Native Client 11.0}"
denodo_driver = 'DenodoODBC Unicode(x64)'
denodo_server = 'dc1den01c.mondrian.mipl.com'
denodo_port = '9996'

def run_sql(servername: str, database: str, command: str, database_type: str = "sql_server"):

    conn = db_connection(db=database, server=servername, database_type=database_type)
    df = pd.read_sql(command, conn)
    return df


def check_address(address):
    if not re.search(regex, address):
        raise Exception("Email address not in a valid format")


def send_mail(
    frm: str,
    to: str,
    subject: str,
    content: str,
    attachment_path: str = None,
    attachment_name: str = None,
    table: DataFrame = None,
) -> None:
    if not frm:
        raise Exception("From not specified")
    if not to:
        raise Exception("To not specified")
    check_address(frm)
    check_address(to)
    msg = MIMEMultipart()
    # msg.set_content(content)
    msg["Subject"] = subject
    msg["To"] = to
    msg["From"] = frm
    # content
    msg.attach(MIMEText(content))
    # attachment
    if attachment_path:
        att1 = MIMEApplication(open(attachment_path, "rb").read())
        att1.add_header("Content-Disposition", "attachment", filename=attachment_name)
        msg.attach(att1)

    if table is not None and not table.empty:
        cols = table.columns.values
        formatted_table = tabulate(table, headers=cols, tablefmt="html", showindex=False)
        
        html = f"""
                    <html>
                    <head>
                    <style> 
                    table, th, td {{ border: 1px solid black; border-collapse: collapse; }}
                    th, td {{ padding: 5px; }}
                    </style>
                    </head>
                    <body>
                    <p></p>
                    {formatted_table}
                    <p></p>
                    </body></html>
                """
        
        msg.attach(MIMEText(html, 'html'))

    s = smtplib.SMTP("mail.mondrian.com")
    s.send_message(msg)
    s.quit()


def mondrian_logger(
    logger, log_file: str, log_folder: str, level=logging.INFO
) -> logger:

    try:
        log_folder.mkdir(777)
    except FileExistsError:
        pass

    today = date.today().isoformat()
    log_file = f"{today}_{log_file}.txt"

    console_handler = logging.StreamHandler(stream=stdout)
    file_handler = logging.FileHandler(log_folder / log_file)
    logger.setLevel(level)
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(module)s | %(funcName)s | %(message)s"
    )
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger

def get_environment() -> str:
    
    env_dict = {
        '10.202.100.3':'PROD',
        '10.202.208.3':'UAT',
        '10.202.209.3':'PRJ',
        '10.202.207.3':'DEV'
    }

    for env_ip in env_dict.keys():
        env_check = os.popen(f'ping -n 1 {env_ip}').read()
        if 'Destination net unreachable' not in env_check and 'Timeout' not in env_check:
            env_name = env_dict[env_ip]

    return env_name

def create_db_engine(db: str, server: str = "DC1SQL01C"):

    conn_string = (
        f"DRIVER={sql_driver};SERVER={server};DATABASE={db};Trusted_Connection=yes"
    )
    conn_url = URL.create("mssql+pyodbc", query={"odbc_connect": conn_string})
    engine = create_engine(conn_url)

    return engine


def insert_db(df: DataFrame, db: str, table: str, server: str = "DC1SQL01C") -> None:
    engine = create_db_engine(db=db, server=server)

    with engine.connect() as conn:
        stmt = f"""SELECT * FROM INFORMATION_SCHEMA.TABLES 
           WHERE TABLE_NAME = N'{table}'"""
        output = conn.execute(stmt)
        res = output.fetchall()
        if len(res) == 0:
            raise Exception(f"Table {table} does not exist")
        df.to_sql(table, con=conn, index=False, if_exists="append")


def db_connection(db: str = 'Interface', server: str = "DC1SQL01C", database_type: str = "sql_server") -> pyodbc.Connection:

    conn_string_dict = {
        'denodo':f"DRIVER={denodo_driver};SERVER={denodo_server};PORT={denodo_port};DATABASE={db};krbsrvname=HTTP;Integrated Security=true;",
        'sql_server':f"DRIVER={sql_driver};SERVER={server};DATABASE={db};Trusted_Connection=yes"
    }

    if database_type.lower() not in conn_string_dict.keys():
        raise ValueError(f"{database_type} not in {','.join(conn_string_dict.keys())}")
    else:
        conn_string = conn_string_dict[database_type.lower()]

    con = pyodbc.connect(conn_string, autocommit=True)

    return con

#%%