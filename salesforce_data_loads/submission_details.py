#%%
import simple_salesforce
from common.common_module import DataFrame, Path, run_sql, mondrian_logger, get_environment
from pleasant import Pleasant
import logging
import os
from configparser import ConfigParser
import pandas as pd
import datetime 
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.runtime.http.request_options import RequestOptions
import requests
import rsa
import pypleasant


#%%
logger = logging.getLogger()
root_logger = mondrian_logger(
        logger = logger, 
        log_file = 'LetterRequestSync',
        log_folder = (Path(os.path.abspath('')).parent / 'Logs')
)
env = get_environment()

#%%
def connect_salesforce() -> simple_salesforce.api.Salesforce:
    root_logger.info(f'Connecting to Salesforce')

    os.environ['https_proxy'] = 'http://lon3.sme.zscaler.net:443'
    config = ConfigParser()
    config.read(r'\\shrappsprd.app.mondrian.mipl.com\APPS\Salesforce\Script\Credentials\salesforceCreds.ini')
    if env == 'UAT':
        sf = simple_salesforce.Salesforce(
                username=config['salesforce--uat.lightning.com']['username'],
                password=config['salesforce--uat.lightning.com']['password'],
                security_token=config['salesforce--uat.lightning.com']['security_token'],
                domain='test'
                
        )
    if env == 'DEV':
        sf = simple_salesforce.Salesforce(
                username=config['salesforce--uat.lightning.com']['username'],
                password=config['salesforce--uat.lightning.com']['password'],
                security_token=config['salesforce--uat.lightning.com']['security_token'],
                domain='test'
                
        )
    if env == 'PROD':
        sf = simple_salesforce.Salesforce(
                username=config['salesforce.lightning.com']['username'],
                password=config['salesforce.lightning.com']['password'],
                security_token=config['salesforce.lightning.com']['security_token']
                
        )

    root_logger.info(f'Successfully connected to Salesforce')
    return sf

#%%
def runSQLPreRenderedSubmissionUpdate(SubmissionID):
    Sql = "UPDATE dbo.tblSubmissionQueue SET Status = 3 WHERE SubmissionID = " + SubmissionID + "SELECT Status FROM dbo.tblSubmissionQueue WHERE SubmissionID = " + SubmissionID
    output = run_sql("DC1SQL01C", "WebReporter", command=Sql)
    return output

#%%
def validate_sf_load(json_response_list) -> tuple:
    success_record_count = 0
    failed_records = []
    for json_response in json_response_list:
        if json_response['success'] == True:
            success_record_count += 1
        else:
            error_message = f"{json_response['id']}:{json_response['errors'][0]['statusCode']}-{json_response['errors'][0]['message']}"
            failed_records.append(error_message)
    return success_record_count, failed_records
    

#%%
def main():
    sf_conn = connect_salesforce()

    root_logger.info(f'Getting lists of requests in letter generation that have been submitted to WebReporter')
    salesforce_request_raw = sf_conn.query_all(
        "Select Id, Name, Submission_ID__c, Account_Internal_Account_ID__c, Valuation_Date__c from request__c where Submission_ID__c <> NULL and Letter_Status__c = 'Letter Generation'"
    )
    sf_record_count = len(salesforce_request_raw['records'])
    if sf_record_count > 0:
        root_logger.info(f'{sf_record_count} records found in Salesforce')
        salesforce_requests = pd.DataFrame(salesforce_request_raw['records']).drop(columns='attributes')
    else:
        root_logger.info(f'No records found in Salesforce')
        salesforce_requests = pd.DataFrame({'Id':[],'Name':[], 'Submission_ID__c':[], 'Account_Internal_Account_ID__c':[], 'Valuation_Date__c':[]})
    salesforce_requests.set_index('Submission_ID__c')
    print(salesforce_requests)
    salesforce_requests['Submission_ID__c']=pd.to_numeric(salesforce_requests['Submission_ID__c'])
    salesforce_requests['valuation_date_format'] = pd.to_datetime(salesforce_requests['Valuation_Date__c']).dt.strftime('%B %Y')


    salesforce_account_raw = sf_conn.query_all(
        "Select Internal_Account_ID__c, reporting_name__c, Product__c from FinancialAccount__c"
    )
    sf_account_record_count = len(salesforce_account_raw['records'])
    if sf_account_record_count > 0:
        salesforce_accounts = pd.DataFrame(salesforce_account_raw['records']).drop(columns='attributes')
    else:
        salesforce_accounts = pd.DataFrame({'Internal_Account_ID__c':[], 'reporting_name__c':[], 'Product__c':[]})
    salesforce_accounts.set_index('Internal_Account_ID__c')
    salesforce_records = pd.merge(
        salesforce_requests,
        salesforce_accounts,
        left_on="Account_Internal_Account_ID__c",
        right_on="Internal_Account_ID__c",
        how= "inner"
    )

    root_logger.info(f'Getting all pre-rendered submissions from WebReporter')
    webreporter_submissions = run_sql("DC1SQL01C", "WebReporter", command="SELECT SubmissionID FROM dbo.tblSubmissionQueue where Status = 2")
    webreporter_submissions.set_index('SubmissionID')
    root_logger.info(f'{len(webreporter_submissions)} records found in WebReporter')


    root_logger.info(f'Comparing Salesforce and WebReporter')
    comparison = pd.merge(
        salesforce_records,
        webreporter_submissions,
        left_on='Submission_ID__c',
        right_on='SubmissionID',
        how='inner'
    )
    comparison['Letter_Status__c'] = ""

    if env == 'DEV':
        siteurl = 'https://mondriansp.sharepoint.com/sites/Salesforce_Requests'
        library_name = 'Requests'
    elif env == 'PRJ':
        siteurl = 'https://mondriansp.sharepoint.com/sites/Salesforce_Requests'
        library_name = 'Requests'
    elif env == 'UAT':
        siteurl = 'https://mondriansp.sharepoint.com/sites/Salesforce_Requests'
        library_name = 'Requests'
    elif env == 'PROD':
        siteurl = 'https://mondriansp.sharepoint.com/sites/Salesforce'
        library_name = 'Letters'
    
    """os.environ['https_proxy'] = 'http://lon3.sme.zscaler.net:443'
    config = ConfigParser()
    config.read(r'\\shrappsprd.app.mondrian.mipl.com\APPS\Sharepoint\Script\Credentials\sharepointCreds.ini')
    username=config['DEFAULT']['username']
    password=config['DEFAULT']['password']
    print(username)
    print(password)"""
    username = '----'
    password = '----'

    ctx_auth = AuthenticationContext(siteurl)
    ctx_auth.acquire_token_for_user(username,password)
    ctx = ClientContext(siteurl, ctx_auth)
    
    

    for index, row in comparison.iterrows():
        root_logger.info(f'Retrieving PDF')
        submission_id = str(row['SubmissionID'])
        request_number = str(row['Name'])
        product = str(row['Product__c'])
        reporting_name = str(row['reporting_name__c'])
        valuation_date = str(row['valuation_date_format'])
        path = r"\\dc1COR21c\Coric\WebReporter\SelfService\PreRender" + "\\" + submission_id + ".pdf"
        localpath_item = os.path.normpath(path)
        filename = f"Mondrian Monthly Letter - {product} - {valuation_date} - {reporting_name}.pdf"
        root_logger.info(f'Uploading PDF to Sharepoint')
        with open(localpath_item, 'rb') as file:
            content = file.read()
        target_file_url = f"{library_name}/{request_number}/{filename}"
        dir, name = os.path.split(target_file_url)
        file = ctx.web.get_folder_by_server_relative_url(dir).upload_file(name,content).execute_query()
        root_logger.info(f'Updating DataFrame for upload')
        comparison.loc[:,'Letter_Status__c'] = "Letter Sign Off - GPT"
        comparison.loc[:,'Submission_ID__c'] = ""

        
    """    
        root_logger.info(f'Updating WebReporter Submission status to trigger full render')
        runSQLPreRenderedSubmissionUpdate(SubmissionID = submission_id)
    """

    upload = comparison.drop(columns=['Name','Account_Internal_Account_ID__c','Valuation_Date__c','valuation_date_format','Internal_Account_ID__c','Product__c','reporting_name__c','SubmissionID'])
    root_logger.info(f'Update Salesforce status to internal sign off')
    bulk_update = []
    bulk_update.extend(upload.to_dict('records'))
    root_logger.info(f'{len(upload)} record(s) will be updated in Salesforce')
    if len(bulk_update) > 0:
        return_info = sf_conn.bulk.Request__c.update(bulk_update, batch_size=1000, use_serial=True)
        success_count, failures = validate_sf_load(return_info)
        if success_count > 0:
            root_logger.info(f'Successfully updated {success_count} record(s) into Salesforce')
        if len(failures) > 0:
            root_logger.error(f'{len(failures)} failures occurred during the load process.')
            for failure in failures:
                root_logger.error(f'{failure}')
            raise ImportError(f'{len(failures)} failures occured during the load process.')
    else:
        root_logger.info(f'No changes to be made in Salesforce')
        
#%% Run if primary script
if __name__ == '__main__':
    main()
