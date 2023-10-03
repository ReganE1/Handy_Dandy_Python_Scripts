#%%
import simple_salesforce
from common.common_module import DataFrame, Path, run_sql, mondrian_logger, get_environment
import logging
import os
from configparser import ConfigParser
import pandas as pd
import datetime 

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
def runSQLBookStoredProc(UserId, RequestId, ReportType, ClientName, EndDate, MeetingDate, PerformancePeriod):
    Sql = "EXEC [dbo].[MOND_UpdateLandscapeCapturePage] @UserID = '" + UserId + "', @Notes  = '" + RequestId +"',	@ReportType = '" + ReportType + "', @ClientName = '" + ClientName + "',	@DateOfMeeting = '" + MeetingDate + "', @EndDate = '"+ EndDate + "', @PerformancePeriod = '" + PerformancePeriod + "', @PerformanceType = '',	@RebasedCash = 0, @MIPL_Rep1  = '', @MIPL_Rep2  = '', @MIPL_Rep3  = '', @MIPL_Rep4  = '', @MIPL_Rep5  = '', @MIPL_Rep6  = '', @MIPL_Rep7  = '', @MIPL_Rep8  = '', @CoverName = ''"
    output = run_sql("DC1SQL01C", "WebReporter", command=Sql)
    return output

#%%
def runSQLSubmissionQueue(UserId,ClientName):
    Sql = "SELECT TOP 1 SubmissionID FROM dbo.tblSubmissionQueue WHERE UserID = '" + UserId + "' AND ClientName = '" + ClientName + "' ORDER BY SubmittedOn desc"
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

    # Get list of requests in Salesforce
    root_logger.info(f'Getting list of requests from Salesforce')
    salesforce_requests_raw = sf_conn.query_all(
        """
        SELECT Id,
            Name,
            RecordTypeId,
            Account__c,
            Account_Internal_Account_ID__c,
            Letter_Type__c,
            Due_Date_Text__c,
            OwnerId,
            Letter_Status__c,
            Valuation_Date__c
        FROM Request__c
        WHERE Letter_Status__c = 'Letter Generation'
        AND Submission_ID__c = null"""
    )
    sf_record_count = len(salesforce_requests_raw['records'])
    if sf_record_count > 0:
        root_logger.info(f'{sf_record_count} records found in Salesforce')
        salesforce_request = pd.DataFrame(salesforce_requests_raw['records']).drop(columns={'attributes'})
    else:
        root_logger.info(f'No records found in Salesforce')
        salesforce_request = pd.DataFrame({'Name':[], 'RecordTypeId':[],'Account__c':[],'Account_Internal_Account_ID__c':[],'Letter_Type__c':[],'Due_Date_Text__c':[],'OwnerId':[],'Letter_Status__c':[],'Valuation_Date__c':[]})
    salesforce_request.set_index('Name')
    
    # return salesforce_record

    root_logger.info(f'Getting Record Types for requests from Salesforce')
    salesforce_recordtypes_raw = sf_conn.query_all(
        "SELECT Id, Name FROM RecordType" 
    )
    
    salesforce_recordtypes = pd.DataFrame(salesforce_recordtypes_raw['records']).drop(columns={'attributes'})
    salesforce_recordtypes.rename(columns = {
        'Id':'RecordTypeId',
        'Name':'RecordTypeName'
    }, inplace = True)
    merge_recordtype = pd.merge(salesforce_request,salesforce_recordtypes)

    
    # return salesforce_request_owner

    root_logger.info(f'Getting Owner details for requests from Salesforce')
    salesforce_owner_raw = sf_conn.query_all(
        "SELECT Id, Alias FROM User" 
    )
    
    salesforce_owner = pd.DataFrame(salesforce_owner_raw['records']).drop(columns={'attributes'})
    salesforce_owner.rename(columns = {
        'Id':'OwnerId',
        'Alias':'OwnerAlias'
    }, inplace = True)
    merge_owner = pd.merge(merge_recordtype,salesforce_owner)

    # return salesforce_account

    root_logger.info(f'Getting Account details for requests from Salesforce')
    salesforce_account_raw = sf_conn.query_all(
        "SELECT Id, reporting_name__c FROM FinancialAccount__c" 
    )
    
    salesforce_account = pd.DataFrame(salesforce_account_raw['records']).drop(columns={'attributes'})
    salesforce_account.rename(columns = {
        'Id':'Account__c',
        'reporting_name__c':'AccountReportingName'
    }, inplace = True)
    merge_account = pd.merge(merge_owner,salesforce_account)
    print(merge_account)
    if len(merge_account) > 0:
        merge_account['ClientName'] = merge_account['Account_Internal_Account_ID__c'] + "-" + merge_account['AccountReportingName']
    else:
        merge_account['ClientName'] = merge_account['Account_Internal_Account_ID__c'] + merge_account['AccountReportingName']
    output = pd.DataFrame(columns=['Submission_ID__c','Id'])

    #Exec Stored Proc and get SubmissionId
    for index, row in merge_account.iterrows():
        root_logger.info(f'Submitting WebReporter Request')
        valuation_date = datetime.datetime.strptime(row['Valuation_Date__c'],'%Y-%m-%d').strftime('%d/%m/%Y')
        runSQLBookStoredProc(UserId = row['OwnerAlias'],RequestId= row['Name'], ReportType = row['RecordTypeName'], ClientName = row['ClientName'], EndDate = valuation_date, MeetingDate = datetime.date.today().strftime("%d/%m/%Y"), PerformancePeriod = row['Letter_Type__c'])
        root_logger.info(f'Getting SubmissionId')
        submission_details = runSQLSubmissionQueue(UserId=row['OwnerAlias'], ClientName = row['ClientName'])
        submission_details['Id'] = row['Id']
        submission_details.rename(columns = {
            'SubmissionID':'Submission_ID__c'
            }, inplace = True)
        
        output = pd.concat([output,submission_details],ignore_index=True)
    
    root_logger.info(f'Building JSON Load for Salesforce')
    bulk_update = []
    bulk_update.extend(output.to_dict('records'))
    root_logger.info(f'{len(output)} record(s) will be updated in Salesforce')
    if len(bulk_update) > 0:
        return_info = sf_conn.bulk.Request__c.update(bulk_update, batch_size=1000, use_serial=True)
        print(return_info)
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

     