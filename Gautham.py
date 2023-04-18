import boto3

def is_file_present_in_s3(bucket_name, file_key):
    s3 = boto3.resource('s3')
    try:
        s3.Object(bucket_name, file_key).load()
        return True
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            return False
        else:
            print(f'Error while checking if {file_key} exists in the {bucket_name} bucket: {e}')
            return None

#Check Lambda Functionality
if not is_file_present_in_s3('edp-inbound-qsales-dev','snapshots/'):
    print("File deleted from s3://edp-inbound-qsales-dev/snapshots/ Sucessfully!!")
if is_file_present_in_s3('edp-inbound-qsales-dev','archives/'):
    print("File moved to s3://edp-inbound-qsales-dev/archives/ Sucessfully!!")    
if is_file_present_in_s3('edp-datalake-raw-dev','qsales/'):
    print("File moved to s3://edp-datalake-raw-dev/qsales/ Sucessfully!!")
  
#Taking inputs
inp='qsales_opportunity_line_item_20230303_023012.csv'
inp=inp.split('_')
source=inp[0]
obj=inp[1]+'_'+inp[2]+'_'+inp[3]
event_date=inp[4]
  
#Check Gluejob 1 status
key='source='+source+'/object='+obj+'/event_date='+event_date+'/job=edp_preprocessing/'+'file'
if is_file_present_in_s3('edp-datalake-int-dev',key):
    print("Parquet file has been created Sucessfully!!")
#Check Gluejob 2 status
key='source='+source+'/object='+obj+'/event_date='+event_date+'/job=cdc/'+'file'
if is_file_present_in_s3('edp-datalake-int-dev',key):
    print("Parquet file has been created Sucessfully!!")
#Check Gluejob 3 status
key='source='+source+'/object='+obj+'/event_date='+event_date+'/job=field_mapping/'+'file'
if is_file_present_in_s3('edp-datalake-int-dev',key):
    print("Parquet file has been created Sucessfully!!")
