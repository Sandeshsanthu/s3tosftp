import datetime
import io
import logging
import os

import boto3
import botocore.exceptions
import paramiko


logger = logging.getLogger()
logger.setLevel(os.getenv('LOGGING_LEVEL', 'DEBUG'))

SSH_HOST     = os.environ['SSH_HOST']
SSH_USERNAME = os.environ['SSH_USERNAME']
SSH_PASSWORD = os.getenv('SSH_PASSWORD')

# optional
SSH_PORT = int(os.getenv('SSH_PORT', 22))
SSH_DIR = os.getenv('SSH_DIR')
# filename mask used for the remote file
SSH_FILENAME = os.getenv('SSH_FILENAME', 'data_{current_date}')


def lambda_handler(event, context):
    # TODO implement
    sftp, transport = connect_to_SFTP(hostname=SSH_HOST,port=SSH_PORT,username=SSH_USERNAME,password=SSH_PASSWORD)
    s3 = boto3.client('s3')
    if SSH_DIR:
        sftp.chdir(SSH_DIR)
    with transport:
        for record in event['Records']:
            uploaded = record['s3']
            filename = uploaded['object']['key'].split('/')[-1]
            print(filename)
            try:
                transfer_file(s3_client=s3,bucket=uploaded['bucket']['name'],key=uploaded['object']['key'],sftp_client=sftp,sftp_dest=filename)
            except Exception:
                print 'Could not upload file to SFTP'
                raise
            
            else:
                print 'S3 file "{}" uploaded to SFTP successfully'.format(uploaded['object']['key'])


def connect_to_SFTP(hostname, port, username, password):
    transport = paramiko.Transport((hostname, port))
    transport.connect(username=username,password=password)
    sftp = paramiko.SFTPClient.from_transport(transport)
    return sftp, transport
    
    
def transfer_file(s3_client, bucket, key, sftp_client, sftp_dest):
    with sftp_client.file(sftp_dest, 'w') as sftp_file:
        s3_client.download_fileobj(Bucket=bucket,Key=key,Fileobj=sftp_file)

    
    
    
    
    
