#===============================================================================================================================================#
# Description: Delivers daily alerts providing comprehensive updates on the current status of our pipeline executing on Amazon EMR.
# Author: Gustavo Barreto
# Date: 12/12/2023
# Languages: Python3, boto3
#===============================================================================================================================================#
import pandas as pd
import boto3 
from datetime import datetime,timedelta
import gzip
from io import BytesIO
import smtplib # Lib para enviar e-mail
import io
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from io import BytesIO
from tabulate import tabulate

#===============================================================================================================================================#
# Change the variables here:

# Recipients
email_list = ['youremail@company.com']
email_title = f"PIPELINE JOBS EMR DAILY ALERT - {datetime.today().strftime(format='%d-%m-%Y')}" # Title for the email

# The bucket where Amazon EMR outputs its logs.
bucket_logs = 'bucket-emr-output' # The bucket where Amazon EMR outputs its logs.

# The bucket where yout .csv containing your organization email is located.
bucket_cred = 'your-s3-bucket'
key = 'credentials/credentials_email.csv' # Path to the .csv file
#===============================================================================================================================================#

today = datetime.today()
# today = datetime(2024,1,1) # for testing any given date 

before = datetime((today.date() + timedelta(days=1)).year,
                   (today.date() + timedelta(days=1)).month,
                     (today.date() + timedelta(days=1)).day)

after = datetime((today.date() - timedelta(days=1)).year,
                  (today.date() - timedelta(days=1)).month,
                    (today.date() - timedelta(days=1)).day)

# Clients
emr = boto3.client('emr')
s3 = boto3.client('s3')

info = emr.list_clusters(CreatedAfter=after,
                         CreatedBefore=before)
frame = []
running_clusters = []
for f in info['Clusters']:
    creation_date = f['Status']['Timeline']['CreationDateTime'].date()
    state =  f['Status']['State']
    if creation_date == today.date():
        frame.append({'CLUSTER_ID': f['Id'],
                    'CLUSTER_NAME': f['Name'].upper(),
                    'CLUSTER_STATE': state,
                    'CLUSTER_MESSAGE': f['Status']['StateChangeReason']['Message'].upper(),
                    'CREATION_DATE': creation_date})
    
        if state.lower() in ['running', 'waiting']:
            running_clusters.append(f['Id'])
    else:
        pass
        
frame = pd.DataFrame(frame)
order = ['CREATION_DATE','CLUSTER_ID', 'CLUSTER_NAME', 'CLUSTER_STATE', 'CLUSTER_MESSAGE']
frame = frame[order]

# Accessing steps and listing clusters using boto3.
ids = list(frame['CLUSTER_ID'])
other_info = []
for spath in ids:
    if spath in running_clusters:
        other_info.append({
                            'CLUSTER_ID': spath,
                            'TOTAL_JOBS': 0,
                            'SUCCEEDED': 0,
                            'FAILED': 0
                            })
    else:

        try:
            # Provide the prefix that points to EMR steps in your s3 bucket to list all the running jobs and outputs.
            local = f'logsemr2/{spath}/steps/' 
            obj = s3.list_objects_v2(Bucket=bucket_logs, Prefix=local)['Contents']
            for ob in obj:
                if ob['Key'].endswith('stderr.gz'):
                    file_local = ob['Key']
                    cluster_id = file_local.split('/')[1]

                    s3_log = boto3.resource("s3")
                    obj_log = s3_log.Object(bucket_logs, key=file_local).get()['Body'].read()
                    gzipfile = BytesIO(obj_log)
                    gzipfile = gzip.GzipFile(fileobj=gzipfile)
                    content = gzipfile.read()
                    content = content.decode('utf-8')
                    jobs_sucesso = content.count('final status: SUCCEEDED')
                    jobs_falha = content.count('final status: FAILED')  
                    total_jobs = content.count('final status: SUCCEEDED') + content.count('final status: FAILED') 

                    other_info.append({
                                        'CLUSTER_ID': cluster_id,
                                        'TOTAL_JOBS': total_jobs,
                                        'SUCCEEDED': jobs_sucesso,
                                        'FAILED': jobs_falha
                                        })
                else: 
                    pass    
        except:
            pass

frame2 = pd.DataFrame(other_info).groupby('CLUSTER_ID').sum().reset_index()
report = frame.merge(frame2, left_on='CLUSTER_ID', right_on='CLUSTER_ID', how='inner')
df = report
if report['FAILED'].sum() > 1:
    adicional = f"Something went wrong! {report['FAILED'].sum()} jobs with erros."
    observation = [x[0] for x in report[report['FAILED'] >0 ][['CLUSTER_NAME', 'FAILED']].values]

elif report['FAILED'].sum() > 0 and report['FAILED'].sum() <= 1:
    adicional = f"Something went wrong! {report['FAILED'].sum()} job with error."
    observation = [x[0] for x in report[report['FAILED'] >0 ][['CLUSTER_NAME', 'FAILED']].values]

else:
    adicional = f"Good job team! {report['FAILED'].sum()} jobs with erro."
    observation = 'None'
# #====================================================================================================================================#
# Sending the email

# Accessing the credential file
obj = s3.get_object(Bucket=bucket_cred, Key=key)
url = obj['Body']

credential = (pd.read_csv(io.BytesIO(url.read()), sep=';'))

email = credential['email'][1]
password = credential['password'][1]
# #====================================================================================================================================#
# Email message
hour = datetime.now().hour 
intro = ''
if hour < 12:
    intro = 'Good Morning!'
elif hour >= 12 and hour < 18:
    intro = 'Good Afternoon!'
else:
    intro = 'Good evening!'

# Creating a csv in temporary memory
in_memory_fp = BytesIO()
df.to_csv(in_memory_fp, index=False)
in_memory_fp.seek(0,0)
binary_csv = in_memory_fp.read()

# Emal message
html = f"""
<html>
<head>
<title>Pipeline Status EMR Data Report {today.strftime(format='%d-%m-%Y')}</title>
    <link rel="stylesheet" href="styles.css" />
</head>
<body>
<h1 class="title">Pipeline Status EMR Data Report {today.strftime(format='%d-%m-%Y')}</h1>
<p> {adicional} <p> 
Summary: <br>
Successful jobs: {report["SUCCEEDED"].sum()} <br>
Jobs that failed: {df["FAILED"].sum()} <br>
Total jobs: {report["TOTAL_JOBS"].sum()} <br>
Failed cluster: {observation}
{{table}}
</body>
</html>
"""

a = df.columns.values.tolist()
b = df.values.tolist()
b.insert(0, a)
data = b

html = html.format(table=tabulate(data, headers="firstrow", tablefmt="html"))

msg = MIMEMultipart("alternative", None,  [MIMEText(html,'html')])
part = MIMEBase('application', "octet-stream")

msgFrom = email_list
smtpObj = smtplib.SMTP('smtp.outlook.com', 587)
smtpObj.ehlo()
smtpObj.starttls()

msgTo = email
toPass = password
smtpObj.login(msgTo, toPass)
smtpObj.sendmail(msgTo,msgFrom,f'Subject: {email_title}\n{msg}')
smtpObj.quit()
