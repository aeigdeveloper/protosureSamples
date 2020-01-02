# coding: utf-8

# In[1]:


import json
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import pandas as pd
import pyodbc
import requests
from pandas.io.json import json_normalize


# In[2] protosure login:


def protosure_login():
    return requests.post('https://api.protosure.net/auth/ajax_login/',
                         json={"password": "password",
                               "email": "emailaddress"}
                         )


# In[3] email connect:


def email_connect():
    email_server = smtplib.SMTP('server', 587)

    email_server.starttls()

    email_server.login('email', 'password')
    return email_server


# In[4] as400 connect:


def as400_connect():
    connection = pyodbc.connect(
        'DRIVER={iSeries Access ODBC Driver};SYSTEM=systemname;SERVER=servername;DATABASE=dbname;UID=user;PWD=password')

    return connection.cursor()


# In[5] :


as400_connection = as400_connect()

as400_connection.execute(
    "SELECT CONCAT(SYMBOL,POLICY0NUM) AS policynumber,ENTER0DATE AS date,TOT0AG0PRM AS premium,type0act as iscanceled,trans0stat as status FROM As400Table WHERE SYMBOL='CMP' OR SYMBOL='BPP'")

results = as400_connection.fetchall()

as400_connection.close()

# In[6] set as400 results:


results = [(i[0], i[1], i[2], i[3], i[4]) for i in results]

point_data_df = pd.DataFrame(results)
point_data_df = point_data_df.rename(
    columns={0: 'policynumber', 1: 'date_entered', 2: 'premium', 3: 'endorsement', 4: 'status'})

# In[7] get protosure results:


login = protosure_login()

requestData = requests.get(
    'https://api.protosure.net/api/reports/protosurereporttoken/data/?page=1&ordering=-metaData'
    '.modifiedAt&search=&pageSize=100',
    cookies=login.cookies)

jsonData = json.loads(requestData.content)

json_protosure_data_df = json_normalize(jsonData['results'])

# In[8] merge diff protosure to as400:


merge_df = json_protosure_data_df.merge(point_data_df, how='left', left_on=['raterData.op_full_policy_number'],
                                        right_on='policynumber')

diff_df = merge_df[merge_df['policynumber'].isna()]
diff_df = diff_df[~diff_df['raterData.op_full_policy_number'].str.contains('HAB')]

# In[9] prettify results:


diff_df = diff_df.rename(
    columns={'raterData.op_full_policy_number': 'Policy Number', 'raterData.op_premium': 'Premium'})

final_pd = diff_df[['Policy Number']]

# In[10] email results:


emailHtml = final_pd.to_html()

email_from = 'emailfrom@gmail.com'
email_to = 'emailto@aeiginsurance.com'

msg = MIMEMultipart('alternative')
msg['Subject'] = "TEST table"
msg['From'] = email_from
msg['To'] = email_to

msg.attach(MIMEText(emailHtml, 'html'))

# In[11]:


email_connect().sendmail(email_from, email_to, msg.as_string())
