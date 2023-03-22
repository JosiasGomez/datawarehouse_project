import pandas as pd
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
import os.path
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.errors import HttpError
from sqlalchemy import create_engine,text
from exec_queries import get_resume_table
from ausentismo import write_shape
from datetime import datetime
import re
#from ausentismo import upload_base_values


#shape = upload_base_values()

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

def read_shape():
    with open('shape.txt','r') as reader:
        shape = int(reader.read())
    return shape

def get_new_values(page_name, spreadsheet_code):

    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                r'direccion\del\token\.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    try:
        service = build('sheets','v4', credentials=creds)
        
        values = service.spreadsheets().values().get(spreadsheetId=spreadsheet_code,
                                    range=page_name).execute()
        
        print('todo trank pa')

    except HttpError as err:
        print(err)

    return values['values'], creds 

# the main difference between this function and the another one in ausentismo_base.py is that this one
# takes the information and slides only the new values only

def process_new_values():

    SAMPLE_SPREADSHEET_ID = 'spreadsheet_id'
    SAMPLE_RANGE_NAME = 'range_name'
    
    ex,cred = get_new_values(SAMPLE_RANGE_NAME,SAMPLE_SPREADSHEET_ID)

    dates = pd.date_range('2022-03-01',datetime.now()).to_pydatetime().tolist()
    columns = ['empresa','apellido_nombre','legajo','sucursal','lugar_trabajo','mes']+dates

    df = pd.DataFrame(ex).iloc[2:226,:len(columns)].reset_index(drop=True).set_axis([columns],axis=1)
    
    shape = read_shape()
    write_shape(df.shape)

    df_aux = df.iloc[:,[0,1,2,3,4,5]]
    df = df.iloc[:,shape[1]-1:]
    df = pd.concat([df_aux,df], axis=1)

    melted = pd.melt(df,id_vars=df.columns[:6].tolist(),
            value_vars=df.columns[6:].tolist()).set_axis(['empresa',
                                                        'apellido_nombre'
                                                        ,'legajo'
                                                        ,'sucursal'
                                                        ,'lugar_trabajo'
                                                        ,'mes'
                                                        ,'fecha'
                                                        ,'condicion']
                                                        ,axis=1)

    melted['asistencia'] = melted['condicion']
    melted['asistencia'] = [re.sub('Carpeta Médica|L.D. Carpeta Médica|Suspensiones LD|carp.med.ART|Licen.especiales|Ausentes-injust|Suspensión Pnal. Suc.|Ausente-Susp.|INHABILITADO|Ausente-just|Permiso sindical|Carpeta Médicaa|L.D. Ausencia|ausentea','ausente',str(i)) for i in melted['asistencia']]
    melted['asistencia'] = [re.sub('Licencia Ordinaria|L.D. Licencia Ordinaria','vacaciones',str(i)) for i in melted['asistencia']]
    melted['mes'] = melted['fecha'].dt.strftime('%Y-%m')

    return melted, cred

#these new values are uploaded to the database, keeping it updated.

def upload_new_values():

    df, creds = process_new_values()
    engine = create_engine('mysql://username:password@host:port/database')


    return df.to_sql('test_1_',engine,if_exists='append',index=False), creds

#finally, this function uploads a resume table to another spredsheet.
#Notice the get_resume_table function. It excecutes a query that summarises
#the needed data.

def post_values():
    
    SAMPLE_SPREADSHEET_ID_input = 'spreadsheet_id'
    SAMPLE_RANGE_NAME = 'range_name'

    df, creds = upload_new_values()

    engine = create_engine('mysql://username:password@host:port/database')
    conn = engine.connect()
    df = get_resume_table(conn)
    
    df = df.T.reset_index().T.values.tolist()
    #del df[0]
    
    service = build('sheets','v4', credentials=creds)
    request = service.spreadsheets().values().update(spreadsheetId=SAMPLE_SPREADSHEET_ID_input, 
                                                     valueInputOption= 'RAW',
                                                     range=SAMPLE_RANGE_NAME,
                                                     body=dict(majorDimension='ROWS',
                                                               values= df)).execute()
    return 'todo ok'