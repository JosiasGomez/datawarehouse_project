import pandas as pd
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
import os.path
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.errors import HttpError
from sqlalchemy import create_engine
from datetime import datetime
from dim_tables_base import tables
from exec_queries import exec_queries
import re


SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

SAMPLE_SPREADSHEET_ID = 'spread_sheet_id'
SAMPLE_RANGE_NAME = 'range_name'

#read the spreadsheet and get the needed values

def get_base_values(page_name, spreadsheet_code):

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



#transform the values, give it format, melt the dataframe in order to get a long-format dataframe
#then creates the facts and dim tables

def process_base_values():

    SAMPLE_SPREADSHEET_ID = 'spread_sheet_id'
    SAMPLE_RANGE_NAME = 'range_name'
    
    ex,cred = get_base_values(SAMPLE_RANGE_NAME,SAMPLE_SPREADSHEET_ID)


    dates = pd.date_range('2022-03-01',datetime.now()).to_pydatetime().tolist()
    columns = ['empresa','apellido_nombre','legajo','sucursal','lugar_trabajo','mes']+dates
    filtro = len(columns)

    df = pd.DataFrame(ex).iloc[2:226,:filtro].reset_index(drop=True).set_axis([columns],axis=1)

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
    melted['semana'] = melted['fecha'].dt.strftime('%W')


    facts,dim_personal, dim_emp, dim_suc, dim_LdT, dim_asist, dim_cond = tables(melted)


    return facts, dim_personal,dim_emp,dim_suc,dim_LdT,dim_asist,dim_cond, cred, df.shape

#upload to MySQL server the previous tables 

def upload_base_values():

    facts, dim_personal,dim_emp,dim_suc,dim_LdT,dim_asist,dim_cond, creds, shape = process_base_values()
    write_shape(shape)

    engine = create_engine('mysql://username:password@host:port/database')

    return exec_queries(facts,dim_personal,dim_emp,dim_suc,dim_LdT,dim_asist,dim_cond,engine)


def write_shape(shape):
    with open('shape.txt','w') as writer:
        writer.write(str(shape[1]))