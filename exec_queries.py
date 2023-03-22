from sqlalchemy import text
import pandas as pd


def create_tables(facts,dim_personal,dim_emp,dim_suc,dim_LdT,dim_asist,dim_cond,engine):
    dim_personal.to_sql('dim_personal',engine,if_exists='replace',index=False)
    dim_emp.to_sql('dim_emp',engine,if_exists='replace',index=False)
    dim_suc.to_sql('dim_suc',engine,if_exists='replace',index=False)
    dim_LdT.to_sql('dim_LdT',engine,if_exists='replace',index=False)
    dim_asist.to_sql('dim_asist',engine,if_exists='replace',index=False)
    dim_cond.to_sql('dim_cond',engine,if_exists='replace',index=False)
    facts.to_sql('facts',engine,if_exists='append',index=False)
    
    return 'todo ok'

def get_resume_table(conn):
    query = """ SELECT t.value_lugar_trabajo,
                dim_asist.value_asistencia,
                date_format(fecha,'%m') mes,
                date_format(fecha,'%Y') año,
                COUNT(value_asistencia)

                FROM test_1.facts

                INNER JOIN test_1.dim_asist ON facts.id_asistencia = dim_asist.id_asistencia
                
                INNER JOIN (SELECT dim_personal.id_personal,
                            dim_ldt.value_lugar_trabajo,
                            dim_suc.value_sucursal 
                            
                            FROM test_1.dim_personal 
                            
                            INNER JOIN test_1.dim_ldt ON dim_ldt.id_lugar_trabajo = dim_personal.id_lugar_trabajo
                            INNER JOIN test_1.dim_suc ON dim_suc.id_sucursal = dim_personal.id_sucursal) AS t 
                            
                            ON facts.id_personal = t.id_personal
                
                WHERE value_asistencia = 'ausente' OR value_asistencia = 'vacaciones'

                GROUP BY value_sucursal,value_lugar_trabajo, value_asistencia,date_format(fecha,'%m'),date_format(fecha,'%Y')
                ORDER BY value_lugar_trabajo ASC,año ASC;"""
    df = pd.read_sql(text(query),conn)
    return df.pivot_table(df,index=["año","mes",], columns=['value_lugar_trabajo','value_asistencia'],fill_value=0)