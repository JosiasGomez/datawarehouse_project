import pandas as pd

def create_dim(id, values,name):
    return pd.DataFrame([id,values]).T.set_axis([f'id_{name}',f'value_{name}'],axis=1)

def dim_tables(df):
    emp = df['empresa'].unique()
    AyN = df ['apellido_nombre'].unique()
    suc = df['sucursal'].unique()
    LdT = df['lugar_trabajo'].unique()

    cond = df['condicion'].unique()
    asist = df['asistencia'].unique()

    emp_index = [i for i in range(1,len(emp)+1)]
    AyN_index = [i for i in range(emp_index[-1]+1,emp_index[-1] + len(AyN)+1)]
    suc_index = [i for i in range(AyN_index[-1]+1,AyN_index[-1] + len(suc)+1)]
    LdT_index = [i for i in range(suc_index[-1]+1,suc_index[-1] + len(LdT)+1)]

    cond_index = [i for i in range(LdT_index[-1]+1,LdT_index[-1] + len(cond)+1)]
    asist_index = [i for i in range(cond_index[-1]+1,cond_index[-1] + len(asist)+1)]

    dim_personal = create_dim(AyN_index,AyN,'personal')
    dim_emp = create_dim(emp_index,emp,'empresa')
    dim_suc = create_dim(suc_index,suc,'sucursal')
    dim_LdT = create_dim(LdT_index,LdT,'lugar_trabajo')
    dim_asist = create_dim(asist_index,asist,'asistencia')
    dim_cond = create_dim(cond_index,cond,'condicion')
    
    return dim_personal, dim_emp, dim_suc, dim_LdT, dim_asist, dim_cond

def dim_personal(df,dim_per,dim_emp,dim_suc,dim_LdT):

    personal = df[['apellido_nombre','empresa','legajo','sucursal','lugar_trabajo']]
    personal.drop_duplicates(subset=['apellido_nombre'],inplace=True)
    personal = pd.merge(left=personal,right=dim_per,left_on='apellido_nombre',right_on='value_personal')
    personal = pd.merge(left=personal,right=dim_emp,left_on='empresa',right_on='value_empresa')
    personal = pd.merge(left=personal,right=dim_suc,left_on='sucursal',right_on='value_sucursal')
    personal = pd.merge(left=personal,right=dim_LdT,left_on='lugar_trabajo',right_on='value_lugar_trabajo')
    return personal[['id_personal','value_personal','id_empresa','id_sucursal','id_lugar_trabajo']]

def tables(df):
 
    dim_pers, dim_emp, dim_suc, dim_LdT, dim_asist, dim_cond = dim_tables(df)

    facts = df[['apellido_nombre','mes','semana','fecha','condicion','asistencia']]

    facts = pd.merge(left=facts,right=dim_pers,how='left',left_on='apellido_nombre',right_on='value_personal',copy=False)
    facts = pd.merge(left=facts,right=dim_cond,how='left',left_on='condicion',right_on='value_condicion',copy=False)
    facts = pd.merge(left=facts,right=dim_asist,how='left',left_on='asistencia',right_on='value_asistencia',copy=False)
    
    dim_per = dim_personal(df,dim_pers,dim_emp,dim_suc,dim_LdT)


    return facts[['id_personal','mes', 'fecha', 'id_condicion', 'id_asistencia']], dim_per,dim_emp,dim_suc,dim_LdT,dim_asist,dim_cond




    
