# -*- coding: utf-8 -*-
"""
Created on Fri Oct 28 16:20:14 2022

@author: jcgarciam
"""
# Este codigo busca automatizar el procedimiento que se realiza en excel para asignar las reclamaciones 
# que llegan por parte de las ARL para que sean pagadas y deben ser investigadas para determinar si son 
# fraude o no

import pandas as pd
import numpy as np
import holidays_co
from datetime import datetime

Tiempo_Total = datetime.now()


path_int = r'D:\DATOS\Users\jcgarciam\OneDrive - AXA Colpatria Seguros\Documentos\Informes\Proyecto Fraude\Input'
path_sal = r'D:\DATOS\Users\jcgarciam\OneDrive - AXA Colpatria Seguros\Documentos\Informes\Proyecto Fraude\Output'

# Se solicita la fecha ya que los archivos vienen con esta en el nombre


hoy = datetime.now().date()
fecha = hoy.strftime('%Y') + hoy.strftime('%m') + hoy.strftime('%d')  #input('Ingrese la fecha de con que vienen los archivos (aaaammdd), ejemplo 20221214: ')

fecha_asignada = hoy.strftime('%d') + '/' + hoy.strftime('%m') + '/' + hoy.strftime('%Y')

#%%
### EXTRACCION DE FUENTES

# En este archivo llegan las nuevas reclamaciones que se van asignar a los proveedores para determinar si hay Fraude
print('\n Cargando el informe AXA Estado Aseguradora_', fecha)
axa_estado_Aseguradora = pd.read_excel(path_int + '/AXA Estado Aseguradora_' + fecha + '.xlsx', header = 0)
print('Informe AXA Estado Aseguradora_', fecha, ' cargado \n')

# Este archivo tiene el historico de las aignaciones el cual se convierte en la salida de este programa y el insumo
print('Cargando la raf historico')
macro = pd.read_excel(path_int + '/Raf.xlsx', header = 0)
print('Raf historica cargada \n')

# Los campos que se llaman de comparativo sirven para complementar los de la raf
print('Cargando base, ', fecha, ' Comparativo Reserva pendiente y En proceso')
comparativo = pd.read_csv(path_int + '/' + fecha +' Comparativo Reserva pendiente y En proceso.csv', sep = ';', header = 0, encoding='latin-1', usecols = ['Reclamacion','Fecha ocurrencia','Cedula Accidentado','Tipo glosa/objeción','Fec Liberacion Reserva','Placa','Siniestro SISE','Nombres Accidentado'])
print('Base ',fecha, ' Comparativo Reserva pendiente y En proceso cargada \n')

# El archivo esquema permite identificar donde las reclamaciones y las ciudades son iguales para traer los comentarios de gestion
print('Cargando base esquema')
esquema = pd.read_excel(path_int + '/Esquema.xlsx', header = 0)
print('Base esquema cargada \n')

# Esta base contiene las investigaciones anteriores al anio actual, es decir que cuando llegue el anio siguiente se debe actualizar con las anio pasado
print('Cargando investigados años anteriores')
investigados_anteriores = pd.read_excel(path_int + '\Investigados historico.xlsx', header = 0, usecols = ['LLAVE POLIZA+SINIESTRO','Fecha Asignación Axa Colpatria (Investigador)', 'Proveedor'])
print('Investigados años anteriores cargados \n')

# Esta tabla contiene los parametros que se pueden moficar para la asignacion de reclamaciones que no tiene asignacion anterior por siniestro
print('Cargando condiciones para la ruleta asignación')
ruleta = pd.read_excel(path_int + '\Fuentes.xlsx', sheet_name = 'Ruleta', header = 0)
print('Condiciones para la ruleta de asignación cargadas \n')

# Este base contiene los nits que se pueden enviar reclamaciones con fraude y se actualiza mensualmente
print('Cargando las alertas mensuales')
Alertas_Mensuales = pd.read_excel(path_int + '\Fuentes.xlsx', sheet_name = 'Alertas Mensuales', header = 0)
print('Alertas mensuales cargadas \n')

# Trae el dato de la poliza que es de Tulua y puede ser de fraude
print('Cargando validador de casos TULUA')
Validador_Casos_TULUA = pd.read_excel(path_int + '\Fuentes.xlsx', sheet_name = 'Validador Casos TULUA', header = 0)
print('Validador de casos TULUA cargado \n')

# Contiene las ciudades a las que se les debe asignar un proveedor especifivo
print('Cargado Rango de ciudades')
Rango_ciudades = pd.read_excel(path_int + '\Fuentes.xlsx', sheet_name = 'Rango ciudades', header = 0)
print('Rango de ciudades cargado \n')

# Con esta base se complementan otros campos importantes para la raf final
print('Cargando base de Descargas')
base_de_descargas = pd.read_excel(path_int + '/2022.xlsx', header = 0, sheet_name = 'BASE DESCARGAS')
print('Base de Descargas cargada')

columnas = ['Reclamación','Fecha de Entrega Proveedor Inves','Provedor','Conclusión Estatus Investigación',	
            'Comentario Claim','Causal (No Cubiertos)','Fecha gestionado CLAIM','Fecha de Carga Informe',	
            'Dias tramite','Persona Fraude','Valor viaticos','Tipo Informe Escritorio/Campo']
base_de_descargas.columns = base_de_descargas.columns.str.strip()
base_de_descargas = base_de_descargas[columnas]


#%%
# esta funcion se crea con el fin de colocar un formato string a los numeros sin que salga con .0 al final
def CambioFormato(df, a = 'a'):
    df[a] = df[a].astype(str)
    df[a] = np.where(df[a].str[-2::] == '.0', df[a].str[0:-2], df[a])
    df.loc[(df[a].str.contains('nan') == True),a] = np.nan

    return df[a]
    
#%%
### ALISTAR LA RAF PRINCIPAL

macro_hist = macro.copy()

macro_hist['Reclamacion'] = CambioFormato(macro_hist, a = 'Reclamacion')

# Colocando formato fecha
macro_hist['Fecha Cambio de Estado'] = pd.to_datetime(macro_hist['Fecha Cambio de Estado'], format = '%d/%m/%Y')
macro_hist['Fecha Asignación Axa Colpatria (Investigador)'] = pd.to_datetime(macro_hist['Fecha Asignación Axa Colpatria (Investigador)'], format = '%d/%m/%Y')
macro_hist['Fecha Recepción'] = pd.to_datetime(macro_hist['Fecha Recepción'], format = '%d/%m/%Y')
macro_hist['Fecha  Asignación RAF? (STICKER)'] = pd.to_datetime(macro_hist['Fecha  Asignación RAF? (STICKER)'], format = '%d/%m/%Y')

# eL 'Fecha de Entrega Proveedor Inves' contiene información diferente a las fechas, por eso toca separarlo para
# dar formato fechas a los datos que si sin fechas
macro_hist_a = macro_hist[(macro_hist['Fecha de Entrega Proveedor Inves'].str.upper().str.isupper() == True) | (macro_hist['Fecha de Entrega Proveedor Inves'].isnull() == True)]
macro_hist_b = macro_hist[macro_hist['Reclamacion'].isin(macro_hist_a['Reclamacion']) == False]


macro_hist_b['Fecha de Entrega Proveedor Inves'] = pd.to_datetime(macro_hist_b['Fecha de Entrega Proveedor Inves'], format = '%d/%m/%Y')

macro_hist = pd.concat([macro_hist_a, macro_hist_b]).sort_index()

macro_hist = macro_hist.sort_values('Fecha Cambio de Estado', ascending = False)

macro_hist = macro_hist.drop_duplicates('Reclamacion', keep = 'last')

macro_hist['LLAVE POLIZA+SINIESTRO'] = CambioFormato(macro_hist, a = 'LLAVE POLIZA+SINIESTRO')


macro_hist['Fecha de asignacion (No es formula )'] = pd.to_datetime(macro_hist['Fecha de asignacion (No es formula )'], format = '%d/%m/%Y')
macro_hist['Fecha de asignacion (No es formula )'] = macro_hist['Fecha de asignacion (No es formula )'].dt.strftime('%d/%m/%Y') 

# Se complementa la informacion de la raf con la que viene del comparativo
comparativo['Reclamacion'] = CambioFormato(comparativo, a = 'Reclamacion')
comparativo = comparativo.drop_duplicates('Reclamacion', keep = 'last')
comparativo['Cedula Accidentado'] = CambioFormato(comparativo, a = 'Cedula Accidentado')
comparativo['Fecha ocurrencia'] = pd.to_datetime(comparativo['Fecha ocurrencia'], format = '%d/%m/%Y')


macro_hist = macro_hist.merge(comparativo, how = 'left', on = 'Reclamacion')

macro_hist.loc[(macro_hist['Tipo glosa Comparativo'].isnull() == True), 'Tipo glosa Comparativo'] = macro_hist['Tipo glosa/objeción']
macro_hist.loc[(macro_hist['Fecha liberacion reserva (Mes cuantificacion ahorro)'].isnull() == True), 'Fecha liberacion reserva (Mes cuantificacion ahorro)'] = macro_hist['Fec Liberacion Reserva']
macro_hist.loc[(macro_hist['Id lesionado'].isnull() == True), 'Id lesionado'] = macro_hist['Cedula Accidentado']
macro_hist.loc[(macro_hist['Fecha de Evento'].isnull() == True), 'Fecha de Evento'] = macro_hist['Fecha ocurrencia']
macro_hist['POLIZA'] = CambioFormato(macro_hist, a = 'POLIZA')
macro_hist['Id lesionado'] = CambioFormato(macro_hist, a = 'Id lesionado')
macro_hist['Fecha de Evento'] = pd.to_datetime(macro_hist['Fecha de Evento'], format = '%d/%m/%Y')
macro_hist['Fecha Llave'] = macro_hist['Fecha de Evento'].dt.strftime('%d/%m/%Y')
macro_hist['Llave Poliza+Fecha accidente+Cedula'] = macro_hist['POLIZA'].astype(str) + macro_hist['Fecha Llave'].astype(str) + macro_hist['Id lesionado'].astype(str)


#%%

# Del archivo axa solo nos interesa los que tengan los filtros en el campo 'Estado Actual': 'Pendiente aseguradora - investigación' y 'Pendiente Verificación'
# y en el campo 'Amparo': 'Gastos médicos' y 'Gastos de transporte y movilización'
axa_estado_Aseguradora2 = axa_estado_Aseguradora[(axa_estado_Aseguradora['Estado Actual'] == 'Pendiente aseguradora - investigación') | (axa_estado_Aseguradora['Estado Actual'] == 'Pendiente Verificación')]
axa_estado_Aseguradora2 = axa_estado_Aseguradora2[(axa_estado_Aseguradora2['Amparo'] == 'Gastos médicos') | (axa_estado_Aseguradora2['Amparo'] == 'Gastos de transporte y movilización')]

# Ademas, no nos interesa todas las reclaciones sino solamente las nuevas
axa_estado_Aseguradora2['Reclamación'] = CambioFormato(axa_estado_Aseguradora2, a = 'Reclamación')

axa_estado_Aseguradora3 = axa_estado_Aseguradora2[axa_estado_Aseguradora2['Reclamación'].isin(macro_hist['Reclamacion']) == False]

# Se renombran algunas columnas
columnas = {'Reclamación':'Reclamacion','Ultimo Usuario':'USUARIO DESDE 10 05 2017','poliza':'POLIZA',
            'Ciudad Siniestro':'Ciudad Siniestro (NUEVO DESDE 18/06/2018)','reclamante':'Reclamante',
            'Ciudad Reclamante':'Ciudad Reclamante NUEVO 18 06 2018'}

axa_estado_Aseguradora3 = axa_estado_Aseguradora3.rename(columns = columnas)

#%%
macro2 = axa_estado_Aseguradora3

# Se crea el campo 'Fecha de asignacion (No es formula )' y se llena con los siguiente pasos
macro2['Fecha de asignacion (No es formula )'] = np.nan
macro2.loc[(macro2['Estado Actual'] == 'Pendiente Verificación'),'Fecha de asignacion (No es formula )'] = macro2['Fecha Cambio de Estado']
macro2.loc[(macro2['Estado Actual'] != 'Pendiente Verificación'),'Fecha de asignacion (No es formula )'] = hoy

macro2['Fecha Asignación Axa Colpatria (Investigador)'] = np.nan
macro2.loc[(macro2['Estado Actual'] == 'Pendiente Verificación'),'Fecha Asignación Axa Colpatria (Investigador)'] = macro2['Fecha de asignacion (No es formula )']


macro2['ESTADO PARA INFORME STEPHANIE'] = np.nan
macro2.loc[(macro2['Estado Actual'] == 'Pendiente Verificación'), 'ESTADO PARA INFORME STEPHANIE'] = 'Asignado'

macro2['ESTADO PARA INFORME'] = np.nan
macro2.loc[(macro2['Estado Actual'] == 'Pendiente Verificación'), 'ESTADO PARA INFORME'] = 'En investigación'

# Todos las reclamaciones que tienen en 'Estado Actual' == 'Pendiente Verificación', se les asigna el proveedor 'Valuative esquema'
macro2['Proveedor'] = np.nan
macro2.loc[(macro2['Estado Actual'] == 'Pendiente Verificación'),'Proveedor'] = 'Valuative esquema'


macro2['Reclamacion'] = CambioFormato(macro2, a = 'Reclamacion')


#%%
# Se cruza como anteriormente la macro 2 con el comparativo
macro2 = macro2.merge(comparativo, how = 'left', on = 'Reclamacion')

macro2['POLIZA'] = CambioFormato(macro2, a = 'POLIZA')
macro2['Siniestro Aseguradora'] = CambioFormato(macro2, a = 'Siniestro Aseguradora')
macro2['Cedula Accidentado'] = CambioFormato(macro2, a = 'Cedula Accidentado')

# Se crea la llave POLIZA-SINIESTRO  y la llave Poliza-Fecha accidente-Cedula
macro2['LLAVE POLIZA+SINIESTRO'] = macro2['POLIZA'].astype(str) + macro2['Siniestro Aseguradora'].astype(str)

macro2['Fecha ocurrencia2'] = macro2['Fecha ocurrencia'].dt.strftime('%d/%m/%Y')
macro2['Llave Poliza+Fecha accidente+Cedula'] = macro2['POLIZA'].astype(str) + macro2['Fecha ocurrencia2'].astype(str) + macro2['Cedula Accidentado'].astype(str)

macro2['Mes Evento'] = macro2['Fecha ocurrencia'].dt.year 

#%%

# Se cruza la macro con la base esquema por el nit y la ciudad
esquema['NIT DE BENEFICIARIO'] = CambioFormato(esquema, a = 'NIT DE BENEFICIARIO')
esquema['Ciudad'] = esquema['Ciudad'].str.upper()

macro2['nit_reclamante'] = CambioFormato(macro2, a = 'nit_reclamante')
macro2['Ciudad Reclamante NUEVO 18 06 2018'] = macro2['Ciudad Reclamante NUEVO 18 06 2018'].str.upper()


macro2 = macro2.merge(esquema, how = 'left', left_on = ['nit_reclamante','Ciudad Reclamante NUEVO 18 06 2018'], right_on = ['NIT DE BENEFICIARIO','Ciudad'])
macro2 = macro2.drop_duplicates('Reclamacion')

# Se renombran algunos campos y se crean otros
macro2 = macro2.rename( columns = {'Ciudad':'Nueva formula ciudad','Fecha ocurrencia':'Fecha de Evento','Cedula Accidentado':'Id lesionado'})
macro2['Nueva formula ciudad'] = macro2['Nueva formula ciudad'].fillna('Sin esquema')

macro2['Comentario Esquema CONTROL IPS VALUATIVE'] = macro2['Comentario de gestion']
macro2['Comentario Esquema CONTROL IPS VALUATIVE'] = macro2['Comentario Esquema CONTROL IPS VALUATIVE'].fillna('No esquema')

macro2['Fecha Cambio de Estado'] = pd.to_datetime(macro2['Fecha Cambio de Estado'], format = '%d/%m/%Y')
macro2['Fecha Cambio de Estado'] = macro2['Fecha Cambio de Estado'].dt.date
macro2['Fecha Cambio de Estado'] = pd.to_datetime(macro2['Fecha Cambio de Estado'], format = '%Y-%m-%d')

# Se crea el campo Rango para separa los valores de la reclamacion
macro2['Valor Reclamación'] = macro2['Valor Reclamación'].astype(int)

condictions2 = [
       (macro2['Valor Reclamación'] <= 500000),
       (macro2['Valor Reclamación'] > 500000) & (macro2['Valor Reclamación'] <= 1000000),
       (macro2['Valor Reclamación'] > 1000000) & (macro2['Valor Reclamación'] <= 3000000),
       (macro2['Valor Reclamación'] > 3000000) & (macro2['Valor Reclamación'] <= 5000000),
       (macro2['Valor Reclamación'] > 5000000) & (macro2['Valor Reclamación'] <= 10000000),
       (macro2['Valor Reclamación'] > 10000000)
    ]

choices2 = ['Menor 500','Entre 500 y 1M','Entre 1M y 3M','Entre 3M y 5M','Entre 5M y 10M','Mayor a 10M']

macro2['Rango'] = np.select(condictions2, choices2)

# Se cruza la macro con la base de investigados por la llave POLIZA+SINIESTRO'. Esto con el fin de
# asignarle a las reclamaciones el mismo proveedor que ya tenía el siniestro si llegase a ocurrir que el 
# siniestro ya estaba
investigados_anteriores = investigados_anteriores.drop_duplicates('LLAVE POLIZA+SINIESTRO', keep = 'last')
investigados_anteriores = investigados_anteriores[investigados_anteriores['LLAVE POLIZA+SINIESTRO'].isnull() == False]
investigados_anteriores = investigados_anteriores.rename(columns = {'Fecha Asignación Axa Colpatria (Investigador)':'Fecha Asignacion RAF 2018, 2019, 2020, 2021 (LLAVE)',
                                                                  'Proveedor':'Proveedor RAF 2018, 2019, 2020, 2021 (LLAVE)'})

investigados_anteriores['LLAVE POLIZA+SINIESTRO'] = CambioFormato(investigados_anteriores, a = 'LLAVE POLIZA+SINIESTRO')

macro3 = macro2.merge(investigados_anteriores, how = 'left', on = 'LLAVE POLIZA+SINIESTRO')

macro3.loc[((macro3['Proveedor RAF 2018, 2019, 2020, 2021 (LLAVE)'].isnull() == False) & (macro3['Estado Actual'] == 'Pendiente aseguradora - investigación')),'Proveedor'] = macro3['Proveedor RAF 2018, 2019, 2020, 2021 (LLAVE)']
macro3.loc[((macro3['Proveedor RAF 2018, 2019, 2020, 2021 (LLAVE)'].isnull() == False) & (macro3['Estado Actual'] == 'Pendiente aseguradora - investigación')),'Fecha Asignación Axa Colpatria (Investigador)'] = macro3['Fecha Asignacion RAF 2018, 2019, 2020, 2021 (LLAVE)']

macro3['Fecha Asignacion RAF 2018, 2019, 2020 (LLAVE)'] = macro3['Proveedor RAF 2018, 2019, 2020, 2021 (LLAVE)']
macro3['Proveedor RAF 2018, 2019, 2020 (LLAVE)'] = macro3['Proveedor RAF 2018, 2019, 2020, 2021 (LLAVE)']

macro3['Fecha Asignacion RAF 2018, 2019, 2020 (LLAVE)'] = macro3['Fecha Asignacion RAF 2018, 2019, 2020 (LLAVE)'].fillna('No')
macro3['Proveedor RAF 2018, 2019, 2020 (LLAVE)'] = macro3['Proveedor RAF 2018, 2019, 2020 (LLAVE)'].fillna('No')
#%%
# Se realiza un cruce similar al anterior de la macro con el historico del año actual para no reasignar proveedores
proveedores_ya_asignados = macro_hist[['LLAVE POLIZA+SINIESTRO','Proveedor','Fecha Asignación Axa Colpatria (Investigador)']]
proveedores_ya_asignados = proveedores_ya_asignados.drop_duplicates('LLAVE POLIZA+SINIESTRO', keep = 'last')
proveedores_ya_asignados = proveedores_ya_asignados.rename(columns = {'Proveedor':'Proveedor2','Fecha Asignación Axa Colpatria (Investigador)':'Fecha2'})

proveedores_ya_asignados['LLAVE POLIZA+SINIESTRO'] = CambioFormato(proveedores_ya_asignados, a = 'LLAVE POLIZA+SINIESTRO')
macro4 = macro3.merge(proveedores_ya_asignados, how = 'left', on = 'LLAVE POLIZA+SINIESTRO')

macro4.loc[((macro4['Proveedor2'].isnull() == False) & (macro4['Estado Actual'] == 'Pendiente aseguradora - investigación')),'Proveedor'] = macro4['Proveedor2']
macro4.loc[((macro4['Proveedor2'].isnull() == False) & (macro4['Estado Actual'] == 'Pendiente aseguradora - investigación')),'Fecha Asignación Axa Colpatria (Investigador)'] = macro4['Fecha2']

macro4['Fecha Asignacion RAF 2021 (LLAVE)'] = macro4['Proveedor2']
macro4['Proveedor RAF 2021 (LLAVE)'] = macro4['Fecha2']

macro4['Fecha Asignacion RAF 2021 (LLAVE)'] = macro4['Fecha Asignacion RAF 2021 (LLAVE)'].fillna('No')
macro4['Proveedor RAF 2021 (LLAVE)'] = macro4['Proveedor RAF 2021 (LLAVE)'].fillna('No')
#%%

# El cruce con la tabla de casos tulua es importante para la asignacion de las reclamaciones que en definitiva no tienen asignado proveedor
Validador_Casos_TULUA['POLIZA'] = Validador_Casos_TULUA['POLIZA'].astype(str)

macro5 = macro4.merge(Validador_Casos_TULUA, how = 'left', on = 'POLIZA')

macro5['Validador Casos TULUA'] = np.where(macro5['Validador Casos TULUA'].isnull() == True, 0, macro5['Validador Casos TULUA'])

#%%
# El cruce con la tabla Rango ciudades es para tener en claro los parametro para asignar por ciudad
macro7 = macro5.merge(Rango_ciudades, how = 'left', on = 'Ciudad Siniestro (NUEVO DESDE 18/06/2018)')

macro7['Formula ciudad'] = np.where(macro7['Formula ciudad'].isnull() == True, 0, macro7['Formula ciudad'])
#%%

# Se le cambia el formato a las columnas de la tabla ruleta
for i in ruleta.columns:
    ruleta[i] = CambioFormato(ruleta, a = i)
    
# Se convierte cada columna como una lista sin vacios
lista_formula_piloto = list(ruleta['Formula Piloto'].str.upper().dropna().unique())
lista_formula_medical = list(ruleta['Formula Medical'].dropna().unique())
lista_formula_bolivariana = list(ruleta['Formula Bolivariana'].dropna().unique())
lista_formula_especialistas = list(ruleta['Especialistas'].dropna().unique())
lista_formula_costa = list(ruleta['COSTA'].dropna().unique())
lista_formula_primera_mayo = list(ruleta['Primero de Mayo'].dropna().unique())
lista_formula_reina_lucia = list(ruleta['Reina Lucia'].dropna().unique())
lista_formula_asotrauma = list(ruleta['Asotrauma'].dropna().unique())
lista_formula_debia = list(ruleta['Debia'].dropna().unique())
lista_formula_invipro = list(ruleta['Invipro'].dropna().unique())
lista_formula_formula_esquema = list(ruleta['Formula esquema'].str.title().dropna().unique())
ruleta['Ciudad'] = ruleta['Ciudad'].str.upper()
lista_ciudad = list(ruleta['Ciudad'].dropna().unique())
valor = ruleta['Valor'].str.title().dropna().unique()

# Con las listas anteriores se empiezan a crear nuevas columnas para la macro como se evidencia
macro7['Formula esquema'] = np.where(macro7['Comentario Esquema CONTROL IPS VALUATIVE'].str.title().isin(lista_formula_formula_esquema) == True, 0, 1)

macro7['nit_reclamante'] = CambioFormato(macro7, a = 'nit_reclamante')

macro7['Formula piloto'] = np.where(macro7['USUARIO DESDE 10 05 2017'].str.upper().isin(lista_formula_piloto) == True, 1, 0)

macro7['Formula Medical'] = np.where(macro7['nit_reclamante'].isin(lista_formula_medical) == True, 1, 0) 

macro7['Formula Bolivariana'] = np.where(macro7['nit_reclamante'].isin(lista_formula_bolivariana) == True, 1, 0)

macro7['Especialistas'] = np.where(macro7['nit_reclamante'].isin(lista_formula_especialistas) == True, 1, 0)

macro7[['Ciudad Rec','Departamento Rec']] = macro7['Ciudad Reclamante NUEVO 18 06 2018'].str.split('-', expand = True)

macro7['COSTA'] = np.where(macro7['Departamento Rec'].isin(lista_formula_costa) == True, 1, 0)

macro7['Primero de Mayo'] = np.where(macro7['nit_reclamante'].isin(lista_formula_primera_mayo) == True, 1, 0)

macro7['Reina Lucia'] = np.where(macro7['nit_reclamante'].isin(lista_formula_reina_lucia) == True, 1, 0)

macro7['Asotrauma'] = np.where(macro7['nit_reclamante'].isin(lista_formula_asotrauma) == True, 1, 0)

Alertas_Mensuales = Alertas_Mensuales.rename(columns = {'Provedor':'Piloto M&G'})
Alertas_Mensuales['Nit'] = CambioFormato(Alertas_Mensuales, a = 'Nit')
Alertas_Mensuales = Alertas_Mensuales.drop_duplicates('Nit', keep = 'last')

macro7 = macro7.merge(Alertas_Mensuales, how = 'left', left_on = 'nit_reclamante', right_on = 'Nit')

macro7['Piloto M&G'] = np.where((macro7['Ciudad Reclamante NUEVO 18 06 2018'].str.upper().isin(lista_ciudad) == True) & (macro7['Valor Reclamación'] >= int(valor)), 'Debia', macro7['Piloto M&G'])

macro7['Debia'] = np.where(macro7['Piloto M&G'].isin(lista_formula_debia) == True, 1, 0)

macro7['Invipro'] = np.where(macro7['Piloto M&G'].isin(lista_formula_invipro) == True, 1, 0)

# Si el campo 'Ultima Observación' contiene la palabra 'AVS' ese va a ser el proveedor que se va asignar
macro7.loc[(macro7['Ultima Observación'].str.upper().str.contains('AVS') == True),'Proveedor'] = 'AVS'
macro7.loc[(macro7['Ultima Observación'].str.upper().str.contains('AVS') == True),'Fecha Asignación Axa Colpatria (Investigador)'] = hoy

#%%
# Se apartan de la macro las reclamaciones que en definitiva no tienen un proveedor asignado
# y se agrupan por siniestro, sumando el valor de la reclamación y los campos que creamos con las condiciones y listas
Investigador = macro7[macro7['Proveedor'].isnull() == True]
Investigador = Investigador.groupby('Siniestro Aseguradora', as_index = False)['Valor Reclamación','Formula ciudad','Formula esquema','Formula piloto',
             'Formula Medical','Formula Bolivariana','COSTA','Especialistas','Primero de Mayo','Reina Lucia',
             'Asotrauma','Debia','Invipro'].sum()

#%%
# Se crea la siguiente funcion para asignar en base a ciertas reglas los proveedores a las reclamaciones que no tienen asignado proveedor
def Investigador_For(df):
    
    if df['Formula Medical'] > 0:
        a = 'Piloto Medical'
    elif df['Especialistas'] > 0:
        a = 'Piloto Especialistas'
    elif df['Formula Bolivariana'] > 0:
        a = 'Piloto Bolivariana'    
    elif df['COSTA'] > 0:
        a = 'MYG'
    elif df['Primero de Mayo'] > 0:
        a = 'Piloto P. Mayo'
    elif df['Reina Lucia'] > 0:
        a = 'Piloto R. Lucia'
    elif df['Asotrauma'] > 0:
        a = 'Piloto Asotrauma'
    elif df['Debia'] > 0:
        a = 'Debia'
    elif df['Invipro'] > 0:
        a = 'Invipro'    
    elif (df['Formula piloto'] > 0) & (df['Formula esquema'] > 0):
        a = 'Valuative Censo-pil'
    elif (df['Formula piloto'] > 0) & (df['Formula ciudad'] == 0) & (df['Valor Reclamación'] < 5000000):
        a = 'Alianza Piloto'
    elif (df['Formula piloto'] > 0) & (df['Formula ciudad'] == 0) & (df['Valor Reclamación'] > 5000000):
        a = 'Valuative Piloto'
    elif (df['Formula piloto'] > 0) & (df['Formula ciudad'] > 0):
        a = 'Alianza Piloto'
    elif df['Formula esquema'] > 0:
        a = 'Valuative Censo'
    elif df['Valor Reclamación'] <= 400000:
        a = 'Inveajustes'
    elif df['Valor Reclamación'] <= 1000000:
        a = 'Invipro'
    elif df['Valor Reclamación'] <= 3000000:
        a = 'Inveajustes'
    elif df['Valor Reclamación'] <= 5000000:
        a = 'Debia'
    elif (df['Formula ciudad'] == 0) & (df['Valor Reclamación'] <= 10000000):
        a = 'Debia'
    elif (df['Formula ciudad'] == 0) & (df['Valor Reclamación'] > 10000000):
        a = 'Debia'
    elif (df['Formula ciudad'] > 0) & (df['Valor Reclamación'] <= 10000000):
        a = 'Valuative'
    elif (df['Formula ciudad'] > 0) & (df['Valor Reclamación'] > 10000000):
        a = 'Valuative'
    else: 
        a == 0
        
    return a

#%%
# Se aplica la funcion anterior a la tabla Investigador
Investigador['Investigador'] = Investigador.apply(Investigador_For, axis = 1)

#%%
# de la tabla Investigador solo nos interesa el siniestro y la asignacion
Investigador2 = Investigador[['Siniestro Aseguradora','Investigador']]
Investigador2['Ruleta'] = 'si'

# Cruzamos la macro con la tabla Investigador2 por el siniestro
macro8 = macro7.merge(Investigador2, how = 'left', on = 'Siniestro Aseguradora')

#%%
# Los proveedores vacios se llenan con el campo Investigador
macro8['Proveedor'] = np.where((macro8['Proveedor'].isnull() == True), macro8['Investigador'], macro8['Proveedor'])
macro8['Fecha Asignación Axa Colpatria (Investigador)'] = np.where((macro8['Fecha Asignación Axa Colpatria (Investigador)'].isnull() == True), hoy, macro8['Fecha Asignación Axa Colpatria (Investigador)'])
macro8['ESTADO PARA INFORME STEPHANIE'] = 'Asignado'     
macro8['ESTADO PARA INFORME'] = 'En investigación'     
macro8['Fecha  Asignación RAF? (STICKER)'] = macro8['Fecha Asignación Axa Colpatria (Investigador)']
macro8['Proveedor RAF (STICKER)'] = macro8['Proveedor']
macro8['Fecha de asignacion (No es formula )'] = pd.to_datetime(macro8['Fecha de asignacion (No es formula )'], format = '%Y-%m-%d')
macro8['Fecha de asignacion (No es formula )'] = macro8['Fecha de asignacion (No es formula )'].dt.strftime('%d/%m/%Y')
       
macro8['nuevos'] = 'si'

#%%
# Existen reclamaciones que quedan sin proveedor porque no tenian numero de siniestro
# a esas reclamaciones nuevamente se les pasa por la ruleta pero se cruzan ya no por el siniestro sino
# por la reclamacion

Investigador3 = macro8[macro8['Proveedor'].isnull() == True]
Investigador3 = Investigador3.groupby('Reclamacion', as_index = False)['Valor Reclamación','Formula ciudad','Formula esquema','Formula piloto',
             'Formula Medical','Formula Bolivariana','COSTA','Especialistas','Primero de Mayo','Reina Lucia',
             'Asotrauma','Debia','Invipro'].sum()
if len(Investigador3['Reclamacion']) > 0:
    Investigador3['Investigador2'] = Investigador3.apply(Investigador_For, axis = 1)
else:
    Investigador3['Investigador2'] = np.nan
    
Investigador4 = Investigador3[['Reclamacion','Investigador2']]
Investigador4['Ruleta'] = 'si'

macro8_a = macro8[macro8['Proveedor'].isnull() == True]
macro8_b = macro8[macro8['Proveedor'].isnull() == False]

macro8_a = macro8_a.merge(Investigador4, how = 'left', on = 'Reclamacion')
macro8_a['Proveedor'] = macro8_a['Investigador2']
macro8_a['Fecha Asignación Axa Colpatria (Investigador)'] = hoy
macro8_a['ESTADO PARA INFORME STEPHANIE'] = 'Asignado'     
macro8_a['ESTADO PARA INFORME'] = 'En investigación'     
macro8_a['Fecha  Asignación RAF? (STICKER)'] = macro8_a['Fecha Asignación Axa Colpatria (Investigador)']
macro8_a['Proveedor RAF (STICKER)'] = macro8_a['Proveedor']

macro9 = pd.concat([macro8_b, macro8_a]).sort_index()
#%%
# Se concatenan las anteriores asignaciones con lasnuevas
raf = pd.concat([macro_hist, macro9]).reset_index(drop = True)

# Esta funcion permite obtener los días festivos de cada anio en Colombia
dic = {}
for i in raf['Fecha Cambio de Estado'].dt.year.unique():
    print('Obteniendo días festivos del año: ',i)
    df = holidays_co.get_colombia_holidays_by_year(i)
    df = pd.DataFrame(df, columns = ['festivos','2'])    
    dic[i] = df
    
festivos = pd.concat(dic).reset_index(drop = True)

festivos['festivos'] = pd.to_datetime(festivos['festivos'], format = '%Y-%m-%d')
festivos = festivos.drop(columns = ['2'])
festivos = list(festivos['festivos'].astype(str))

# Se actuliza el campo de 'Total dias habiles desde cambio de estado' que son los dias laborados desde la Fecha Cambio de Estado hasta hoy
raf['Total días hábiles desde cambio estado'] = np.busday_count(raf['Fecha Cambio de Estado'].values.astype('datetime64[D]'), datetime.today().strftime('%Y-%m-%d'), holidays = festivos)

condictions = [
               (raf['Total días hábiles desde cambio estado'] <= 2),
               (raf['Total días hábiles desde cambio estado'] > 2) & (raf['Total días hábiles desde cambio estado'] <= 4),
               (raf['Total días hábiles desde cambio estado'] > 4) & (raf['Total días hábiles desde cambio estado'] <= 8),
               (raf['Total días hábiles desde cambio estado'] > 8) & (raf['Total días hábiles desde cambio estado'] <= 10),
               (raf['Total días hábiles desde cambio estado'] > 10) & (raf['Total días hábiles desde cambio estado'] <= 12),
               (raf['Total días hábiles desde cambio estado'] > 12) & (raf['Total días hábiles desde cambio estado'] <= 20),
               (raf['Total días hábiles desde cambio estado'] > 20) & (raf['Total días hábiles desde cambio estado'] <= 30),
               (raf['Total días hábiles desde cambio estado'] > 30) & (raf['Total días hábiles desde cambio estado'] <= 40),
               (raf['Total días hábiles desde cambio estado'] > 40)
    ]
# Se separa en rangos los dias habiles
choices = ['0-2 días','Entre 2 y 4','Entre 4 y 8','Entre 8 y 10','Entre 10 y 12','Entre 12 y 20','Entre 20 y 30','Entre 30 y 40','Mayor a 40']

raf['Nuevos Rangos desde Cambio de estado'] = np.select(condictions,choices)

#%%

# Se obtienes los dias laborados de manera similar al paso anterior
raf['Fecha Asignacion Axa Colpatria (Investigador) temp'] = pd.to_datetime(raf['Fecha Asignación Axa Colpatria (Investigador)'], format = '%Y-%m-%d')
raf['Dias promedio asignacion'] = np.busday_count(raf['Fecha Asignacion Axa Colpatria (Investigador) temp'].values.astype('datetime64[D]'), datetime.today().strftime('%Y-%m-%d'), holidays = festivos)

#%%
# Se obtienes la diferencia de dias entre la fecha Cambio de estado y hoy
raf['hoy'] = pd.to_datetime(hoy, format = '%Y-%m-%d')
raf['Días Ult mov Calendario'] = (raf['hoy'] - raf['Fecha Cambio de Estado']).dt.days

#%%

condictions = [
               (raf['Días Ult mov Calendario'] <= 2),
               (raf['Días Ult mov Calendario'] > 2) & (raf['Días Ult mov Calendario'] <= 4),
               (raf['Días Ult mov Calendario'] > 4) & (raf['Días Ult mov Calendario'] <= 8),
               (raf['Días Ult mov Calendario'] > 8) & (raf['Días Ult mov Calendario'] <= 10),
               (raf['Días Ult mov Calendario'] > 10) & (raf['Días Ult mov Calendario'] <= 12),
               (raf['Días Ult mov Calendario'] > 12) & (raf['Días Ult mov Calendario'] <= 20),
               (raf['Días Ult mov Calendario'] > 20) & (raf['Días Ult mov Calendario'] <= 30),
               (raf['Días Ult mov Calendario'] > 30) & (raf['Días Ult mov Calendario'] <= 40),
               (raf['Días Ult mov Calendario'] > 40)
    ]

choices = ['0-2 días','Entre 2 y 4','Entre 4 y 8','Entre 8 y 10','Entre 10 y 12','Entre 12 y 20','Entre 20 y 30','Entre 30 y 40','Mayor a 40']

raf['Nuevos Rangos'] = np.select(condictions, choices)

#%%
# Se calculan la cantidad de siniestros que hay en la RAF
raf = raf.drop(columns = ['Reclamaciones por siniestro Mas de 1?','Validador Repetidos Llave','Validador Repetidos sticker'])
raf['Siniestro Aseguradora'] = CambioFormato(raf, a = 'Siniestro Aseguradora')


cant_siniestros = raf[raf['Siniestro Aseguradora'].isnull() == False]
cant_siniestros = cant_siniestros.groupby('Siniestro Aseguradora')['Siniestro Aseguradora'].count()
cant_siniestros = pd.DataFrame(cant_siniestros)
cant_siniestros = cant_siniestros.rename(columns = {'Siniestro Aseguradora':'Reclamaciones por siniestro Mas de 1?'})
cant_siniestros = cant_siniestros.reset_index()

#%%
raf = raf.merge(cant_siniestros, how = 'left', on = 'Siniestro Aseguradora')

raf['Reclamacion'] = CambioFormato(raf, a = 'Reclamacion')

cant_Reclamaciones = raf[raf['Reclamacion'].isnull() == False]
cant_Reclamaciones = cant_Reclamaciones.groupby('Reclamacion')['Reclamacion'].count()
cant_Reclamaciones = pd.DataFrame(cant_Reclamaciones)
cant_Reclamaciones = cant_Reclamaciones.rename(columns = {'Reclamacion':'Validador Repetidos sticker'})
cant_Reclamaciones = cant_Reclamaciones.reset_index()

raf = raf.merge(cant_Reclamaciones, how = 'left', on = 'Reclamacion')


#%%
# Se calculan la cantidad de Poliza-Siniestro
raf['LLAVE POLIZA+SINIESTRO'] = CambioFormato(raf, a = 'LLAVE POLIZA+SINIESTRO')

cant_Polizas_mas_siniestros = raf[raf['LLAVE POLIZA+SINIESTRO'].isnull() == False]
cant_Polizas_mas_siniestros = cant_Polizas_mas_siniestros.groupby('LLAVE POLIZA+SINIESTRO')['LLAVE POLIZA+SINIESTRO'].count()
cant_Polizas_mas_siniestros = pd.DataFrame(cant_Polizas_mas_siniestros)
cant_Polizas_mas_siniestros = cant_Polizas_mas_siniestros.rename(columns = {'LLAVE POLIZA+SINIESTRO':'Validador Repetidos Llave'})
cant_Polizas_mas_siniestros = cant_Polizas_mas_siniestros.reset_index()

raf = raf.merge(cant_Polizas_mas_siniestros, how = 'left', on = 'LLAVE POLIZA+SINIESTRO')

#%%
# Se cruza la raf con la base de descargas
base_de_descargas = base_de_descargas.drop_duplicates('Reclamación', keep = 'last')
base_de_descargas['Reclamación'] = CambioFormato(base_de_descargas, a = 'Reclamación')
base_de_descargas = base_de_descargas.rename(columns = {'Fecha de Entrega Proveedor Inves':'Fech de Ent Prov Inves',
                                                        'Conclusión Estatus Investigación':'Conc Est Inves'})

raf = raf.merge(base_de_descargas, how = 'left', left_on = 'Reclamacion', right_on = 'Reclamación')
raf['Cruce 2022'] = np.where((raf['Reclamación'].isnull() == False) & (raf['Fecha de Entrega Proveedor Inves'].isnull() == True) & (raf['Estado Actual'] == 'Pendiente aseguradora - investigación'), 'Si','No')

#%%
# Se actualizan los campos de la raf que estaban vacion y cruzaron con la base de descargas
raf['fecha mov'] = raf['Fecha gestionado CLAIM']
raf.loc[(raf['Fecha Gestionado SQG'].isnull() == True) & (raf['Estado Actual'] == 'Pendiente aseguradora - investigación'),'Fecha Gestionado SQG'] = raf['fecha mov']
raf['Coment Claim'] = raf['Comentario Claim']
raf.loc[(raf['Comentario CLAIM\nSQG'].isnull() == True) & (raf['Estado Actual'] == 'Pendiente aseguradora - investigación'),'Comentario CLAIM\nSQG'] = raf['Coment Claim']
raf['Tipo de investigacion'] = raf['Tipo Informe Escritorio/Campo']
raf.loc[(raf['Tipo de Investigacion'].isnull() == True) & (raf['Estado Actual'] == 'Pendiente aseguradora - investigación'),'Tipo de Investigacion'] = raf['Tipo de investigacion']
raf['fecha entrega'] = raf['Fech de Ent Prov Inves']
raf.loc[(raf['Fecha de Entrega Proveedor Inves'].isnull() == True) & (raf['Estado Actual'] == 'Pendiente aseguradora - investigación'),'Fecha de Entrega Proveedor Inves'] = raf['fecha entrega']
raf['conclucion'] = raf['Conc Est Inves']
raf.loc[(raf['Conclusión Estatus Investigación'].isnull() == True) & (raf['Estado Actual'] == 'Pendiente aseguradora - investigación'),'Conclusión Estatus Investigación'] = raf['conclucion']
raf['Causal'] = raf['Causal (No Cubiertos)']
raf.loc[(raf['TIPO FRAUDE'].isnull() == True) & (raf['Estado Actual'] == 'Pendiente aseguradora - investigación'),'TIPO FRAUDE'] = raf['Causal']
raf['proveedor'] = raf['Provedor']
raf['FRAUDE IPS O PERSONA NATURAL'] = raf['Persona Fraude']
raf.loc[(raf['Valor Viatico'].isnull() == True) & (raf['Estado Actual'] == 'Pendiente aseguradora - investigación'),'Valor Viatico'] = raf['Valor viaticos']

raf['Conclusión Estatus Investigación'] = raf['Conclusión Estatus Investigación'].str.strip()

# La siguiente funcion convierte el mes de una fecha en Texto
def ConvertirMes(mes):
    m = {
        '01': "Enero",
        '02': "Febrero",
        '03': "Marzo",
        '04': "Abril",
        '05': "Mayo",
        '06': "Junio",
        '07': "Julio",
        '08': "Agosto",
        '09': "Septiembre",
        '10': "Octubre",
        '11': "Noviembre",
        '12': "Diciembre"
        }
    return str(m[mes])


raf.loc[(raf['MES CUANTIFICACION AHORRO'].isnull() == True) & (raf['Conclusión Estatus Investigación'].str.upper().isin(['CUBIERTO','OCURRENCIA NO CONFIRMADA']) == True) & (raf['Estado Actual'] == 'Pendiente aseguradora - investigación'),'MES CUANTIFICACION AHORRO'] = ConvertirMes(hoy.strftime('%m')) + ' de ' + hoy.strftime('%Y')
raf.loc[(raf['MES CUANTIFICACION AHORRO'].isnull() == True) & (raf['Conclusión Estatus Investigación'].str.upper().isin(['NO CUBIERTO','COSTOS NO PERTINENTES']) == True) & (raf['Estado Actual'] == 'Pendiente aseguradora - investigación'),'MES CUANTIFICACION AHORRO'] = 'Seguimiento a glosa'

raf.loc[(raf['Fecha de Entrega Proveedor Inves'].isnull() == False) & (raf['Estado Actual'] == 'Pendiente aseguradora - investigación'),'Estatus Informe'] ='Entregado'
raf.loc[(raf['Fecha de Entrega Proveedor Inves'].isnull() == True) & (raf['Estado Actual'] == 'Pendiente aseguradora - investigación'),'Estatus Informe'] ='Pendiente'
#%%


# se separan el campo 'Fecha de Entrega Proveedor Inves' en los que si tienen fecha y los que no
raf_a = raf[(raf['Fecha de Entrega Proveedor Inves'].str.upper().str.isupper() == True) | (raf['Fecha de Entrega Proveedor Inves'].isnull() == True)]
raf_b = raf[raf['Reclamacion'].isin(raf_a['Reclamacion']) == False]

# Se obtienen los dias festivos
raf_b['Fecha Asignación Axa Colpatria (Investigador)'] = pd.to_datetime(raf_b['Fecha Asignación Axa Colpatria (Investigador)'], format = '%Y-%m-%d')
dic = {}
for i in raf_b['Fecha Asignación Axa Colpatria (Investigador)'].dt.year.unique():
    print('Obteniendo días festivos del año: ',i)
    df = holidays_co.get_colombia_holidays_by_year(i)
    df = pd.DataFrame(df, columns = ['festivos','2'])    
    dic[i] = df
    
festivos = pd.concat(dic).reset_index(drop = True)

festivos['festivos'] = pd.to_datetime(festivos['festivos'], format = '%Y-%m-%d')
festivos = festivos.drop(columns = ['2'])
festivos = list(festivos['festivos'].astype(str))

# Y se calculan los dias laborales desde 'Fecha Asignación Axa Colpatria (Investigador)' hasta 'Fecha de Entrega Proveedor Inves'
raf_b['Fecha de Entrega Proveedor Inves'] = pd.to_datetime(raf_b['Fecha de Entrega Proveedor Inves'], format = '%Y-%m-%d')
raf_b['Dias'] = np.busday_count(raf_b['Fecha Asignación Axa Colpatria (Investigador)'].values.astype('datetime64[D]'), raf_b['Fecha de Entrega Proveedor Inves'].values.astype('datetime64[D]'), holidays = festivos)
raf_b['Año entrega'] = raf_b['Fecha de Entrega Proveedor Inves'].dt.year
raf_b['Mes Entrega'] = raf_b['Fecha de Entrega Proveedor Inves'].dt.month
raf_b['Fecha de Entrega Proveedor Inves'] = raf_b['Fecha de Entrega Proveedor Inves'].dt.strftime('%d/%m/%Y')

#%%
raf_2 = pd.concat([raf_a, raf_b]).sort_index()



# Se cambian los formatos fechas de algunos campos
raf_2['Fecha Recepción'] = raf_2['Fecha Recepción'].dt.strftime('%d/%m/%Y')
raf_2['Fecha Cambio de Estado'] = raf_2['Fecha Cambio de Estado'].dt.strftime('%d/%m/%Y')
raf_2['Fecha  Asignación RAF? (STICKER)'] = pd.to_datetime(raf_2['Fecha  Asignación RAF? (STICKER)'], format = '%Y-%m-%d')
raf_2['Fecha  Asignación RAF? (STICKER)'] = raf_2['Fecha  Asignación RAF? (STICKER)'].dt.strftime('%d/%m/%Y')
raf_2['Fecha Asignación Axa Colpatria (Investigador)'] = pd.to_datetime(raf_2['Fecha Asignación Axa Colpatria (Investigador)'], format = '%Y-%m-%d')
raf_2['Fecha Asignación Axa Colpatria (Investigador)'] = raf_2['Fecha Asignación Axa Colpatria (Investigador)'].dt.strftime('%d/%m/%Y')
raf_2['Con ahorro?'] = np.where(raf_2['Gross Savings'] > 0, 'Positivo', 'Negativo')
raf_2['Fecha de Evento'] = raf_2['Fecha de Evento'].dt.strftime('%d/%m/%Y')
#%%
# Se filtran solamente los campos a usar
raf_3 = raf_2[['LLAVE POLIZA+SINIESTRO','Llave Poliza+Fecha accidente+Cedula','Number of cases detected in the period',	
          'Valor Viatico','Investigation Costs (expenses)','Valor Total Costo +Viatico','Gross Savings','Reclamacion',
          'Valor Reclamación','Estado Actual','Ultima Observación','USUARIO DESDE 10 05 2017','Fecha Cambio de Estado',	
          'dias_ult_mov','dias_tramite','Fecha Recepción','POLIZA','poliza_faltante','Siniestro April','Siniestro Aseguradora',	
          'Ciudad Siniestro (NUEVO DESDE 18/06/2018)','Amparo','nit_reclamante','Reclamante','Ciudad Reclamante NUEVO 18 06 2018',	
          'Nueva formula ciudad','Fecha de asignacion (No es formula )','Comentario Esquema CONTROL IPS VALUATIVE',	
          'ESTADO PARA INFORME STEPHANIE','ESTADO PARA INFORME','Fecha Gestionado SQG','Comentario CLAIM\nSQG',
          'Otros comentarios','Total días hábiles desde cambio estado','Nuevos Rangos desde Cambio de estado',
          'Reclamaciones por siniestro Mas de 1?','Validador Repetidos Llave','Rango','ACTUALIZADO 05-02-2019',	
          'Fecha  Asignación RAF? (STICKER)','Proveedor RAF (STICKER)','Trámite Modelo (No lo estamos usando)',	
          'Fecha Asignacion RAF 2021 (LLAVE)','Proveedor RAF 2021 (LLAVE)','Fecha Asignacion RAF 2018, 2019, 2020 (LLAVE)',	
          'Proveedor RAF 2018, 2019, 2020 (LLAVE)','Marca Mayores 26 dias','Orden','Validador Repetidos sticker',	
          'Mes Evento','Fecha de Evento','Id lesionado','Dias promedio movimiento','Fecha Asignación Axa Colpatria (Investigador)',	
          'Tipo de Investigacion','Proveedor','Fecha de Entrega Proveedor Inves','Estatus Informe','Dias','Año entrega',	
          'Mes Entrega','Conclusión Estatus Investigación','TIPO FRAUDE','MES CUANTIFICACION AHORRO','MES CUANTIFICACION COSTOS',
          'Tipo glosa Comparativo','Fecha liberacion reserva (Mes cuantificacion ahorro)','COMENTARIO CUANTIFICACION',
          'ESTADO FINAL','Vr Recobrar','Valor ahorrado','Dias promedio asignacion','Días Promedio Investigacion',
          'COMENTARIOS PARA CUANTIFICAR COSTOS','Seguimiento / Notas\n A partir del 7/3/2018 esta es la fecha de cargue o actualizacion',	
          'Fecha Para Q KPI','Van en KPI? (Nuevo para Q1 2018)','Con ahorro?','fecha entrega','proveedor','conclucion',
          'fecha mov','Coment Claim','Causal','FRAUDE IPS O PERSONA NATURAL','Tipo de investigacion','Piloto Modelo',	
          'Días Ult mov Calendario','Nuevos Rangos','Formula - Costo Beneficio','Ahorro Fuente','Fila',
          'Piloto Medical / Uros / Devoluciones','REGLAS','MODELO','VERIFICACIÓN','nuevos','Ruleta','Cruce 2022','Placa',
          'Nombres Accidentado']]



#%%
# Se cambian ciertos formatos
lista = ['Siniestro Aseguradora','LLAVE POLIZA+SINIESTRO','POLIZA','poliza_faltante','nit_reclamante','Id lesionado',
         'REGLAS','MODELO','VERIFICACIÓN','Reclamacion','Llave Poliza+Fecha accidente+Cedula']

for i in lista:
    print(i)
    raf_3[i] = CambioFormato(raf, a = i)
    raf_3[i] = raf_3[i].replace('nan',np.nan)
#%%
print('\n Los datos originales de la RAF contenían ' ,str(macro.shape[0]), ' registros')
print('Los datos nuevos contienen ' ,str(macro9.shape[0]), ' registros')
print('Ahora los datos de la RAF contienen ' ,str(raf_3.shape[0]), ' registros \n')


print('Guardando nuevo archivo Raf')
raf_3.to_excel(path_sal + '\Raf.xlsx', index = False)
print('Archivo Raf guardado \n')

print('Proceso finalizado')

print("Tiempo del Proceso: " , datetime.now()-Tiempo_Total)




