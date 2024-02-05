#Modul Python
import pandas as pd                 # digunakan untuk pengolahan data frames, data tabels, data array. data pivot
#from datetime import time
import psycopg2                     #ini modul database opc
from psycopg2 import Error
import warnings             
warnings.filterwarnings('ignore')
import numpy as np
import streamlit as st

st.markdown(
    """
    <style>
        body {
            background-color: #f0f0f0;
        }
    <style>
    """,
    unsafe_allow_html=True
)
#Koneksi Modul
try:
    connection = psycopg2.connect(user="opc",password="opc12345",host="10.7.19.140",port="5432",database="opc")
    print("****************")
    print("OPC UJKT ................... Koneksi Tersambung")
    print("****************")
except (Exception, Error) as error:
    print("Error cek koneksi dan pastikan satu Jaringan Kantor", error)

#List Taglist DCS
lst=[]

MW1 = "'KALTIM1.SIGNAL.AI.10MKA01CE004'"
MW2 = "'KALTIM2.SIGNAL.AI.20MKA01CE004'"
SUA = "'KALTIM 0.SIGNAL.DI.J0EAA01AF001XB54'"
SUB = "'KALTIM 0.SIGNAL.DI.J0EAA02AF001XB54'"
C1A = "'KALTIM 0.SIGNAL.DI.J0EAC11AF001XB01'"
C1B = "'KALTIM 0.SIGNAL.DI.J0EAC12AF001XB01'"
C2U = "'KALTIM 0.SIGNAL.DI.J0EAC10AF002XB00'"
C2L = "'KALTIM 0.SIGNAL.DI.J0EAC10AF002XB01'"
C3 = "'KALTIM 0.SIGNAL.DI.J0EAC10AF003'"
C4A = "'KALTIM 0.SIGNAL.DI.J0EAC11AF004'"
C4B = "'KALTIM 0.SIGNAL.DI.J0EAC12AF004'"
CRA = "'KALTIM 0.SIGNAL.DI.J0EBC11AJ001XB00'"
CRB = "'KALTIM 0.SIGNAL.DI.J0EBC12AJ001XB00'"
C5A = "'KALTIM 0.SIGNAL.DI.J0EAC11AF005XB01'"
C5B = "'KALTIM 0.SIGNAL.DI.J0EAC12AF005XB01'"
C6A = "'KALTIM 0.SIGNAL.DI.J0EAC11AF006'"
C6B = "'KALTIM 0.SIGNAL.DI.J0EAC12AF006'"

B1A = 'KALTIM_0.SIGNAL.AI.10HFA10CL101'
B1B = 'KALTIM_0.SIGNAL.AI.10HFA20CL101'
B1C = 'KALTIM_0.SIGNAL.AI.10HFA30CL101'
B1D = 'KALTIM_0.SIGNAL.AI.10HFA40CL101'
B1E = 'KALTIM_0.SIGNAL.AI.10HFA50CL101'
B1F = 'KALTIM_0.SIGNAL.AI.10HFA60CL101'
B2A = 'KALTIM_0.SIGNAL.AI.20HFA10CL101'
B2B = 'KALTIM_0.SIGNAL.AI.20HFA20CL101'
B2C = 'KALTIM_0.SIGNAL.AI.20HFA30CL101'
B2D = 'KALTIM_0.SIGNAL.AI.20HFA40CL101'
B2E = 'KALTIM_0.SIGNAL.AI.20HFA50CL101'
B2F = 'KALTIM_0.SIGNAL.AI.20HFA60CL101'

CF1A = 'KALTIM1.SIGNAL.AI.10HFB10AF001XQ02'
CF1B = 'KALTIM1.SIGNAL.AI.10HFB20AF001XQ02'
CF1C = 'KALTIM1.SIGNAL.AI.10HFB30AF001XQ02'
CF1D = 'KALTIM1.SIGNAL.AI.10HFB40AF001XQ02'
CF1E = 'KALTIM1.SIGNAL.AI.10HFB50AF001XQ02'
CF1F = 'KALTIM1.SIGNAL.AI.10HFB60AF001XQ02'
CF2A = 'KALTIM2.SIGNAL.AI.20HFB10AF001XQ02'
CF2B = 'KALTIM2.SIGNAL.AI.20HFB20AF001XQ02'
CF2C = 'KALTIM2.SIGNAL.AI.20HFB30AF001XQ02'
CF2D = 'KALTIM2.SIGNAL.AI.20HFB40AF001XQ02'
CF2E = 'KALTIM2.SIGNAL.AI.20HFB50AF001XQ02'
CF2F = 'KALTIM2.SIGNAL.AI.20HFB60AF001XQ02'

st.sidebar.header('Input Tanggal')
tglstart = st.sidebar.date_input("Masukkan tanggal mulai")
tmstart = st.sidebar.time_input("Masukkan waktu mulai")
tglover = st.sidebar.date_input("Masukkan tanggal berakhir")
tmover = st.sidebar.time_input("Masukkan waktu berakhir")

start = "'" + str(tglstart) + " " + str(tmstart) + "'"
finish = "'" + str(tglover) + " " + str(tmover) + "'"

#Data yang akan dipanggil
lst=[]
conv=[]
list = [B1A, B1B, B1C,B1D, B1E, B1F, B2A, B2B, B2C, B2D, B2E, B2F, CF1A, CF1B, CF1C, CF1D, CF1E, CF1F, CF2A, CF2B, CF2C, CF2D, CF2E, CF2F]
list_conv = [MW1, MW2, SUA, SUB, C1A, C1B, C2U, C2L, C3, C4A, C4B, CRA, CRB, C5A, C5B, C6A, C6B]
tglin = start
tglout = finish
menit="0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59"

#Resume
print("*****************")
print('Tanggal Tarikan OPC dari',tglin,"Sampai",tglout)
print("Input Tag DCS berjumlah",len(list),"Item Tag")
print("*****************")

# =================================== DATA REALTIME ========================================
sql2= connection.cursor()
query2="select round(value,2) as nilai from current "
func2="where address_no= " 
dat2=" and a.address_no =b.address_no"

for lop2 in list_conv:
	var2 = query2 +func2 +lop2
	sql2.execute(var2)
	conv.extend(sql2.fetchall())
df2 = pd.DataFrame.from_records(conv, columns=[x[0] for x in sql2.description])
df2.insert(0,'Deskripsi', ['MW1', 'MW2', 'Ship Unloader A', 'Ship Unloader B', 'Conveyor C01A', 'Conveyor C01B', 'Conveyor C2 Unloading', 'Conveyor C2 Loading', 'Conveyor 3', 'Conveyor C4A', 'Conveyor C4B', 'Crusher A', 'Crusher B', 'Conveyor C5A', 'Conveyor C5B', 'Conveyor C6A', 'Conveyor C6B'])
dataconveyor=df2.pivot(columns='Deskripsi',values='nilai')

dataconvey = dataconveyor.fillna(0)
MWA = dataconvey['MW1'].sum()
MWB = dataconvey['MW2'].sum()
tot_SUA = dataconvey['Ship Unloader A'].sum()
tot_SUB = dataconvey['Ship Unloader B'].sum()
tot_C1A = dataconvey['Conveyor C01A'].sum()
tot_C1B = dataconvey['Conveyor C01B'].sum()
tot_C2L = dataconvey['Conveyor C2 Loading'].sum()
tot_C2U = dataconvey['Conveyor C2 Unloading'].sum()
tot_C3 = dataconvey['Conveyor 3'].sum()
tot_C4A = dataconvey['Conveyor C4A'].sum()
tot_C4B = dataconvey['Conveyor C4B'].sum()
tot_CrusA = dataconvey['Conveyor C4B'].sum()
tot_CrusB = dataconvey['Conveyor C4B'].sum()
tot_C5A = dataconvey['Conveyor C5A'].sum()
tot_C5B = dataconvey['Conveyor C5B'].sum()
tot_C6A = dataconvey['Conveyor C6A'].sum()
tot_C6B = dataconvey['Conveyor C6B'].sum()

# Membuat DataFrame dari Array NumPy
daw = np.array([[tot_SUA, tot_SUB, tot_C1A, tot_C1B, tot_C2U, tot_C2L, tot_C3, tot_C4A, tot_C4B, tot_CrusA, tot_CrusB, tot_C5A, tot_C5B, tot_C6A, tot_C6B],])
dw = pd.DataFrame(daw, columns=['Ship Unloader A', 'Ship Unloader B', 'Conveyor C01A', 'Conveyor C01B', 'Conveyor C2 Unloading', 'Conveyor C2 Loading', 'Conveyor 3', 'Conveyor C4A', 'Conveyor C4B', 'Crusher A', 'Crusher B', 'Conveyor C5A', 'Conveyor C5B','Conveyor C6A', 'Conveyor C6B'])#.astype(str)

# =================================== DATA TARIKAN =========================================
#query
print("Tarikan OPC Loading.....")
print("Tanggal Tarikan dari",tglin,"Sampai",tglout)
sql= connection.cursor()
query="select a.date_rec as tanggal_penarikan,b.address_no,b.description, round(a.value,2) as nilai_operasi,b.satuan from history a,address b where a.address_no="
func=" and a.address_no=b.address_no "
dat="and a.date_rec >= timestamp "+tglin+" and a.date_rec <= timestamp "+tglout+" "
ext="and extract (minute from a.date_rec) in ("+menit+") order by a.date_rec"
#tag disesuaikan dengan taglis, silahkan tambahkan sesuaikan dengan keinginan 
#loop Query
if menit=="0":
    for lop in list:
        var=query + "'" + lop + "'" + func + dat + ext
        sql.execute(var)
        lst.extend(sql.fetchall())
else:
    for lop in list:
        var=query + "'" + lop + "'" + func + dat + ext
        sql.execute(var.replace('KALTIM_0','KALTIM 0'))
        lst.extend(sql.fetchall())
df = pd.DataFrame.from_records(lst, columns=[x[0] for x in sql.description])

#Pivot of DataFrame Query SQL
data=df.pivot(index='tanggal_penarikan',columns='description',values='nilai_operasi').astype(float)

st.markdown("""
🏂 Skripsi UNIBA
         
Anshori
197030717

""")

col1, col2, col3 = st.columns([1,1,1])

col1.text("MW Unit 1")
col1.markdown(MWA)

col2.text("MW Unit 2")
col2.markdown(MWB)

col1.text("Ship Unloader A")
if float(dw['Ship Unloader A']) == 1:
    col1.markdown(
        f'<div style="background-color:{"#DB343A"}; padding:5px; border-radius:3px;">' 'SU A RUN' '</div>', unsafe_allow_html=True)
else:
    col1.markdown(
        f'<div style="background-color:{"#58EA0F"}; padding:5px; border-radius:3px;">' 'SU A STOP' '</div>', unsafe_allow_html=True)

col2.text("Ship Unloader B")
if float(dw['Ship Unloader B']) == 1:
    col2.markdown(
        f'<div style="background-color:{"#DB343A"}; padding:8px; border-radius:3px;">' 'SU B RUN' '</div>', unsafe_allow_html=True)
else:
    col2.markdown(
        f'<div style="background-color:{"#58EA0F"}; padding:8px; border-radius:3px;">' 'SU B STOP' '</div>', unsafe_allow_html=True)

col3.text("Conveyor C01A")
if float(dw['Conveyor C01A']) == 1:
    col3.markdown(
        f'<div style="background-color:{"#DB343A"}; padding:8px; border-radius:3px;">' 'C01A RUN' '</div>', unsafe_allow_html=True)
else:
    col3.markdown(
        f'<div style="background-color:{"#58EA0F"}; padding:8px; border-radius:3px;">' 'C01A STOP' '</div>', unsafe_allow_html=True)

col1.text("Conveyor C01B")
if float(dw['Conveyor C01B']) == 1:
    col1.markdown(
        f'<div style="background-color:{"#DB343A"}; padding:8px; border-radius:3px;">' 'C01B RUN' '</div>', unsafe_allow_html=True)
else:
    col1.markdown(
        f'<div style="background-color:{"#58EA0F"}; padding:8px; border-radius:3px;">' 'C01B STOP' '</div>', unsafe_allow_html=True)

col2.text("Conveyor C2 Unloading")
if float(dw['Conveyor C2 Unloading']) == 1:
    col2.markdown(
        f'<div style="background-color:{"#DB343A"}; padding:8px; border-radius:3px;">' 'C2 UNLOADING RUN' '</div>', unsafe_allow_html=True)
else:
    col2.markdown(
        f'<div style="background-color:{"#58EA0F"}; padding:8px; border-radius:3px;">' 'C2 UNLOADING STOP' '</div>', unsafe_allow_html=True)

col3.text("Conveyor C2 Loading")
if float(dw['Conveyor C2 Loading']) == 1:
    col3.markdown(
        f'<div style="background-color:{"#DB343A"}; padding:8px; border-radius:3px;">' 'C2 LOADING RUN' '</div>', unsafe_allow_html=True)
else:
    col3.markdown(
        f'<div style="background-color:{"#58EA0F"}; padding:8px; border-radius:3px;">' 'C2 LOADING STOP' '</div>', unsafe_allow_html=True)

col1.text("Conveyor C3")
if float(dw['Conveyor 3']) == 1:
    col1.markdown(
        f'<div style="background-color:{"#DB343A"}; padding:8px; border-radius:3px;">' 'C3 RUN' '</div>', unsafe_allow_html=True)
else:
    col1.markdown(
        f'<div style="background-color:{"#58EA0F"}; padding:8px; border-radius:3px;">' 'C3 STOP' '</div>', unsafe_allow_html=True)

col2.text("Conveyor C4A")
if float(dw['Conveyor C4A']) == 1:
    col2.markdown(
        f'<div style="background-color:{"#DB343A"}; padding:8px; border-radius:3px;">' 'C4A RUN' '</div>', unsafe_allow_html=True)
else:
    col2.markdown(
        f'<div style="background-color:{"#58EA0F"}; padding:8px; border-radius:3px;">' 'C4A STOP' '</div>', unsafe_allow_html=True)

col3.text("Conveyor C4B")
if float(dw['Conveyor C4B']) == 1:
    col3.markdown(
        f'<div style="background-color:{"#DB343A"}; padding:8px; border-radius:3px;">' 'C4B RUN' '</div>', unsafe_allow_html=True)
else:
    col3.markdown(
        f'<div style="background-color:{"#58EA0F"}; padding:8px; border-radius:3px;">' 'C4B STOP' '</div>', unsafe_allow_html=True)

col1.text("Crusher A")
if float(dw['Crusher A']) == 1:
    col1.markdown(
        f'<div style="background-color:{"#DB343A"}; padding:8px; border-radius:3px;">' 'CRUSHER A RUN' '</div>', unsafe_allow_html=True)
else:
    col1.markdown(
        f'<div style="background-color:{"#58EA0F"}; padding:8px; border-radius:3px;">' 'CRUSHER A STOP' '</div>', unsafe_allow_html=True)

col2.text("Crusher B")
if float(dw['Crusher B']) == 1:
    col2.markdown(
        f'<div style="background-color:{"#DB343A"}; padding:8px; border-radius:3px;">' 'CRUSHER B RUN' '</div>', unsafe_allow_html=True)
else:
    col2.markdown(
        f'<div style="background-color:{"#58EA0F"}; padding:8px; border-radius:3px;">' 'CRUSHER B STOP' '</div>', unsafe_allow_html=True)

col3.text("Conveyor C5A")
if float(dw['Conveyor C5A']) == 1:
    col3.markdown(
        f'<div style="background-color:{"#DB343A"}; padding:8px; border-radius:3px;">' 'C5A RUN' '</div>', unsafe_allow_html=True)
else:
    col3.markdown(
        f'<div style="background-color:{"#58EA0F"}; padding:8px; border-radius:3px;">' 'C5A STOP' '</div>', unsafe_allow_html=True)

col1.text("Conveyor C5B")
if float(dw['Conveyor C5B']) == 1:
    col1.markdown(
        f'<div style="background-color:{"#DB343A"}; padding:8px; border-radius:3px;">' 'C5B RUN' '</div>', unsafe_allow_html=True)
else:
    col1.markdown(
        f'<div style="background-color:{"#58EA0F"}; padding:8px; border-radius:3px;">' 'C5B STOP' '</div>', unsafe_allow_html=True)

col2.text("Conveyor C6A")
if float(dw['Conveyor C6A']) == 1:
    col2.markdown(
        f'<div style="background-color:{"#DB343A"}; padding:8px; border-radius:3px;">' 'C6A RUN' '</div>', unsafe_allow_html=True)
else:
    col2.markdown(
        f'<div style="background-color:{"#58EA0F"}; padding:8px; border-radius:3px;">' 'C6A STOP' '</div>', unsafe_allow_html=True)

col3.text("Conveyor C6B")
if float(dw['Conveyor C6B']) == 1:
    col3.markdown(
        f'<div style="background-color:{"#DB343A"}; padding:8px; border-radius:3px;">' 'C6B RUN' '</div>', unsafe_allow_html=True)
else:
    col3.markdown(
        f'<div style="background-color:{"#58EA0F"}; padding:8px; border-radius:3px;">' 'C6B STOP' '</div>', unsafe_allow_html=True)

st.text(" ")
st.markdown("<br>", unsafe_allow_html=True)
st.write(data)
st.text(" ")
st.markdown("<br>", unsafe_allow_html=True)



