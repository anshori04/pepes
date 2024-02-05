#Modul Python
import pandas as pd                 # digunakan untuk pengolahan data frames, data tabels, data array. data pivot
import psycopg2                     #ini modul database opc
from psycopg2 import Error
from datetime import datetime, timedelta
import numpy as np
import streamlit as st

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
lstCF = []
listCF = [CF1A, CF1B, CF1C, CF1D, CF1E, CF1F, CF2A, CF2B, CF2C, CF2D, CF2E, CF2F]

st.sidebar.header('Input Tanggal')
tglstart = st.sidebar.date_input("Masukkan tanggal mulai")
tmstart = st.sidebar.time_input("Masukkan waktu mulai")
tglover = st.sidebar.date_input("Masukkan tanggal berakhir")
tmover = st.sidebar.time_input("Masukkan waktu berakhir")

start = "'" + str(tglstart) + " " + str(tmstart) + "'"
finish = "'" + str(tglover) + " " + str(tmover) + "'"

#Data yang akan dipanggil
lst=[]
list = [MW1, MW2, SUA, SUB, C1A, C1B, C2U, C2L, C3, C4A, C4B, CRA, CRB, C5A, C5B, C6A, C6B]

bnk=[]
bunker = [B1A, B1B, B1C,B1D, B1E, B1F, B2A, B2B, B2C, B2D, B2E, B2F]
tglin = start
tglout = finish
menit="0,5,10,15,20,25,30,35,40,45,50,55"

curr_now=datetime.now()
saiki =curr_now.strftime("%d %B %Y")

col1, col2 = st.columns([3,1])
moving_text = f"Skripsi iOT - {saiki}"
col1.write(
    f"""
    <div style="color: red; font-size: 24px;" id="moving-text">
        {moving_text}
    </div>
    <style>
    @keyframes moveText {{
        0% {{ transform: translateX(100%); }}
        100% {{ transform: translateX(-100%); }}
    }}

    #moving-text {{
        animation: moveText 10s linear infinite;
    }}
    </style>
    """,
    unsafe_allow_html=True,
)


#Resume
print("*****************")
print("Input Tag DCS berjumlah",len(list),"Item Tag")
print("*****************")

#query
print("Tarikan OPC Loading.....")

# =================================== DATA REALTIME ========================================
sql= connection.cursor()
query="select b.description, round(a.value,2) as nilai, b.satuan from current a,address b "
func="where a.address_no=" 
dat=" and a.address_no =b.address_no"

for lop in list:
	var = query +func +lop + dat
	sql.execute(var)
	lst.extend(sql.fetchall())
df = pd.DataFrame.from_records(lst, columns=[x[0] for x in sql.description])

#Pivot of DataFrame Query SQL
data=df.pivot(index=None,columns='description',values='nilai').astype(float)

# =================================== DATA BUNKER ==========================================
sql2= connection.cursor()
query2="select a.date_rec as tanggal_penarikan,b.address_no,b.description, round(a.value,2) as nilai_operasi,b.satuan from history a,address b where a.address_no="
func2=" and a.address_no=b.address_no "
dat2="and a.date_rec >= timestamp "+tglin+" and a.date_rec <= timestamp "+tglout+" "
ext2="and extract (minute from a.date_rec) in ("+menit+") order by a.date_rec"
#tag disesuaikan dengan taglis, silahkan tambahkan sesuaikan dengan keinginan 
#loop Query

if menit=="0":
    for lop2 in bunker:
        var2=query2 + "'" + lop2 + "'" + func2 + dat2 + ext2
        sql2.execute(var2)
        bnk.extend(sql2.fetchall())
else:
    for lop2 in bunker:
        var2=query2 + "'" + lop2 + "'" + func2 + dat2 + ext2
        sql2.execute(var2.replace('KALTIM_0','KALTIM 0'))
        bnk.extend(sql2.fetchall())
df2 = pd.DataFrame.from_records(bnk, columns=[x[0] for x in sql2.description])

# =================================== DATA COAL FEEDER ==========================================
sql3= connection.cursor()
query3="select a.date_rec as tanggal_penarikan,b.address_no,b.description, round(a.value,2) as nilai_operasi,b.satuan from history a,address b where a.address_no="
func3=" and a.address_no=b.address_no "
dat3="and a.date_rec >= timestamp "+tglin+" and a.date_rec <= timestamp "+tglout+" "
ext3="and extract (minute from a.date_rec) in ("+menit+") order by a.date_rec"
#tag disesuaikan dengan taglis, silahkan tambahkan sesuaikan dengan keinginan 
#loop Query

for lop3 in listCF:
    var3=query3 + "'" + lop3 + "'" + func3 + dat3 + ext3
    sql3.execute(var3)
    lstCF.extend(sql3.fetchall())
    
cok = ['Coal Feeder 1A', 'Coal Feeder 1B', 'Coal Feeder 1C', 'Coal Feeder 1D', 'Coal Feeder 1E', 'Coal Feeder 1F']
df3 = pd.DataFrame.from_records(lstCF, columns=[x[0] for x in sql3.description])

data = data.fillna(0)
MWA = data['Unit #1 - #1 Gen. Active Power'].sum()
MWB = data['Unit #2 - #2 Gen. Active Power'].sum()
tot_SUA = data['Ship Unloader A'].sum()
tot_SUB = data['Ship Unloader B'].sum()
tot_C1A = data['Conveyor C01A'].sum()
tot_C1B = data['Conveyor C01B'].sum()
tot_C2L = data['Conveyor C2 Loading'].sum()
tot_C2U = data['Conveyor C2 Unloading'].sum()
tot_C3 = data['Conveyor 3'].sum()
tot_C4A = data['Conveyor C4A'].sum()
tot_C5A = data['Conveyor C5A'].sum()
tot_C6A = data['Conveyor C6A'].sum()

# Membuat DataFrame dari Array NumPy
daw = np.array([[tot_SUA, tot_SUB, tot_C1A, tot_C1B, tot_C2U, tot_C2L, tot_C3, tot_C4A, tot_C5A, tot_C6A],])

dw = pd.DataFrame(daw, columns=['Ship Unloader A', 'Ship Unloader B', 'Conveyor C01A', 'Conveyor C01B', 'Conveyor C2 Unloading', 'Conveyor C2 Loading', 'Conveyor 3', 'Conveyor C4A', 'Conveyor C5A', 'Conveyor C6A'])#.astype(str)
dataaa=df2.pivot(index='tanggal_penarikan',columns='description',values='nilai_operasi').astype(float)
dataCF=df3.pivot(index='tanggal_penarikan',columns='description',values='nilai_operasi').astype(float)

st.title("""
🏂 Skripsi UNIBA
         
Anshori
197030717

""")
         
col1, col2, col3 = st.columns([1, 1, 1])

col1.text("MW Unit 1")
col1.markdown(MWA)

col2.text("MW Unit 2")
col2.markdown(MWB)

col3.text("Ship Unloader A")
if float(dw['Ship Unloader A']) == 1:
    col3.markdown(
        f'<div style="background-color:{"#DB343A"}; padding:5px; border-radius:3px;">' 'SU A RUN' '</div>', unsafe_allow_html=True)
else:
    col3.markdown(
        f'<div style="background-color:{"#58EA0F"}; padding:5px; border-radius:3px;">' 'SU A STOP' '</div>', unsafe_allow_html=True)

col1.text("Ship Unloader B")
if float(dw['Ship Unloader B']) == 1:
    col1.markdown(
        f'<div style="background-color:{"#DB343A"}; padding:8px; border-radius:3px;">' 'SU B RUN' '</div>', unsafe_allow_html=True)
else:
    col1.markdown(
        f'<div style="background-color:{"#58EA0F"}; padding:8px; border-radius:3px;">' 'SU B STOP' '</div>', unsafe_allow_html=True)

col2.text("Conveyor C01A")
if float(dw['Conveyor C01A']) == 1:
    col2.markdown(
        f'<div style="background-color:{"#DB343A"}; padding:8px; border-radius:3px;">' 'C01A RUN' '</div>', unsafe_allow_html=True)
else:
    col2.markdown(
        f'<div style="background-color:{"#58EA0F"}; padding:8px; border-radius:3px;">' 'C01A STOP' '</div>', unsafe_allow_html=True)

col3.text("Conveyor C01B")
if float(dw['Conveyor C01B']) == 1:
    col3.markdown(
        f'<div style="background-color:{"#DB343A"}; padding:8px; border-radius:3px;">' 'C01B RUN' '</div>', unsafe_allow_html=True)
else:
    col3.markdown(
        f'<div style="background-color:{"#58EA0F"}; padding:8px; border-radius:3px;">' 'C01B STOP' '</div>', unsafe_allow_html=True)

col1.text("Conveyor C2 Unloading")
if float(dw['Conveyor C2 Unloading']) == 1:
    col1.markdown(
        f'<div style="background-color:{"#DB343A"}; padding:8px; border-radius:3px;">' 'C2 UNLOADING RUN' '</div>', unsafe_allow_html=True)
else:
    col1.markdown(
        f'<div style="background-color:{"#58EA0F"}; padding:8px; border-radius:3px;">' 'C2 UNLOADING STOP' '</div>', unsafe_allow_html=True)

col2.text("Conveyor C2 Loading")
if float(dw['Conveyor C2 Loading']) == 1:
    col2.markdown(
        f'<div style="background-color:{"#DB343A"}; padding:8px; border-radius:3px;">' 'C2 LOADING RUN' '</div>', unsafe_allow_html=True)
else:
    col2.markdown(
        f'<div style="background-color:{"#58EA0F"}; padding:8px; border-radius:3px;">' 'C2 LOADING STOP' '</div>', unsafe_allow_html=True)

col3.text("Conveyor C3")
if float(dw['Conveyor 3']) == 1:
    col3.markdown(
        f'<div style="background-color:{"#DB343A"}; padding:8px; border-radius:3px;">' 'C3 RUN' '</div>', unsafe_allow_html=True)
else:
    col3.markdown(
        f'<div style="background-color:{"#58EA0F"}; padding:8px; border-radius:3px;">' 'C3 STOP' '</div>', unsafe_allow_html=True)

col1.text("Conveyor C4")
if float(dw['Conveyor C4A']) == 1:
    col1.markdown(
        f'<div style="background-color:{"#DB343A"}; padding:8px; border-radius:3px;">' 'C4 RUN' '</div>', unsafe_allow_html=True)
else:
    col1.markdown(
        f'<div style="background-color:{"#58EA0F"}; padding:8px; border-radius:3px;">' 'C4 STOP' '</div>', unsafe_allow_html=True)

col2.text("Conveyor C5")
if float(dw['Conveyor C5A']) == 1:
    col2.markdown(
        f'<div style="background-color:{"#DB343A"}; padding:8px; border-radius:3px;">' 'C5 RUN' '</div>', unsafe_allow_html=True)
else:
    col2.markdown(
        f'<div style="background-color:{"#58EA0F"}; padding:8px; border-radius:3px;">' 'C5 STOP' '</div>', unsafe_allow_html=True)

col3.text("Conveyor C6")
if float(dw['Conveyor C6A']) == 1:
    col3.markdown(
        f'<div style="background-color:{"#DB343A"}; padding:8px; border-radius:3px;">' 'C6 RUN' '</div>', unsafe_allow_html=True)
else:
    col3.markdown(
        f'<div style="background-color:{"#58EA0F"}; padding:8px; border-radius:3px;">' 'C6 STOP' '</div>', unsafe_allow_html=True)

st.text(" ")
st.markdown("<br", unsafe_allow_html=True)
st.write(dataaa)
st.write(dataCF)

col1, col2, col3 = st.columns([1, 1, 1])

col1.area_chart(dataaa['Bunker 1A'], width = 100, height = 200)
col2.area_chart(dataaa['Bunker 1B'], width = 100, height = 200)
col3.area_chart(dataaa['Bunker 1C'], width = 100, height = 200)
col1.area_chart(dataaa['Bunker 1D'], width = 100, height = 200)
col2.area_chart(dataaa['Bunker 1E'], width = 100, height = 200)
col3.area_chart(dataaa['Bunker 1F'], width = 100, height = 200)
col1.area_chart(dataaa['Bunker 2A'], width = 100, height = 200)
col2.area_chart(dataaa['Bunker 2B'], width = 100, height = 200)
col3.area_chart(dataaa['Bunker 2C'], width = 100, height = 200)
col1.area_chart(dataaa['Bunker 2D'], width = 100, height = 200)
col2.area_chart(dataaa['Bunker 2E'], width = 100, height = 200)
col3.area_chart(dataaa['Bunker 2F'], width = 100, height = 200)
col1.area_chart(dataCF['Unit #1 - A coal feeder flow feedback'], width = 100, height = 200)
col2.area_chart(dataCF['Unit #1 - B coal feeder flow feedback'], width = 100, height = 200)
col3.area_chart(dataCF['Unit #1 - C coal feeder flow feedback'], width = 100, height = 200)
col1.area_chart(dataCF['Unit #1 - D coal feeder flow feedback'], width = 100, height = 200)
col2.area_chart(dataCF['Unit #1 - E coal feeder flow feedback'], width = 100, height = 200)
col3.area_chart(dataCF['Unit #1 - F coal feeder flow feedback'], width = 100, height = 200)
col1.area_chart(dataCF['Unit #2 - A coal feeder flow feedback'], width = 100, height = 200)
col2.area_chart(dataCF['Unit #2 - B coal feeder flow feedback'], width = 100, height = 200)
col3.area_chart(dataCF['Unit #2 - C coal feeder flow feedback'], width = 100, height = 200)
col1.area_chart(dataCF['Unit #2 - D coal feeder flow feedback'], width = 100, height = 200)
col2.area_chart(dataCF['Unit #2 - E coal feeder flow feedback'], width = 100, height = 200)
col3.area_chart(dataCF['Unit #2 - F coal feeder flow feedback'], width = 100, height = 200)