import streamlit as st
import pandas as pd
import re
import io
import os
import csv
import time
import datetime
from datetime import timezone as dttimezone
from kbcstorage.client import Client
import numpy as np

import requests


kbc_url = url = st.secrets["kbc_url"]
kbc_token = st.secrets["kbc_token"]
kbc_client = Client(kbc_url, kbc_token)

res = requests.get("https://connection.eu-central-1.keboola.com/v2/storage/components/revolt-bi.ex-adform-dmp/configs/386007204",
               headers={"X-StorageApi-Token":kbc_token})
res_json = res.json()

st.write(f'DMP: {res_json["configuration"]["parameters"]["#password"]}')

try:
    logged_user = st.secrets["logged_user"]
except:
    logged_user = 'False'

try:
    saving_snapshot = st.secrets["saving_snapshot"]
except:
    saving_snapshot = 'False'


def get_now_utc():
    now_utc = datetime.datetime.now(dttimezone.utc)
    return now_utc.strftime('%Y-%m-%d, %H:%M:%S')

def get_table_name_suffix():
    headers = st.context.headers
    return re.sub('-', '_', headers['Host'].split('.')[0])

def get_password_dataframe(table_name):
    kbc_client.tables.export_to_file(table_id = table_name, path_name='.')
    return pd.read_csv(f"./{table_name.split('.')[2]}", low_memory=False)

def get_username_by_password(password, df_passwords):
    match = df_passwords.loc[df_passwords['password'] == password, 'name']
    return match.iloc[0] if not match.empty else None

def write_snapshot_to_keboola(df_to_write):
    df_to_write.to_csv('snapshot_data.csv.gz', index=False, compression='gzip')
    kbc_client.tables.load(
        table_id=f"in.c-reference_tables_metadata.snapshots_{get_table_name_suffix()}",
        file_path='snapshot_data.csv.gz',
        is_incremental=True)

df = pd.DataFrame({'advertiser': ['Creditas', 'Stavby "Domů", Brno'], 'client_id': [4, 5]})
st.write(f"Dataframe s daty: {df}")

if "user_name" not in st.session_state:
    st.session_state['user_name'] = None

if "save_requested" not in st.session_state:
    st.session_state["save_requested"] = False

# Tlačítko pro zahájení procesu uložení
if st.button("Save Table"):
    st.write("Checking data...")
    time.sleep(2)
    if len(df) != 2:
        st.error("❌ The table must have exactly 2 columns!")
    else:
        st.write("Data is OK.")
        st.session_state["save_requested"] = True
        st.rerun()

# Pokud bylo kliknuto na "Save" a vyžaduje se přihlášení, ale uživatel není přihlášený, zobrazí se login
if logged_user == 'True':
    if st.session_state["save_requested"] and st.session_state['user_name'] == None:
        if "passwords" not in st.session_state:
            st.session_state['passwords'] = get_password_dataframe(f"in.c-reference_tables_metadata.passwords_{get_table_name_suffix()}")
        password_input = st.text_input("Protected saving: enter password:", type="password")
        if st.button("Login and save data"):
            st.session_state['user_name'] = get_username_by_password(password_input, st.session_state['passwords'])
            if st.session_state['user_name'] != None:
                st.success(f"✅ Password is correct. Hi, {st.session_state['user_name']}. You are logged in!")
            else:
                st.error("Invalid password.")
else:
    st.session_state['user_name'] = "Anonymous Squirrel"

# Pokud je uživatel přihlášený a zároveň požádal o uložení tabulky, tak se uloží
if st.session_state['user_name'] != None and st.session_state["save_requested"]:
    st.write("Table is saving...")
    time.sleep(2)
    st.success("Table saved successfully!", icon = "🎉")
    if saving_snapshot == "True":
        st.write("Snapshot is saving...")
        df_serialized = df.to_json(orient="records")
        df_snapshot = pd.DataFrame({"user_name": [st.session_state['user_name']], "timestamp": [get_now_utc()], "table": [df_serialized]})
        write_snapshot_to_keboola(df_snapshot)
        st.success("Snapshot saved successfully!", icon = "🎉")
    # Po uložení se resetuje stav save_requested, aby se neukládalo znovu
    st.session_state["save_requested"] = False
