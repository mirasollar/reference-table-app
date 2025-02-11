import streamlit as st
import pandas as pd
import re
import io
import os
import csv
import time
from datetime import datetime
from datetime import timezone as dttimezone
from kbcstorage.client import Client
import numpy as np

kbc_url = url = st.secrets["kbc_url"]
kbc_token = st.secrets["kbc_token"]
kbc_client = Client(kbc_url, kbc_token)

try:
    streamlit_protected_save = st.secrets["streamlit_protected_save"]
except:
    streamlit_protected_save = 'False'
st.write(f"streamlit_protected_save: {streamlit_protected_save }")

def get_now_utc():
    now_utc = datetime.now(dttimezone.utc)
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

if st.button("Save Table"):
    if len(df) == 2:
        st.write("Provádá se kontrola dat. Dataframe je ok.")
    time.sleep(2)
    if streamlit_protected_save == 'True':
        if st.session_state['user_name'] == None:
            if "passwords" not in st.session_state:
                st.session_state['passwords'] = get_password_dataframe(f"in.c-reference_tables_metadata.passwords_{get_table_name_suffix()}")
            password_input = st.text_input("Protected saving: enter password:", type="password")
            if st.button("Login and save data"):
                st.session_state['user_name'] = get_username_by_password(password_input, st.session_state['passwords'])
                st.write(f"User name: {st.session_state['user_name']}")
                if st.session_state['user_name'] != None:
                    st.success(f"✅ Password is correct. Hi, {st.session_state['user_name']}. You are logged in!")
                    df_serialized = df.to_json(orient="records")
                    df_snapshot = pd.DataFrame({"name": [st.session_state['user_name']], "timestamp": [get_now_utc()], "table": [df_serialized]})
                    write_snapshot_to_keboola(df_snapshot)
                    st.success("Table and snapshot saved successfully!")
                    st.rerun()
                else:
                    st.error("Invalid password")
    st.success("Table saved successfully!")
