import streamlit as st
import streamlit.components.v1 as components
from kbcstorage.client import Client
import os
import csv
import pandas as pd
import datetime
from datetime import timezone as dttimezone
import time
from pathlib import Path
import re
import json
import numpy as np
import io
from charset_normalizer import from_bytes

# Setting page config
st.set_page_config(page_title="Keboola Data Editor", page_icon=":robot:", layout="wide")

# Constants
token = st.secrets["kbc_storage_token"]
kbc_url = url = st.secrets["kbc_url"]
kbc_token = st.secrets["kbc_token"]
LOGO_IMAGE_PATH = os.path.abspath("./app/static/keboola.png")

# Initialize Client
client = Client(kbc_url, token)
kbc_client = Client(kbc_url, kbc_token)

try:
    logged_user = st.secrets["logged_user"]
except:
    logged_user = 'False'

try:
    saving_snapshot = st.secrets["saving_snapshot"]
except:
    saving_snapshot = 'False'

if 'data_load_time_table' not in st.session_state:
        st.session_state['data_load_time_table'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

if 'data_load_time_overview' not in st.session_state:
        st.session_state['data_load_time_overview'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')


# Fetching data 
@st.cache_data(ttl=60,show_spinner=False)
def get_dataframe(table_name):
    table_detail = client.tables.detail(table_name)

    client.tables.export_to_file(table_id = table_name, path_name='')
    list = client.tables.list()
    
    with open('./' + table_detail['name'], mode='rt', encoding='utf-8') as in_file:
        lazy_lines = (line.replace('\0', '') for line in in_file)
        reader = csv.reader(lazy_lines, lineterminator='\n')
    if os.path.exists('data.csv'):
        os.remove('data.csv')
    else:
        print("The file does not exist")
    
    os.rename(table_detail['name'], 'data.csv')
    df = pd.read_csv('data.csv')
    df = cast_columns(df)
    return df

# Initialization
def init():
    if 'selected-table' not in st.session_state:
        st.session_state['selected-table'] = None
        
    if "uploaded_table_id" not in st.session_state:
        st.session_state["uploaded_table_id"] = None

    if 'tables_id' not in st.session_state:
        st.session_state['tables_id'] = pd.DataFrame(columns=['table_id'])
    
    if 'data' not in st.session_state:
        st.session_state['data'] = None 

    if 'upload-tables' not in st.session_state:
        st.session_state["upload-tables"] = False

    if "user_name" not in st.session_state:
        st.session_state['user_name'] = None
    
    if "save_requested" not in st.session_state:
        st.session_state["save_requested"] = False

def update_session_state(table_id):
    with st.spinner('Loading ...'):
        st.session_state['selected-table'] = table_id
        st.session_state['data'] = get_dataframe(st.session_state['selected-table'])
        st.session_state['data_load_time_table'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    st.rerun()
    
# Fetch and prepare table IDs and short description
@st.cache_data(ttl=60)

def write_to_keboola(data, table_name, table_path, purpose):
    data.to_csv(table_path, index=False, compression='gzip')

    # Load the CSV file into Keboola, updating existing records
    if purpose == "reference_table":
        client.tables.load(
            table_id=table_name,
            file_path=table_path,
            is_incremental=False
        )
    elif purpose == "snapshot":
        kbc_client.tables.load(
            table_id=table_name,
            file_path=table_path,
            is_incremental=True
        )

def cast_columns(df):
    """Ensure that columns that should be boolean are explicitly cast to boolean."""
    for col in df.columns:
        # If a column in the DataFrame has only True/False values, cast it to bool, NaN cast to string
        if df[col].dropna().isin([True, False]).all() and not df[col].dropna().isin([np.nan]).all():
            df[col] = df[col].astype(bool)
            # df[col] = pd.Series(df[col], dtype="string")
        elif df[col].dropna().isin([np.nan]).all():
            df[col] = pd.Series(df[col], dtype="string")
    return df
        
def get_setting(tkn, kbc_bucket_id, kbc_table_id):
    c = Client('https://connection.eu-central-1.keboola.com', tkn)
    description = c.tables.detail(kbc_table_id)["metadata"][0]["value"]
    table_columns = c.tables.detail(kbc_table_id)["columns"]
    col_metadata = c.tables.detail(kbc_table_id)["columnMetadata"]
    primary_key = c.tables.detail(kbc_table_id)["primaryKey"]
    if 'Upload setting' in description:
        description = description.replace('\n','')
        description = re.sub(r'.*Upload setting:?\s*```\{', '{', description)
        description = re.sub(r'```.*', '', description)
        description = re.sub(r"'", '"', description)
        col_setting = re.sub(r"\}.*", '}', description)
        col_setting = json.loads(col_setting)
    else:
        col_setting = {}
    case_sensitive = {}
    for col in table_columns:
        case_sensitive[col] = ''
        for k, v in col_metadata.items():
            if col == k and v[0]["value"] == 'case sensitive':
                case_sensitive[col] = v[0]["value"]
    return col_setting, primary_key, table_columns, case_sensitive
        
def check_columns_diff(current_columns, file_columns):
    missing_columns = [x for x in current_columns if x not in set(file_columns)]
    extra_columns = [x for x in file_columns if x not in set(current_columns)]
    return missing_columns, extra_columns

def split_dict(setting_dict, n):
    d = setting_dict.copy()
    modified_dict = {}
    for key, value in d.items():
        value = re.sub(r'\s*,\s*', ',', value)
        value_lst = value.split(",")
        d[key] = value_lst[-n]
        modified_dict = {k:v for k,v in d.items() if v != 'ignore'}
    return modified_dict

def split_table_id(selected_table_id):
    table_id_split = selected_table_id.split('.')
    bucket_name = table_id_split[0] + '.' + table_id_split[1]
    table_name = table_id_split[2]
    return bucket_name, table_name

def split_datetime(dt):
    return f"Date: {dt.split('T')[0]}, Time: {dt.split('T')[1]}"
        
def date_setting(column_setting_dict):
    date_setting = {k: v for k, v in column_setting_dict.items() if re.search("%", v)}
    return date_setting

def check_null_rows(df_to_check):
    col_names = df_to_check.columns.values.tolist()
    all_col_null_check = df_to_check[col_names].isnull().apply(lambda x: all(x), axis=1)
    return any(all_col_null_check.tolist())

def create_column_config(df_to_edit):
    column_config = {}
    col_types_dict = df_to_edit.dtypes.astype(str).to_dict()
    for k, v in col_types_dict.items():
        if v == 'int64':
            column_config[k] = st.column_config.NumberColumn(format="%d")
    return column_config

def check_col_types(df_to_check, col_setting):
    col_types_dict = df_to_check.dtypes.astype(str).to_dict()
    for x, y in col_types_dict.items():
        if y == 'object':
            col_types_dict.update({x: 'string'})
        elif re.search("(int|float).*", y):
            col_types_dict.update({x: 'number'})
        elif y == 'bool':
            col_types_dict.update({x: 'logical'})
        else:
            pass
    dict_filter = lambda x, y: dict([ (i,x[i]) for i in x if i in set(y) ])
    col_setting = {k: v for k, v in col_setting.items() if not re.search("%", v)}
    wanted_keys = tuple(col_setting.keys())
    col_types_dict = dict_filter(col_types_dict, wanted_keys)
    # st.write(f"Detected column formatting: {col_types_dict}")
    wrong_columns = [k for k in col_types_dict if col_types_dict[k] != col_setting.get(k)]
    return wrong_columns

def modifying_nas(df_for_editing):
    # df_for_editing = df_for_editing.astype(str)
    mod_df = df_for_editing.replace(r'^(\s*|None|none|NONE|NaN|nan|Null|null|NULL|n\/a|N\/A|<NA>)$', np.nan, regex=True)
    return mod_df


def check_duplicates(df_to_check, cs_setting, pk_setting = []):
    df_to_check = df_to_check.astype(str)
    for k, v in cs_setting.items():
        if v == '':
            df_to_check[k] = df_to_check[k].apply(str.lower)
    if pk_setting:
        df_to_check = df_to_check[pk_setting]
    duplicity_value = len(df_to_check.duplicated().unique().tolist())
    return duplicity_value

    
# Protected saving & snapshoting
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
        
# Display tables
init()

selected_bucket = "in.c-mso_dev_reference_tables"
uploaded_file = st.file_uploader("Upload a file", type=['csv', 'xlsx'])
table_name = "mso_dev_client"

# Upload button
if st.button('Upload'):
    with st.spinner('Uploading table and checking data...'):
        table_id = selected_bucket + '.' + table_name
        st.session_state["uploaded_table_id"] = table_id
        column_setting = get_setting(token, selected_bucket, table_id)[0]
        format_setting = split_dict(column_setting, 2)
        null_cells_setting = split_dict(column_setting, 1)
        case_sensitive_setting = get_setting(token, selected_bucket, table_id)[3]
        primary_key_setting = get_setting(token, selected_bucket, table_id)[1]
        date_setting = date_setting(column_setting)
        if Path(uploaded_file.name).suffix == '.csv':
            file_content = uploaded_file.read()
            try:
                df = pd.read_csv(io.BytesIO(file_content), sep=None, engine='python', encoding='utf-8-sig')
            except:
                result = from_bytes(file_content).best()
                detected_encoding = result.encoding
                df = pd.read_csv(io.BytesIO(file_content), sep=None, engine='python', encoding=detected_encoding)
        else:
            df=pd.read_excel(uploaded_file)
        st.write(f"Dataframe před kontrolama: {df}")
        if date_setting:
            checking_date = check_date_format(modifying_nas(df), date_setting)
    
        missing_columns = check_columns_diff(get_setting(token, selected_bucket, table_id)[2], df.columns.values.tolist())[0]
        extra_columns = check_columns_diff(get_setting(token, selected_bucket, table_id)[2], df.columns.values.tolist())[1]

        if missing_columns:
            st.error(f"Some columns are missing in the file. Affected columns: {', '.join(missing_columns)}. The column names are case-sensitive. Please edit it before proceeding.")
        elif extra_columns:
            st.error(f"There are extra columns. Adding new columns is not allowed. Affected columns: {', '.join(extra_columns)}. The column names are case-sensitive. If you want to add new columns, please contact the analytics team.")
        elif check_null_rows(modifying_nas(df)):
            st.error("The file contains null rows. Please remove them before proceeding.")
        elif check_col_types(df, format_setting):
            st.error(f"The file contains data in the wrong format. Affected columns: {', '.join(check_col_types(df, format_setting))}. Please edit it before proceeding.")
        elif date_setting and checking_date[0]:
            st.error(f"The file contains date in the wrong format. Affected columns: {', '.join(checking_date[0])}. Please edit it before proceeding.")         
        elif primary_key_setting and check_duplicates(df, case_sensitive_setting, primary_key_setting) == 2:
            st.error(f"The table contains columns with duplicate values. Affected columns: {', '.join(primary_key_setting)}. Please edit it before proceeding.")
            st.write(f"Dataframe po kontrole PK: {df}")
            st.stop()
        elif check_duplicates(df, case_sensitive_setting) == 2:
            st.error("The table contains duplicate rows. Please remove them before proceeding.")
        else:
            if date_setting:
                st.session_state['data'] = checking_date[1]
            else:
                st.session_state['data'] = modifying_nas(df)
        st.success("File uploaded and data checked successfully!", icon = "🎉")
        st.session_state["save_requested"] = True
        st.rerun()


# Pokud bylo kliknuto na "Save" a vyžaduje se přihlášení, ale uživatel není přihlášený, zobrazí se login
if logged_user == 'True':
    if st.session_state["save_requested"] and st.session_state['user_name'] == None:
        password_input = st.text_input("Enter password:", type="password")
        if "passwords" not in st.session_state:
            st.session_state['passwords'] = get_password_dataframe(f"in.c-reference_tables_metadata.passwords_{get_table_name_suffix()}")
        if st.button("Login and save data"):
            st.session_state['user_name'] = get_username_by_password(password_input, st.session_state['passwords'])
            if st.session_state['user_name'] != None:
                st.success(f"✅ Password is correct. Hi, {st.session_state['user_name']}. You are logged in!")
            else:
                st.error("Invalid password.")      
else:
    st.session_state['user_name'] = "Anonymous Squirrel"

st.write(f"Save requests: {st.session_state['save_requested']}")
st.write(f"User name: {st.session_state['user_name']}")
# Pokud je uživatel přihlášený a zároveň požádal o uložení tabulky, tak se uloží
if st.session_state['user_name'] != None and st.session_state["save_requested"]:
    try:
        with st.spinner('Saving table...'):
            write_to_keboola(st.session_state['data'], st.session_state["uploaded_table_id"],'uploaded_data.csv.gz', "reference_table") 
        st.success('Table saved successfully!', icon = "🎉")
        if saving_snapshot == "True":
            with st.spinner('Saving snapshot...'):
                df_serialized = st.session_state['data'].to_json(orient="records")
                df_snapshot = pd.DataFrame({"user_name": [st.session_state['user_name']], "timestamp": [get_now_utc()], "table_id": [st.session_state["uploaded_table_id"]], "data": [df_serialized]})
                write_to_keboola(df_snapshot, f"in.c-reference_tables_metadata.snapshots_{get_table_name_suffix()}",'snapshot_data.csv.gz', "snapshot")
                st.success("Snapshot saved successfully!", icon = "🎉")
    except Exception as e:
        st.error(f"Error: {str(e)}")
    # Po uložení se resetuje stav save_requested, aby se neukládalo znovu
    st.session_state["save_requested"] = False
    st.session_state['upload-tables'] = False
    st.session_state['selected-table'] = st.session_state["uploaded_table_id"]
    st.session_state["uploaded_table_id"] = None
    st.cache_data.clear()
    time.sleep(3)
    st.rerun()
