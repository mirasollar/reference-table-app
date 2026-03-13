import streamlit as st
import streamlit.components.v1 as components
from streamlit_card import card
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
    saving_snapshot = st.secrets["saving_snapshot"]
except:
    saving_snapshot = 'False'

try:
    allowed_users = st.secrets["allowed_users"]
except:
    allowed_users = 'False'

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

    if "edited_data" not in st.session_state:
        st.session_state["edited_data"] = None 

    if 'upload-tables' not in st.session_state:
        st.session_state["upload-tables"] = False
    
    if "show_downloads" not in st.session_state:
        st.session_state["show_downloads"] = False

    if "user_name" not in st.session_state:
        st.session_state['user_name'] = None
    
    if "save_requested" not in st.session_state:
        st.session_state["save_requested"] = False

    if 'settings_df' not in st.session_state:
        st.session_state['settings_df'] = None

def update_session_state(table_id):
    with st.spinner('Loading ...'):
        st.session_state['selected-table'] = table_id
        st.session_state['data'] = get_dataframe(st.session_state['selected-table'])
    st.rerun()
     
def display_table_card(row):
    card(
        title=row["displayName"],
        text=[f"Table ID: {row['table_id']}"],
        styles={
            "card": {
                "width": "100%",
                "height": "80px",
                "box-shadow": "2px 2px 12px rgba(0,0,0,0.1)",
                "margin": "0px",
                "flex-direction": "column",  # Stack children vertically
                "align-items": "flex-start",
            },
            "filter": {
                "background-color": "#FFFFFF"
            },
        "div": {
            "padding":"0px",
            "display": "flex",
            "align-items": "flex-start", 
        },
         "text": {
                "color": "#999A9F",
                "padding-left":"5%",
                "align-self": "flex-start",
                "font-size": "15px",
                "font-weight": "lighter",
            },
         "title": {
                "font-size": "24px",
                "color": "#1F8FFF",
                "padding-left":"5%",
                "align-self": "flex-start",}
        
        },
        image="https://upload.wikimedia.org/wikipedia/en/4/48/Blank.JPG" ,
        key=row['table_id'],
        on_click=lambda table_id=row['table_id']: update_session_state(table_id)
    )

def ChangeButtonColour(widget_label, font_color, background_color, border_color):
    htmlstr = f"""
        <script>
            var elements = window.parent.document.querySelectorAll('button');
            for (var i = 0; i < elements.length; ++i) {{ 
                if (elements[i].innerText == '{widget_label}') {{ 
                    elements[i].style.color ='{font_color}';
                    elements[i].style.background = '{background_color}';
                    elements[i].style.borderColor = '{border_color}';
                }}
            }}
        </script>
        """
    components.html(f"{htmlstr}", height=0, width=0)

# Fetch and prepare table IDs and short description
@st.cache_data(ttl=60)

def fetch_all_ids():
    df = pd.DataFrame()
    bucket_ids = [bucket["id"] for bucket in client.buckets.list()]
    all_tables_in_buckets = [client.buckets.list_tables(bucket_id) for bucket_id in bucket_ids]
    for tables in all_tables_in_buckets:
        ids_list = [{
            'table_id': table["id"],
            'displayName': table["displayName"],
            'lastImportDate': table['lastImportDate'],
            'created': table['created']
        } for table in tables]
        df_stage = pd.DataFrame(ids_list)
        df = pd.concat([df, df_stage])
    return df

def string_to_list_lowercase(string):
    if not string:
        return []
    return [item.strip() for item in string.lower().split(',')]

# Definujte callback funkci pro tlačítko
def on_click_uploads():
    st.session_state["upload-tables"] = True

# Definujte callback funkci pro tlačítko
def on_click_back():
    st.session_state["upload-tables"] = False

# Function to display a table section
# table_name, table_id ,updated,created
def display_table_section(row):
    with st.container():
        display_table_card(row)

def display_footer_section():
    left_aligned, space_col, right_aligned = st.columns((2,7,1))
    with left_aligned:
        st.caption(f"© Keboola & Seznam.cz {datetime.datetime.now().year}")
    # with right_aligned:
    #    st.caption("Version 2.0")

def write_to_keboola(data, table_name, table_path, purpose):
    """
    Writes the provided data to the specified table in Keboola Connection, updating existing records as needed.

    Args:
        data (pandas.DataFrame): The data to write to the table.
        table_name (str): The name of the table to write the data to.
        table_path (str): The local file path to write the data to before uploading.
    """

    # Write the DataFrame to a CSV file with compression
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

def resetSetting():
    st.session_state['selected-table'] = None
    st.session_state['data'] = None
    st.session_state["show_downloads"] = False

def toggle_downloads():
    st.session_state["show_downloads"] = not st.session_state["show_downloads"]

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
        
def get_table_metadata(tkn, kbc_bucket_id, kbc_table_id):
    c = Client('https://connection.eu-central-1.keboola.com', tkn)
    table_columns = c.tables.detail(kbc_table_id)["columns"]
    primary_key = c.tables.detail(kbc_table_id)["primaryKey"]
    return primary_key, table_columns
        
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
    for key, value in col_types_dict.items():
        if value == 'int64':
            column_config[key] = st.column_config.NumberColumn(format="%d")
    return column_config

def check_col_types(df_to_check, col_setting):
    col_types_dict = df_to_check.dtypes.astype(str).to_dict()
    for key, value in col_types_dict.items():
        if value == 'object':
            col_types_dict.update({key: 'string'})
        elif re.search("(int|float).*", value):
            col_types_dict.update({key: 'number'})
        elif value == 'bool':
            col_types_dict.update({key: 'logical'})
        else:
            pass
    dict_filter = lambda x, y: dict([ (i,x[i]) for i in x if i in set(y) ])
    col_setting = {key: value for key, value in col_setting.items() if not re.search("%", value)}
    wanted_keys = tuple(col_setting.keys())
    col_types_dict = dict_filter(col_types_dict, wanted_keys)
    wrong_columns = [key for key in col_types_dict if col_types_dict[key] != col_setting.get(key)]
    return wrong_columns

def modifying_nas(df_for_editing):
    mod_df = df_for_editing.replace(r'^(\s*|None|none|NONE|NaN|nan|Null|null|NULL|n\/a|N\/A|<NA>)$', np.nan, regex=True)
    return mod_df

def delete_decimal_zero(df_for_editing):
    for key, value in df_for_editing.dtypes.astype(str).to_dict().items():
        if re.search("(int|float).*", value):
            df_for_editing[key] = df_for_editing[key].astype(str)
            df_for_editing[key] = df_for_editing[key].replace(r'\.0$', '', regex=True)
    return df_for_editing

def check_date_format(df_to_check, date_setting_dict):
    col_names = df_to_check.columns.values.tolist()
    col_names_to_check = list(set(col_names).intersection(list(date_setting_dict.keys())))
    wrong_cols = []
    for key, value in date_setting_dict.items():
        for col_name in col_names_to_check:
            if key == col_name:
                try:
                    df_to_check[col_name] = pd.to_datetime(df_to_check[col_name], format=value.split(",")[0])
                    df_to_check[col_name] = df_to_check[col_name].dt.strftime(value.split(",")[0])
                except:
                    wrong_cols.append(key)
    return wrong_cols, df_to_check

def delete_null_rows(df_for_editing):
    col_names = df_for_editing.columns.values.tolist()
    # df_for_editing = df_for_editing.replace(r'^(\s*|None|none|NONE|NaN|nan|null|n\/a|N\/A)$', np.nan, regex=True)
    df_for_editing.reset_index(drop=True, inplace=True)
    bool_columns = []
    for col in col_names:
        if df_for_editing[col].dropna().isin([True, False]).all():
            bool_columns.append(col)
    df_without_bool = df_for_editing.drop(columns=bool_columns)
    col_names_without_bool = df_without_bool.columns.values.tolist()
    all_col_null_check = df_without_bool[col_names_without_bool].isnull().apply(lambda x: all(x), axis=1)
    all_col_null_check_lst = all_col_null_check.tolist()
    for i in range(len(all_col_null_check_lst)):
        item = all_col_null_check_lst[i]
        if item == True:
            df_for_editing = df_for_editing.drop([i, i])
    return df_for_editing

def check_null_cells(df_to_check, col_setting):
    # df_to_check = df_to_check.replace(r'^(\s*|None|none|NaN|nan|null|n\/a|N\/A)$', np.nan, regex=True)
    df_to_check = df_to_check.astype(str)
    wrong_cols = []
    col_names = df_to_check.columns.values.tolist()
    col_names_to_check = list(set(col_names).intersection(list(col_setting.keys())))
    for i in col_names_to_check:
        if [x for x in df_to_check[i].tolist() if re.search("^(nan|None|<NA>)$", x)]:
            wrong_cols.append(i)
    return wrong_cols

def check_duplicates(df_to_check, columns, case_sensitive_columns, pk_setting = []):
    cs_setting = {column: case_sensitive_columns.get(column, '') for column in columns}
    df_to_check = df_to_check.astype(str)
    for key, value in cs_setting.items():
        if value == '':
            df_to_check[key] = df_to_check[key].apply(str.lower)
    if pk_setting:
        df_to_check = df_to_check[pk_setting]
    duplicity_value = len(df_to_check.duplicated().unique().tolist())
    return duplicity_value

def create_table_info(json_data, column_setting, case_sensitive_setting):
    table_id = json_data['id']
    display_name = json_data['displayName']
    primary_key = ', '.join(json_data['primaryKey'])
    last_import_date = json_data['lastImportDate']
    rows_count = json_data['rowsCount']
    created = json_data['created']
    description = ', '.join(f"*{key}*: {value}" for key, value in column_setting.items())
    case_sensitive_columns = ', '.join(f"{key}" for key in case_sensitive_setting.keys())
    data = {
        'table_id': [table_id],
        'displayName': [display_name],
        'primaryKey': [primary_key],
        'lastImportDate': [last_import_date],
        'rowsCount': [rows_count],
        'created': [created],
        'description': [description],
        'case_sensitive_columns': [case_sensitive_columns]
    }
    df = pd.DataFrame(data)
    return df

def prepare_downloaded_data():
    downloaded_data = cast_columns(st.session_state['data'])
    downloaded_data = delete_null_rows(modifying_nas(downloaded_data))
    downloaded_data = delete_decimal_zero(downloaded_data)
    return downloaded_data

def generate_download_file(data, file_format):
    buffer = io.BytesIO() if file_format == "xlsx" else io.StringIO()
    if file_format == "csv":
        data.to_csv(buffer, index=False)
        mime = 'text/csv'
        ext = "csv"
    elif file_format == "tsv":
        data.to_csv(buffer, sep='\t', index=False)
        mime = 'text/tab-separated-values'
        ext = "txt"
    elif file_format == "xlsx":
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            data.to_excel(writer, index=False)
        mime = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        ext = "xlsx"
    else:
        return None, None, None
    return buffer.getvalue(), mime, ext
    
# Protected saving & snapshoting
def get_now_utc():
    now_utc = datetime.datetime.now(dttimezone.utc)
    return now_utc.strftime('%Y-%m-%d, %H:%M:%S')

def get_table_name_suffix():
    headers = st.context.headers
    return re.sub('-', '_', headers['Host'].split('.')[0])

def save_settings_df(tkn, settings_table_id):
    client = Client('https://connection.eu-central-1.keboola.com', tkn)
    client.tables.export_to_file(table_id=settings_table_id, path_name='.')
    settings_table_name = settings_table_id.split(".")[2]
    df = pd.read_csv(f'./{settings_table_name}')
    st.session_state['settings_df'] = df

def read_settings_df(settings_df, selected_table_id):
    settings_df = settings_df.fillna('')
    column_settings_str = settings_df[settings_df["table_id"] == selected_table_id]['setting'].iloc[0]
    case_sensitive_str = settings_df[settings_df["table_id"] == selected_table_id]['case_sensitive'].iloc[0]
    if column_settings_str:
        row_column_setting = re.sub(r"'", '"', column_settings_str)
        column_setting = json.loads('{' + row_column_setting + '}')
    else:
        column_setting = {}
    if case_sensitive_str:
        case_sensitive_setting = case_sensitive_str
        keys = case_sensitive_str.split(', ')
        case_sensitive_setting = {key: "case sensitive" for key in keys}
    else:
        case_sensitive_setting = {}
    return column_setting, case_sensitive_setting
        
# Display tables
init()
st.session_state["tables_id"] = fetch_all_ids()


col1,col2,col4= st.columns((2,7,2))
st.title("Data Editor")
# st.write(f"Data app: {get_table_name_suffix()}")

if get_table_name_suffix() == 'rtb_reference_tables_741376305':
    st.info('This app is closed. Use [RTB app](https://reference-tables-rtb-973891772.hub.eu-central-1.keboola.com/) instead.', icon="ℹ️")
else:
    st.info('This app is closed.', icon="ℹ️")
