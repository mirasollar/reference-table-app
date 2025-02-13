import streamlit as st
import streamlit.components.v1 as components
from streamlit_card import card
from kbcstorage.client import Client
import os
import csv
import pandas as pd
import datetime
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

    if 'tables_id' not in st.session_state:
        st.session_state['tables_id'] = pd.DataFrame(columns=['table_id'])
    
    if 'data' not in st.session_state:
        st.session_state['data'] = None 

    if 'upload-tables' not in st.session_state:
        st.session_state["upload-tables"] = False
    
    if 'log-exists' not in st.session_state:
        st.session_state["log-exists"] = False

    # if st.session_state["log-exists"] == False:
    #     try: 
    #         kbc_client.buckets.detail("in.c-keboolasheets")
    #         print("Bucket exists")
    #     except:
    #         kbc_client.buckets.create("in.c-keboolasheets", "keboolasheets")
    #         print("Bucket created")
    #     try:
    #         kbc_client.tables.detail("in.c-keboolasheets.log")
    #         print("Table exists")
    #         st.session_state["log-exists"] = True
    #     except:
    #         kbc_client.tables.create(name="log", bucket_id='in.c-keboolasheets', file_path=f'app/static/init_log.csv', primary_key=['table_id', 'log_time', 'user', 'new'])
    #         print("Table created")
    #         st.session_state["log-exists"] = True

def update_session_state(table_id):
    with st.spinner('Loading ...'):
        st.session_state['selected-table'] = table_id
        st.session_state['data'] = get_dataframe(st.session_state['selected-table'])
        st.session_state['data_load_time_table'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    st.rerun()
     

def display_table_card(row):
    card(
        title=row["displayName"],
        # title=row["displayName"].upper(),  
        # text=[f"Primary key: {row['primaryKey']}", f"Table ID: {row['table_id']}", f"Updated at: {row['lastImportDate']}", f"Created at: {row['created']}", f"Rows count: {str(row['rowsCount'])}"],
        text=[f"Table ID: {row['table_id']}", f"Created at: {split_datetime(row['created'])}", f"Updated at: {split_datetime(row['lastImportDate'])}", f"Rows count: {str(row['rowsCount'])}"],
        styles={
            "card": {
                "width": "100%",
                "height": "200px",
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


# def fetch_all_ids():
#    all_tables = client.tables.list()
#    ids_list = [{'table_id': table["id"], 'displayName': table["displayName"], 'primaryKey': ', '.join(table["primaryKey"]) if table["primaryKey"] else "",
#                  'lastImportDate': table['lastImportDate'], 'rowsCount': table['rowsCount'], 'created': table['created']} for table in all_tables]
#    return pd.DataFrame(ids_list)

def fetch_all_ids():
    df = pd.DataFrame()
    bucket_ids = [bucket["id"] for bucket in client.buckets.list()]
    all_tables_in_buckets = [client.buckets.list_tables(bucket_id) for bucket_id in bucket_ids]
    for tables in all_tables_in_buckets:
        ids_list = [{
            'table_id': table["id"],
            'displayName': table["displayName"],
            'primaryKey': ', '.join(table["primaryKey"]) if table["primaryKey"] else "",
            'lastImportDate': table['lastImportDate'],
            'rowsCount': table['rowsCount'],
            'created': table['created'],
            'description': next((item['value'] for item in table["metadata"] if item['key'] == 'KBC.description'), None),
            'column_metadata': client.tables.detail(table["id"])["columnMetadata"]
        } for table in tables]
        df_stage = pd.DataFrame(ids_list)
        df = pd.concat([df, df_stage])
    return df

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
        # st.subheader(f":blue[{table_name}]")
        # st.caption(table_id)
        # st.caption(f"Created: {created}")
        # st.caption(f"Updated: {updated}")
        # st.markdown("""---""")

        display_table_card(row)


def display_footer_section():
    left_aligned, space_col, right_aligned = st.columns((2,7,1))
    with left_aligned:
        st.caption(f"© Keboola & Seznam.cz {datetime.datetime.now().year}")
    # with right_aligned:
    #    st.caption("Version 2.0")

def write_to_keboola(data, table_name, table_path, incremental):
    """
    Writes the provided data to the specified table in Keboola Connection,
    updating existing records as needed.

    Args:
        data (pandas.DataFrame): The data to write to the table.
        table_name (str): The name of the table to write the data to.
        table_path (str): The local file path to write the data to before uploading.

    Returns:
        None
    """

    # Write the DataFrame to a CSV file with compression
    data.to_csv(table_path, index=False, compression='gzip')

    # Load the CSV file into Keboola, updating existing records
    client.tables.load(
        table_id=table_name,
        file_path=table_path,
        is_incremental=incremental
    )

def resetSetting():
    st.session_state['selected-table'] = None
    st.session_state['data'] = None 

def write_to_log(data):
    now = datetime.datetime.now()
    log_df = pd.DataFrame({
            'table_id': "in.c-keboolasheets.log",
            'new': [data],
            'log_time': now,
            'user': "PlaceHolderUserID"
        })
    log_df.to_csv(f'updated_data_log.csv.gz', index=False, compression='gzip')

    # Load the CSV file into Keboola, updating existing records
    kbc_client.tables.load(
        table_id="in.c-keboolasheets.log",
        file_path=f'updated_data_log.csv.gz',
        is_incremental=True)

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

def delete_decimal_zero(df_for_editing):
    for k, v in df_for_editing.dtypes.astype(str).to_dict().items():
        if re.search("(int|float).*", v):
            # st.write(f"Column name: {k}, Format: {v}")
            df_for_editing[k] = df_for_editing[k].astype(str)
            df_for_editing[k] = df_for_editing[k].replace(r'\.0$', '', regex=True)
    return df_for_editing

def check_date_format(df_to_check, date_setting_dict):
    col_names = df_to_check.columns.values.tolist()
    col_names_to_check = list(set(col_names).intersection(list(date_setting_dict.keys())))
    wrong_cols = []
    for k, v in date_setting_dict.items():
        for col_name in col_names_to_check:
            if k == col_name:
                try:
                    df_to_check[col_name] = pd.to_datetime(df_to_check[col_name], format=v.split(",")[0])
                    df_to_check[col_name] = df_to_check[col_name].dt.strftime(v.split(",")[0])
                except:
                    wrong_cols.append(k)
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

def check_duplicates(df_to_check, cs_setting, pk_setting = []):
    df_to_check = df_to_check.astype(str)
    for k, v in cs_setting.items():
        if v == '':
            df_to_check[k] = df_to_check[k].apply(str.lower)
    if pk_setting:
        df_to_check = df_to_check[pk_setting]
    duplicity_value = len(df_to_check.duplicated().unique().tolist())
    return duplicity_value
        
# Display tables
init()
st.session_state["tables_id"] = fetch_all_ids()

if st.session_state['selected-table'] is None and (st.session_state['upload-tables'] is None or st.session_state['upload-tables'] == False):
    #LOGO
   
      # Place an image in the first column
    col1, col2, col3 = st.columns((1,7,2))
    with col1:
        st.image(LOGO_IMAGE_PATH)

        hide_img_fs = '''
        <style>
        button[title="View fullscreen"]{
            visibility: hidden;}
        </style>
        '''

        st.markdown(hide_img_fs, unsafe_allow_html=True)

    with col3:
        st.markdown(f"**Data Freshness:** \n {st.session_state['data_load_time_overview']}")

    #Keboola title
    st.markdown("""<h1 style="font-size:32px;"><span style="color:#1F8FFF;">Keboola</span> Data Editor</h1>""", unsafe_allow_html=True)
    st.info('Select the table you want to edit. If the data is not up-to-data, click on the Reload Data button. Data freshness is displayed in the right corner.', icon="ℹ️")

    # Title of the Streamlit app
    st.subheader("Tables")

    # Search bar and sorting options
    search_col, sort_col, but_col1, col_upload = st.columns((50,25,10,15))

    with but_col1:
        if st.button("Reload Data", key="reload-tables", use_container_width = True, type="secondary"):
            st.session_state["tables_id"] = fetch_all_ids()
            st.toast('Tables List Reloaded!', icon = "✅")

    with search_col:
        search_query = st.text_input("Search for table", placeholder="Table Search",label_visibility="collapsed")

    with sort_col:
        sort_option = st.selectbox("Sort By Name", ["Sort By Name", "Sort By Date Created", "Sort By Date Updated"],label_visibility="collapsed")
    
    with col_upload:
        if st.button("Upload New Data", on_click=on_click_uploads, use_container_width = True):
            pass

    # Filtrace dat podle vyhledávacího dotazu
    if search_query:
        filtered_df = st.session_state["tables_id"][st.session_state["tables_id"].apply(lambda row: search_query.lower() in str(row).lower(), axis=1)]
    else:
        filtered_df = st.session_state["tables_id"]
    
    # Třídění dat
    if sort_option == "Sort By Name":
        filtered_df = filtered_df.sort_values(by="displayName", ascending=True)
    elif sort_option == "Sort By Date Created":
        filtered_df = filtered_df.sort_values(by="created", ascending=False)
    elif sort_option == "Sort By Date Updated":
        filtered_df = filtered_df.sort_values(by="lastImportDate", ascending=False)

    # Looping through each row of the Tables ID
    for index, row in filtered_df.iterrows():
        display_table_section(row)
        # row['displayName'], row['table_id'],row['lastImportDate'],row['created']

elif st.session_state['selected-table']is not None:
    col1,col2,col4= st.columns((2,7,2))
    with col1:
        st.button(":gray[:arrow_left: Back to Tables]", on_click=resetSetting, type="secondary")
    with col4:
         st.markdown(f"**Data Freshness:** \n {st.session_state['data_load_time_table']}")

    # Data Editor
    st.title("Data Editor")
  
    # Info
    st.info('After clicking the Save Data button, the data will be sent to Keboola Storage using a full load. If the data is not up-to-date, click on the Reload Data button. Data freshness is displayed in the right corner.', icon="ℹ️")
    # Reload Button
    if st.button("Reload Data", key="reload-table",use_container_width=True ):
            st.session_state["tables_id"] = fetch_all_ids()
            st.toast('Tables List Reloaded!', icon = "✅")

    #Select Box
    option = st.selectbox("Select Table", st.session_state["tables_id"], index=None, placeholder="Select table",label_visibility="collapsed")
    
    if option:
        st.session_state['selected-table'] = option
        st.session_state['data'] = get_dataframe(st.session_state['selected-table'])
       

    # Expander with info about table
    with st.expander("Table Info"):
         # Filter the DataFrame to find the row for the selected table_id
        selected_row = st.session_state["tables_id"][st.session_state["tables_id"]['table_id'] == st.session_state['selected-table']]

        # Ensure only one row is selected
        if len(selected_row) == 1:
            # Convert the row to a Series to facilitate access
            selected_row = selected_row.iloc[0]
            # st.write(selected_row)
            # Displaying data in bold using Markdown
            st.markdown(f"**Table ID:** {selected_row['table_id']}")
            # dt_created = selected_row['created']
            # st.markdown(f"**Created:** {dt_created.split('T')[0]}, {dt_created.split('T')[1]}")
            st.markdown(f"**Created at:** {split_datetime(selected_row['created'])}")
            # st.markdown(f"**Updated:** {selected_row.get('lastImportDate', 'N/A')}")
            st.markdown(f"**Updated at:** {split_datetime(selected_row['lastImportDate'])}")
            st.markdown(f"**Primary Key:** {selected_row.get('primaryKey', 'N/A')}")
            description = selected_row['description']
            # table_setting_str_dict = re.sub(r'```.*', '', re.sub(r'.*Upload setting:?\s*```\{', '{', description))
            table_setting_str_dict = re.sub("'", '"', re.sub(r'```.*', '', re.sub(r'.*Upload setting:?\s*```\{', '{', description)))
            table_setting_str = ', '.join(f"*{key}*: {value}" for key, value in json.loads(table_setting_str_dict).items())
            st.markdown(f"**Table Setting:** {table_setting_str}")
            case_insensitive_columns = [outer_key for outer_key, items in selected_row['column_metadata'].items() if any(item.get('value') == 'case sensitive' for item in items)]
            if case_insensitive_columns:
                st.markdown(f"**Case Sensitive Columns:** {', '.join(case_insensitive_columns)}")
            st.markdown(f"**Rows Count:** {selected_row['rowsCount']}")

    # Download table as CSV, TSV or Excel
    downloaded_data = cast_columns(st.session_state['data'])
    downloaded_data = delete_null_rows(modifying_nas(downloaded_data))
    downloaded_data = delete_decimal_zero(downloaded_data)
    downloaded_file_name = split_table_id(selected_row['table_id'])[1]
        
    # Uložení dataframe do CSV v paměti
    csv_buffer = io.StringIO()
    downloaded_data.to_csv(csv_buffer, index=False)
    csv_data = csv_buffer.getvalue()

    # Uložení dataframe do TSV v paměti
    tsv_buffer = io.StringIO()
    downloaded_data.to_csv(tsv_buffer, sep='\t', index=False)
    tsv_data = tsv_buffer.getvalue()

    # Uložení dataframe do Excelu v paměti
    xlsx_buffer = io.BytesIO()
    with pd.ExcelWriter(xlsx_buffer, engine='openpyxl') as writer:
        downloaded_data.to_excel(writer, index=False)
    xlsx_data = xlsx_buffer.getvalue()

    col5, col6, col7 = st.columns([1, 1, 6])

    with col5:
        st.download_button(
            label="Download CSV",
            data=csv_data,
            file_name=f"{downloaded_file_name}.csv",
            mime='text/csv'
        )

    with col6:
        st.download_button(
            label="Download TSV",
            data=tsv_data,
            file_name=f"{downloaded_file_name}.txt",
            mime='text/tab-separated-values'
        )

    with col7:
        st.download_button(
            label="Download XLSX",
            data=xlsx_data,
            file_name=f"{downloaded_file_name}.xlsx",
            mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        
    edited_data = st.data_editor(st.session_state["data"], num_rows="dynamic", height=500, use_container_width=True,
                                 column_config=create_column_config(st.session_state["data"]))

    if st.button("Save Data", key="save-data-tables"):
        with st.spinner('Saving Data...'):
            edited_data = cast_columns(edited_data)
            # st.write(edited_data)
            edited_data = delete_null_rows(modifying_nas(edited_data))
            edited_data = delete_decimal_zero(edited_data)
            
            selected_bucket = split_table_id(selected_row['table_id'])[0]
            # show column formatting settings
            column_setting = get_setting(token, selected_bucket, selected_row['table_id'])[0]
            # st.write(f"Required column setting: {column_setting}")
            format_setting = split_dict(column_setting, 2)
            # st.write(f"Required column formatting: {format_setting}")
            null_cells_setting = split_dict(column_setting, 1)
            # st.write(f"Required not null cells setting: {null_cells_setting}")
            case_sensitive_setting = get_setting(token, selected_bucket, selected_row['table_id'])[3]
            # st.write(f"Required case sensitive setting: {case_sensitive_setting}")
            primary_key_setting = get_setting(token, selected_bucket, selected_row['table_id'])[1]
            # st.write(f"Required primary key setting: {primary_key_setting}")
            date_setting = date_setting(column_setting)
            # st.write(f"Required date setting: {date_setting}")
            if date_setting:
                checking_date = check_date_format(edited_data, date_setting)
            if date_setting and checking_date[0]:
                st.error(f"The file contains date in the wrong format. Affected columns: {', '.join(checking_date[0])}. Please edit it before proceeding.")
            elif check_null_cells(edited_data, null_cells_setting):
                st.error(f"The table contains data with null values. Affected columns: {', '.join(check_null_cells(edited_data, null_cells_setting))}. Please edit it before proceeding.")
            elif primary_key_setting and check_duplicates(edited_data, case_sensitive_setting, primary_key_setting) == 2:
                st.error(f"The table contains columns with duplicate values. Affected columns: {', '.join(primary_key_setting)}. Please edit it before proceeding.")
            elif check_duplicates(edited_data, case_sensitive_setting) == 2:
                st.error("The table contains duplicate rows. Please remove them before proceeding.")
            else:                            
                if date_setting:
                    st.session_state["data"] = checking_date[1]
                    edited_data = modifying_nas(checking_date[1])
                else:
                    st.session_state["data"] = edited_data
                edited_data = modifying_nas(edited_data)
                # is_incremental = bool(selected_row.get('primaryKey', False))   
                write_to_keboola(edited_data, st.session_state["selected-table"],f'updated_data.csv.gz', False)
                st.success('Data Updated!', icon = "🎉")
                st.cache_data.clear()

    ChangeButtonColour('Save Data', '#FFFFFF', '#1EC71E','#1EC71E')
elif st.session_state['upload-tables']:
    if st.button(":gray[:arrow_left: Go back]", on_click=on_click_back):
        pass
    st.title('Import Data into :blue[Keboola Storage]')
    # List and display available buckets
    buckets = client.buckets.list()
    # bucket_names = [bucket['id'] for bucket in buckets]
    bucket_names = ["Choose a bucket"]  # Add option to choose a bucket at the beginning
    bucket_names.extend([bucket['id'] for bucket in buckets])
    selected_bucket = st.selectbox('Choose a bucket', bucket_names, placeholder="Choose an option")

    if selected_bucket and selected_bucket != "Choose a bucket":
        # File uploader
        uploaded_file = st.file_uploader("Upload a file", type=['csv', 'xlsx'])
            
        # List and display available tables
        tables = client.tables.list()
        table_names = ["Choose a table"]  # Add option to choose a table at the beginning
        table_names.extend([re.sub('.*\.', '', table["id"]) for table in tables if re.search(f"^{selected_bucket}\.", table["id"])])
        table_name = st.selectbox('Choose a table', table_names, placeholder="Choose an option")
        # table_name = st.text_input("Enter table name")

        # Main button for an action
        if 'action_clicked' not in st.session_state:
            st.session_state.action_clicked = False

        # Upload button
        if st.button('Upload'):
            st.session_state.action_clicked = True

        if st.session_state.action_clicked:
            if selected_bucket != "Choose a bucket" and uploaded_file and table_name != "Choose a table":
                # string_check = '^[a-zA-Z-_\d]*$'
                # check a valid table name
                # if bool(re.match(string_check, table_name)) == False:
                #    st.error('Error: In a table name are allowed only alphanumeric characters without diacritical marks, dashes, and underscores.')
                # Check if the table name already exists in the selected bucket
                existing_tables = client.buckets.list_tables(bucket_id=selected_bucket)
                existing_table_names = [table['name'] for table in existing_tables]
                if table_name in existing_table_names:
                    st.warning(f"Please check the table name again. The table '{table_name}' will be completely overwritten by your file!", icon="⚠️")
                    if st.button('I know what I am doing...'):
                        table_id = selected_bucket + '.' + table_name
                        # show column formatting settings
                        column_setting = get_setting(token, selected_bucket, table_id)[0]
                        # st.write(f"Required column setting: {column_setting}")
                        format_setting = split_dict(column_setting, 2)
                        # st.write(f"Required column formatting: {format_setting}")
                        null_cells_setting = split_dict(column_setting, 1)
                        # st.write(f"Required not null cells setting: {null_cells_setting}")
                        case_sensitive_setting = get_setting(token, selected_bucket, table_id)[3]
                        # st.write(f"Required case sensitive setting: {case_sensitive_setting}")
                        primary_key_setting = get_setting(token, selected_bucket, table_id)[1]
                        # st.write(f"Required primary key setting: {primary_key_setting}")
                        date_setting = date_setting(column_setting)
                        # st.write(f"Required date setting: {date_setting}")

                        st.success('The action has been confirmed successfully!', icon = "🎉")
                        # Resetování stavu
                        st.session_state.action_clicked = False
                        if st.session_state.action_clicked == False:
                            # Save the uploaded file to a temporary path
                            temp_file_path = f"/tmp/{uploaded_file.name}"
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
                            if date_setting:
                                checking_date = check_date_format(modifying_nas(df), date_setting)
                        
                            missing_columns = check_columns_diff(get_setting(token, selected_bucket, table_id)[2], df.columns.values.tolist())[0]
                            extra_columns = check_columns_diff(get_setting(token, selected_bucket, table_id)[2], df.columns.values.tolist())[1]

                            # st.write(f"Columns in dataframe: {df.columns.values.tolist()}")
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
                            elif check_null_cells(modifying_nas(df), null_cells_setting):
                                st.error(f"The file contains data with null values. Affected columns: {', '.join(check_null_cells(modifying_nas(df), null_cells_setting))}. Please edit it before proceeding.")
                            elif primary_key_setting and check_duplicates(df, case_sensitive_setting, primary_key_setting) == 2:
                                st.error(f"The table contains columns with duplicate values. Affected columns: {', '.join(primary_key_setting)}. Please edit it before proceeding.")
                            elif check_duplicates(df, case_sensitive_setting) == 2:
                                st.error("The table contains duplicate rows. Please remove them before proceeding.")
                            else:
                                if date_setting:
                                    df = checking_date[1]
                                else:
                                    df = modifying_nas(df)
                                df.to_csv(temp_file_path, index=False)
                                try:
                                    with st.spinner('Uploading...'):
                                        client.tables.load(table_id=table_id, file_path=temp_file_path, is_incremental=False)
                                        st.session_state['upload-tables'] = False
                                        st.session_state['selected-table'] = None
                                        # st.session_state['selected-table'] = selected_bucket+"."+table_name
                                        time.sleep(2)
                                    st.success('File uploaded and table created successfully!', icon = "🎉")
                                    st.cache_data.clear()
                                    st.session_state["tables_id"] = fetch_all_ids()
                                    time.sleep(2)
                                    st.rerun()

                                except Exception as e:
                                    st.error(f"Error: {str(e)}")
                    else:
                        st.write("Waiting for a confirmation...")
                else:
                    st.error("It is not allowed to create new tables. You need to overwrite the existing one. If you want to create a new table, contact the analytics team.")
            else:
                st.error('Error: Please upload a file and select a table name.') 

display_footer_section()
