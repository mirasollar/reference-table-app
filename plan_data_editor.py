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
        text=[f"Primary key: {row['primaryKey']}", f"Table ID: {row['table_id']}", f"Updated at: {row['lastImportDate']}", f"Created at: {row['created']}", f"Rows count: {str(row['rowsCount'])}"],
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

def fetch_all_ids():
    all_tables = client.tables.list()
    ids_list = [{'table_id': table["id"], 'displayName': table["displayName"], 'primaryKey': table["primaryKey"][0] if table["primaryKey"] else "",
                  'lastImportDate': table['lastImportDate'], 'rowsCount': table['rowsCount'], 'created': table['created']} for table in all_tables]
    return pd.DataFrame(ids_list)

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
        st.caption("© Keboola 2024")
    with right_aligned:
        st.caption("Version 2.0")

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

def cast_bool_columns(df):
    """Ensure that columns that should be boolean are explicitly cast to boolean."""
    for col in df.columns:
        # If a column in the DataFrame has only True/False or NaN values, cast it to bool
        if df[col].dropna().isin([True, False]).all():
            df[col] = df[col].astype(bool)
    return df

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
    st.markdown("""<h2 style="font-size:32px;">Discover how Streamlit can seamlessly integrate with <span style="color:#1F8FFF;">Keboola Storage!</span></h2>""", unsafe_allow_html=True)
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
    st.info('After clicking the Save Data button, the data will be sent to Keboola Storage using an incremental load when primary keys are set; otherwise, a full load is used. If the data is not up-to-date, click on the Reload Data button. Data freshness is displayed in the right corner.', icon="ℹ️")
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
            # Displaying data in bold using Markdown
            st.markdown(f"**Table ID:** {selected_row['table_id']}")
            st.markdown(f"**Created:** {selected_row['created']}")
            st.markdown(f"**Updated:** {selected_row.get('lastImportDate', 'N/A')}")
            st.markdown(f"**Primary Key:** {selected_row.get('primaryKey', 'N/A')}")
            st.markdown(f"**Rows Count:** {selected_row['rowsCount']}")
        
    edited_data = st.data_editor(st.session_state["data"], num_rows="dynamic", height=500, use_container_width=True)

    if st.button("Save Data", key="save-data-tables"):
        with st.spinner('Saving Data...'):
            kbc_data = cast_bool_columns(get_dataframe(st.session_state["selected-table"]))
            edited_data = cast_bool_columns(edited_data)
            st.session_state["data"] = edited_data
            concatenated_df = pd.concat([kbc_data, edited_data])
            sym_diff_df = concatenated_df.drop_duplicates(keep=False)
            #write_to_log(sym_diff_df)
            is_incremental = bool(selected_row.get('primaryKey', False))   
            write_to_keboola(edited_data, st.session_state["selected-table"],f'updated_data.csv.gz', is_incremental)
        st.success('Data Updated!', icon = "🎉")

    ChangeButtonColour('Save Data', '#FFFFFF', '#1EC71E','#1EC71E')
elif st.session_state['upload-tables']:
    if st.button(":gray[:arrow_left: Go back]", on_click=on_click_back):
        pass
    st.title('Import Data into :blue[Keboola Storage]')
    # List and display available buckets
    buckets = client.buckets.list()
    bucket_names = ["Create new bucket"]  # Add option to create a new bucket at the beginning
    bucket_names.extend([bucket['id'] for bucket in buckets])
    
    selected_bucket = st.selectbox('Choose a bucket or create a new one', bucket_names, placeholder="Choose an option")

    if selected_bucket == "Create new bucket":
        new_bucket_name = st.text_input("Enter new bucket name")
        create_bucket_button = st.button("Create Bucket")

        if create_bucket_button and new_bucket_name:
            # Check if the bucket name is original
            new_bucket_id = f"out.c-{new_bucket_name}"
            if new_bucket_id in bucket_names:
                st.error(f"Error: Bucket name '{new_bucket_id}' already exists.")
            else:
                try:
                    # Create new bucket
                    client.buckets.create(new_bucket_id, new_bucket_name)
                    st.success(f"Bucket '{new_bucket_id}' created successfully!")
                    bucket_names.append(new_bucket_id)  # Update the list of buckets
                    selected_bucket = new_bucket_id  # Set the newly created bucket as selected
                except Exception as e:
                    st.error(f"Error creating bucket: You don't have permission to create a new bucket. Please select one from the available options.")

    elif selected_bucket and selected_bucket != "Choose an option":
        # File uploader
        uploaded_file = st.file_uploader("Upload a file", type=['csv', 'xlsx'])

        # Input for table name
        table_name = st.text_input("Enter table name")

        # Upload button
        if st.button('Upload'):
            if selected_bucket and uploaded_file and table_name:
                string_check = '^[a-zA-Z-]*$'
                # check a valid table name
                if bool(re.match(string_check, table_name)) == False:
                    st.error('Error: In a table name are allowed only alphanumeric characters, dashes, and underscores.')
                else:
                    # Check if the table name already exists in the selected bucket
                    existing_tables = client.buckets.list_tables(bucket_id=selected_bucket)
                    existing_table_names = [table['name'] for table in existing_tables]

                    if table_name in existing_table_names:
                        table_id = 'out.c-MSO_ADHOC_dummy_data.aab_customer'
                        client.tables.delete(table_id=table_id)
                        # Save the uploaded file to a temporary path
                        temp_file_path = f"/tmp/{uploaded_file.name}"
                        if Path(uploaded_file.name).suffix == '.csv':
                            df=pd.read_csv(uploaded_file)
                        else:
                            df=pd.read_excel(uploaded_file)
                        df.to_csv(temp_file_path, index=False)

                        # Create the table in the selected bucket
                        try:
                            with st.spinner('Uploading...'):
                                client.tables.create(
                                name=table_name,
                                bucket_id=selected_bucket,
                                file_path=temp_file_path,
                                primary_key=[]
                                )
                                st.session_state['upload-tables'] = False
                                st.session_state['selected-table'] = None
                                # st.session_state['selected-table'] = selected_bucket+"."+table_name
                                time.sleep(4)
                            st.success('File uploaded and table created successfully!', icon = "🎉")
                            st.cache_data.clear()
                            st.session_state["tables_id"] = fetch_all_ids()
                            time.sleep(4)
                            st.rerun()

                        except Exception as e:
                            st.error(f"Error: {str(e)}")
                    else:
                        # Save the uploaded file to a temporary path
                        temp_file_path = f"/tmp/{uploaded_file.name}"
                        if Path(uploaded_file.name).suffix == '.csv':
                            df=pd.read_csv(uploaded_file)
                        else:
                            df=pd.read_excel(uploaded_file)
                        df.to_csv(temp_file_path, index=False)

                        # Create the table in the selected bucket
                        try:
                            with st.spinner('Uploading...'):
                                client.tables.create(
                                name=table_name,
                                bucket_id=selected_bucket,
                                file_path=temp_file_path,
                                primary_key=[]
                                )
                                st.session_state['upload-tables'] = False
                                st.session_state['selected-table'] = None
                                # st.session_state['selected-table'] = selected_bucket+"."+table_name
                                time.sleep(4)
                            st.success('File uploaded and table created successfully!', icon = "🎉")
                            st.cache_data.clear()
                            st.session_state["tables_id"] = fetch_all_ids()
                            time.sleep(4)
                            st.rerun()

                        except Exception as e:
                            st.error(f"Error: {str(e)}")
                        
            else:
                st.error('Error: Please select a bucket, upload a file, and enter a table name. Please check if you have permission to create a new bucket and table.')

display_footer_section()
