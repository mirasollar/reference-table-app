import csv
import pandas as pd
import io
import streamlit as st
from charset_normalizer import from_path

uploaded_file = st.file_uploader("Upload CSV file", type=["csv"])

if uploaded_file is not None:
    file_content = uploaded_file.read()
    result = from_path(uploaded_file).best()
    encoding = result.encoding
    try:
        df = pd.read_csv(io.BytesIO(file_content), sep=None, engine='python', encoding='utf-8-sig')
    except:

        st.write(f"Detekované kodování: {encoding}")
        df = pd.read_csv(io.BytesIO(file_content), sep=None, engine='python', encoding=encoding)
        
    st.write(f"Datové typy: {df.dtypes}")
    st.write(f"Dataframe: {df.head()}")
