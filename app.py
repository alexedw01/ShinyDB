# Streamlit Documentation: https://docs.streamlit.io/
# Inspiration: https://github.com/jkanner/streamlit-dataview/blob/master/app.py 
# Run app: streamlit run app.py

import os
from dotenv import load_dotenv
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from psycopg2.extras import RealDictCursor
import psycopg2

load_dotenv()

# --- Configuration ---
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
}

# Validate env vars
missing = [k for k, v in DB_CONFIG.items() if not v]
if missing:
    st.error(f"Missing environment variables: {', '.join(missing)}")
    st.stop()

DATABASE_URL = (
    f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
    f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
)
engine = create_engine(DATABASE_URL, future=True)

def get_connection():
    return psycopg2.connect(
        host=DB_CONFIG['host'],
        port=DB_CONFIG['port'],
        dbname=DB_CONFIG['dbname'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password'],
        cursor_factory=RealDictCursor
    )

@st.cache_data
def fetch_tables():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name;")
    tables = [r['table_name'] for r in cur.fetchall()]
    cur.close()
    conn.close()
    return tables

@st.cache_data
def fetch_columns(table: str):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        ORDER BY ordinal_position;
    """, (table,))
    cols = [r['column_name'] for r in cur.fetchall()]
    cur.close()
    conn.close()
    return cols

# --- Shortcuts Configuration ---
SHORTCUTS = {
    "None": {},
    "Naive Antigens": {
        "table": "all_sequenced_data_3",
        "columns": ["aa_heavy", "aa_light", "frequency_of_paired_aa", "contig_id_heavy", "contig_id_light", "barcodes", "count_barcodes", "donors", "antigens", "c_gene_igh", "c_gene_igkl", "v_gene_igh", "d_gene_igh", "j_gene_igh", "v_gene_igkl", "j_gene_igkl", "exis_runs", "trimmed_aa_heavy", "trimmed_aa_light", "shm_vgene_igh", "fwr1_igh", "cdr1_igh", "fwr2_igh", "cdr2_igh", "fwr3_igh", "cdr3_igh", "fwr4_igh", "fwr1_igkl", "cdr1_igkl", "fwr2_igkl", "cdr2_igkl", "fwr3_igkl", "cdr3_igkl", "fwr4_igkl", "is_cell_count_igh", "high_confidence_igh", "is_cell_count_igkl", "high_confidence_igkl", "mabs_id_expressed", "ka_1_ms", "kdis_1_s", "full_r_squared"],
        "where": {"column": "antigens", "operator": "CONTAINS", "value": "naive"},
        "limit": 100
    }
}

# --- Streamlit App ---
app_title = '🔗 Query Builder'
st.set_page_config(page_title=app_title)
st.title(app_title)

# --- Custom CSS Styling ---
logo_url = "https://cdn.sanity.io/images/km3brrze/production/de004b1975dd180c5f50932d4120e3651fd7f601-368x154.svg"
custom_css = f"""
<style>
  [data-testid="stAppViewContainer"] {{
    background: radial-gradient(56.7% 113.48% at 58.19% -5.81%, rgb(207,252,252) 0%, rgb(204,242,249) 100%);
    color: rgb(13, 29, 32);
  }}
  section[data-testid="stSidebar"] {{
    background: radial-gradient(56.7% 113.48% at 58.19% -5.81%, rgb(155,220,220) 0%, rgb(150,210,215) 100%);
    color: white;
  }}
  section[data-testid="stSidebar"] h1,
  section[data-testid="stSidebar"] h2,
  section[data-testid="stSidebar"] h3,
  section[data-testid="stSidebar"] h4,
  section[data-testid="stSidebar"] h5,
  section[data-testid="stSidebar"] h6 {{
    color: white !important;
  }}
  [data-baseweb="tag"] {{
    background-color: #2a8687 !important;
    color: white !important;
    border-radius: 16px !important;
    padding: 4px 10px !important;
    font-weight: 500 !important;
  }}
  [data-baseweb="tag"] svg {{
    color: white !important;
  }}
  .custom-logo {{
    position: fixed;
    top: 4rem;
    right: 1rem;
    width: 120px;
    z-index: 999;
  }}
  div.stButton > button {{
    background-color: #007d8a !important;
    color: white !important;
    border: none !important;
    border-radius: 8px !important;
    font-weight: 600 !important;
    font-size: 1rem !important;
    padding: 0.6rem 1.4rem !important;
  }}
  div.stButton > button:hover {{
    background-color: #005f69 !important;
    transform: translateY(-1px);
  }}
  .stAlert > div {{
    background-color: #2a8687 !important;
    color: white !important;
    font-weight: 600 !important;
    font-size: 1rem !important;
    padding: 1rem 1.25rem !important;
    border-radius: 10px !important;
  }}
</style>
<img src="{logo_url}" class="custom-logo" />
"""
st.markdown(custom_css, unsafe_allow_html=True)

# --- Info Panel ---
st.markdown("""
 * Use the menu at left to select data and set the query you would like  
 * Use shortcuts for common queries  
 * Your data sheet will appear below  
""")

# --- Sidebar Controls ---
st.sidebar.header("Query Builder")

# Shortcut selection
selected_shortcut = st.sidebar.selectbox("Shortcuts", list(SHORTCUTS.keys()))
shortcut_data = SHORTCUTS[selected_shortcut]

# Table
tables = fetch_tables()
selected_table = shortcut_data.get("table", tables[0])
table_index = tables.index(selected_table) if selected_table in tables else 0
selected_table = st.sidebar.selectbox("Select table", tables, index=table_index)

# Columns
columns = fetch_columns(selected_table)
default_cols_raw = shortcut_data.get("columns", columns)
default_cols = [col for col in default_cols_raw if col in columns]
selected_cols = st.sidebar.multiselect("Select columns", columns, default=default_cols)

# WHERE clause
st.sidebar.subheader("WHERE clause")
filter_col_index = 0
if "where" in shortcut_data and shortcut_data["where"]["column"] in columns:
    filter_col_index = columns.index(shortcut_data["where"]["column"]) + 1
filter_col = st.sidebar.selectbox("Column", [None] + columns, index=filter_col_index)

filter_op = st.sidebar.selectbox(
    "Operator",
    ["=", "!=", "<", "<=", ">", ">=", "BEGINS_WITH", "ENDS_WITH", "CONTAINS"],
    index=["=", "!=", "<", "<=", ">", ">=", "BEGINS_WITH", "ENDS_WITH", "CONTAINS"].index(shortcut_data["where"]["operator"]) if "where" in shortcut_data else 0
)

filter_val = st.sidebar.text_input(
    "Value (enter exactly as it would appear in table)",
    value=shortcut_data["where"]["value"] if "where" in shortcut_data else ""
)

# Limit
limit_enabled = st.sidebar.checkbox("Enable limit", value=True)
limit = st.sidebar.number_input("Limit", min_value=1, value=shortcut_data.get("limit", 100)) if limit_enabled else None

# --- Query Building ---
select_part = ", ".join(selected_cols) if selected_cols else "*"
query = f"SELECT {select_part} FROM {selected_table}"

if filter_col and filter_val:
    custom_op = filter_op.upper()

    # Handle CONTAINS / BEGINS_WITH / ENDS_WITH
    if custom_op in ["CONTAINS", "BEGINS_WITH", "ENDS_WITH"]:
        filter_op_sql = "ILIKE"
        if custom_op == "CONTAINS":
            filter_val = f"%%{filter_val}%%"
        elif custom_op == "BEGINS_WITH":
            filter_val = f"{filter_val}%%"
        elif custom_op == "ENDS_WITH":
            filter_val = f"%%{filter_val}"
        safe_val = f"'{filter_val}'"
        query += f" WHERE {filter_col}::text {filter_op_sql} {safe_val}"
    else:
        try:
            float_val = float(filter_val)
            is_numeric = float_val.is_integer() or filter_val.replace('.', '', 1).isdigit()
        except ValueError:
            is_numeric = False

        # Quote string values unless already quoted
        if not is_numeric:
            safe_val = f"'{filter_val}'" if not (filter_val.startswith("'") and filter_val.endswith("'")) else filter_val
        else:
            safe_val = str(int(float_val)) if float_val.is_integer() else str(float_val)

        if is_numeric:
            query += f" WHERE {filter_col}::float {filter_op} {safe_val}"
        else:
            query += f" WHERE {filter_col}::text {filter_op} {safe_val}"

if limit_enabled:
    query += f" LIMIT {limit};"


# --- Execute and Display ---
st.subheader("Generated SQL Query")
st.code(query, language='sql')

if st.button("Run Query"):
    try:
        df = pd.read_sql(sql=query, con=engine)
        st.success(f"Query returned {len(df)} rows.")
        st.dataframe(df)
    except Exception as e:
        st.error(f"Error executing query: {e}")
