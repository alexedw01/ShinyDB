# app.py
import os
from dotenv import load_dotenv
import psycopg2
import query
from shiny import App, reactive, ui

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

def get_connection():
    return psycopg2.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        dbname=DB_CONFIG["dbname"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
    )

con = get_connection()

with con.cursor() as cur:
    cur.execute("SELECT * FROM public.clone_exis_end_to_end LIMIT 1")
    columns = [desc[0] for desc in cur.description]

app_ui = ui.page_sidebar(
    ui.sidebar(
        ui.input_action_button("clone_eis_end_to_end", "CLONE EIS E2E", class_="btn btn-info"),
        ui.input_action_button("sbc_geneious_seq", "SBC GEN SEQ", class_="btn btn-primary"),
        ui.input_action_button("sbc_tap_dev", "SBC TAP DEV", class_="btn btn-secondary"),
        ui.markdown(
            """
            This app lets you explore a dataset using SQL and PostgreSQL.
            """
        ),
    ),
    ui.tags.div(
        query.query_output_ui_clone_eis_e2e("0", remove_id="0", columns=columns),
        id="module_container",
    ),
    title="PostgreSQL query explorer",
    class_="bslib-page-dashboard",
)

def server(input, output, session):
    mod_counter = reactive.value(0)
    query.query_output_server_clone_eis_e2e("0", con=con, remove_id="0", query="SELECT * FROM public.clone_exis_end_to_end")

    @reactive.effect
    @reactive.event(input.clone_eis_end_to_end)
    def _():
        counter = mod_counter.get()
        ui.remove_ui(selector=f"div#{counter}")
        counter += 1
        mod_counter.set(counter)
        id = f"{counter}"

        # Fetch column names
        with con.cursor() as cur:
            cur.execute("SELECT * FROM public.clone_exis_end_to_end LIMIT 1")
            columns = [desc[0] for desc in cur.description]

        # Insert card with column dropdown
        ui.insert_ui(
            selector="#module_container",
            where="afterBegin",
            ui=query.query_output_ui_clone_eis_e2e(id, remove_id=id, columns=columns),
        )

        # Bind query server logic
        query.query_output_server_clone_eis_e2e(id, con=con, remove_id=id, query="SELECT * FROM public.clone_exis_end_to_end")


    @reactive.effect
    @reactive.event(input.sbc_geneious_seq)
    def _():
        counter = mod_counter.get()
        ui.remove_ui(selector=f"div#{counter}")
        counter += 1
        mod_counter.set(counter)
        id = f"{counter}"

        # Fetch column names
        with con.cursor() as cur:
            cur.execute("SELECT * FROM public.sbc_geneious_seq LIMIT 1")
            columns = [desc[0] for desc in cur.description]

        # Insert card with column dropdown
        ui.insert_ui(
            selector="#module_container",
            where="afterBegin",
            ui=query.query_output_ui_sbc_geneious_seq(id, remove_id=id, columns=columns),
        )

        # Bind query server logic
        query.query_output_server_sbc_geneious_seq(id, con=con, remove_id=id, query="SELECT * FROM public.sbc_geneious_seq")

    @reactive.effect
    @reactive.event(input.sbc_tap_dev)
    def _():
        counter = mod_counter.get()
        ui.remove_ui(selector=f"div#{counter}")
        counter += 1
        mod_counter.set(counter)
        id = f"{counter}"

        # Fetch column names
        with con.cursor() as cur:
            cur.execute("SELECT * FROM public.sbc_tap_dev LIMIT 1")
            columns = [desc[0] for desc in cur.description]

        # Insert card with column dropdown
        ui.insert_ui(
            selector="#module_container",
            where="afterBegin",
            ui=query.query_output_ui_sbc_tap_dev(id, remove_id=id, columns=columns),
        )

        # Bind query server logic
        query.query_output_server_sbc_tap_dev(id, con=con, remove_id=id, query="SELECT * FROM public.sbc_tap_dev")

app = App(app_ui, server)
