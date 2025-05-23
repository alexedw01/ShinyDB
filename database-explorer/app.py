# app.py
import os
from dotenv import load_dotenv
import psycopg2
from query import query_output_server, query_output_ui
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

app_ui = ui.page_sidebar(
    ui.sidebar(
        ui.input_action_button("add_query", "Add Query", class_="btn btn-primary"),
        ui.input_action_button("show_meta", "Show Metadata", class_="btn btn-secondary"),
        ui.markdown(
            """
            This app lets you explore a dataset using SQL and PostgreSQL.
            """
        ),
    ),
    ui.tags.div(
        query_output_ui("initial_query", remove_id="initial_query"),
        id="module_container",
    ),
    title="PostgreSQL query explorer",
    class_="bslib-page-dashboard",
)

def server(input, output, session):
    mod_counter = reactive.value(0)
    query_output_server("initial_query", con=con, remove_id="initial_query")

    @reactive.effect
    @reactive.event(input.add_query)
    def _():
        counter = mod_counter.get() + 1
        mod_counter.set(counter)
        id = f"query_{counter}"
        ui.insert_ui(selector="#module_container", where="afterBegin", ui=query_output_ui(id, remove_id=id))
        query_output_server(id, con=con, remove_id=id)

    @reactive.effect
    @reactive.event(input.show_meta)
    def _():
        counter = mod_counter.get() + 1
        mod_counter.set(counter)
        id = f"query_{counter}"
        ui.insert_ui(
            selector="#module_container",
            where="afterBegin",
            ui=query_output_ui(id, qry="SELECT * FROM information_schema.columns", remove_id=id),
        )
        query_output_server(id, con=con, remove_id=id)

app = App(app_ui, server)
