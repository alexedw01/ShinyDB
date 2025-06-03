import pandas as pd
from shiny import module, reactive, render, ui

def dynamic_filters_ui(remove_id, columns, max_filters=3):
    def filter_row(i):
        return ui.layout_columns(
            ui.input_select(
                f"column_select_{i}_{remove_id}",
                f"Column {i+1}:",
                choices=columns,
                selected=columns[0] if columns else None,
            ),
            ui.input_text(
                f"contains_filter_{i}_{remove_id}",
                f"Contains {i+1}:",
                placeholder="Enter filter text",
            ),
            col_widths=[6, 6],
        )

    rows = [ui.card_header(f"Query: {remove_id}"), filter_row(0)]
    for i in range(1, max_filters):
        condition = f"input.contains_filter_{i-1}_{remove_id} != ''"
        rows.append(ui.panel_conditional(condition, filter_row(i)))

    return rows

def query_output_ui_common(remove_id, columns, range_filters=[], max_filters=3):
    elements = [*dynamic_filters_ui(remove_id, columns)]

    for rf in range_filters:
        label = rf["label"]
        column = rf["column"]
        min_range = rf.get("min", 0)
        max_range = rf.get("max", 100)

        elements.extend([
            ui.markdown(label),
            ui.layout_columns(
                ui.input_numeric(
                    f"column_start_range_{column}_{remove_id}",
                    "From:",
                    value=min_range,
                    min=min_range,
                    max=max_range,
                ),
                ui.input_numeric(
                    f"column_end_range_{column}_{remove_id}",
                    "To:",
                    value=max_range,
                    min=min_range,
                    max=max_range,
                ),
                col_widths=[6, 6],
            ),
        ])

    elements.extend([
        ui.layout_columns(
            ui.input_action_button("run", "Run Query", class_="btn btn-primary"),
            ui.download_button("download", "Download CSV"),
            col_widths=[6, 6]
        ),
        ui.output_data_frame("results"),
    ])

    return ui.card({"id": remove_id}, *elements)

def query_output_server_common(input, output, session, con, remove_id, query, range_filters=[], max_filters=3):
    @reactive.calc
    @reactive.event(input.run)
    def query_df():
        qry = query.rstrip(";")
        or_filters = []
        and_filters = []

        NON_DIGIT_COLUMNS = {"entity_registry_id", "prellis_mabs_expressed"}

        for i in range(max_filters):
            col_id = f"column_select_{i}_{remove_id}"
            val_id = f"contains_filter_{i}_{remove_id}"
            if col_id in input and val_id in input:
                col = input[col_id]()
                val = input[val_id]().strip()
                if val:
                    safe_val = val.replace("'", "''")
                    or_filters.append(f"{col}::text ILIKE '%{safe_val}%'")

        for rf in range_filters:
            column = rf["column"]
            start = input[f"column_start_range_{column}_{remove_id}"]()
            end = input[f"column_end_range_{column}_{remove_id}"]()

            if start is not None and end is not None:
                if column in NON_DIGIT_COLUMNS:
                    and_filters.append(
                        f"regexp_replace({column}, '\\D', '', 'g')::int BETWEEN {start} AND {end}"
                    )
                else:
                    and_filters.append(
                        f"""
                        CASE 
                            WHEN {column} ~ '^[-]?\\d+(\\.\\d+)?$' 
                            THEN CAST({column} AS FLOAT) 
                            ELSE NULL 
                        END BETWEEN {start} AND {end}
                        """
                    )

        where_clauses = []
        if or_filters:
            where_clauses.append(f"({' OR '.join(or_filters)})")
        if and_filters:
            where_clauses.append(" AND ".join(and_filters))

        if where_clauses:
            qry += " WHERE " + " AND ".join(where_clauses)

        with con.cursor() as cur:
            cur.execute(qry)
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
        return pd.DataFrame(rows, columns=columns)

    @render.data_frame
    def results():
        return query_df()

    @render.download(filename="results.csv")
    def download():
        yield query_df().to_csv(index=False)

# CLONE EIS E2E MODULE
@module.ui
def query_output_ui_clone_eis_e2e(remove_id, columns=[]):
    return query_output_ui_common(
        remove_id,
        columns,
        range_filters=[
            {"label": "Entity Registry ID Range:", "column": "entity_registry_id", "min": 0, "max": 15000}
        ]
    )


@module.server
def query_output_server_clone_eis_e2e(input, output, session, con, remove_id, query):
    query_output_server_common(
        input,
        output,
        session,
        con,
        remove_id,
        query,
        range_filters=[
            {"column": "entity_registry_id", "min": 0, "max": 15000}
        ]
    )


# SBC GENEIOUS SEQ MODULE
@module.ui
def query_output_ui_sbc_geneious_seq(remove_id, columns=[]):
    return query_output_ui_common(
        remove_id,
        columns,
        range_filters=[
            {"label": "Prellis Mabs Expressed Range:", "column": "prellis_mabs_expressed", "min": 0, "max": 10000}
        ]
    )

@module.server
def query_output_server_sbc_geneious_seq(input, output, session, con, remove_id, query):
    query_output_server_common(
        input,
        output,
        session,
        con,
        remove_id,
        query,
        range_filters=[
            {"column": "prellis_mabs_expressed", "min": 0, "max": 10000}
        ]
    )

# SBC TAP DEV MODULE
@module.ui
def query_output_ui_sbc_tap_dev(remove_id, columns=[]):
    return query_output_ui_common(
        remove_id,
        columns,
        range_filters=[
            {"label": "Total Imgt CRD Length Range:", "column": "total_imgt_cdr_length", "min": 0, "max": 100},
            {"label": "Patch CDR Surface Hydrophobicity Score Range:", "column": "patch_cdr_surface_hydrophobicity_score", "min": 0, "max": 200},
            {"label": "Patch CDR Positive Charge Score:", "column": "patch_cdr_positive_charge_score", "min": 0, "max": 5},
            {"label": "Patch CDR Negative Charge Score:", "column": "patch_cdr_negative_charge_score", "min": 0, "max": 5},
            {"label": "SFVCSP Score Range:", "column": "sfvcsp_score", "min": -20, "max": 20}
        ]
    )

@module.server
def query_output_server_sbc_tap_dev(input, output, session, con, remove_id, query):
    query_output_server_common(
        input,
        output,
        session,
        con,
        remove_id,
        query,
        range_filters=[
            {"column": "total_imgt_cdr_length", "min": 0, "max": 100},
            {"column": "patch_cdr_surface_hydrophobicity_score", "min": 0, "max": 200},
            {"column": "patch_cdr_positive_charge_score", "min": 0, "max": 5},
            {"column": "patch_cdr_negative_charge_score", "min": 0, "max": 5},
            {"column": "sfvcsp_score", "min": -20, "max": 20}
        ]
    )

