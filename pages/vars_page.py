import streamlit as st
from acs1 import ACS1
from st_aggrid import AgGrid, GridOptionsBuilder, JsCode, GridUpdateMode
import pandas as pd

st.set_page_config(layout='wide')

st.title("Variables for Group Code")

selected = st.session_state.get("selected_gcode")

if not selected:
    st.warning("No group code selected. Please return to the main page.")
    if st.button("⬅️ Back to Main Page"):
        st.session_state.pop("selected_gcode", None)
        st.session_state.pop("selected_vars", None)
        st.switch_page("table_page.py")
else:
    if "selected_vars" not in st.session_state:
        st.session_state.selected_vars = []

    a = ACS1()
    row = a.groups_df[a.groups_df['gcode'] == selected].iloc[0]
    
    st.markdown(f"### GCode: `{row['gcode']}`")
    st.markdown(f"**Description:** {row['description']}")

    if st.button("⬅️ Back to Main Page"):
        st.session_state.pop("selected_gcode", None)
        st.switch_page("table_page.py")

    vars_df = a.vars_df[a.vars_df['group'] == selected]
    vars_df.loc[:, 'orgHierarchy'] = vars_df['label'].str.replace('!!', '/')
    vars_df.reset_index(inplace=True)
    vars_df = vars_df[['orgHierarchy', 'code', 'concept']]

    # Build Grid Options
    gb = GridOptionsBuilder.from_dataframe(vars_df)

    gb.configure_default_column(
        wrapText=True,
        autoHeight=True
    )

    # Define the tree column with word wrap
    autoGroupColumnDef = {
        "headerName": "Organisation Hierarchy",
        "cellRendererParams": {
            "suppressCount": True,
            "checkbox": True
        },
        "maxWidth": 350,
        "field": "orgHierarchy",
        "wrapText": True,
        "autoHeight": True,
        "cellStyle": {
            "whiteSpace": "normal",
            "lineHeight": "1.2"
        }
    }

    # Enable tree data
    gb.configure_grid_options(
        treeData=True,
        animateRows=True,
        groupDefaultExpanded=-1,
        getDataPath=JsCode("""function(data) { return data.orgHierarchy.split("/"); }"""),
        autoGroupColumnDef=autoGroupColumnDef,
        rowSelection="multiple",
        groupSelectsChildren=True
    )

    # Hide the raw orgHierarchy column
    gb.configure_column("orgHierarchy", hide=True)

    # Final gridOptions
    gridOptions = gb.build()

    # Render the AgGrid
    response = AgGrid(
        vars_df,
        gridOptions=gridOptions,
        allow_unsafe_jscode=True,
        enable_enterprise_modules=True,
        update_mode=GridUpdateMode.SELECTION_CHANGED,
        height=600,
        theme="streamlit",
    )

    selected_rows = response['selected_rows']

    if st.button("Generate"):
        if len(selected_rows) > 0:
            st.subheader("Output DataFrame:")
            output_df = a.scrape_vars(list(selected_rows['code']))
            st.dataframe(output_df)
        else:
            st.info("No rows selected.")
