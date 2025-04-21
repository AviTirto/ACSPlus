import pandas as pd
import streamlit as st
from acs1 import ACS1
from st_aggrid import AgGrid, GridOptionsBuilder, JsCode, GridUpdateMode
a = ACS1()
vars_df = a.vars_df
vars_df = vars_df[vars_df['group'] == 'B17015']
vars_df['orgHierarchy'] = vars_df['label'].str.replace('!!','/')
vars_df.reset_index(inplace=True)

df=vars_df
st.write(df)
st.dataframe(df)
gb = GridOptionsBuilder.from_dataframe(df)
gridOptions = gb.build()


gridOptions["columnDefs"]= [
    { "field": 'jobTitle' },
    { "field": 'employmentType' },
]
gridOptions["defaultColDef"]={
      "flex": 1,
    },
gridOptions["autoGroupColumnDef"]= {
    "headerName": 'Organisation Hierarchy',
    "minWidth": 300,
    "cellRendererParams": {
      "suppressCount": True,
    },
  },
gridOptions["treeData"]=True
gridOptions["animateRows"]=True
gridOptions["groupDefaultExpanded"]= -1
gridOptions["getDataPath"]=JsCode(""" function(data){
    return data.orgHierarchy.split("/");
  }""").js_code

r = AgGrid(
    df,
    gridOptions=gridOptions,
    height=500,
    allow_unsafe_jscode=True,
    enable_enterprise_modules=True,
    filter=True,
    update_mode=GridUpdateMode.SELECTION_CHANGED,
    theme="material",
    tree_data=True
)


# Show the raw DataFrame
st.write("Raw DataFrame:")
st.write(df)

# Build Grid Options
gb = GridOptionsBuilder.from_dataframe(df)
gb.configure_default_column(flex=1)

# Enable tree data
gb.configure_grid_options(
    treeData=True,
    animateRows=True,
    groupDefaultExpanded=-1,
    getDataPath=JsCode("""function(data) { return data.orgHierarchy.split("/"); }"""),
    autoGroupColumnDef={
        "headerName": "Organisation Hierarchy",
        "cellRendererParams": {
            "suppressCount": True,
            "checkbox": True
        },
        "minWidth": 300,
        "field": "orgHierarchy"
    },
    rowSelection="multiple",
    groupSelectsChildren=True
)

# Final gridOptions
gridOptions = gb.build()

# Render the AgGrid
response = AgGrid(
    df,
    gridOptions=gridOptions,
    height=500,
    allow_unsafe_jscode=True,
    enable_enterprise_modules=True,
    update_mode=GridUpdateMode.SELECTION_CHANGED,
    theme="material"
)



# Store selected rows temporarily
selected_rows = response['selected_rows']

# Add a Generate button
if st.button("Generate"):
    if selected_rows is not None and len(selected_rows) > 0:
        st.subheader("Selected Rows:")
        st.dataframe(pd.DataFrame(selected_rows))
    else:
        st.info("No rows selected.")
