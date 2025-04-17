import streamlit as st
import pandas as pd
from acs1 import ACS1

st.set_page_config(layout='wide')

@st.cache_data
def get_acs1():
    return ACS1()

a = get_acs1()
st.title("ACS1 Data Explorer")

gcode_search = st.text_input("Enter a group code to filter tables:")

header1, header2, header3 = st.columns([1, 8, 1])
header1.markdown("**GCode**")
header2.markdown("**Description**")
header3.markdown("")

with st.container(height=500):

    if gcode_search:
        filtered_df = a.groups_df[a.groups_df['gcode'].str.contains(gcode_search, case=False, na=False)]
        for index, row in filtered_df.iterrows():
            col1, col2, col3 = st.columns([1, 8, 1])
            col1.markdown(f"**{row['gcode']}**")
            col2.markdown(row['description'])
            with col3:
                if st.button("Open", key=f"button_{index}_{row['gcode']}"):
                    st.session_state["selected_gcode"] = row['gcode']
                    st.switch_page("pages/vars_page.py")

    else:
        for index, row in a.groups_df.iterrows():
            col1, col2, col3 = st.columns([1, 8, 1])
            col1.markdown(f"**{row['gcode']}**")
            col2.markdown(row['description'])
            with col3:
                if st.button("Open", key=f"button_{index}_{row['gcode']}"):
                    st.session_state["selected_gcode"] = row['gcode']
                    st.switch_page("pages/vars_page.py")

        st.markdown("---")
