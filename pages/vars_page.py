import streamlit as st
from acs1 import ACS1

def update_selected(index):
    checkbox_key = f"var_checkbox_{index}"
    # Check the state of the checkbox directly in session_state
    if st.session_state.get(checkbox_key):
        if index not in st.session_state.selected_vars:
            st.session_state.selected_vars.append(index)
    else:
        if index in st.session_state.selected_vars:
            st.session_state.selected_vars.remove(index)

st.set_page_config(layout='wide')

st.title("Variables for Group Code")

selected = st.session_state.get("selected_gcode")

if not selected:
    st.warning("No group code selected. Please return to the main page.")
    if st.button("⬅️ Back to Main Page"):
        st.session_state.pop("selected_gcode", None)
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

    header1, header2, header3, header4 = st.columns([1, 4, 4, 1])
    header1.markdown("**Code**")
    header2.markdown("**Concept**")
    header3.markdown("**Label**")
    header4.markdown("")

    with st.container(height=500):
        for index, row in vars_df.iterrows():
            col1, col2, col3, col4 = st.columns([1, 4, 4, 1])
            col1.markdown(f"**{index}**")
            col2.markdown(row['concept'])
            col3.markdown(row['label'])
            checkbox_key = f"var_checkbox_{index}"
            with col4:
                # Use the current checkbox value in session_state and connect the on_change
                st.checkbox(
                    "",
                    key=checkbox_key,
                    value=index in st.session_state.selected_vars,
                    on_change=update_selected,
                    args=(index,)
                )

            st.markdown("---")
        
    if st.button("Generate"):
        st.write(st.session_state.selected_vars)

