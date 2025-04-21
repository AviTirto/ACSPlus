import streamlit as st

# Example label
label = "Estimate!!Total!!Income in the past 12 months at or above poverty level!!Other family!!Male householder, no wife present!!Without Social Security income in the past 12 months"

# Step 1: Split the label into levels (and remove "Estimate")
levels = label.split("!!")
if levels[0] == "Estimate":
    levels = levels[1:]

# Step 2: Create cascading dropdowns
selected_levels = []
current_options = levels.copy()
st.write(current_options)
for i in range(len(current_options)):
    if i == 0:
        options = list(set([l.split("!!")[i] for l in [label] if len(l.split("!!")) > i]))
    else:
        prefix = "!!".join(selected_levels)
        options = list(set([
            l.split("!!")[i] for l in [label]
            if l.startswith("Estimate!!" + prefix) and len(l.split("!!")) > i
        ]))

    if not options:
        break

    selection = st.selectbox(f"Level {i + 1}", options, key=f"level_{i}")
    selected_levels.append(selection)

# Final result
if selected_levels:
    st.write("You selected:", " ➝ ".join(selected_levels))
