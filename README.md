# ACSPlus

ACSPlus is a Streamlit application designed to efficiently retrieve and scrape data from the ACS 1-Year API. It compiles and caches group codes that map to tables in ACS1 and also caches variable codes. This functionality enables users to select multiple codes and scrape values for all available years corresponding to these variables.

Currently, the application is tailored for a **BioKind Analytics** project at **UW-Madison**, and it primarily collects data at the **county level**.

## Features
- **Group Code Mapping**: Compiles and caches all group codes that map to tables in the ACS1 dataset.
- **Variable Code Mapping**: Compiles and caches variable codes, making it easier to select and retrieve data.
- **Multi-Year Support**: Allows users to select multiple codes and scrape data across all years available for the given variables.
- **County-Level Data**: Currently collects data on a county level, customized for BioKind Analytics' project.

## Installation

### Python Dependencies

First, install the required Python dependencies:

```bash
python3 -m pip install -r requirements.txt
```

## Usage

To run the application locally, execute the following command:

```bash
streamlit run --client.showSidebarNavigation=False table_page.py
```

This will launch the Streamlit app without displaying the sidebar navigation.

## Project Structure

- `table_page.py`: The main Streamlit app page that loads and displays the data.
- `requirements.txt`: Contains all the required Python dependencies for the project.

## Contributing

If you'd like to contribute to this project, feel free to open an issue or submit a pull request. Any contributions are welcome!

---

Let me know if you need anything else or further refinements!