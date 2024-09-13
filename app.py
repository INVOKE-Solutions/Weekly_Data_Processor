import streamlit as st
import pandas as pd
import json
import googlemaps
from datetime import datetime
import io  # For handling in-memory file objects

# Initialize Google Maps Client
def init_gmaps_client():
    """Initialize Google Maps client with API key from Streamlit secrets."""
    try:
        key = st.secrets["google_maps_api_key"]["google_maps_api_key"]
        return googlemaps.Client(key=key)
    except KeyError as e:
        st.error(f"Missing key in secrets file: {e}")
    except Exception as e:
        st.error(f"Error initializing Google Maps client: {e}")
    return None

gmaps = init_gmaps_client()

# Function to convert JSON data to a DataFrame
def json_to_dataframe(json_data):
    """Convert JSON data to a pandas DataFrame."""
    try:
        data = json.loads(json_data)
        df = pd.json_normalize(data)
        return df
    except json.JSONDecodeError:
        st.error("Failed to decode JSON. Please check your file.")
    except Exception as e:
        st.error(f"Error converting JSON to DataFrame: {e}")
    return None

# Function to drop specified columns from the DataFrame
def drop_columns(dataframe, columns_to_drop):
    """Drop specified columns from the DataFrame."""
    return dataframe.drop(columns=columns_to_drop, axis=1, errors='ignore')

# Function to reorder the DataFrame columns based on original order
def reorder_columns(dataframe):
    """Reorder DataFrame columns according to the specified list."""
    original_order = [
        'program', 'date', 'ic', 'name', 'age', 'ethnicity', 'sex',
        'state', 'district', 'postcode', 'lat', 'lon', 'address', 'phone',
        'email', 'salary_monthly', 'miskin', 'miskin_tegar', 'str_mof', 'belum_disemak'
    ]
    existing_columns = [col for col in original_order if col in dataframe.columns]
    additional_columns = [col for col in dataframe.columns if col not in original_order]
    new_order = existing_columns + additional_columns
    return dataframe[new_order]

# Function to rename columns according to the provided mapping
def rename_columns(dataframe):
    """Rename columns based on the predefined mapping."""
    column_mapping = {
        'form_category': 'program',
        'createdAt': 'date',
        'ic_number': 'ic',
        'name': 'name',
        'race': 'ethnicity',
        'gender': 'sex',
        'state': 'state',
        'postcode': 'postcode',
        'address': 'address',
        'mobile_number': 'phone',
        'email': 'email',
        'monthly_income': 'salary_monthly'
    }
    dataframe = dataframe.rename(columns=column_mapping)
    return dataframe

# Function to rename values in the 'program' column
def rename_program_values(dataframe, value_mapping):
    """Rename values in the 'program' column based on the provided mapping."""
    if 'program' in dataframe.columns:
        dataframe['program'] = dataframe['program'].map(value_mapping).fillna(dataframe['program'])
    return dataframe

# Function to reformat the 'date' column by extracting the first 10 characters
def reformat_dates(dataframe):
    """Extract the first 10 characters from each date string in the 'date' column."""
    if 'date' in dataframe.columns:
        dataframe['date'] = dataframe['date'].astype(str).str[:10]
    return dataframe

# Function to format values in 'name' and 'address' columns to uppercase
def format_uppercase(dataframe):
    """Convert values in 'name' and 'address' columns to uppercase."""
    if 'name' in dataframe.columns:
        dataframe['name'] = dataframe['name'].str.upper()
    if 'address' in dataframe.columns:
        dataframe['address'] = dataframe['address'].str.upper()
    return dataframe

# Function to format phone numbers
def format_phone_numbers(dataframe):
    """Format phone numbers according to the specified rules."""
    if 'phone' in dataframe.columns:
        def format_number(number):
            number = str(number).strip()
            if number.startswith('1'):
                return '+60' + number
            return '+601' + number
        
        dataframe['phone'] = dataframe['phone'].apply(format_number)
    return dataframe

# Function to format salary values to two decimal places
def format_salary(dataframe):
    """Format values in the 'salary_monthly' column to two decimal places."""
    if 'salary_monthly' in dataframe.columns:
        dataframe['salary_monthly'] = dataframe['salary_monthly'].apply(
            lambda x: f"{float(x):.2f}" if pd.notna(x) and str(x).replace('.', '', 1).isdigit() else x
        )
    return dataframe

# Function to calculate and add the 'age' column based on the 'ic' column without altering the original 'ic' values
def age_format(dataframe):
    """Add an 'age' column based on the 'ic' column without altering the original IC numbers."""
    def calculate_age_from_ic(ic_number):
        if len(ic_number) != 12 or not ic_number.isdigit():
            return "IC ERROR"

        year_str = ic_number[:2]
        year_int = int(year_str)
        birth_year = 2000 + year_int if year_int <= 20 else 1900 + year_int
        current_year = 2024
        return current_year - birth_year

    # Add 'age' column without altering 'ic' column
    dataframe['age'] = dataframe['ic'].apply(calculate_age_from_ic)
    return dataframe

# Function to clean addresses
def clean_address(address_list):
    def process_address(address):
        address = address.replace('\n', ' ').replace(', ,', ',').rstrip(',').strip()
        if not address.endswith(', MALAYSIA'):
            address += ', MALAYSIA'
        return address

    return [process_address(address) for address in address_list]

# Function to get geocoding information
def geocode_address(address):
    """Geocode an address to get latitude and longitude with error status."""
    try:
        geocode_result = gmaps.geocode(address)
        if geocode_result:
            location = geocode_result[0]['geometry']['location']
            return 'success', location['lat'], location['lng']
        return 'no_result', None, None
    except googlemaps.exceptions.ApiError as e:
        return 'api_error', None, None
    except Exception as e:
        return 'error', None, None



# Function to load postcode data from a fixed path
def load_postcode_data():
    """Load postcode data from a fixed path."""
    postcode_file_path = "file/Malaysia-Postcodes-City-State-Mapping.xlsx"
    try:
        return pd.read_excel(postcode_file_path)
    except FileNotFoundError:
        st.error(f"Postcode file not found: {postcode_file_path}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Error loading postcode data: {e}")
        return pd.DataFrame()

def clean_and_process_dataframe(df, postcode_df):
    """Automatically drop unwanted columns and perform data cleaning while keeping IC numbers unchanged."""
    if df is None or df.empty:
        st.error("No data available to process.")
        return

    # Drop unwanted columns early
    unwanted_columns = [
        'form_id', 'user_id', 'proof_of_income', 'proof_of_income_type',
        'ic_image', 'status', 'is_b40', 'race_other'
    ]
    df = drop_columns(df, unwanted_columns)

    # Proceed with data cleaning
    df = rename_columns(df)
    program_mapping = {
        'food': 'INSAN',
        'agriculture': 'INTAN',
        'maintenance': 'IKHSAN'
    }
    df = rename_program_values(df, program_mapping)
    df = reformat_dates(df)
    df = format_uppercase(df)
    df = format_phone_numbers(df)
    df = format_salary(df)
    df = age_format(df)  # Calculate and add 'age' column based on IC

    if 'address' in df.columns:
        df['address'] = clean_address(df['address'])

        # Add a progress bar
        with st.spinner('Geocoding addresses...'):
            geocode_results = df['address'].apply(lambda addr: pd.Series(geocode_address(addr)))
            df[['geocode_status', 'lat', 'lon']] = geocode_results

    # Reorder columns based on the specified order
    df = reorder_columns(df)

    # Load postcode data and merge
    if postcode_df.empty:
        st.error("Postcode data could not be loaded.")
        return

    df['postcode'] = df['postcode'].astype(str).str.strip()
    postcode_df['postcode'] = postcode_df['postcode'].astype(str).str.strip()
    combined_df = pd.merge(df, postcode_df, on='postcode', how='left')

    # Rename columns if needed
    if 'city' in combined_df.columns:
        combined_df = combined_df.rename(columns={'city': 'district'})
    if 'state_x' in combined_df.columns:
        combined_df = combined_df.rename(columns={'state_x': 'state'})
    if 'state_y' in combined_df.columns:
        combined_df = combined_df.drop(columns=['state_y'])

    # Add new columns with default values (adjust values as needed)
    new_columns = {
        'miskin': '',
        'miskin_tegar': '',
        'str_mof': '',
        'belum_disemak': ''
    }
    for col, value in new_columns.items():
        combined_df[col] = value

    # Save the cleaned DataFrame to Excel
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        combined_df.to_excel(writer, index=False, sheet_name='CleanedData')
    
    # Set the cursor to the beginning of the BytesIO stream
    output.seek(0)

    # Save the cleaned DataFrame to session state
    st.session_state.df = combined_df

    # Display success message and updated data
    st.success("Data cleaned, geocoded, and combined with postcode information successfully!")
    st.write(f"### Updated Data Preview ({combined_df.shape[0]} rows)")
    st.dataframe(combined_df)

    # Display geocoding results
    geocoding_summary = combined_df['geocode_status'].value_counts()
    st.write("### Geocoding Summary")
    st.write(geocoding_summary)

    # Display addresses with errors
    if 'geocode_status' in combined_df.columns:
        error_addresses = combined_df[combined_df['geocode_status'] != 'success']
        st.write("### Addresses with Geocoding Errors")
        st.dataframe(error_addresses[['address', 'geocode_status']])

    # Provide download button for the cleaned data in Excel format
    st.download_button(
        label="Download Combined Data as Excel",
        data=output,
        file_name="combined_data_with_districts.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

def main():
    # Hide Streamlit header and footer with CSS
    hide_streamlit_style = """
        <style>
        header {visibility: hidden;}
        footer {visibility: hidden;}
        #root > div:nth-child(1) > div > div > div > div > section > div {padding-top: 0rem; padding-bottom: 4rem;}
        </style>
        """
    st.markdown(hide_streamlit_style, unsafe_allow_html=True)

    # Display the image and title at the top
    st.image("Photo/invoke_logo.png", use_column_width=True)
    st.title("Weekly Data Processor")
    
    # Password protection
    password = st.secrets["password"]["value"]
    entered_password = st.text_input("Enter password to access the app", type="password")
    
    if entered_password == password:
        # Show the rest of the app content after successful password entry
        st.write("Upload your JSON file and get the cleaned Excel output.")

        # Attempt to load postcode data
        postcode_df = load_postcode_data()

        uploaded_file = st.file_uploader("Choose a JSON file", type="json")

        if uploaded_file is not None:
            json_data = uploaded_file.read().decode("utf-8")
            df = json_to_dataframe(json_data)

            if df is not None:
                st.write(f"### Data Preview ({df.shape[0]} rows)")
                st.dataframe(df.head())
                st.write("Click the button below to process the data.")
                if st.button("Process Data"):
                    clean_and_process_dataframe(df, postcode_df)
    

if __name__ == "__main__":
    main()










