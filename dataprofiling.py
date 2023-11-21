import pandas as pd
from datetime import datetime, timedelta

def save_to_csv(df, filename):
    """
    Save DataFrame to CSV.

    Args:
    - df (DataFrame): Data to save.
    - filename (str): Name of the CSV file.
    """
    df.to_csv(filename, index=False)
    print(f"Data saved to {filename}.")

df = pd.read_csv("C:/Users/rolls/Downloads/streaming_viewership_data.csv")
profile_report = pd.DataFrame(columns=['Attribute', 'Count', 'Missing Values'])
# Profile each attribute
for column in df.columns:
        count = len(df)
        missing_values = df[column].isnull().sum()
        unique_values = df[column].nunique() if not isinstance(df[column].iloc[0], list) else None
        if not isinstance(df[column].iloc[0], list):
            profile_report = pd.concat([profile_report, pd.DataFrame([{
                'Attribute': column,
                'Count': count,
                'Missing Values': missing_values,
                'Unique Values': unique_values
            }])], ignore_index=True)
current_time = datetime.now().strftime('%Y%m%d_%H%M')
output_path = "./output"
filename = f"streaming_ott_{current_time}_data_profiling.csv"
file_path = f"{output_path}/{filename}"
save_to_csv(profile_report, file_path)

