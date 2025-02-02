"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel
import pandas as pd
import zipfile
import os
from datetime import datetime
def clean_campaign_data():

    output_dir = "files/output/"
    os.makedirs(output_dir, exist_ok=True)
    
    input_dir = "files/input/"

    client_records = []
    campaign_records = []
    economics_records = []

    for zip_filename in os.listdir(input_dir):
        if not zip_filename.endswith(".zip"):
            continue
        
        with zipfile.ZipFile(os.path.join(input_dir, zip_filename), 'r') as zip_ref:
 
            for csv_filename in zip_ref.namelist():
                with zip_ref.open(csv_filename) as csv_file:
                    df = pd.read_csv(csv_file)
                    
                    for _, row in df.iterrows():
                        education = row["education"].replace(".", "_")
                        if education == "unknown":
                            education = pd.NA
                        client_records.append({
                            "client_id": row["client_id"],
                            "age": row["age"],
                            "job": row["job"].replace(".", "").replace("-", "_"),
                            "marital": row["marital"],
                            "education": education,
                            "credit_default": 1 if row["credit_default"] == "yes" else 0,
                            "mortgage": 1 if row["mortgage"] == "yes" else 0
                        })
                    
                    for _, row in df.iterrows():
                        campaign_records.append({
                            "client_id": row["client_id"],
                            "number_contacts": row["number_contacts"],
                            "contact_duration": row["contact_duration"],
                            "previous_campaign_contacts": row["previous_campaign_contacts"],
                            "previous_outcome": 1 if row["previous_outcome"] == "success" else 0,
                            "campaign_outcome": 1 if row["campaign_outcome"] == "yes" else 0,
                            "last_contact_date": datetime.strptime(f"2022-{row['month']}-{row['day']}", "%Y-%b-%d").strftime("%Y-%m-%d")
                        })
                    
                    for _, row in df.iterrows():
                        economics_records.append({
                            "client_id": row["client_id"],
                            "cons_price_idx": row["cons_price_idx"],
                            "euribor_three_months": row["euribor_three_months"]
                        })

    client_df = pd.DataFrame(client_records)
    campaign_df = pd.DataFrame(campaign_records)
    economics_df = pd.DataFrame(economics_records)

    client_df.to_csv(os.path.join(output_dir, "client.csv"), index=False)
    campaign_df.to_csv(os.path.join(output_dir, "campaign.csv"), index=False)
    economics_df.to_csv(os.path.join(output_dir, "economics.csv"), index=False)

    print("Data processing complete. Files saved in 'files/output/'.")

if __name__ == "__main__":
    clean_campaign_data()
