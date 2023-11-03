# Goal : To determine if a safety-related defect trend exists

""" 
# Goal: Clean/transform/store data in a specific format, to analyse the trend (of complaints) across the years for each component (in product models)
Creator: EPP

"""


import requests as r
import pandas as pd
import datetime as dt
import connector


model_year = "2021"
make = "JEEP"
model = "WRANGLER"


def get_data(url_complaints):
        """
        Retrieves data from a given API URL using the 'requests' library.

        Parameters:
        url_complaints (str): The URL of the API endpoint containing complaint data.

        Returns:
        pandas.DataFrame: A DataFrame containing the complaint data extracted from the API.

        This function sends an HTTP GET request to the specified URL, retrieves the data, and converts it into a pandas DataFrame. It handles potential HTTP exceptions and raises an error if the request is unsuccessful.
        """

        # Catch HTTP (400, 500, etc..) exceptions that may occur
        try:
                resp_complaints = r.get(url_complaints)
                resp_complaints.raise_for_status()
                return pd.DataFrame(resp_complaints.json()['results'])

        except r.exceptions.RequestException as e:
                raise SystemExit(e)



def process_data(df_complaints):

        """
        Process and clean a dataset of complaints for analysis.

        Parameters:
        df_complaints (pandas.DataFrame): The dataset containing complaint information.

        Returns:
        pandas.DataFrame: A cleaned and processed DataFrame ready for analysis.

        This function performs several data processing and cleaning operations on the input DataFrame.
        It casts date columns to datetime format, normalizes data from a dictionary in a list, removes unnecessary columns, concatenates columns from the normalized data, 
                and filters the DataFrame based on the specified product model and make.
        The resulting DataFrame is prepared for analysis of complaints.

        Args:
        df_complaints (pandas.DataFrame): The dataset containing complaint information.

        Returns:
        pandas.DataFrame: A cleaned and processed DataFrame for complaints analysis.

        Example:
        >> import pandas as pd
        >> df = pd.read_csv("complaints_data.csv")
        >> cleaned_data = process_data(df)
        >> print(cleaned_data)
        """

        # Cast to datetime
        df_complaints[['dateComplaintFiled','dateOfIncident']] = df_complaints[['dateComplaintFiled','dateOfIncident']].apply(pd.to_datetime, format='%m/%d/%Y')
        
        # Get dict from list
        df_complaints= df_complaints.explode(column='products', ignore_index=True)

        # Each field in dict -> new column
        normalized_df = pd.json_normalize(df_complaints['products'])

        # remove columns
        normalized_df = normalized_df.drop(columns=['manufacturer', 'size'] , axis=1)

        # Concat columns from the normalized df
        df_complaints = pd.concat([df_complaints, normalized_df], axis=1)
        df_complaints = df_complaints.drop(columns=['products'] , axis=1)

        # remove null values
        df_complaints = df_complaints[df_complaints['components'].notna()]

        # Cast the column values to a list -> then I can apply "explode" 
        df_complaints["components"] = df_complaints["components"].apply(lambda x: x.split(","))
        df_complaints = df_complaints.explode("components")
        df_complaints["components"] = df_complaints["components"].str.strip()

        # Filter the dataframe (its returning more data than the models we requested)
        df_complaints = df_complaints[(df_complaints['productModel']==model) & (df_complaints['productMake']==make)]

        return df_complaints

        

def transform_data(df_process_data):
        """
        Transform and aggregate a processed dataset for complaint analysis.

        Parameters:
        df_process_data (pandas.DataFrame): The processed dataset containing complaint information.

        Returns:
        pandas.DataFrame: An aggregated DataFrame ready for complaint analysis.

        This function takes the input DataFrame and aggregates the data to analyze the number of complaints for each component of a product model across different manufacturers, makes,
                 model years, and the extraction date.
        It groups the data by manufacturer, product make, product model, product year, and components, counting the number of complaints.
        The resulting DataFrame is suitable for analyzing and visualizing complaints data trends.

        Args:
        df_process_data (pandas.DataFrame): The processed dataset containing complaint information.

        Returns:
        pandas.DataFrame: An aggregated DataFrame for complaints analysis.

        Example:
        >> import pandas as pd
        >> df = pd.read_csv("processed_complaints_data.csv")
        >> aggregated_data = transform_data(df)
        >> print(aggregated_data)
        """
                
        df_process_data = df_process_data.groupby(["manufacturer", "productMake", "productModel", "productYear", "components"]).size().to_frame("numComplaints").reset_index()
        df_process_data["dateExtraction"] = dt.date.today()

        return df_process_data




if __name__ == "__main__":


        try:
                print("Executing script...")
                
                url_complaints = f"https://api.nhtsa.gov/complaints/complaintsByVehicle?make={make}&model={model}&modelYear={model_year}"
                df_complaints = get_data(url_complaints)
                
                df_proc_complaints = process_data(df_complaints)
                df_trans_complaints = transform_data(df_proc_complaints)
                
                # Saves data to csv
                df_proc_complaints.to_csv(r"datasets\complaints\df_proc_complaints.csv", index = False)
                df_trans_complaints.to_csv(r"datasets\complaints\df_trans_complaints.csv", index = False)

                # Uploads data to the database
                connector.main(df_trans_complaints, "df_trans_complaints")

                print("** Finished **")
        except:
                print("Something went wrong")


