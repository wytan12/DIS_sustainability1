import pandas as pd
import datetime

def calculate_carbon(row, variable, intensity):
    """
    Calculate the carbon emissions for a given row of data based on 'energy' and 'water'.
    Args:
    row: An object represents a row of dataframe that includes key like month, energy and water. 
    variable: A string as the type of variable to calculate emissions for 'energy' or 'water'.
    intensity: A dictionary with the value'grid_emission_factor' for energy and 'water_factor' for water.
    Returns: A float that calculated carbon emissions based on the input row, variable and intensity.
    """
    year = row["month"].year
    if variable == "energy":
        factor_index = intensity[year]['grid_emission_factor']
        return row["energy"] * factor_index
    else:
        factor_index = intensity[year]['water_factor']
        return row["water"] * factor_index

def calculate_working_days(dataframe):
    """
    Calculates the number of working days for each month in a DataFrame.
    Args: dataframe: A DataFrame with a 'month' column containing datetime objects.
    Returns: A DataFrame with the same index as the input DataFrame, containing a
    'working_days' column with the number of working days for each month.
    """
    dataframe['working_day'] = 0
    for index, row in dataframe.iterrows():
        month = row['month']
        start_date = month.replace(day=1)
        end_date = start_date + pd.offsets.MonthEnd(0)

        # Exclude weekends
        weekdays = pd.date_range(start_date, end_date, freq='B')

        # Exclude public holidays
        holidays = df_holiday[(df_holiday['Date'].dt.month == month.month) & (df_holiday['Year'] == month.year)]['Date'].tolist()

        working_days = len(weekdays) - len(holidays)

        dataframe.at[index, 'working_day'] = working_days
    return dataframe

# read and load the necessary excel file 
df_holiday = pd.read_excel("store/MOM_PublicHoliday.xlsx")
df_basic = pd.read_excel("store/basic_data.xlsx")
df_basic.set_index("code", inplace=True)
data_basic = df_basic.to_dict(orient="index")
df_intensity = pd.read_excel("store/basic_data.xlsx", sheet_name='power')
df_intensity.set_index("year", inplace=True)
data_intensity = df_intensity.to_dict(orient='index')

dataframe_building = {}

for codes, details in data_basic.items():
    data_building = data_basic[codes]
    df_building = pd.read_excel("store/singland mock.xlsx", sheet_name=data_building['tab'], skiprows=11)

    # only keep columns that have value
    df_building = df_building[['Month',
                               'Total building energy consumption (TBEC) (kWh/month)',
                               'Total water consumption (m3/mth)', 'No of Working days']]

    # rename the columns
    df_building.columns = ["month", "energy", "water", "working_day"]

    # drop the row if the "Month" column is not date
    z = df_building.to_dict("dict")
    row_to_drop = []
    for keys, values in z["month"].items():
        if not isinstance(values, datetime.datetime):
            row_to_drop.append(keys)
    df_building = df_building.drop(index=row_to_drop)

    # drop na based on energy and water
    df_building.dropna(subset=['energy', 'water'], inplace=True)

    df_building.reset_index(inplace=True, drop=True)
    last_row_index = df_building.index[-1]

    # calculate working days within each month. Exclude every Saturday, Sunday, and public holiday
    df_building = calculate_working_days(df_building)

    # calculate carbon emission
    df_building['carbon_energy'] = df_building.apply(lambda x: calculate_carbon(x, 'energy', data_intensity), axis=1)
    df_building['carbon_water'] = df_building.apply(lambda x: calculate_carbon(x, 'water', data_intensity), axis=1)

    # create a codes column to store the sheetname
    df_building['codes'] = data_building['tab']

    dataframe_building[data_building['tab']] = df_building

# combine all DataFrames into a single DataFrame
combined_df = pd.concat(dataframe_building.values(), ignore_index=True)

output_file_path = 'store/clean_data.xlsx'
combined_df.to_excel(output_file_path, sheet_name="Summary", index=False)
