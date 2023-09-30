import glob
import pandas as pd
from datetime import datetime

#The downloaded files will be in the folder "raw data"
raw_data = glob.glob('raw_data/*.csv')

#Function to assign a name to each entry of this record, so it allows then to group the info by name, no matter the ammount files processed
def nombres(file):
    for n in file:
        read_file = pd.read_csv(n, delimiter=',')

        rute = n.split('raw_data\\')[1]
        name = rute.split('.csv')[0]

        read_file.insert(0, 'Name', name)
        read_file.to_excel(r'named_data/'+ name +'.xlsx', index=False)

#The named files will be in the folder "named data"
named_data = glob.glob('named_data/*.xlsx')

#Function to count the amount of calls and group by name and date
def conteo(file):
    #Read the file and create a new table with the important info (Name, date and the required columns for the call count) 
    read_file = (pd.read_excel(x) for x in file)
    m_read_file = pd.concat(read_file, ignore_index=True)
    new_table = m_read_file[['Name','Source UserId', 'Result', 'Date/Time']].copy()

    #Filter the data so we only get the calls we need, i.e., the calls that were answered, no matter if they were transfered or not 
    filtered_table = new_table[(new_table['Result'] == 'answered') | ((new_table['Source UserId'].notna()) & (new_table['Result'] == 'voicemail'))]

    #Give the date a valid format
    date = []
    for n in filtered_table['Date/Time']:
        date_no_time = n.split(" ")[0]
        date_format = '%m/%d/%Y'
        date_obj = datetime.strptime(date_no_time, date_format)
        date_obj = date_obj.date()
        date.append(date_obj)

    #Add the correct date as a new column
    filtered_table.insert(len(filtered_table.columns), 'Fecha', date)
    
    #Group the call count by name and date
    count = filtered_table.groupby(['Name', 'Fecha'])['Result'].count().reset_index()
    count.columns = ['Nombre', 'Fecha', 'Llamadas']

    #Sort it by name
    count = count.sort_values(by=['Nombre'], ascending=True)

    #Pivot the dates, so there's a column per date, which makes it easier to read
    pivot_count = count.pivot(index='Nombre', columns='Fecha', values='Llamadas').reset_index()

    #Fill empty values with 0
    pivot_count.fillna(0, inplace=True)

    #Export the document
    pivot_count.to_excel("added_data.xlsx", index=False)

nombres(raw_data)
conteo(named_data)