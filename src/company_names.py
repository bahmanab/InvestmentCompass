from sqlalchemy import create_engine
import wget
import os
import pandas as pd

engine = create_engine('postgresql://postgres:'+"seCurepassword"+'@localhost:5432/edgar')

# download the cik lookup data
url = 'https://www.sec.gov/Archives/edgar/cik-lookup-data.txt'
output_directory = '/home/ubuntu/InsightDataEngineeringProject'
os.system('rm ' + os.path.join(output_directory, 'cik-lookup-data.txt'))  # remove current copy to avoid duplicate
filename = wget.download(url, out=output_directory)
print(os.path.join(output_directory, filename))

# parse the cik lookup data and extract name and cik
i = 0
rows_list = []
added_cik = set()
with open(os.path.join(output_directory, filename), encoding="ISO-8859-1") as input_file:
    print('Reading the input file...')
    for line in input_file:
        i += 1
        company_name, cik = line.split(':')[:2]
        if cik not in added_cik:
            line_dict = {'company_name': company_name.strip(), 'cik': cik.strip()}
            rows_list.append(line_dict)
            added_cik.add(cik)
print('Reading the input file is completed.')

columns = ['company_name', 'cik']
df = pd.DataFrame(rows_list, columns=columns)

# write the company_name_cik dataframe to the database
print('Writing the records to database ...')
df.to_sql('company_name_cik', engine, if_exists="replace")
print('Writing the records to database is completed.')
