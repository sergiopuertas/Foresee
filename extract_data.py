import pandas as pd

def load_data():
    data = pd.read_csv('data.csv')
    return data

def select_rows(data):
    filtered_data = data[data['Crm Cd Desc'].str.contains('VEHICLE') & (pd.to_datetime(data['DATE OCC']).dt.year != 2024)]
    filtered_data = filtered_data[['DATE OCC', 'Crm Cd Desc','AREA NAME']]
    filtered_data['raw_pond'] = filtered_data.apply(
        lambda row: 0.035 if 'ATTEMPT' in row['Crm Cd Desc'] or 'PETTY' in row['Crm Cd Desc'] or 'THROWING' in row[
            'Crm Cd Desc'] else
        0.1 if 'BURGLARY' in row['Crm Cd Desc'] else
        0.125 if 'SHOTS' in row['Crm Cd Desc'] else
        0.2,
        axis=1
    )

    # Normalizar los pesos para que la media sea 1
    avg_weight = filtered_data['raw_pond'].mean()
    filtered_data['pond'] = filtered_data['raw_pond'] / avg_weight
    return filtered_data


if __name__ == '__main__':
    data = load_data()
    data = select_rows(data)
    data.to_csv('data_cleaned.csv')