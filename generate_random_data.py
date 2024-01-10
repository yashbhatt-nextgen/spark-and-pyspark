import pandas as pd
import random
from datetime import datetime, timedelta

def generate_unique_dates(start_date, end_date, num_dates, max_iterations=10000):
    date_set = set()
    iterations = 0
    while len(date_set) < num_dates and iterations < max_iterations:
        random_days = random.randint(0, (end_date - start_date).days)
        random_date = start_date + timedelta(days=random_days)
        formatted_date = random_date.strftime("%d-%m-%Y")
        date_set.add(formatted_date)
        iterations += 1

    if iterations == max_iterations:
        print("Maximum iterations")

    return list(date_set)

data = {
    'IMSI': [random.randint(100, 999) for _ in range(50)],
    'Activation_date': generate_unique_dates(datetime(2023, 1, 1), datetime(2024, 1, 1), 50),
    'account_number': [random.randint(1,10) for _ in range(50)] 
}

df = pd.DataFrame(data)

for i in range(5):
    start_index = i * 10
    end_index = start_index + 10
    df_subset = df.iloc[start_index:end_index]
    df_subset.to_csv(f'data_{i + 1}.csv', index=False)


print(df)
