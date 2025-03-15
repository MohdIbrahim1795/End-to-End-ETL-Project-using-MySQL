import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Generate old dataset (3 columns)
old_data = pd.DataFrame({
    'timestamp': pd.date_range(start='2020-01-01', end='2020-12-31', freq='D'),
    'value': np.random.randint(0, 100, size=366),
    'category': np.random.choice(['A', 'B', 'C'], 366)
})

# Generate new dataset (5 columns)
new_data = pd.DataFrame({
    'timestamp': pd.date_range(start='2023-01-01', end='2023-12-31', freq='D'),
    'value': np.random.randint(0, 100, size=365),
    'category': np.random.choice(['A', 'B', 'C'], 365),
    'new_column1': np.random.normal(0, 1, size=365),
    'new_column2': np.random.choice([True, False], size=365)
})

# Save to CSV files
old_data.to_csv("old_data.csv", index=False)
new_data.to_csv("new_data.csv", index=False)

print("Old and new datasets generated and saved to CSV files.")