import pandas as pd

# Create data with mixed types in same columns
data = {
    'id': [1, 2, 3, 4, 5],
    'mixed_field': [123, "text", 456, "another_text", 789],  # int and string
    'nullable_int': [100, None, 200, "300", 400],  # int, null, string
    'date_field': ['2023-01-01', '2023-02-01', 20230301, '2023-04-01', None]  # string, int, null
}

df = pd.DataFrame(data)
df.to_csv('/tmp/choice_data.csv', index=False)
print("Choice data created with mixed types")