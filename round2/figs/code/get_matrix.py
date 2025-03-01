import pandas as pd

# Load the CSV file
df = pd.read_csv("/home/uribe055/experiment-kit/round2/results/5c_all.csv")

# Define columns
x = "s_res"
y = "total_time"
line = "sys"

df_filtered = df[df[line] == "Polaris"]

# Create pivot table with mean values
matrix = df_filtered.pivot_table(index="t_res", columns=x, values=y, aggfunc="mean")

# Display the result
print(matrix)
