import pandas as pd

# Load the CSV file
df = pd.read_csv("/home/uribe055/experiment-kit/round2/results/5c_all.csv")

# Define columns
x = "t_res"
y = "total_time"
line = "sys"

df_filtered = df[df[line] == "Polaris"]
# df_filtered = df_filtered[df_filtered["s_res"]=="day"]

# Create pivot table with mean values
matrix = df_filtered.pivot_table(index="s_res", columns=x, values=y, aggfunc="max")

# Display the result
print(matrix)
