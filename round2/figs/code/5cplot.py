import pandas as pd
import matplotlib.pyplot as plt
import matplotlib

# Load the CSV file
csv_file_path = "/data/experiment-kit/round2/results/5c_all.csv"
df = pd.read_csv(csv_file_path)

cur_plot = "t_res"
x = "s_res"
line = "sys"
y = "total_time"

# Get unique plot values
unique_plots = df[cur_plot].unique()

marker_size = 25
m_fill = "none"
font_size = 30
tick_font_size = 30
tick_size = 30
tick_list = [0.25, 0.5, 1.0]
tick_labels = tick_list
line_width = 4
above = "bottom"
below = "top"
y_label = "Execution time (sec)"
viridis = matplotlib.colormaps["viridis"]
colors = [viridis(i) for i in [0, 0.25, 0.5, 0.75]]
x_label = "Spatial resolution (degrees)"

# Define style dictionary based on 'line' values
style_dict = {
    "Polaris": {"marker": "o", "markersize": marker_size, "linewidth": line_width, "color": "red", "labelsize": font_size, "ticksize": tick_size, "ticklist": tick_list, "ticklabels": tick_labels},
    "Vanilla": {"marker": "v", "markersize": marker_size, "linewidth": line_width, "color": colors[1], "labelsize": font_size, "ticksize": tick_size, "ticklist": tick_list, "ticklabels": tick_labels},
    "TDB": {"marker": "s", "markersize": marker_size, "linewidth": line_width, "color": colors[3], "labelsize": font_size, "ticksize": tick_size, "ticklist": tick_list, "ticklabels": tick_labels},
}

# Determine global y-axis limits
y_min = df[y].min()
y_max = df[y].max()

# Generate and save individual plots
for plot_value in unique_plots:
    fig, ax = plt.subplots(figsize=(8, 6))
    subset = df[df[cur_plot] == plot_value] # df[df[t_res]]==hour
    
    for line_value in subset[line].unique():
        line_data = subset[subset[line] == line_value]  # df[df[system] == polaris]

        # if line_value == "Polaris":
        #     line_data = line_data.groupby(x, as_index=False)["tr"].mean()   # Average over tr values
        # else:
        line_data = line_data.groupby(x, as_index=False)[y].mean()  # Average over x values
        
        line_data = line_data.sort_values(by=x)  # Ensure lines are connected correctly

        # Get style properties from dictionary, use defaults if not found
        style = style_dict.get(line_value, {"marker": "o", "markersize": 4, "linewidth": 1.5, "color": "black", "labelsize": 10, "ticksize": 8})
        
        # if line_value == "Polaris":
        #     ax.plot(line_data[x], line_data["tr"], 
        #             marker=style["marker"], markersize=style["markersize"], fillstyle=m_fill,
        #             linewidth=style["linewidth"], color=style["color"], label=f"{line_value}")
        # else:
        ax.plot(line_data[x], line_data[y], 
                marker=style["marker"], markersize=style["markersize"], fillstyle=m_fill,
                linewidth=style["linewidth"], color=style["color"], label=f"{line_value}")
    
    ax.set_xlabel(x_label, fontsize=font_size)
    if style["ticklist"] is not None:
        ax.set_xticks(ticks=style["ticklist"], labels=style["ticklabels"])
    ax.set_ylabel(y_label, fontsize=font_size)
    ax.set_yscale("log")  # Set y-axis to log scale
    ax.set_ylim(y_min, y_max)
    ax.legend(fontsize=font_size)
    ax.tick_params(axis='both', labelsize=tick_font_size)
    
    # test
    # plt.tight_layout()
    # plt.savefig(f"/data/experiment-kit/round2/figs/f1_test/5c_{plot_value}.png")  # Save the plot to a file
    # plt.close(fig)

    # final
    plt.tight_layout()
    plt.savefig(f"/data/experiment-kit/round2/figs/5c_eps/5c_{plot_value}.eps")  # Save the plot to a file
    plt.close(fig)
