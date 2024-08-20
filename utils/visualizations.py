
import matplotlib as mpl
# reset 
import matplotlib.pyplot as plt
import seaborn as sns
import math

df_titles = [
    "Sales Volume",
    "Complaints",
    "Citibike",
    "Operating Businesses",
    "Evictions",
    "Health Inspections",
]

df_colors = [
    "#1f77b4",
    "#ff7f0e",
    "#2ca02c",
    "#d62728",
    "#9467bd",
    "#8c564b",
    "#e377c2",
    "#7f7f7f",
    "#bcbd22",
    "#17becf",
]

def plot_all_time_series(processed_data, title="Time Series of Features on Housing Market Sales", for_ieee=False, save_path=None):
    norm = (processed_data - processed_data.mean()) / processed_data.std()
    num_cols = len(norm.columns)
    rows = math.ceil(num_cols / 2)

    plt.figure(figsize=(12, 16))
    plt.suptitle(title, fontsize=14)

    # Plot rei
    plt.subplot(rows + 1, 2, 1)
    plt.plot(norm.index, norm["avg_sales"],color="black", alpha=1, linestyle="--")
    plt.title("Real Estate Index", fontsize=10)
    plt.ylabel("Percent Change", fontsize=10)

    # feature dists
    plt.subplot(rows + 1, 2, 2)
    for i, col in enumerate(norm.columns[1:]):
        sns.histplot(norm[col], label=df_titles[i], color=df_colors[i % len(df_colors)], kde=False)
    plt.title("Feature Distributions", fontsize=10)
    plt.legend()

    # Plot each feature ts
    for i, col in enumerate(norm.columns[1:], start=3):
        plt.subplot(rows + 1, 2, i)
        dates = norm[norm[col].notnull()].index
        plt.plot(dates, norm["avg_sales"][dates], label="Real Estate Index", color="black", alpha=0.7, linestyle="--")
        plt.plot(dates, norm[col][dates], alpha=0.7, label=df_titles[i-3], color='red', )
        plt.title(df_titles[i-3], fontsize=10)
        plt.ylabel("Percent Change", fontsize=10)

        for ind, label in enumerate(plt.gca().get_xticklabels()):
            label.set_visible(ind % 2 == 0)

    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches="tight")
    plt.tight_layout(pad=2)
    plt.show()
