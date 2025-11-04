import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np

pdf = df.select(numeric_cols).toPandas()

# Compute correlation matrix
corr_matrix = pdf.corr(method="pearson")

# Plot heatmap without annotations
plt.figure(figsize=(10, 8))
ax = sns.heatmap(corr_matrix, cmap="coolwarm", annot=False, vmin=-1, vmax=1)

# Manually add correlation values
for i in range(corr_matrix.shape[0]):
    for j in range(corr_matrix.shape[1]):
        value = corr_matrix.iloc[i, j]
        ax.text(j + 0.5, i + 0.5, f"{value:.2f}", 
                color="white" if abs(value) > 0.5 else "black", 
                ha="center", va="center", fontsize=10)

# Axis labels and layout
ax.set_xticks(np.arange(len(corr_matrix.columns)) + 0.5)
ax.set_xticklabels(corr_matrix.columns, rotation=45, ha="right")
ax.set_yticks(np.arange(len(corr_matrix.index)) + 0.5)
ax.set_yticklabels(corr_matrix.index, rotation=0)
plt.title("Manually Annotated Correlation Heatmap")
plt.tight_layout()
plt.show()
