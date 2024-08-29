import pandas as pd 
import seaborn as sns 
import matplotlib.pyplot as plt 

feature_columns = [e for e in df.columns if e not in ["rowkey"] ] 
pandas_df = df.select( feature_columns ).sample(0.01).toPandas()



correlation_matrix = pandas_df.corr()

plt.figure(figsize=(15, 6)) 
sns.heatmap(correlation_matrix, cmap='coolwarm', fmt=".2f", linewidths=.5) 

for i in range(len(correlation_matrix)): 
    for j in range(len(correlation_matrix.columns)): 
        plt.text(j + 0.5, i + 0.5, f"{correlation_matrix.iloc[i, j]:.2f}", 
                 ha='center', va='center', color='black') 

plt.tick_params(axis='x', which='both', bottom=True, top=True) 
plt.xticks(rotation=15) 

plt.show() 