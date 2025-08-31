import pandas as pd
from utils.train import train_model 

df = pd.read_csv("preprocessed/Event_occurrence_matrix_train.csv")

clf, report = train_model(df, version="v1")

print("Training done. Model saved and metrics logged to MLflow.")
