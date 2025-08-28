import pandas as pd
from sklearn.model_selection import train_test_split

# Load your dataset
df = pd.read_csv("preprocessed/Event_occurrence_matrix.csv")

# Split 70% train, 30% test (stratify on Label for balanced classes)
train_df, test_df = train_test_split(
    df,
    test_size=0.3,
    random_state=42,
    stratify=df["Label"]  # ensures Fail/Success ratio stays the same
)

# Save to CSV
train_df.to_csv("preprocessed/Event_occurrence_matrix_train.csv", index=False)
test_df.to_csv("preprocessed/Event_occurrence_matrix_test.csv", index=False)

print(f"Train shape: {train_df.shape}")
print(f"Test shape: {test_df.shape}")
