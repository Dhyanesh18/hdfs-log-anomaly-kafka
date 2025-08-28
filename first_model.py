import os
import pandas as pd
import joblib
import mlflow
import mlflow.sklearn
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report
from sklearn.model_selection import train_test_split

os.makedirs("mlruns", exist_ok=True)

# Point MLflow to local directory store
mlflow.set_tracking_uri("file:./mlruns")

def train_model(data_path, version):
    # Load dataset
    df = pd.read_csv(data_path)
    X = df.drop(columns=["BlockId", "Label", "Type"]).astype("float64")
    y = df["Label"].map({"Success": 0, "Fail": 1})

    # Train/test split
    X_train, X_val, y_train, y_val = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    # Train model
    clf = RandomForestClassifier(
        n_estimators=50,
        class_weight="balanced",
        random_state=40,
        n_jobs=-1
    )
    clf.fit(X_train, y_train)

    # Evaluate
    y_pred = clf.predict(X_val)
    report = classification_report(y_val, y_pred, digits=4, output_dict=True)
    print(classification_report(y_val, y_pred, digits=4))

    # Ensure models folder exists
    os.makedirs("models", exist_ok=True)

    # Save model with joblib (optional since MLflow already logs it)
    model_path = f"models/model_base.joblib"
    joblib.dump(clf, model_path)

    # Log to MLflow
    mlflow.set_experiment("hdfs_anomaly_detection")
    with mlflow.start_run(run_name=f"rf_{version}"):
        mlflow.log_param("n_estimators", 50)
        mlflow.log_param("class_weight", "balanced")
        mlflow.log_param("train_size", len(X_train))
        mlflow.log_param("val_size", len(X_val))
        mlflow.log_metric("precision_fail", report["1"]["precision"])
        mlflow.log_metric("recall_fail", report["1"]["recall"])
        mlflow.log_metric("f1_fail", report["1"]["f1-score"])

        # Fix warning: use "name" instead of deprecated artifact_path
        mlflow.sklearn.log_model(clf, name="random_forest_model", 
                                    input_example=X_train.iloc[:1])

    return clf

# Run it
clf = train_model("preprocessed/Event_occurrence_matrix_train.csv", version="v1")