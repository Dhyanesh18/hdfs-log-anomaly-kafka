import os
import joblib
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
import mlflow
import mlflow.sklearn

def train_model(df_batch, version):
    """
    Train RandomForest on a batch of data and log to MLflow.
    Returns trained model and classification report.
    """
    X = df_batch.drop(columns=['BlockId', 'Label', 'Type'], errors='ignore').astype('float64')
    y = df_batch['Label'].map({'Success': 0, 'Fail': 1})

    X_train, X_val, y_train, y_val = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    clf = RandomForestClassifier(
        n_estimators=50,
        class_weight='balanced',
        random_state=42,
        n_jobs=-1
    )
    clf.fit(X_train, y_train)

    y_pred = clf.predict(X_val)
    report = classification_report(y_val, y_pred, digits=4, output_dict=True)
    print(classification_report(y_val, y_pred, digits=4))

    # Save model locally
    os.makedirs('models', exist_ok=True)
    model_path = f"models/model_{version}.joblib"
    joblib.dump(clf, model_path)

    # Log to MLflow
    os.makedirs("mlruns", exist_ok=True)
    mlflow.set_tracking_uri("file:./mlruns")
    mlflow.set_experiment("hdfs_anomaly_detection")
    with mlflow.start_run(run_name=f"rf_{version}"):
        mlflow.log_param("n_estimators", 200)
        mlflow.log_param("class_weight", "balanced")
        mlflow.log_param("train_size", len(X_train))
        mlflow.log_param("val_size", len(X_val))
        mlflow.log_metric("precision_fail", report['1']['precision'])
        mlflow.log_metric("recall_fail", report['1']['recall'])
        mlflow.log_metric("f1_fail", report['1']['f1-score'])
        mlflow.sklearn.log_model(clf, name="random_forest_model", input_example=X_train.iloc[:1])

    return clf, report
