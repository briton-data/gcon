#!/usr/bin/env python3
"""
Train and serialize a small, real scikit-learn model for GCON's ML
inference demo job (scripts/ml_inference_job.py).

Run this once, locally:

    python scripts/train_demo_model.py

It writes models/iris_classifier.joblib (a few KB) which gets committed
to the repo -- both worker-01 and worker-02 just need the repo checked
out (already true; nothing extra to upload) to run real inference.

Uses the classic iris dataset (bundled with scikit-learn, no download)
so this works identically offline, on Render, and on Colab with zero
external network dependency at training OR inference time.
"""

import os

from sklearn.datasets import load_iris
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
import joblib


def main():
    iris = load_iris()
    X_train, X_test, y_train, y_test = train_test_split(
        iris.data, iris.target, test_size=0.2, random_state=42
    )

    model = RandomForestClassifier(n_estimators=50, random_state=42)
    model.fit(X_train, y_train)

    accuracy = model.score(X_test, y_test)
    print(f"Test accuracy: {accuracy:.4f}")

    os.makedirs("models", exist_ok=True)
    out_path = "models/iris_classifier.joblib"
    joblib.dump({"model": model, "target_names": list(iris.target_names)}, out_path)
    print(f"Saved model to {out_path}")


if __name__ == "__main__":
    main()
