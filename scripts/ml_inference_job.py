#!/usr/bin/env python3
"""
Real ML inference job for GCON workers -- loads the trained model from
models/iris_classifier.joblib and classifies one sample.

This is what actually runs as a job's `command` on worker-01/worker-02,
via subprocess, same as any other GCON job -- the only difference from
an echo job is that this one does real, meaningful compute and returns
a real prediction, not a canned string.

Usage:
    python scripts/ml_inference_job.py --features 5.1,3.5,1.4,0.2

Prints a single JSON line to stdout -- the job's captured stdout is
what shows up in the receipt, so keep output machine-parseable.
"""

import argparse
import json
import sys

import joblib


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--features",
        required=True,
        help="4 comma-separated floats: sepal_length,sepal_width,petal_length,petal_width",
    )
    parser.add_argument(
        "--model-path",
        default="models/iris_classifier.joblib",
    )
    args = parser.parse_args()

    try:
        features = [float(x) for x in args.features.split(",")]
    except ValueError:
        print(json.dumps({"error": f"could not parse --features '{args.features}' as 4 floats"}))
        sys.exit(1)

    if len(features) != 4:
        print(json.dumps({"error": f"expected 4 features, got {len(features)}"}))
        sys.exit(1)

    bundle = joblib.load(args.model_path)
    model = bundle["model"]
    target_names = bundle["target_names"]

    prediction = model.predict([features])[0]
    probabilities = model.predict_proba([features])[0]

    result = {
        "input_features": features,
        "predicted_class": target_names[prediction],
        "probabilities": {
            target_names[i]: round(float(p), 4) for i, p in enumerate(probabilities)
        },
    }
    print(json.dumps(result))


if __name__ == "__main__":
    main()
