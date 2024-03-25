import os
import json

from app.services.model import MyModel

MODEL_FOLDER = os.getenv(
    "MODEL_FOLDER"
)  # Note that the variable has to be set in the Dockerfile or in the env file.
ESTIMATOR = MyModel.from_pretrained(MODEL_FOLDER)


def get_predictions(X):
    return ESTIMATOR.predict(X)
