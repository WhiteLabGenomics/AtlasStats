from typing import List
from pydantic import BaseModel


# Inputs
class InputData(BaseModel):
    """
    An object to define the input data for the price estimator model.
    It contains the features we want to use to get a prediction.
    """

    x: float
    y: float
    name: str

    class Config:
        schema_extra = {
            "example": {
                "x": 0.09178,
                "y": 0.0,
                "name": "Oscar",
            }
        }


# Outputs
class ModelResponse(BaseModel):
    """
    Output of the model.
    """

    z: float


class ResponseDataAPI(BaseModel):
    """
    The response object to be received back from the API.
    """

    inputText: InputData
    classifications: List[ModelResponse]
