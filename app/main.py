from fastapi import FastAPI, HTTPException

from app.core import config
from app.schemas.schemas import InputData, ModelResponse, ResponseDataAPI
from app.services.predict import get_predictions

app = FastAPI(
    title=config.PROJECT_NAME, version=config.VERSION, openapi_url="/v1/openapi.json"
)


@app.get("/")
def read_root():
    return {"message": "Hello World"}


@app.get("/status")
def status():
    return {"status": "ok", "HTTP Status Code": 200}


@app.post("/v1/services/predict", tags=["services"], response_model=ResponseDataAPI)
async def predict(input_data: InputData):
    """
    Use to model to predict on the input data.
    """
    try:
        input_data = input_data.dict()
        X = [[input_data["x"], input_data["y"]]]
        predictions = get_predictions(X)
        return {"inputText": input_data, "classifications": predictions}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
