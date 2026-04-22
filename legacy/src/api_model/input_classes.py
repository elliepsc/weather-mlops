from pydantic import BaseModel
from typing import Optional

class InputData(BaseModel):
    """Input data model for prediction."""
    Date: int
    Location : int
    MinTemp : float
    MaxTemp : float
    Rainfall : float
    Evaporation : float
    Sunshine : float
    WindGustDir : int
    WindGustSpeed : float
    WindDir9am : int
    WindDir3pm : int
    WindSpeed9am : float
    WindSpeed3pm : float
    Humidity9am : float
    Humidity3pm : float
    Pressure9am : float
    Pressure3pm : float
    Cloud9am : float
    Cloud3pm : float
    Temp9am : float
    Temp3pm : float
    RainToday : int

class ModelData(BaseModel):
    """Input for loading model."""
    model_name: Optional[str] = "model"
    version: Optional[str] = None
    alias: Optional[str] = None

    def __str__(self):
        text = f"Name: {self.model_name}"
        if self.version: text += f", version: {self.version}" 
        if self.alias: text += f", alias: {self.alias}"
        return text