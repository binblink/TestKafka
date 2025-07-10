# -*- coding: utf-8 -*-
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, HTTPException, Response, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from confluent_kafka import Producer
import json

# Fast Api + parametrage pour �viter les erreurs cors
app = FastAPI()

origins = [
    "http://localhost:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_methods=["*"],  # autoriser toutes m�thodes (POST, OPTIONS, etc.)
    allow_headers=["*"],
    allow_credentials=True,
)

producer = Producer({'bootstrap.servers': 'kafka:9092'})  
TOPIC = "messages"

class Message(BaseModel):
    message: str

@app.api_route("/message", methods=["OPTIONS", "POST"])
async def message_handler(request: Request):
    if request.method == "OPTIONS":
        return Response(status_code=200)
    elif request.method == "POST":
        msg = await request.json()
        try:
            data = json.dumps(msg).encode("utf-8")
            producer.produce(TOPIC, data)
            producer.flush()
            # Utilisation de JSONResponse pour s'assurer de l'encodage UTF-8
            return JSONResponse(content={"status": "Message envoyé à Kafka"})
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))


