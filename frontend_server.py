# frontend_server.py
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

app = FastAPI()
app.mount("/", StaticFiles(directory="frontend", html=True), name="static")
