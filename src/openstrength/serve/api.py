from fastapi import FastAPI

app = FastAPI(title="OpenStrength API")

@app.get("/health")
async def health():
    return {"status": "ok"}
