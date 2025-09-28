from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routes import health

app = FastAPI(
    title="Book Scraping API",
    description="API for managing book scraping operations",
    version="1.0.0"
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(health.router, prefix="/api/v1", tags=["health"])

@app.get("/")
async def root():
    return {"message": "Book Scraping API", "version": "1.0.0"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)