"""
Configuration management for Government Document AI System
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Base paths
BASE_DIR = Path(__file__).parent
UPLOAD_DIR = Path(os.getenv("UPLOAD_DIR", "./uploads"))
DATA_DIR = BASE_DIR / "data"
BLOCKCHAIN_DIR = BASE_DIR / "blockchain"

# Create directories
UPLOAD_DIR.mkdir(exist_ok=True)
DATA_DIR.mkdir(exist_ok=True)
BLOCKCHAIN_DIR.mkdir(exist_ok=True)

# API Keys
PERPLEXITY_API_KEY = os.getenv("PERPLEXITY_API_KEY", "")
PERPLEXITY_BASE_URL = "https://api.perplexity.ai"

# Tesseract configuration
TESSERACT_CMD = os.getenv("TESSERACT_CMD", "/usr/local/bin/tesseract")

# File settings
MAX_FILE_SIZE_MB = int(os.getenv("MAX_FILE_SIZE_MB", 50))
ALLOWED_EXTENSIONS = {".pdf", ".png", ".jpg", ".jpeg", ".tiff", ".bmp"}

# Supported languages for OCR
SUPPORTED_LANGUAGES = {
    "hindi": "hin",
    "english": "eng",
    "hindi+english": "hin+eng"
}

# Model settings
EMBEDDING_MODEL = "all-MiniLM-L6-v2"
PERPLEXITY_MODEL = "llama-3.1-sonar-large-128k-online"
PERPLEXITY_FALLBACK_MODEL = "llama-3.1-sonar-small-128k-online"

# Summary length limits
SUMMARY_LIMITS = {
    "secretary": {"max_words": 50, "description": "1 sentence max"},
    "director": {"max_words": 150, "description": "1 paragraph max"},
    "officer": {"max_words": 500, "description": "Detailed with action items"}
}

# Server settings
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", 8000))
