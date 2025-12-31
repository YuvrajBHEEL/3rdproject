"""
Database Module for eFile Sathi
Uses Supabase for cloud-based PostgreSQL storage
"""
import os
import json
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Supabase configuration
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")

# Initialize Supabase client
supabase = None

def get_supabase_client():
    """Get or initialize Supabase client"""
    global supabase
    if supabase is None and SUPABASE_URL and SUPABASE_KEY:
        try:
            from supabase import create_client
            supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
            print("✓ Supabase client initialized")
        except Exception as e:
            print(f"⚠ Could not initialize Supabase: {e}")
    return supabase


def init_db():
    """Initialize database - creates tables if they don't exist via Supabase"""
    client = get_supabase_client()
    if client:
        print("✓ Database initialized successfully")
    else:
        # Fallback to SQLite for local development
        _init_sqlite()
        print("✓ Database initialized successfully")


# ========================================
# SQLite Fallback for Legacy Modules
# ========================================
import sqlite3

DB_PATH = Path('data/digifest.db')

def get_db_connection():
    """Get SQLite database connection (for legacy modules like grievance, workflow)"""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _init_sqlite():
    """Initialize SQLite schema for legacy modules"""
    conn = get_db_connection()
    c = conn.cursor()
    
    c.execute("PRAGMA foreign_keys = ON")
    
    # Grievance Table
    c.execute("""
    CREATE TABLE IF NOT EXISTS grievances (
        id TEXT PRIMARY KEY,
        subject TEXT NOT NULL,
        details TEXT,
        priority TEXT DEFAULT 'normal',
        status TEXT DEFAULT 'pending',
        submitted_date TEXT,
        due_date TEXT,
        resolved_date TEXT,
        department TEXT,
        citizen_name TEXT,
        contact TEXT,
        source_doc_id TEXT,
        updates_json TEXT
    )
    """)
    
    # Workflows Table
    c.execute("""
    CREATE TABLE IF NOT EXISTS workflows (
        doc_id TEXT PRIMARY KEY,
        title TEXT,
        current_status TEXT,
        created_at TEXT,
        updated_at TEXT,
        priority TEXT,
        expected_completion TEXT
    )
    """)
    
    # Workflow Steps Table
    c.execute("""
    CREATE TABLE IF NOT EXISTS workflow_steps (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        doc_id TEXT NOT NULL,
        status TEXT NOT NULL,
        timestamp TEXT,
        officer TEXT,
        remarks TEXT,
        FOREIGN KEY (doc_id) REFERENCES workflows (doc_id) ON DELETE CASCADE
    )
    """)
    
    conn.commit()
    conn.close()


# ========================================
# Document Storage Functions
# ========================================

def save_document(doc_id: str, filename: str, file_path: str, ocr_text: str, 
                  file_type: str = None, file_size: int = 0, 
                  summary: Dict = None, metadata: Dict = None):
    """Save a document to Supabase"""
    client = get_supabase_client()
    if not client:
        print(f"⚠ Supabase not available, document {doc_id} not saved")
        return False
    
    try:
        data = {
            "id": doc_id,
            "filename": filename,
            "file_path": file_path,
            "upload_date": datetime.now().isoformat(),
            "file_type": file_type or Path(filename).suffix.lower(),
            "file_size": file_size,
            "ocr_text": ocr_text,
            "summary_json": json.dumps(summary or {}),
            "metadata_json": json.dumps(metadata or {})
        }
        
        # Upsert (insert or update)
        result = client.table("documents").upsert(data).execute()
        print(f"✓ Document {doc_id} saved to Supabase")
        return True
    except Exception as e:
        print(f"⚠ Failed to save document {doc_id}: {e}")
        return False


def get_all_documents_from_db() -> List[Dict]:
    """Get all documents from Supabase"""
    client = get_supabase_client()
    if not client:
        return []
    
    try:
        result = client.table("documents").select("*").order("upload_date", desc=True).execute()
        
        documents = []
        for row in result.data:
            doc = {
                'doc_id': row['id'],
                'filename': row.get('filename', ''),
                'file_path': row.get('file_path', ''),
                'upload_date': row.get('upload_date', ''),
                'file_type': row.get('file_type', ''),
                'file_size': row.get('file_size', 0),
                'text_preview': (row.get('ocr_text', '')[:300] + '...') if row.get('ocr_text') and len(row.get('ocr_text', '')) > 300 else row.get('ocr_text', ''),
                'word_count': len(row.get('ocr_text', '').split()) if row.get('ocr_text') else 0,
                'title': row.get('filename') or row['id']
            }
            documents.append(doc)
        
        return documents
    except Exception as e:
        print(f"⚠ Failed to get documents: {e}")
        return []


def get_document_by_id(doc_id: str) -> Optional[Dict]:
    """Get a specific document by ID from Supabase"""
    client = get_supabase_client()
    if not client:
        return None
    
    try:
        result = client.table("documents").select("*").eq("id", doc_id).execute()
        
        if result.data and len(result.data) > 0:
            row = result.data[0]
            return {
                'doc_id': row['id'],
                'filename': row.get('filename', ''),
                'file_path': row.get('file_path', ''),
                'upload_date': row.get('upload_date', ''),
                'file_type': row.get('file_type', ''),
                'file_size': row.get('file_size', 0),
                'full_text': row.get('ocr_text', ''),
                'word_count': len(row.get('ocr_text', '').split()) if row.get('ocr_text') else 0,
                'title': row.get('filename') or row['id'],
                'summary': json.loads(row.get('summary_json', '{}')) if row.get('summary_json') else {},
                'metadata': json.loads(row.get('metadata_json', '{}')) if row.get('metadata_json') else {}
            }
        return None
    except Exception as e:
        print(f"⚠ Failed to get document {doc_id}: {e}")
        return None


def delete_document(doc_id: str) -> bool:
    """Delete a document from Supabase"""
    client = get_supabase_client()
    if not client:
        return False
    
    try:
        client.table("documents").delete().eq("id", doc_id).execute()
        print(f"✓ Document {doc_id} deleted from Supabase")
        return True
    except Exception as e:
        print(f"⚠ Failed to delete document {doc_id}: {e}")
        return False


def find_similar_documents(new_text: str, threshold: float = 0.7) -> List[Dict]:
    """
    Find documents similar to the provided text.
    Uses simple word overlap similarity for detection.
    Returns list of similar documents with similarity scores.
    """
    client = get_supabase_client()
    if not client or not new_text:
        return []
    
    try:
        # Get all existing documents
        result = client.table("documents").select("id, filename, ocr_text").execute()
        
        if not result.data:
            return []
        
        # Extract words from new document (normalized)
        new_words = set(new_text.lower().split())
        if len(new_words) < 10:  # Too short to compare
            return []
        
        similar_docs = []
        
        for row in result.data:
            existing_text = row.get('ocr_text', '')
            if not existing_text:
                continue
            
            existing_words = set(existing_text.lower().split())
            if len(existing_words) < 10:
                continue
            
            # Calculate Jaccard similarity
            intersection = len(new_words & existing_words)
            union = len(new_words | existing_words)
            
            if union > 0:
                similarity = intersection / union
                
                if similarity >= threshold:
                    similar_docs.append({
                        'doc_id': row['id'],
                        'filename': row.get('filename', 'Unknown'),
                        'similarity': round(similarity * 100, 1)
                    })
        
        # Sort by similarity (highest first)
        similar_docs.sort(key=lambda x: x['similarity'], reverse=True)
        
        return similar_docs[:5]  # Return top 5 similar docs
        
    except Exception as e:
        print(f"⚠ Failed to check similar documents: {e}")
        return []
