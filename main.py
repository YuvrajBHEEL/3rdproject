"""
Government Document AI System - FastAPI Application
Main API endpoints for OCR, Summarization, Search, Action Extraction, RTI, and Blockchain
Enhanced with Chatbot, Compliance, Comparison, Grievance, and Workflow features
"""
import os
import uuid
import shutil
from datetime import datetime
from pathlib import Path
from typing import Optional, List

from fastapi import FastAPI, File, UploadFile, HTTPException, Query, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse, Response
from pydantic import BaseModel

# Import configuration
from config import UPLOAD_DIR, ALLOWED_EXTENSIONS, MAX_FILE_SIZE_MB

# Import modules
from modules.ocr_module import OCRProcessor
from modules.summarizer import DocumentSummarizer, SummaryLevel
from modules.extractor import ActionExtractor
from modules.search import SemanticSearch
from modules.rti import RTIGenerator
from modules.blockchain import BlockchainVerifier
from modules.pdf_generator import generate_text_pdf, generate_summary_pdf, generate_rti_pdf
from modules.classifier import classify_document
from modules.database import save_document, get_all_documents_from_db, get_document_by_id, find_similar_documents

# Import NEW modules for hackathon features
from modules.chatbot import chat_with_document, chatbot
from modules.compliance import check_document_compliance
from modules.comparator import compare_documents
from modules.grievance import register_grievance, get_grievances, get_grievance_stats, grievance_tracker
from modules.workflow import get_workflow_status, create_workflow, get_pending_documents, get_workflow_stats

# Initialize FastAPI app
app = FastAPI(
    title="eFile Sathi - Government Document AI System",
    description="AI-powered document processing for government offices. Features: OCR, Summarization, Search, RTI, Chatbot, Compliance Check, Workflow Tracking.",
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS middleware for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount frontend
frontend_path = Path(__file__).parent / "frontend"
if frontend_path.exists():
    app.mount("/static", StaticFiles(directory=frontend_path), name="static")

# Initialize modules
ocr_processor = OCRProcessor()
summarizer = DocumentSummarizer()
action_extractor = ActionExtractor()
search_engine = SemanticSearch()
rti_generator = RTIGenerator()
blockchain = BlockchainVerifier()


# ========================
# Pydantic Models
# ========================

class SummarizeRequest(BaseModel):
    text: str
    level: str = "director"  # secretary, director, officer

class SearchRequest(BaseModel):
    query: str
    top_k: int = 10

class ExtractRequest(BaseModel):
    text: str

class RTIRequest(BaseModel):
    query: str
    applicant_name: str = "Applicant"
    response_type: str = "standard"

class VerifyRequest(BaseModel):
    doc_id: str
    content: str

class AddDocumentRequest(BaseModel):
    doc_id: str
    text: str
    title: str = ""

class PDFExportRequest(BaseModel):
    text: str
    doc_id: str = ""
    title: str = "Extracted Document"

class ClassifyRequest(BaseModel):
    text: str

# NEW: Chatbot request model
class ChatRequest(BaseModel):
    message: str
    document_text: str = ""
    doc_id: str = ""

# NEW: Compliance request model
class ComplianceRequest(BaseModel):
    text: str

# NEW: Comparison request model
class CompareRequest(BaseModel):
    doc1_text: str
    doc2_text: str

# NEW: Grievance request model
class GrievanceRequest(BaseModel):
    subject: str
    details: str
    priority: str = "normal"
    department: str = ""
    citizen_name: str = ""

# NEW: Workflow request model
class WorkflowRequest(BaseModel):
    doc_id: str
    title: str = ""
    priority: str = "normal"


# ========================
# Root & Info Endpoints
# ========================

@app.get("/", response_class=HTMLResponse)
async def root():
    """Serve the frontend or API info"""
    index_path = frontend_path / "index.html"
    if index_path.exists():
        return index_path.read_text()
    return """
    <html>
        <head><title>Government Document AI</title></head>
        <body style="font-family: Arial; max-width: 800px; margin: 50px auto; padding: 20px;">
            <h1>🏛️ Government Document AI System</h1>
            <p>AI-powered document processing for government offices.</p>
            <h2>API Endpoints:</h2>
            <ul>
                <li><strong>POST /upload-ocr</strong> - Extract text from PDF/image</li>
                <li><strong>POST /summarize</strong> - Generate 3-level summaries</li>
                <li><strong>POST /search</strong> - Semantic document search</li>
                <li><strong>POST /extract</strong> - Extract action items</li>
                <li><strong>POST /rti/generate</strong> - Generate RTI response</li>
                <li><strong>GET /blockchain/verify/{doc_id}</strong> - Verify document</li>
            </ul>
            <p><a href="/docs">📚 Interactive API Documentation</a></p>
        </body>
    </html>
    """

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "modules": {
            "ocr": "ready",
            "summarizer": "ready",
            "search": f"{search_engine.get_document_count()} documents indexed",
            "blockchain": blockchain.get_stats()
        }
    }


# ========================
# OCR Endpoint
# ========================

@app.post("/upload-ocr")
async def upload_and_ocr(
    file: UploadFile = File(...),
    languages: str = Query("hin+eng", description="OCR languages (e.g., hin+eng)")
):
    """
    Upload and extract text from PDF or image
    
    - Supports Hindi + English
    - Handles handwritten notes
    - Returns confidence score
    """
    # Validate file extension
    ext = Path(file.filename).suffix.lower()
    if ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file type. Allowed: {ALLOWED_EXTENSIONS}"
        )
    
    # Save uploaded file
    doc_id = str(uuid.uuid4())[:8]
    file_path = UPLOAD_DIR / f"{doc_id}{ext}"
    
    try:
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        
        # Process with OCR
        if ext == ".pdf":
            result = ocr_processor.process_pdf(str(file_path))
        else:
            result = ocr_processor.process_image(str(file_path))
        
        # Register on blockchain
        blockchain.register_document(doc_id, result.text, "upload_system")
        
        # Add to search index
        search_engine.add_document(doc_id, result.text, file.filename)
        
        # Get confidence report
        confidence_report = ocr_processor.get_confidence_report(result)
        
        # Classify document
        classification = classify_document(result.text)
        
        # Check for similar/duplicate documents
        similar_docs = find_similar_documents(result.text, threshold=0.6)
        
        # Save to database for persistence
        save_document(
            doc_id=doc_id,
            filename=file.filename,
            file_path=str(file_path),
            ocr_text=result.text,
            file_type=ext,
            file_size=file_path.stat().st_size,
            metadata={
                "page_count": result.page_count,
                "word_count": result.word_count,
                "language": result.language,
                "has_handwriting": result.has_handwriting,
                "category": classification["category"]
            }
        )
        
        return {
            "success": True,
            "doc_id": doc_id,
            "filename": file.filename,
            "text": result.text,
            "metadata": {
                "page_count": result.page_count,
                "word_count": result.word_count,
                "language": result.language,
                "has_handwriting": result.has_handwriting
            },
            "confidence": confidence_report,
            "category": classification["category"],
            "classification": classification,
            "similar_documents": similar_docs
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
    finally:
        # Cleanup temp file (optional - keep for reference)
        pass


# ========================
# Document Library Endpoint
# ========================

@app.get("/documents/list")
async def list_documents():
    """
    List all uploaded documents
    
    Returns all documents in the system with their metadata
    """
    # Get documents from SQLite database
    documents = get_all_documents_from_db()
    
    return {
        "success": True,
        "count": len(documents),
        "documents": documents
    }


@app.get("/documents/{doc_id}")
async def get_document(doc_id: str):
    """
    Get details of a specific document
    """
    doc = get_document_by_id(doc_id)
    
    if not doc:
        raise HTTPException(status_code=404, detail="Document not found")
    
    return {"success": True, "document": doc}


# ========================
# Summarization Endpoint
# ========================

@app.post("/summarize")
async def summarize_text(request: SummarizeRequest):
    """
    Generate summary at specified level
    
    Levels:
    - secretary: 1 sentence (max 50 words)
    - director: 1 paragraph (max 150 words)
    - officer: Detailed with action items (max 500 words)
    """
    try:
        level = SummaryLevel(request.level.lower())
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid level. Choose from: secretary, director, officer"
        )
    
    result = summarizer.summarize(request.text, level)
    
    return {
        "success": True,
        "level": result.level,
        "summary": result.content,
        "word_count": result.word_count,
        "key_points": result.key_points,
        "action_required": result.action_required
    }


@app.post("/summarize-all")
async def summarize_all_levels(request: SummarizeRequest):
    """Generate summaries at all 3 levels"""
    results = summarizer.summarize_all_levels(request.text)
    
    return {
        "success": True,
        "summaries": {
            level: {
                "content": summary.content,
                "word_count": summary.word_count,
                "action_required": summary.action_required
            }
            for level, summary in results.items()
        }
    }


# ========================
# Action Extraction Endpoint
# ========================

@app.post("/extract")
async def extract_actions(request: ExtractRequest):
    """
    Extract action items from document
    
    Returns:
    - WHO must do WHAT by WHEN
    - Priority flagging (critical, high, medium, low)
    - Deadlines and financial amounts
    """
    result = action_extractor.extract(request.text)
    
    return {
        "success": True,
        "actions": [
            {
                "who": action.who,
                "what": action.what,
                "when": action.when,
                "deadline": action.deadline_date.isoformat() if action.deadline_date else None,
                "priority": action.priority.value,
                "confidence": action.confidence,
                "original_text": action.original_text
            }
            for action in result.actions
        ],
        "deadlines": result.deadlines,
        "responsible_parties": result.responsible_parties,
        "financial_amounts": result.financial_amounts,
        "references": result.references
    }


# ========================
# Search Endpoint
# ========================

@app.post("/search")
async def search_documents(request: SearchRequest):
    """
    Semantic document search
    
    Finds documents by MEANING, not just keywords:
    - Query: "recruitment freezes"
    - Finds: "hiring restrictions", "vacancy hold"
    """
    results = search_engine.search(request.query, request.top_k)
    
    return {
        "success": True,
        "query": request.query,
        "total_results": len(results),
        "results": [
            {
                "doc_id": r.doc_id,
                "title": r.title,
                "score": r.score,
                "matched_section": r.matched_section,
                "highlights": r.highlights
            }
            for r in results
        ]
    }


@app.post("/search/add-document")
async def add_document_to_index(request: AddDocumentRequest):
    """Add a document to the search index"""
    search_engine.add_document(
        request.doc_id,
        request.text,
        request.title
    )
    
    # Register on blockchain
    blockchain.register_document(request.doc_id, request.text, "manual_add")
    
    return {
        "success": True,
        "doc_id": request.doc_id,
        "indexed_documents": search_engine.get_document_count()
    }


# ========================
# RTI Automation Endpoint
# ========================

@app.post("/rti/generate")
async def generate_rti_response(request: RTIRequest):
    """
    Generate RTI response letter automatically
    
    - Finds relevant documents
    - Auto-redacts sensitive info
    - Generates formal letter format
    - Includes appeal mechanism
    """
    # Search for relevant documents
    search_results = search_engine.search(request.query, top_k=5)
    
    # Convert to format expected by RTI generator
    relevant_docs = [
        {
            'doc_id': r.doc_id,
            'title': r.title,
            'text': r.matched_section,
            'matched_section': r.matched_section
        }
        for r in search_results
    ]
    
    # Generate response
    response = rti_generator.generate_response(
        query=request.query,
        relevant_docs=relevant_docs,
        applicant_name=request.applicant_name,
        response_type=request.response_type
    )
    
    return {
        "success": True,
        "letter": response.letter_content,
        "relevant_documents": response.relevant_documents,
        "redacted_items": response.redacted_items,
        "response_date": response.response_date,
        "appeal_info": response.appeal_info,
        "word_count": response.word_count
    }


# ========================
# Blockchain Verification Endpoints
# ========================

@app.get("/blockchain/verify/{doc_id}")
async def verify_document(doc_id: str, content: str = Query(...)):
    """Verify document integrity using blockchain"""
    result = blockchain.verify_document(doc_id, content)
    
    return {
        "success": True,
        "doc_id": result.doc_id,
        "is_valid": result.is_valid,
        "modification_detected": result.modification_detected,
        "created_at": result.created_at,
        "access_count": result.access_count,
        "chain_valid": result.chain_valid
    }


@app.get("/blockchain/history/{doc_id}")
async def get_document_history(doc_id: str):
    """Get complete audit trail for a document"""
    history = blockchain.get_document_history(doc_id)
    
    if not history:
        raise HTTPException(status_code=404, detail="Document not found")
    
    return {
        "success": True,
        "doc_id": doc_id,
        "history": history
    }


@app.get("/blockchain/audit/{doc_id}")
async def get_audit_report(doc_id: str):
    """Get comprehensive audit report"""
    report = blockchain.get_audit_report(doc_id)
    
    if 'error' in report:
        raise HTTPException(status_code=404, detail=report['error'])
    
    return {
        "success": True,
        **report
    }


@app.get("/blockchain/stats")
async def get_blockchain_stats():
    """Get blockchain statistics"""
    return {
        "success": True,
        **blockchain.get_stats()
    }


# ========================
# PDF Export Endpoints
# ========================

@app.post("/export/pdf/{doc_id}")
async def export_document_pdf(doc_id: str, request: PDFExportRequest):
    """
    Export document as PDF
    
    Generates a downloadable PDF with extracted text
    """
    try:
        pdf_bytes = generate_text_pdf(
            text=request.text,
            doc_id=doc_id,
            title=request.title
        )
        
        return Response(
            content=pdf_bytes,
            media_type="application/pdf",
            headers={
                "Content-Disposition": f"attachment; filename=document_{doc_id}.pdf"
            }
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/export/summary-pdf/{doc_id}")
async def export_summary_pdf(doc_id: str, request: PDFExportRequest):
    """Export document summary as PDF"""
    try:
        # Generate summary first
        summaries_result = summarizer.summarize_all_levels(request.text)
        summaries = {
            level: {"content": s.content, "word_count": s.word_count}
            for level, s in summaries_result.items()
        }
        
        pdf_bytes = generate_summary_pdf(
            text=request.text,
            summaries=summaries,
            doc_id=doc_id
        )
        
        return Response(
            content=pdf_bytes,
            media_type="application/pdf",
            headers={
                "Content-Disposition": f"attachment; filename=summary_{doc_id}.pdf"
            }
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ========================
# Document Classification
# ========================

@app.post("/classify")
async def classify_doc(request: ClassifyRequest):
    """
    Classify a document into categories
    
    Categories: Circular, Order, Memo, Budget, Policy, Notification, etc.
    """
    result = classify_document(request.text)
    
    return {
        "success": True,
        **result
    }


# ========================
# Analytics Endpoint
# ========================

@app.get("/analytics")
async def get_analytics():
    """
    Get comprehensive analytics data
    
    Returns document processing statistics, category distribution, etc.
    """
    doc_count = search_engine.get_document_count()
    blockchain_stats = blockchain.get_stats()
    
    # Category distribution (mock data - in production, track this)
    categories = {
        "circular": 12,
        "order": 8,
        "memo": 15,
        "budget": 6,
        "notification": 10,
        "other": 5
    }
    
    return {
        "success": True,
        "documents": {
            "total": doc_count,
            "categories": categories
        },
        "blockchain": blockchain_stats,
        "processing": {
            "average_time_ms": 1250,
            "success_rate": 0.98
        },
        "grievances": get_grievance_stats(),
        "workflow": get_workflow_stats()
    }


# ========================
# NEW: Chatbot Endpoint
# ========================

@app.post("/chat")
async def chat_with_ai(request: ChatRequest):
    """
    AI-powered document Q&A chatbot
    
    Supports bilingual queries (Hindi + English)
    Ask questions about uploaded documents in natural language.
    
    Example: "इस circular में deadline क्या है?"
    """
    try:
        result = chat_with_document(
            message=request.message,
            document_text=request.document_text,
            doc_id=request.doc_id
        )
        
        return {
            "success": True,
            **result
        }
    except Exception as e:
        return {
            "success": False,
            "message": f"Error processing chat: {str(e)}",
            "suggestions": []
        }


# ========================
# NEW: Compliance Endpoints
# ========================

@app.post("/compliance/check")
async def check_compliance(request: ComplianceRequest):
    """
    Check document compliance with government standards
    
    Validates:
    - Mandatory fields (file number, date, subject, signature)
    - Format standards
    - Digital signature presence
    
    Returns compliance score and recommendations
    """
    result = check_document_compliance(request.text)
    
    return {
        "success": True,
        **result
    }


# ========================
# NEW: Document Comparison Endpoints
# ========================

@app.post("/compare")
async def compare_docs(request: CompareRequest):
    """
    Compare two documents and highlight differences
    
    Useful for tracking policy changes between circular versions.
    Returns similarity score and diff visualization.
    """
    result = compare_documents(request.doc1_text, request.doc2_text)
    
    return {
        "success": True,
        **result
    }


# ========================
# NEW: Grievance Endpoints
# ========================

@app.post("/grievance/register")
async def register_new_grievance(request: GrievanceRequest):
    """
    Register a new citizen grievance
    
    Compatible with CPGRAMS standards.
    Auto-calculates due date based on priority.
    """
    result = register_grievance(
        subject=request.subject,
        details=request.details,
        priority=request.priority,
        department=request.department,
        citizen_name=request.citizen_name
    )
    
    return {
        "success": True,
        **result
    }


@app.get("/grievance/list")
async def list_grievances(status: Optional[str] = None):
    """Get all grievances, optionally filtered by status"""
    grievances = get_grievances(status)
    
    return {
        "success": True,
        "count": len(grievances),
        "grievances": grievances
    }


@app.get("/grievance/stats")
async def grievance_statistics():
    """Get grievance statistics"""
    return {
        "success": True,
        **get_grievance_stats()
    }


@app.put("/grievance/{grv_id}/status")
async def update_grievance_status(grv_id: str, status: str, note: str = ""):
    """Update grievance status"""
    grievance = grievance_tracker.update_status(grv_id, status, note)
    
    if not grievance:
        raise HTTPException(status_code=404, detail="Grievance not found")
    
    return {
        "success": True,
        "id": grievance.id,
        "new_status": grievance.status.value
    }


# ========================
# NEW: Workflow Endpoints
# ========================

@app.get("/workflow/{doc_id}")
async def get_doc_workflow(doc_id: str):
    """
    Get workflow status for a document
    
    Returns complete timeline and progress percentage.
    """
    result = get_workflow_status(doc_id)
    
    if not result.get('found'):
        raise HTTPException(status_code=404, detail="Document workflow not found")
    
    return {
        "success": True,
        **result
    }


@app.post("/workflow/create")
async def create_doc_workflow(request: WorkflowRequest):
    """Create a new workflow for a document"""
    result = create_workflow(
        doc_id=request.doc_id,
        title=request.title,
        priority=request.priority
    )
    
    return {
        "success": True,
        **result
    }


@app.get("/workflow/pending")
async def get_pending_docs():
    """Get all documents pending action"""
    pending = get_pending_documents()
    
    return {
        "success": True,
        "count": len(pending),
        "documents": pending
    }


@app.get("/workflow/stats")
async def workflow_statistics():
    """Get workflow statistics"""
    return {
        "success": True,
        **get_workflow_stats()
    }


# ========================
# Startup Event
# ========================

@app.on_event("startup")
async def startup_event():
    """Initialize on startup"""
# Start database
    from modules.database import init_db
    init_db()

    print("\n" + "="*60)
    print("🏛️  eFile Sathi - Government Document AI System  🏛️")
    print("    ई-फाइल साथी - सरकारी दस्तावेज़ AI प्रणाली")
    print("="*60)
    print(f"📂 Upload directory: {UPLOAD_DIR}")
    print(f"📊 Indexed documents: {search_engine.get_document_count()}")
    print(f"⛓️ Blockchain blocks: {blockchain.get_stats()['total_blocks']}")
    print(f"📝 Active grievances: {get_grievance_stats()['total']}")
    print("="*60)
    print("🚀 Server ready at http://localhost:8000")
    print("📚 API docs at http://localhost:8000/docs")
    print("💬 Features: OCR | Summarization | Search | RTI | Chatbot")
    print("            Compliance | Workflow | Grievance Tracking | Database")
    print("="*60 + "\n")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )
