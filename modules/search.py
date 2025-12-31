"""
Semantic Search Module for Government Document AI System
Finds documents by MEANING, not just keywords
Query: "recruitment freezes" → Finds: "hiring restrictions", "vacancy hold"
"""
import json
import numpy as np
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

# Sentence transformers for embeddings
try:
    from sentence_transformers import SentenceTransformer
    TRANSFORMERS_AVAILABLE = True
except ImportError:
    TRANSFORMERS_AVAILABLE = False
    print("⚠ sentence-transformers not installed, using keyword search fallback")

import sys
sys.path.append(str(Path(__file__).parent.parent))
from config import DATA_DIR, EMBEDDING_MODEL


@dataclass
class SearchResult:
    """Search result with relevance score"""
    doc_id: str
    title: str
    text: str
    score: float
    matched_section: str
    highlights: List[str]


class SemanticSearch:
    """
    Semantic search engine for government documents
    Uses sentence embeddings to find semantically similar content
    """
    
    def __init__(self, model_name: str = EMBEDDING_MODEL):
        """
        Initialize semantic search engine
        
        Args:
            model_name: Sentence transformer model name
        """
        self.model = None
        self.documents = {}
        self.embeddings = {}
        self.embeddings_path = DATA_DIR / "embeddings.json"
        
        # Government term synonyms for query expansion
        self.synonyms = {
            'recruitment': ['hiring', 'vacancy', 'personnel', 'appointment', 'selection', 'भर्ती', 'नियुक्ति'],
            'freeze': ['hold', 'ban', 'restriction', 'moratorium', 'stop', 'रोक', 'प्रतिबंध'],
            'budget': ['expenditure', 'funds', 'allocation', 'finance', 'बजट', 'व्यय'],
            'deadline': ['due date', 'timeline', 'time limit', 'last date', 'अंतिम तिथि'],
            'approval': ['sanction', 'clearance', 'permission', 'authorization', 'स्वीकृति', 'अनुमति'],
            'circular': ['order', 'notification', 'memorandum', 'directive', 'परिपत्र', 'आदेश'],
            'department': ['ministry', 'directorate', 'office', 'division', 'विभाग', 'मंत्रालय'],
            'employee': ['staff', 'official', 'personnel', 'officer', 'कर्मचारी', 'अधिकारी'],
            'transfer': ['posting', 'deputation', 'relocation', 'स्थानांतरण', 'तबादला'],
            'pension': ['retirement', 'gratuity', 'superannuation', 'पेंशन', 'सेवानिवृत्ति'],
            'leave': ['absence', 'vacation', 'holiday', 'छुट्टी', 'अवकाश'],
            'salary': ['pay', 'remuneration', 'emoluments', 'wages', 'वेतन'],
        }
        
        if TRANSFORMERS_AVAILABLE:
            print(f"Loading embedding model: {model_name}")
            self.model = SentenceTransformer(model_name)
            print("✓ Semantic search ready")
        else:
            print("⚠ Using keyword-based search")
        
        # Load existing embeddings
        self._load_embeddings()
    
    def add_document(self, doc_id: str, text: str, title: str = "", metadata: Dict = None):
        """
        Add document to search index
        
        Args:
            doc_id: Unique document identifier
            text: Document text
            title: Document title
            metadata: Additional metadata
        """
        # Split into chunks for better search granularity
        chunks = self._chunk_text(text)
        
        self.documents[doc_id] = {
            'title': title or doc_id,
            'text': text,
            'chunks': chunks,
            'metadata': metadata or {}
        }
        
        # Generate embeddings
        if self.model:
            chunk_embeddings = self.model.encode(chunks)
            self.embeddings[doc_id] = {
                'chunks': chunks,
                'vectors': chunk_embeddings.tolist()
            }
        
        # Save embeddings
        self._save_embeddings()
    
    def search(self, query: str, top_k: int = 10) -> List[SearchResult]:
        """
        Search documents by semantic similarity
        
        Args:
            query: Search query
            top_k: Number of results to return
            
        Returns:
            List of SearchResult objects
        """
        if not self.documents:
            return []
        
        # Expand query with synonyms
        expanded_query = self._expand_query(query)
        
        if self.model:
            return self._semantic_search(expanded_query, top_k)
        else:
            return self._keyword_search(expanded_query, top_k)
    
    def _expand_query(self, query: str) -> str:
        """Expand query with synonyms"""
        words = query.lower().split()
        expanded = list(words)
        
        for word in words:
            if word in self.synonyms:
                expanded.extend(self.synonyms[word][:3])  # Add top 3 synonyms
        
        return ' '.join(expanded)
    
    def _semantic_search(self, query: str, top_k: int) -> List[SearchResult]:
        """Semantic search using embeddings"""
        # Encode query
        query_embedding = self.model.encode([query])[0]
        
        # Calculate similarity with all document chunks
        results = []
        
        for doc_id, doc in self.documents.items():
            if doc_id not in self.embeddings:
                continue
            
            doc_embeddings = self.embeddings[doc_id]
            vectors = np.array(doc_embeddings['vectors'])
            
            # Cosine similarity
            similarities = np.dot(vectors, query_embedding) / (
                np.linalg.norm(vectors, axis=1) * np.linalg.norm(query_embedding) + 1e-10
            )
            
            # Get best matching chunk
            best_idx = np.argmax(similarities)
            best_score = float(similarities[best_idx])
            
            if best_score > 0.3:  # Threshold
                matched_chunk = doc_embeddings['chunks'][best_idx]
                
                results.append(SearchResult(
                    doc_id=doc_id,
                    title=doc['title'],
                    text=doc['text'][:500] + "...",
                    score=round(best_score, 3),
                    matched_section=matched_chunk,
                    highlights=self._get_highlights(query, matched_chunk)
                ))
        
        # Sort by score
        results.sort(key=lambda x: x.score, reverse=True)
        return results[:top_k]
    
    def _keyword_search(self, query: str, top_k: int) -> List[SearchResult]:
        """Fallback keyword-based search"""
        query_words = set(query.lower().split())
        results = []
        
        for doc_id, doc in self.documents.items():
            text_lower = doc['text'].lower()
            
            # Count matching words
            matches = sum(1 for word in query_words if word in text_lower)
            
            if matches > 0:
                score = matches / len(query_words)
                
                # Find matching section
                matched_section = self._find_matching_section(query_words, doc['text'])
                
                results.append(SearchResult(
                    doc_id=doc_id,
                    title=doc['title'],
                    text=doc['text'][:500] + "...",
                    score=round(score, 3),
                    matched_section=matched_section,
                    highlights=self._get_highlights(query, matched_section)
                ))
        
        results.sort(key=lambda x: x.score, reverse=True)
        return results[:top_k]
    
    def _chunk_text(self, text: str, chunk_size: int = 500, overlap: int = 50) -> List[str]:
        """Split text into overlapping chunks"""
        words = text.split()
        chunks = []
        
        for i in range(0, len(words), chunk_size - overlap):
            chunk = ' '.join(words[i:i + chunk_size])
            if chunk.strip():
                chunks.append(chunk)
        
        return chunks if chunks else [text]
    
    def _find_matching_section(self, query_words: set, text: str) -> str:
        """Find section with most query word matches"""
        sentences = text.split('.')
        best_sentence = ""
        best_count = 0
        
        for sentence in sentences:
            count = sum(1 for word in query_words if word in sentence.lower())
            if count > best_count:
                best_count = count
                best_sentence = sentence.strip()
        
        return best_sentence[:300] if best_sentence else text[:300]
    
    def _get_highlights(self, query: str, text: str) -> List[str]:
        """Get highlighted matching phrases"""
        highlights = []
        query_words = query.lower().split()
        
        for word in query_words:
            if word in text.lower():
                # Find context around the word
                idx = text.lower().find(word)
                start = max(0, idx - 20)
                end = min(len(text), idx + len(word) + 20)
                highlights.append(f"...{text[start:end]}...")
        
        return highlights[:5]
    
    def _save_embeddings(self):
        """Save embeddings to disk"""
        try:
            with open(self.embeddings_path, 'w') as f:
                json.dump({
                    'documents': {
                        k: {
                            'title': v['title'],
                            'text': v['text'][:1000],  # Save truncated
                            'chunks': v['chunks']
                        }
                        for k, v in self.documents.items()
                    },
                    'embeddings': self.embeddings
                }, f)
        except Exception as e:
            print(f"Failed to save embeddings: {e}")
    
    def _load_embeddings(self):
        """Load embeddings from disk"""
        if self.embeddings_path.exists():
            try:
                with open(self.embeddings_path, 'r') as f:
                    data = json.load(f)
                    self.documents = data.get('documents', {})
                    self.embeddings = data.get('embeddings', {})
                print(f"✓ Loaded {len(self.documents)} documents")
            except Exception as e:
                print(f"Failed to load embeddings: {e}")
    
    def get_similar_documents(self, doc_id: str, top_k: int = 5) -> List[SearchResult]:
        """Find documents similar to a given document"""
        if doc_id not in self.documents:
            return []
        
        # Use document text as query
        doc_text = self.documents[doc_id]['text'][:1000]
        results = self.search(doc_text, top_k + 1)
        
        # Remove the queried document itself
        return [r for r in results if r.doc_id != doc_id][:top_k]
    
    def get_document_count(self) -> int:
        """Get number of indexed documents"""
        return len(self.documents)
    
    def get_all_documents(self) -> List[Dict]:
        """Get all indexed documents with metadata"""
        docs = []
        for doc_id, doc in self.documents.items():
            docs.append({
                'doc_id': doc_id,
                'title': doc.get('title', doc_id),
                'text_preview': doc.get('text', '')[:300] + '...' if len(doc.get('text', '')) > 300 else doc.get('text', ''),
                'word_count': len(doc.get('text', '').split()),
                'chunk_count': len(doc.get('chunks', [])),
                'metadata': doc.get('metadata', {})
            })
        return docs
    
    def clear_index(self):
        """Clear all indexed documents"""
        self.documents = {}
        self.embeddings = {}
        if self.embeddings_path.exists():
            self.embeddings_path.unlink()


def search_documents(query: str, top_k: int = 10) -> List[Dict]:
    """
    Convenience function for quick search
    
    Args:
        query: Search query
        top_k: Number of results
        
    Returns:
        List of search results as dictionaries
    """
    searcher = SemanticSearch()
    results = searcher.search(query, top_k)
    
    return [
        {
            'doc_id': r.doc_id,
            'title': r.title,
            'score': r.score,
            'matched_section': r.matched_section,
            'highlights': r.highlights
        }
        for r in results
    ]


if __name__ == "__main__":
    print("Semantic Search Module Test")
    print("-" * 50)
    
    # Initialize search
    search = SemanticSearch()
    
    # Add test documents
    search.add_document(
        "DOC001",
        """
        Government of India hereby announces a freeze on all new recruitments
        across Central Government departments effective immediately.
        All vacant positions shall remain unfilled until further notice.
        """,
        "Recruitment Freeze Order 2024"
    )
    
    search.add_document(
        "DOC002",
        """
        The Ministry of Finance has approved the release of Rs. 500 crore
        for infrastructure development under the Digital India programme.
        Funds to be utilized by March 2025.
        """,
        "Budget Allocation Order"
    )
    
    search.add_document(
        "DOC003",
        """
        All departments are directed to complete their annual personnel
        reviews. Hiring restrictions apply to Grade B and above positions.
        """,
        "Personnel Review Circular"
    )
    
    # Test searches
    queries = [
        "recruitment freezes",
        "budget allocation",
        "hiring restrictions",
        "vacancy hold"
    ]
    
    for query in queries:
        print(f"\n🔍 Query: '{query}'")
        results = search.search(query, top_k=3)
        for r in results:
            print(f"  → {r.title} (score: {r.score})")
            print(f"    {r.matched_section[:100]}...")
