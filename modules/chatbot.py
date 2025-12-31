"""
AI Chatbot Module for eFile Sathi
Conversational AI for document queries in Hindi and English
"""
import os
from typing import List, Dict, Optional
from dataclasses import dataclass

try:
    from openai import OpenAI
    PERPLEXITY_AVAILABLE = True
except ImportError:
    PERPLEXITY_AVAILABLE = False

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))
from config import PERPLEXITY_API_KEY, PERPLEXITY_BASE_URL, PERPLEXITY_MODEL, PERPLEXITY_FALLBACK_MODEL


@dataclass
class ChatMessage:
    """Chat message"""
    role: str  # 'user', 'assistant' or 'system'
    content: str
    timestamp: str = ""


@dataclass
class ChatResponse:
    """Chat response"""
    message: str
    sources: List[str]
    confidence: float
    language: str


class DocumentChatbot:
    """
    AI-powered chatbot for government document queries
    Supports bilingual (Hindi + English) conversations using Perplexity
    """
    
    def __init__(self, api_key: Optional[str] = None):
        """Initialize chatbot"""
        self.api_key = api_key or PERPLEXITY_API_KEY
        self.client = None
        self.conversation_history: List[Dict] = []
        self.document_context: str = ""
        
        if PERPLEXITY_AVAILABLE and self.api_key:
            self.client = OpenAI(api_key=self.api_key, base_url=PERPLEXITY_BASE_URL)
    
    def set_document_context(self, text: str, doc_id: str = "", title: str = ""):
        """Set the document context for queries"""
        self.document_context = text
        self.doc_id = doc_id
        self.doc_title = title
        
        # Reset conversation with new context
        self.conversation_history = []
    
    def chat(self, user_message: str) -> ChatResponse:
        """
        Process user message and generate response
        
        Args:
            user_message: User's question in Hindi or English
            
        Returns:
            ChatResponse with answer and metadata
        """
        # Detect language (simple heuristic)
        is_hindi = any('\u0900' <= char <= '\u097F' for char in user_message)
        
        if self.client:
            return self._chat_with_perplexity(user_message, is_hindi)
        else:
            return self._chat_fallback(user_message, is_hindi)
    
    def _chat_with_perplexity(self, user_message: str, is_hindi: bool) -> ChatResponse:
        """Use Perplexity for intelligent responses"""
        system_prompt = f"""You are a helpful government document assistant for eFile Sathi (ई-फाइल साथी).
You help government officers and citizens understand official documents.

CURRENT DOCUMENT CONTEXT:
{self.document_context[:10000] if self.document_context else "No document uploaded yet."}

INSTRUCTIONS:
1. Answer questions based on the document context above
2. If asked in Hindi, respond in Hindi. If asked in English, respond in English.
3. Be precise and cite specific sections when possible
4. For dates, amounts, and deadlines, be very accurate
5. If information is not in the document, say so clearly
6. Use government terminology appropriately
7. Be respectful and professional

EXAMPLE RESPONSES:
- For deadline questions: "इस दस्तावेज़ में अंतिम तिथि 15 जनवरी 2025 है।"
- For summary requests: "This circular directs all departments to..."
- For unclear queries: "I couldn't find specific information about that in this document."
"""
        
        # Build conversation
        messages = [{"role": "system", "content": system_prompt}]
        
        # Add conversation history
        for msg in self.conversation_history[-6:]:
            messages.append(msg)
        
        # Add current message
        messages.append({"role": "user", "content": user_message})
        
        try:
            response = self.client.chat.completions.create(
                model=PERPLEXITY_MODEL,
                messages=messages,
                temperature=0.7
            )
            
            assistant_message = response.choices[0].message.content
            
            # Update history
            self.conversation_history.append({"role": "user", "content": user_message})
            self.conversation_history.append({"role": "assistant", "content": assistant_message})
            
            return ChatResponse(
                message=assistant_message,
                sources=[self.doc_id] if self.document_context else [],
                confidence=0.9,
                language="hindi" if is_hindi else "english"
            )
            
        except Exception as e:
            print(f"Perplexity error: {e}")
            return self._chat_fallback(user_message, is_hindi)
    
    def _chat_fallback(self, user_message: str, is_hindi: bool) -> ChatResponse:
        """Fallback responses without AI"""
        query_lower = user_message.lower()
        
        # Common query patterns
        if not self.document_context:
            if is_hindi:
                response = "कृपया पहले कोई दस्तावेज़ अपलोड करें। फिर मैं आपके प्रश्नों का उत्तर दे सकूंगा।"
            else:
                response = "Please upload a document first. Then I can answer your questions about it."
        
        elif any(word in query_lower for word in ['deadline', 'date', 'अंतिम', 'तिथि', 'तारीख']):
            # Extract dates from document
            import re
            dates = re.findall(r'\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|\d{1,2}\s+(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\s+\d{4}', 
                              self.document_context, re.IGNORECASE)
            if dates:
                if is_hindi:
                    response = f"इस दस्तावेज़ में निम्नलिखित तिथियां पाई गई हैं: {', '.join(dates[:5])}"
                else:
                    response = f"The following dates were found in this document: {', '.join(dates[:5])}"
            else:
                if is_hindi:
                    response = "इस दस्तावेज़ में कोई विशिष्ट तिथि नहीं मिली।"
                else:
                    response = "No specific dates found in this document."
        
        elif any(word in query_lower for word in ['summary', 'सारांश', 'संक्षेप']):
            # Return first 200 words as summary
            words = self.document_context.split()[:200]
            summary = ' '.join(words) + '...'
            if is_hindi:
                response = f"दस्तावेज़ का संक्षिप्त विवरण:\n{summary}"
            else:
                response = f"Brief document summary:\n{summary}"
        
        elif any(word in query_lower for word in ['amount', 'money', 'rupees', 'रुपये', 'राशि', 'बजट']):
            import re
            amounts = re.findall(r'₹\s*[\d,]+(?:\.\d{2})?|Rs\.?\s*[\d,]+(?:\.\d{2})?|\d+\s*(?:crore|lakh|करोड़|लाख)', 
                                self.document_context, re.IGNORECASE)
            if amounts:
                if is_hindi:
                    response = f"इस दस्तावेज़ में निम्नलिखित राशियां पाई गई हैं: {', '.join(amounts[:5])}"
                else:
                    response = f"The following amounts were found: {', '.join(amounts[:5])}"
            else:
                if is_hindi:
                    response = "इस दस्तावेज़ में कोई वित्तीय राशि नहीं मिली।"
                else:
                    response = "No financial amounts found in this document."
        
        else:
            if is_hindi:
                response = "मुझे इस प्रश्न का विशिष्ट उत्तर नहीं मिला। कृपया अधिक विशिष्ट प्रश्न पूछें या 'summary' टाइप करें।"
            else:
                response = "I couldn't find a specific answer to this query. Please ask a more specific question or type 'summary' for an overview."
        
        return ChatResponse(
            message=response,
            sources=[self.doc_id] if self.document_context else [],
            confidence=0.5,
            language="hindi" if is_hindi else "english"
        )
    
    def clear_history(self):
        """Clear conversation history"""
        self.conversation_history = []
    
    def get_suggestions(self) -> List[str]:
        """Get suggested questions based on document context"""
        suggestions = [
            "What is the main purpose of this document?",
            "इस दस्तावेज़ का मुख्य उद्देश्य क्या है?",
            "What are the key deadlines?",
            "Are there any financial amounts mentioned?",
            "Who is responsible for implementation?",
            "क्या कोई अंतिम तिथि है?"
        ]
        return suggestions[:4]


# Singleton instance
chatbot = DocumentChatbot()


def chat_with_document(message: str, document_text: str = "", doc_id: str = "") -> dict:
    """
    Convenience function for chatbot interaction
    
    Args:
        message: User message
        document_text: Optional document context
        doc_id: Optional document ID
        
    Returns:
        Dictionary with response
    """
    if document_text:
        chatbot.set_document_context(document_text, doc_id)
    
    response = chatbot.chat(message)
    
    return {
        'message': response.message,
        'sources': response.sources,
        'confidence': response.confidence,
        'language': response.language,
        'suggestions': chatbot.get_suggestions()
    }


if __name__ == "__main__":
    print("Chatbot Module Test")
    print("-" * 50)
    
    # Test without document
    result = chat_with_document("What is this document about?")
    print(f"Response: {result['message']}")
    
    # Test with document
    test_doc = """
    Government of India
    Ministry of Finance
    
    OFFICE MEMORANDUM
    
    Subject: Revised guidelines for budget allocation FY 2024-25
    
    All departments are directed to submit their budget proposals by 15th January 2025.
    The total allocation for this fiscal year is Rs. 5,00,000 crore.
    
    कृपया सभी विभाग 15 जनवरी 2025 तक अपने बजट प्रस्ताव जमा करें।
    """
    
    result = chat_with_document("What is the deadline?", test_doc, "DOC-001")
    print(f"\nWith document - Response: {result['message']}")
