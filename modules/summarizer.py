"""
Summarization Module for Government Document AI System
Generates 3-level summaries: Secretary, Director, Officer
"""
import os
from typing import Dict, Optional
from dataclasses import dataclass
from enum import Enum

# Try to import OpenAI, fallback to extractive summarization
try:
    from openai import OpenAI
    PERPLEXITY_AVAILABLE = True
except ImportError:
    PERPLEXITY_AVAILABLE = False

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))
from config import PERPLEXITY_API_KEY, PERPLEXITY_BASE_URL, SUMMARY_LIMITS, PERPLEXITY_MODEL, PERPLEXITY_FALLBACK_MODEL


class SummaryLevel(Enum):
    """Summary detail levels for different audiences"""
    SECRETARY = "secretary"    # 1 sentence, max 50 words
    DIRECTOR = "director"      # 1 paragraph, max 150 words
    OFFICER = "officer"        # Detailed with action items, max 500 words


@dataclass
class Summary:
    """Summary result"""
    level: str
    content: str
    word_count: int
    key_points: list
    action_required: bool


class DocumentSummarizer:
    """
    Document Summarizer for government documents
    Generates 3 levels of summaries for different stakeholders
    """
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize summarizer
        
        Args:
            api_key: Perplexity API key (optional, uses env var if not provided)
        """
        self.api_key = api_key or PERPLEXITY_API_KEY
        self.client = None
        
        if PERPLEXITY_AVAILABLE and self.api_key:
            self.client = OpenAI(api_key=self.api_key, base_url=PERPLEXITY_BASE_URL)
            print("✓ Perplexity client initialized")
        else:
            print("⚠ Perplexity not available, using extractive summarization")
    
    def summarize(self, text: str, level: SummaryLevel = SummaryLevel.DIRECTOR) -> Summary:
        """
        Generate summary at specified level
        
        Args:
            text: Document text to summarize
            level: Summary level (secretary, director, officer)
            
        Returns:
            Summary object with content and metadata
        """
        if isinstance(level, str):
            level = SummaryLevel(level.lower())
        
        if self.client:
            return self._summarize_with_perplexity(text, level)
        else:
            return self._summarize_extractive(text, level)
    
    def summarize_all_levels(self, text: str) -> Dict[str, Summary]:
        """
        Generate summaries at all 3 levels
        
        Args:
            text: Document text
            
        Returns:
            Dictionary with summaries for each level
        """
        return {
            'secretary': self.summarize(text, SummaryLevel.SECRETARY),
            'director': self.summarize(text, SummaryLevel.DIRECTOR),
            'officer': self.summarize(text, SummaryLevel.OFFICER)
        }
    
    def _summarize_with_perplexity(self, text: str, level: SummaryLevel) -> Summary:
        """Use Perplexity for intelligent summarization"""
        
        prompts = {
            SummaryLevel.SECRETARY: f"""You are summarizing a government document for a Secretary-level official.
            
Create a ONE SENTENCE summary (maximum 50 words) capturing the most critical point.
Focus on: What is the main decision/order/directive?

Document:
{text[:10000]}

Summary (1 sentence, max 50 words):""",
            
            SummaryLevel.DIRECTOR: f"""You are summarizing a government document for a Director-level official.

Create a ONE PARAGRAPH summary (maximum 150 words) with key details.
Include: Main directive, key stakeholders, timeline if any, budget if mentioned.

Document:
{text[:10000]}

Summary (1 paragraph, max 150 words):""",
            
            SummaryLevel.OFFICER: f"""You are summarizing a government document for an implementing Officer.

Create a DETAILED summary (maximum 500 words) with:
1. Main directive/order
2. Background context
3. Key stakeholders and their responsibilities
4. Deadlines and timelines
5. Budget/financial implications
6. Action items (who must do what by when)
7. Reporting requirements

Document:
{text[:10000]}

Detailed Summary with Action Items:"""
        }
        
        try:
            response = self.client.chat.completions.create(
                model=PERPLEXITY_MODEL,
                messages=[
                    {"role": "system", "content": "You are an expert at summarizing Indian government documents. Be precise, formal, and action-oriented."},
                    {"role": "user", "content": prompts[level]}
                ],
                temperature=0.3
            )
            
            content = response.choices[0].message.content.strip()
            
        except Exception as e:
            print(f"Perplexity error: {e}, falling back to extractive")
            return self._summarize_extractive(text, level)
        
        # Extract key points
        key_points = self._extract_key_points(content)
        
        # Check for action requirement
        action_required = any(word in content.lower() for word in 
            ['must', 'shall', 'required', 'deadline', 'by date', 'action'])
        
        return Summary(
            level=level.value,
            content=content,
            word_count=len(content.split()),
            key_points=key_points,
            action_required=action_required
        )
    
    def _summarize_extractive(self, text: str, level: SummaryLevel) -> Summary:
        """
        Fallback extractive summarization without AI
        Uses sentence scoring based on importance indicators
        """
        # Split into sentences
        sentences = self._split_sentences(text)
        
        if not sentences:
            return Summary(
                level=level.value,
                content="No content to summarize.",
                word_count=0,
                key_points=[],
                action_required=False
            )
        
        # Score sentences
        scored = self._score_sentences(sentences)
        
        # Select top sentences based on level
        limits = SUMMARY_LIMITS[level.value]["max_words"]
        
        selected = []
        word_count = 0
        
        for sentence, score in sorted(scored, key=lambda x: x[1], reverse=True):
            sentence_words = len(sentence.split())
            
            # Stricter checks for distinction
            if level == SummaryLevel.SECRETARY:
                # Secretary: strict 50 words OR max 2 sentences (whichever first)
                if len(selected) >= 2 or word_count + sentence_words > limits:
                    continue 

            if word_count + sentence_words <= limits:
                selected.append(sentence)
                word_count += sentence_words
            
            if word_count >= limits * 0.9:
                break
        
        # Reorder by original position
        ordered = sorted(selected, key=lambda s: sentences.index(s))
        content = ' '.join(ordered)
        
        # Extract key points
        key_points = self._extract_key_points(content)
        
        # Check for action requirement
        action_required = any(word in content.lower() for word in 
            ['must', 'shall', 'required', 'deadline', 'by date'])
        
        return Summary(
            level=level.value,
            content=content,
            word_count=len(content.split()),
            key_points=key_points,
            action_required=action_required
        )
    
    def _split_sentences(self, text: str) -> list:
        """Split text into sentences"""
        import re
        # Handle Hindi and English sentence endings
        sentences = re.split(r'[।.!?]+', text)
        return [s.strip() for s in sentences if s.strip() and len(s.split()) > 3]
    
    def _score_sentences(self, sentences: list) -> list:
        """
        Score sentences by importance for government documents
        """
        importance_words = {
            'high': ['order', 'directed', 'must', 'shall', 'required', 'mandatory',
                     'approved', 'sanctioned', 'pursuant', 'hereby', 'आदेश', 'निर्देश',
                     'अनिवार्य', 'स्वीकृत'],
            'medium': ['Department', 'Ministry', 'Government', 'Committee', 'Board',
                       'Officer', 'Director', 'Secretary', 'मंत्रालय', 'विभाग', 'सरकार'],
            'action': ['deadline', 'by date', 'within', 'days', 'immediately',
                       'forthwith', 'urgent', 'तुरंत', 'तिथि', 'दिनों के भीतर']
        }
        
        scored = []
        for sentence in sentences:
            score = 0
            lower = sentence.lower()
            
            # Score based on importance words
            for word in importance_words['high']:
                if word.lower() in lower:
                    score += 3
            
            for word in importance_words['medium']:
                if word.lower() in lower:
                    score += 2
            
            for word in importance_words['action']:
                if word.lower() in lower:
                    score += 2.5
            
            # Bonus for sentences with numbers (dates, amounts)
            import re
            if re.search(r'\d+', sentence):
                score += 1
            
            # Bonus for sentences with rupee amounts
            if '₹' in sentence or 'crore' in lower or 'lakh' in lower:
                score += 2
            
            scored.append((sentence, score))
        
        return scored
    
    def _extract_key_points(self, text: str) -> list:
        """Extract key points from summary"""
        import re
        
        key_points = []
        
        # Look for numbered points
        numbered = re.findall(r'\d+\.\s*([^.]+)', text)
        key_points.extend(numbered[:5])
        
        # Look for bullet points
        bullets = re.findall(r'[-•]\s*([^-•\n]+)', text)
        key_points.extend(bullets[:5])
        
        # If no structured points found, extract important phrases
        if not key_points:
            sentences = self._split_sentences(text)
            key_points = sentences[:3]
        
        return key_points[:5]


def summarize_document(text: str, level: str = "director") -> str:
    """
    Convenience function for quick summarization
    
    Args:
        text: Document text
        level: Summary level (secretary, director, officer)
        
    Returns:
        Summary text
    """
    summarizer = DocumentSummarizer()
    result = summarizer.summarize(text, SummaryLevel(level))
    return result.content


if __name__ == "__main__":
    # Test summarizer
    print("Summarizer Module Test")
    print("-" * 50)
    
    test_text = """
    Government of India
    Ministry of Finance
    Department of Expenditure
    
    OFFICE MEMORANDUM
    
    Subject: Release of funds for implementation of Digital India programme
    
    The undersigned is directed to convey the sanction of the President of India
    for release of Rs. 500 crore (Rupees Five Hundred Crore only) for the 
    implementation of Digital India programme during FY 2024-25.
    
    The funds shall be released to the Ministry of Electronics and Information
    Technology within 15 days of the issue of this order.
    
    The Ministry of Electronics and IT is required to submit quarterly progress
    reports to this Ministry by the 15th of the month following each quarter.
    
    This issues with the approval of Secretary (Expenditure).
    """
    
    summarizer = DocumentSummarizer()
    
    for level in SummaryLevel:
        result = summarizer.summarize(test_text, level)
        print(f"\n{level.value.upper()} Summary:")
        print(f"  {result.content}")
        print(f"  Words: {result.word_count}")
        print(f"  Action Required: {result.action_required}")
