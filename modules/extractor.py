"""
Action Extraction Module for Government Document AI System
Extracts: WHO must do WHAT by WHEN with Priority flagging
"""
import re
from typing import Dict, List, Optional
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from dateutil import parser as date_parser

# Try OpenAI/Perplexity for intelligent extraction
try:
    from openai import OpenAI
    PERPLEXITY_AVAILABLE = True
except ImportError:
    PERPLEXITY_AVAILABLE = False

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))
from config import PERPLEXITY_API_KEY, PERPLEXITY_BASE_URL, PERPLEXITY_MODEL


class Priority(Enum):
    """Action priority levels"""
    CRITICAL = "critical"   # Immediate action required
    HIGH = "high"          # Within 7 days
    MEDIUM = "medium"      # Within 30 days
    LOW = "low"            # No specific deadline


@dataclass
class ActionItem:
    """Extracted action item"""
    who: str                    # Responsible party
    what: str                   # Required action
    when: Optional[str]         # Deadline
    deadline_date: Optional[datetime]  # Parsed deadline
    priority: Priority
    original_text: str          # Source text
    confidence: float           # Extraction confidence


@dataclass
class ExtractionResult:
    """Complete extraction result"""
    actions: List[ActionItem]
    deadlines: List[Dict]
    responsible_parties: List[str]
    financial_amounts: List[Dict]
    references: List[str]


class ActionExtractor:
    """
    Extract action items, deadlines, and responsibilities from government documents
    """
    
    def __init__(self, api_key: Optional[str] = None):
        """Initialize extractor"""
        self.api_key = api_key or PERPLEXITY_API_KEY
        self.client = None
        
        if PERPLEXITY_AVAILABLE and self.api_key:
            self.client = OpenAI(api_key=self.api_key, base_url=PERPLEXITY_BASE_URL)
            print("✓ Action Extractor with Perplexity")
        else:
            print("⚠ Using rule-based extraction")
        
        # Indian government entity patterns
        self.entity_patterns = [
            r'Ministry of [\w\s]+',
            r'Department of [\w\s]+',
            r'Directorate [\w\s]+',
            r'(?:Joint |Additional |Under |Deputy )?Secretary',
            r'(?:Joint |Additional )?Director',
            r'Commissioner',
            r'Controller',
            r'Head of (?:Department|Office)',
            r'मंत्रालय', r'विभाग', r'निदेशालय', r'सचिव', r'निदेशक'
        ]
        
        # Action verb patterns
        self.action_patterns = [
            r'(?:is |are )?(?:hereby |)(?:directed|required|instructed|ordered)',
            r'(?:shall|must|will|should) (?:ensure|submit|provide|release|complete)',
            r'(?:to |)(?:take (?:immediate |necessary |)action)',
            r'(?:is |)requested to',
            r'आदेश(?:ित|)', r'निर्देश(?:ित|)', r'अपेक्षित'
        ]
        
        # Deadline patterns
        self.deadline_patterns = [
            r'(?:by|before|within|latest by|not later than) (\d{1,2}[-/]\d{1,2}[-/]\d{2,4})',
            r'(?:by|before|within|latest by) (\d{1,2}(?:st|nd|rd|th)? (?:January|February|March|April|May|June|July|August|September|October|November|December),? \d{4})',
            r'within (\d+) (?:days?|weeks?|months?)',
            r'(\d{1,2}[-/]\d{1,2}[-/]\d{2,4}) (?:तक|से पहले)',
            r'forthwith|immediately|तुरंत|तत्काल'
        ]
    
    def extract(self, text: str) -> ExtractionResult:
        """
        Extract all action items and metadata from document
        
        Args:
            text: Document text
            
        Returns:
            ExtractionResult with actions, deadlines, etc.
        """
        if self.client:
            actions = self._extract_with_perplexity(text)
        else:
            actions = self._extract_rule_based(text)
        
        # Extract additional metadata
        deadlines = self._extract_all_deadlines(text)
        parties = self._extract_responsible_parties(text)
        amounts = self._extract_financial_amounts(text)
        references = self._extract_references(text)
        
        return ExtractionResult(
            actions=actions,
            deadlines=deadlines,
            responsible_parties=parties,
            financial_amounts=amounts,
            references=references
        )
    
    def _extract_with_perplexity(self, text: str) -> List[ActionItem]:
        """Use Perplexity for intelligent action extraction"""
        
        prompt = f"""Extract action items from this government document.

For each action item, identify:
1. WHO: The responsible party/entity
2. WHAT: The required action
3. WHEN: The deadline (if specified)
4. PRIORITY: critical/high/medium/low based on urgency

Format your response as a list:
---
WHO: [entity]
WHAT: [action required]
WHEN: [deadline or "Not specified"]
PRIORITY: [critical/high/medium/low]
ORIGINAL: [exact quote from document]
---

Document:
{text[:8000]}

Extract all action items:"""

        try:
            response = self.client.chat.completions.create(
                model=PERPLEXITY_MODEL,
                messages=[
                    {"role": "system", "content": "You are an expert at extracting action items from Indian government documents. Be precise and extract ALL action items."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.2
            )
            
            content = response.choices[0].message.content
            return self._parse_ai_response(content)
            
        except Exception as e:
            print(f"Perplexity extraction failed: {e}")
            return self._extract_rule_based(text)
    
    def _parse_ai_response(self, response: str) -> List[ActionItem]:
        """Parse AI response into ActionItem objects"""
        actions = []
        
        # Split by action item separator
        items = re.split(r'---+', response)
        
        for item in items:
            if not item.strip():
                continue
            
            who = self._extract_field(item, 'WHO')
            what = self._extract_field(item, 'WHAT')
            when = self._extract_field(item, 'WHEN')
            priority_str = self._extract_field(item, 'PRIORITY')
            original = self._extract_field(item, 'ORIGINAL')
            
            if who and what:
                # Parse deadline
                deadline_date = self._parse_deadline(when) if when else None
                
                # Parse priority
                try:
                    priority = Priority(priority_str.lower()) if priority_str else Priority.MEDIUM
                except:
                    priority = Priority.MEDIUM
                
                actions.append(ActionItem(
                    who=who,
                    what=what,
                    when=when if when and when != "Not specified" else None,
                    deadline_date=deadline_date,
                    priority=priority,
                    original_text=original or what,
                    confidence=0.9
                ))
        
        return actions
    
    def _extract_field(self, text: str, field: str) -> Optional[str]:
        """Extract a field from AI response"""
        pattern = rf'{field}:\s*(.+?)(?:\n|$)'
        match = re.search(pattern, text, re.IGNORECASE)
        return match.group(1).strip() if match else None
    
    def _extract_rule_based(self, text: str) -> List[ActionItem]:
        """Rule-based action extraction fallback"""
        actions = []
        
        # Split into sentences
        sentences = re.split(r'[।.!?]\s*', text)
        
        for sentence in sentences:
            if not sentence.strip():
                continue
            
            # Check if sentence contains action indicators
            has_action = any(re.search(pattern, sentence, re.IGNORECASE) 
                           for pattern in self.action_patterns)
            
            if has_action:
                # Extract WHO
                who = self._extract_entity(sentence)
                
                # Extract WHAT (the action verb phrase)
                what = self._extract_action_phrase(sentence)
                
                # Extract WHEN
                when, deadline_date = self._extract_deadline_from_sentence(sentence)
                
                # Determine priority
                priority = self._determine_priority(sentence, deadline_date)
                
                if what:
                    actions.append(ActionItem(
                        who=who or "Concerned authority",
                        what=what,
                        when=when,
                        deadline_date=deadline_date,
                        priority=priority,
                        original_text=sentence.strip(),
                        confidence=0.7
                    ))
        
        return actions
    
    def _extract_entity(self, text: str) -> Optional[str]:
        """Extract responsible entity from text"""
        for pattern in self.entity_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(0)
        return None
    
    def _extract_action_phrase(self, text: str) -> Optional[str]:
        """Extract the main action phrase"""
        # Look for verb + object patterns
        patterns = [
            r'(?:directed|required|instructed) to (.+?)(?:[।.]|$)',
            r'(?:shall|must|will) (.+?)(?:[।.]|$)',
            r'(?:submit|provide|release|complete) (.+?)(?:[।.]|$)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(1).strip()[:200]
        
        return text[:200] if len(text) > 10 else None
    
    def _extract_deadline_from_sentence(self, text: str) -> tuple:
        """Extract deadline from sentence"""
        for pattern in self.deadline_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                deadline_str = match.group(1) if match.lastindex else match.group(0)
                deadline_date = self._parse_deadline(deadline_str)
                return (deadline_str, deadline_date)
        
        # Check for immediate action keywords
        if re.search(r'forthwith|immediately|तुरंत|तत्काल', text, re.IGNORECASE):
            return ("Immediately", datetime.now())
        
        return (None, None)
    
    def _parse_deadline(self, deadline_str: str) -> Optional[datetime]:
        """Parse deadline string to datetime"""
        if not deadline_str:
            return None
        
        try:
            # Handle relative deadlines
            match = re.match(r'within (\d+) (days?|weeks?|months?)', deadline_str, re.IGNORECASE)
            if match:
                num = int(match.group(1))
                unit = match.group(2).lower()
                
                from datetime import timedelta
                if 'day' in unit:
                    return datetime.now() + timedelta(days=num)
                elif 'week' in unit:
                    return datetime.now() + timedelta(weeks=num)
                elif 'month' in unit:
                    return datetime.now() + timedelta(days=num*30)
            
            # Try parsing as date
            return date_parser.parse(deadline_str, fuzzy=True)
            
        except:
            return None
    
    def _determine_priority(self, text: str, deadline: Optional[datetime]) -> Priority:
        """Determine action priority"""
        text_lower = text.lower()
        
        # Critical keywords
        if any(word in text_lower for word in ['immediately', 'forthwith', 'urgent', 'critical', 'तुरंत', 'अविलंब']):
            return Priority.CRITICAL
        
        # Check deadline proximity
        if deadline:
            days_until = (deadline - datetime.now()).days
            if days_until <= 3:
                return Priority.CRITICAL
            elif days_until <= 7:
                return Priority.HIGH
            elif days_until <= 30:
                return Priority.MEDIUM
        
        # High priority keywords
        if any(word in text_lower for word in ['required', 'mandatory', 'must', 'shall']):
            return Priority.HIGH
        
        return Priority.MEDIUM
    
    def _extract_all_deadlines(self, text: str) -> List[Dict]:
        """Extract all deadlines from text"""
        deadlines = []
        
        for pattern in self.deadline_patterns:
            for match in re.finditer(pattern, text, re.IGNORECASE):
                deadline_str = match.group(1) if match.lastindex else match.group(0)
                parsed = self._parse_deadline(deadline_str)
                
                # Get context (surrounding text)
                start = max(0, match.start() - 50)
                end = min(len(text), match.end() + 50)
                context = text[start:end]
                
                deadlines.append({
                    'text': deadline_str,
                    'parsed': parsed.isoformat() if parsed else None,
                    'context': context.strip()
                })
        
        return deadlines
    
    def _extract_responsible_parties(self, text: str) -> List[str]:
        """Extract all responsible parties/entities"""
        parties = set()
        
        for pattern in self.entity_patterns:
            for match in re.finditer(pattern, text, re.IGNORECASE):
                parties.add(match.group(0))
        
        return list(parties)
    
    def _extract_financial_amounts(self, text: str) -> List[Dict]:
        """Extract financial amounts from text"""
        amounts = []
        
        patterns = [
            r'₹\s*([\d,]+(?:\.\d+)?)\s*(crore|lakh|thousand)?',
            r'Rs\.?\s*([\d,]+(?:\.\d+)?)\s*(crore|lakh|thousand)?',
            r'Rupees?\s+([\w\s]+)\s+(crore|lakh|thousand)?',
            r'([\d,]+(?:\.\d+)?)\s*(crore|lakh|thousand)',
        ]
        
        for pattern in patterns:
            for match in re.finditer(pattern, text, re.IGNORECASE):
                amount = match.group(1).replace(',', '')
                unit = match.group(2) if match.lastindex >= 2 else None
                
                # Get context
                start = max(0, match.start() - 30)
                end = min(len(text), match.end() + 30)
                context = text[start:end]
                
                amounts.append({
                    'amount': amount,
                    'unit': unit,
                    'full_text': match.group(0),
                    'context': context.strip()
                })
        
        return amounts
    
    def _extract_references(self, text: str) -> List[str]:
        """Extract document references (file numbers, order numbers)"""
        references = []
        
        patterns = [
            r'(?:File No\.?|F\.No\.?)\s*[\w/-]+',
            r'(?:O\.M\.|Office Memorandum)\s*No\.?\s*[\w/-]+',
            r'(?:Order No\.?|Notification No\.?)\s*[\w/-]+',
            r'(?:Circular No\.?)\s*[\w/-]+',
            r'\d{1,2}/\d{1,2}/\d{4}-[\w]+',
        ]
        
        for pattern in patterns:
            for match in re.finditer(pattern, text, re.IGNORECASE):
                references.append(match.group(0))
        
        return list(set(references))


def extract_actions(text: str) -> List[Dict]:
    """
    Convenience function for quick action extraction
    
    Args:
        text: Document text
        
    Returns:
        List of action items as dictionaries
    """
    extractor = ActionExtractor()
    result = extractor.extract(text)
    
    return [
        {
            'who': action.who,
            'what': action.what,
            'when': action.when,
            'deadline': action.deadline_date.isoformat() if action.deadline_date else None,
            'priority': action.priority.value,
            'confidence': action.confidence
        }
        for action in result.actions
    ]


if __name__ == "__main__":
    print("Action Extractor Module Test")
    print("-" * 50)
    
    test_text = """
    The Ministry of Finance is hereby directed to release Rs. 500 crore 
    to the Ministry of Electronics and IT within 15 days.
    
    The Secretary, MeitY must submit a quarterly progress report by 
    15th January 2025.
    
    All Departments are required to complete the digitization of records
    by 31st March 2025.
    
    The Director of e-Governance shall take immediate action to ensure
    compliance with the Digital India guidelines.
    """
    
    extractor = ActionExtractor()
    result = extractor.extract(test_text)
    
    print(f"\nFound {len(result.actions)} action items:")
    for i, action in enumerate(result.actions, 1):
        print(f"\n{i}. {action.priority.value.upper()}")
        print(f"   WHO: {action.who}")
        print(f"   WHAT: {action.what}")
        print(f"   WHEN: {action.when or 'Not specified'}")
    
    print(f"\nFinancial amounts: {result.financial_amounts}")
    print(f"Responsible parties: {result.responsible_parties}")
