"""
Modelos Pydantic para conversas de chat.
"""
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime

class ChatMessage(BaseModel):
    """Representa uma mensagem individual no chat"""
    id: Optional[str] = None
    conversation_id: Optional[str] = None
    role: str  # 'user', 'assistant', 'tool'
    content: Optional[str] = None
    tool_name: Optional[str] = None
    tool_args: Optional[dict] = None
    tool_result: Optional[dict] = None
    created_at: Optional[datetime] = None
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }

class Conversation(BaseModel):
    """Representa uma conversa completa"""
    id: Optional[str] = None
    session_id: str
    user_id: Optional[str] = None
    title: Optional[str] = None
    messages: List[ChatMessage] = []
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    is_active: bool = True
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }

class ChatRequest(BaseModel):
    """Request para o endpoint de chat"""
    message: str
    session_id: Optional[str] = None
    user_id: Optional[str] = None

class ChatResponse(BaseModel):
    """Response do endpoint de chat"""
    response: str
    session_id: str
    conversation_id: Optional[str] = None
