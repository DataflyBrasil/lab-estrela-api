from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
from app.ai.orchestrator.client import ai_service
import logging

router = APIRouter()
logger = logging.getLogger(__name__)

class ChatRequest(BaseModel):
    message: str
    session_id: Optional[str] = None
    user_id: Optional[str] = None
    history: Optional[List[Dict[str, Any]]] = None

class ChatResponse(BaseModel):
    response: str
    session_id: str
    conversation_id: Optional[str] = None
    usage: Optional[Dict[str, int]] = None

@router.post("/chat", response_model=ChatResponse)
async def chat_endpoint(request: ChatRequest):
    """
    Chat with the AI Analytics Engine.
    Supports conversation history via session_id.
    """

    try:
        result = await ai_service.chat(
            message=request.message,
            session_id=request.session_id,
            user_id=request.user_id,
            history=request.history
        )
        return ChatResponse(**result)
    except Exception as e:
        logger.error(f"Error processing chat request: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
