from sqlalchemy import Column, String, Boolean, Integer, Numeric, Text, DateTime, ForeignKey, Index
from sqlalchemy.dialects.postgresql import UUID, INET, JSONB
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
import uuid
from app.database import Base

class ChatConversation(Base):
    __tablename__ = "chat_conversations"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    session_id = Column(String(100), unique=True, index=True)
    user_id = Column(String(100), index=True)
    title = Column(String(500))
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    is_active = Column(Boolean, default=True)
    ip_address = Column(INET)
    user_agent = Column(Text)
    total_tokens_used = Column(Integer, default=0)
    total_cost_usd = Column(Numeric(10, 6), default=0)

    # Relationships
    messages = relationship("ChatMessage", back_populates="conversation", cascade="all, delete-orphan")

class ChatMessage(Base):
    __tablename__ = "chat_messages"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    conversation_id = Column(UUID(as_uuid=True), ForeignKey("chat_conversations.id", ondelete="CASCADE"), nullable=False, index=True)
    parent_message_id = Column(UUID(as_uuid=True), ForeignKey("chat_messages.id"), nullable=True, index=True)
    role = Column(String(20), nullable=False) # user / assistant / system
    content = Column(Text, nullable=False)
    model_name = Column(String(100))
    input_tokens = Column(Integer, default=0)
    output_tokens = Column(Integer, default=0)
    total_tokens = Column(Integer, default=0)
    cost_usd = Column(Numeric(10, 6), default=0)
    response_time_ms = Column(Integer)
    finish_reason = Column(String(50))
    safety_ratings = Column(JSONB)
    gemini_metadata = Column(JSONB)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), index=True)

    # Relationships
    conversation = relationship("ChatConversation", back_populates="messages")
    parent = relationship("ChatMessage", remote_side=[id], backref="children")
    tool_executions = relationship("ToolExecution", back_populates="message", cascade="all, delete-orphan")

class ToolExecution(Base):
    __tablename__ = "tool_executions"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    message_id = Column(UUID(as_uuid=True), ForeignKey("chat_messages.id", ondelete="CASCADE"), nullable=False, index=True)
    conversation_id = Column(UUID(as_uuid=True), ForeignKey("chat_conversations.id", ondelete="CASCADE"), nullable=False, index=True)
    
    # Tool Info
    tool_name = Column(String(100), nullable=False, index=True)
    tool_description = Column(Text)
    
    # Args/Result
    tool_args = Column(JSONB)
    tool_result = Column(JSONB)
    
    # SQL & Services
    sql_queries = Column(JSONB) # SQLAlchemy doesn't strictly support TEXT[] in all dialects easily without ARRAY, ensuring compatibility. Postgres Supports ARRAY(Text).
    # Using JSONB for arrays is often safer for generic compatibility, but user spec says TEXT[]. 
    # Let's use ARRAY(Text) if we are sure it's Postgres (User said Postgres).
    
    sql_filters = Column(JSONB)
    endpoints_called = Column(JSONB) # Using JSONB for arrays/lists is robust.
    database_name = Column(String(100))
    
    # Performance
    execution_time_ms = Column(Integer)
    rows_returned = Column(Integer)
    data_size_bytes = Column(Integer)
    
    # Status
    status = Column(String(20), index=True) # success / error / timeout
    error_message = Column(Text)
    error_traceback = Column(Text)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), index=True)

    # Relationships
    message = relationship("ChatMessage", back_populates="tool_executions")
