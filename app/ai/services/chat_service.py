import logging
from typing import List, Optional, Dict, Any
from datetime import datetime
import uuid
import json

from app.ai.config.supabase_client import get_supabase_client
from app.ai.models.conversation import ChatMessage, Conversation
from app.ai.utils.cost_calculator import calculate_cost

logger = logging.getLogger(__name__)

class ChatService:
    def __init__(self):
        self.supabase = get_supabase_client()

    def get_or_create_conversation(self, session_id: Optional[str] = None, user_id: Optional[str] = None) -> Conversation:
        """
        Recupera uma conversa existente ou cria uma nova.
        """
        if not session_id:
            session_id = str(uuid.uuid4())
            logger.info(f"Gerando novo session_id: {session_id}")

        try:
            # Tenta buscar conversa existente
            response = self.supabase.table('chat_conversations')\
                .select('*')\
                .eq('session_id', session_id)\
                .execute()
            
            if response.data:
                data = response.data[0]
                logger.info(f"Conversa encontrada: {data['id']}")
                return Conversation(**data)
            
            # Se não existir, cria nova
            new_conversation = {
                'session_id': session_id,
                'user_id': user_id,
                'title': 'Nova Conversa',
                'is_active': True
            }
            
            response = self.supabase.table('chat_conversations')\
                .insert(new_conversation)\
                .execute()
                
            if response.data:
                logger.info(f"Nova conversa criada: {response.data[0]['id']}")
                return Conversation(**response.data[0])
                
            raise Exception("Falha ao criar conversa")
            
        except Exception as e:
            logger.error(f"Erro em get_or_create_conversation: {e}")
            raise e

    def get_conversation_history(self, session_id: str, limit: int = 20) -> List[ChatMessage]:
        """
        Recupera o histórico de mensagens de uma conversa.
        """
        try:
            # Primeiro pega o ID da conversa
            conv = self.get_or_create_conversation(session_id)
            
            # Busca mensagens (apenas user e assistant para contexto)
            response = self.supabase.table('chat_messages')\
                .select('*')\
                .eq('conversation_id', conv.id)\
                .in_('role', ['user', 'assistant'])\
                .order('created_at', desc=True)\
                .limit(limit)\
                .execute()
                
            messages = []
            # Reverter ordem para cronológica (mais antiga -> mais recente)
            for msg_data in reversed(response.data):
                messages.append(ChatMessage(**msg_data))
                
            return messages
            
        except Exception as e:
            logger.error(f"Erro ao buscar histórico: {e}")
            return []

    def save_message(self, 
                     conversation_id: str, 
                     role: str, 
                     content: str, 
                     model_name: Optional[str] = None,
                     usage_metadata: Optional[Dict] = None,
                     response_time_ms: Optional[int] = None) -> str:
        """
        Salva uma mensagem no histórico com metadados e calcula custos.
        """
        try:
            message_data = {
                'conversation_id': conversation_id,
                'role': role,
                'content': content,
                'model_name': model_name,
                'response_time_ms': response_time_ms
            }
            
            # Calcular custos se houver metadados de uso
            if usage_metadata and role == 'assistant':
                prompt_tokens = usage_metadata.get('prompt_token_count', 0)
                candidates_tokens = usage_metadata.get('candidates_token_count', 0)
                total_tokens = usage_metadata.get('total_token_count', 0)
                
                message_data['input_tokens'] = prompt_tokens
                message_data['output_tokens'] = candidates_tokens
                message_data['total_tokens'] = total_tokens
                
                # Calcular custo financeiro
                if model_name:
                    try:
                        # Normaliza nome do modelo (ex: 'models/gemini-2.5-flash' -> 'gemini-2.5-flash')
                        clean_model = model_name.split('/')[-1]
                        cost_info = calculate_cost(
                            input_tokens=prompt_tokens, 
                            output_tokens=candidates_tokens, 
                            model=clean_model
                        )
                        message_data['cost_usd'] = float(cost_info['total_cost'])
                    except Exception as e:
                        logger.warning(f"Erro ao calcular custo: {e}")
            
            response = self.supabase.table('chat_messages').insert(message_data).execute()
            
            if response.data:
                return response.data[0]['id']
            return None
            
        except Exception as e:
            logger.error(f"Erro ao salvar mensagem: {e}")
            return None

    def save_tool_execution(self,
                          message_id: str,
                          conversation_id: str,
                          tool_name: str,
                          tool_args: Dict,
                          tool_result: Any,
                          execution_time_ms: int,
                          status: str = 'success',
                          error_message: Optional[str] = None):
        """
        Salva o registro de execução de uma ferramenta.
        """
        try:
            # Serializar resultado para JSON se necessário
            if not isinstance(tool_result, (dict, list, str, int, float, bool, type(None))):
                tool_result = str(tool_result)
            
            execution_data = {
                'message_id': message_id,
                'conversation_id': conversation_id,
                'tool_name': tool_name,
                'tool_args': tool_args,
                'tool_result': tool_result,
                'execution_time_ms': execution_time_ms,
                'status': status,
                'error_message': error_message
            }
            
            self.supabase.table('tool_executions').insert(execution_data).execute()
            
        except Exception as e:
            logger.error(f"Erro ao salvar tool execution: {e}")

    def update_conversation_title(self, conversation_id: str, title: str):
        """Atualiza o título da conversa"""
        try:
            self.supabase.table('chat_conversations')\
                .update({'title': title})\
                .eq('id', conversation_id)\
                .execute()
        except Exception as e:
            logger.error(f"Erro ao atualizar título: {e}")

chat_service = ChatService()
