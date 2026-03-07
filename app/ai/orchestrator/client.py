import google.generativeai as genai
from app.ai.config.settings import settings
from app.ai.tools.registry import tool_registry
from app.ai.services.chat_service import chat_service
from app.ai.models.conversation import ChatMessage
import logging
from typing import List, Dict, Any, Optional
import time

logger = logging.getLogger(__name__)

class AIService:
    def __init__(self):
        if settings.GEMINI_API_KEY:
            genai.configure(api_key=settings.GEMINI_API_KEY)
            self._tools_map = tool_registry.get_callable_tools()
            # Use tool definitions (function declarations) for Gemini, not the callables
            tool_definitions = tool_registry.get_tool_definitions()
            self.model = genai.GenerativeModel(
                model_name='gemini-2.5-flash',
                tools=tool_definitions
            )
        else:
            logger.warning("Gemini API Key not set. AI Service will be disabled.")
            self.model = None

    async def chat(
        self, 
        message: str, 
        session_id: Optional[str] = None,
        user_id: Optional[str] = None,
        history: List[ChatMessage] = None
    ) -> Dict[str, Any]:
        """
        Processa mensagem do chat e retorna resposta com metadados.
        Agora salva tudo no banco de dados!
        """
        if not self.model:
             return {"response": "AI Service is not configured.", "session_id": session_id}

        try:
            from datetime import datetime
            current_date = datetime.now().strftime("%Y-%m-%d")
            
            # 1. Get or create conversation
            conversation = chat_service.get_or_create_conversation(session_id, user_id)
            
            # Se o título for o padrão, atualiza com a primeira mensagem
            if conversation.title == 'Nova Conversa':
                # Limita a 50 caracteres e remove quebras de linha
                new_title = (message[:50] + '...') if len(message) > 50 else message
                new_title = new_title.replace('\n', ' ').strip()
                chat_service.update_conversation_title(conversation.id, new_title)
                conversation.title = new_title
            
            # 2. Save user message
            user_message_id = chat_service.save_message(
                conversation_id=conversation.id,
                role='user',
                content=message
            )
            
            # 3. Load history from database if not provided
            if history is None:
                history = chat_service.get_conversation_history(conversation.session_id, limit=20)
            
            # 4. Convert history to Gemini format
            gemini_history = []
            if history:
                for msg in history:
                    # Handle both dict (from frontend) and ChatMessage object (from DB)
                    role = msg.get('role') if isinstance(msg, dict) else msg.role
                    content = msg.get('content') if isinstance(msg, dict) else msg.content
                    
                    if role == 'user':
                        gemini_history.append({
                            'role': 'user',
                            'parts': [{'text': content}]
                        })
                    elif role == 'assistant':
                        gemini_history.append({
                            'role': 'model',
                            'parts': [{'text': content}]
                        })
            
            # 5. Context injection
            full_message = f"System Date: {current_date}\nUser Query: {message}"
            
            # 6. Start chat with history
            chat = self.model.start_chat(history=gemini_history)
            
            # Track total response time
            start_time = time.time()
            response = chat.send_message(full_message)
            
            # 7. Manual function calling loop
            max_iterations = 5
            iteration = 0
            
            print("\n" + "="*50)
            print(f"🤖 User Message: {message}")
            print(f"📊 Session: {conversation.session_id}")
            print(f"💬 Context: {len(history)} mensagens anteriores" if history else "💬 Context: Nova conversa")
            print("="*50)
            
            while iteration < max_iterations:
                # Check if response contains function calls
                if not response.candidates[0].content.parts:
                    break
                
                # Check ALL parts for function calls (Gemini can return multiple)
                has_function_call = False
                for part in response.candidates[0].content.parts:
                    if not (hasattr(part, 'function_call') and part.function_call):
                        continue
                    
                    has_function_call = True
                    function_call = part.function_call
                    function_name = function_call.name
                    function_args = dict(function_call.args)
                    
                    print(f"\n⚙️  AI Decided to Call Tool: {function_name}")
                    print(f"    Args: {function_args}")
                    
                    # Execute the function and track time
                    if function_name in self._tools_map:
                        try:
                            tool_start = time.time()
                            result = self._tools_map[function_name](**function_args)
                            tool_time = int((time.time() - tool_start) * 1000)
                            
                            print(f"    ✅ Tool executed in {tool_time}ms")
                            
                            # Save tool execution to database
                            try:
                                chat_service.save_tool_execution(
                                    message_id=user_message_id,
                                    conversation_id=conversation.id,
                                    tool_name=function_name,
                                    tool_args=function_args,
                                    tool_result=result,
                                    execution_time_ms=tool_time,
                                    status='success'
                                )
                            except Exception as save_err:
                                logger.warning(f"Erro ao salvar tool execution: {save_err}")
                            
                            # Send result back to Gemini
                            response = chat.send_message(
                                genai.protos.Content(
                                    parts=[
                                        genai.protos.Part(
                                            function_response=genai.protos.FunctionResponse(
                                                name=function_name,
                                                response={'result': result}
                                            )
                                        )
                                    ]
                                )
                            )
                            iteration += 1
                            break  # Break inner for-loop to re-evaluate new response
                        except Exception as e:
                            logger.error(f"Error executing tool {function_name}: {e}", exc_info=True)
                            
                            # Save failed tool execution
                            try:
                                chat_service.save_tool_execution(
                                    message_id=user_message_id,
                                    conversation_id=conversation.id,
                                    tool_name=function_name,
                                    tool_args=function_args,
                                    tool_result=None,
                                    execution_time_ms=0,
                                    status='error',
                                    error_message=str(e)
                                )
                            except Exception as save_err:
                                logger.warning(f"Erro ao salvar tool error: {save_err}")
                            
                            return {
                                "response": f"Error executing tool: {str(e)}",
                                "session_id": conversation.session_id
                            }
                    else:
                        return {
                            "response": f"Unknown tool: {function_name}",
                            "session_id": conversation.session_id
                        }
                
                if not has_function_call:
                    # No function calls found in any part — we have the final response
                    break
            
            # 8. Calculate total response time
            total_time = int((time.time() - start_time) * 1000)
            
            # 9. Extract final text response safely
            # After the loop, the response might still contain a function_call
            # if max_iterations was reached. Handle gracefully.
            try:
                final_response = response.text
            except ValueError:
                # response.text raises ValueError if parts contain function_call
                # Extract text from parts manually, or provide a fallback
                text_parts = []
                for part in response.candidates[0].content.parts:
                    if hasattr(part, 'text') and part.text:
                        text_parts.append(part.text)
                if text_parts:
                    final_response = "\n".join(text_parts)
                else:
                    logger.warning("AI response contained only function calls after max iterations")
                    final_response = "Desculpe, não consegui processar completamente sua solicitação. Tente reformular sua pergunta."
            
            # 10. Extract usage metadata
            usage_metadata = None
            if hasattr(response, 'usage_metadata'):
                usage_metadata = {
                    'prompt_token_count': response.usage_metadata.prompt_token_count,
                    'candidates_token_count': response.usage_metadata.candidates_token_count,
                    'total_token_count': response.usage_metadata.total_token_count,
                }
                
                print(f"\n📊 Tokens: {usage_metadata['total_token_count']:,} total " +
                      f"({usage_metadata['prompt_token_count']:,} in + " +
                      f"{usage_metadata['candidates_token_count']:,} out)")
            
            # 11. Save assistant response with metadata
            chat_service.save_message(
                conversation_id=conversation.id,
                role='assistant',
                content=final_response,
                model_name=self.model.model_name,
                usage_metadata=usage_metadata,
                response_time_ms=total_time
            )
            
            print(f"\n✅ AI Final Response (in {total_time}ms):")
            print(final_response)
            print("="*50 + "\n")

            return {
                "response": final_response,
                "session_id": conversation.session_id,
                "conversation_id": conversation.id,
                "usage": usage_metadata
            }
            
        except Exception as e:
            logger.error(f"Error in AI chat: {e}", exc_info=True)
            return {
                "response": f"Error: {str(e)}",
                "session_id": session_id
            }

ai_service = AIService()
