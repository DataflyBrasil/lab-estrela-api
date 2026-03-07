-- =====================================================
-- Schema COMPLETO para Auditoria de Chat de IA
-- Database: Supabase (PostgreSQL)
-- Rastreabilidade Total + Análise de Custos
-- =====================================================

-- Extensão para UUID
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- =====================================================
-- Tabela: chat_conversations
-- Armazena informações sobre cada conversa/sessão
-- =====================================================
CREATE TABLE chat_conversations (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    session_id VARCHAR(100) UNIQUE NOT NULL,
    user_id VARCHAR(100),
    title VARCHAR(500),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT true,
    
    -- Metadados de origem
    ip_address INET,
    user_agent TEXT,
    
    -- Metadados de custo agregado
    total_tokens_used INTEGER DEFAULT 0,
    total_cost_usd DECIMAL(10, 6) DEFAULT 0.00
);

CREATE INDEX idx_conversations_session_id ON chat_conversations(session_id);
CREATE INDEX idx_conversations_user_id ON chat_conversations(user_id);
CREATE INDEX idx_conversations_created_at ON chat_conversations(created_at DESC);

COMMENT ON TABLE chat_conversations IS 'Conversas do chat de IA com metadados de auditoria';

-- =====================================================
-- Tabela: chat_messages
-- Armazena cada mensagem com metadados completos
-- =====================================================
CREATE TABLE chat_messages (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    conversation_id UUID NOT NULL,
    parent_message_id UUID,  -- Referência à mensagem anterior (threading)
    
    -- Conteúdo da mensagem
    role VARCHAR(20) NOT NULL CHECK (role IN ('user', 'assistant', 'system')),
    content TEXT,
    
    -- Metadados da IA (quando role='assistant')
    model_name VARCHAR(100),           -- gemini-2.5-flash, etc
    input_tokens INTEGER,              -- tokens de entrada
    output_tokens INTEGER,             -- tokens de saída
    total_tokens INTEGER,              -- total de tokens
    cost_usd DECIMAL(10, 6),           -- custo em USD
    response_time_ms INTEGER,          -- tempo de resposta em ms
    
    -- Metadados Gemini
    finish_reason VARCHAR(50),         -- STOP, MAX_TOKENS, SAFETY, etc
    safety_ratings JSONB,              -- ratings de segurança
    gemini_metadata JSONB,             -- qualquer metadata extra do Gemini
    
    -- Auditoria
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT fk_conversation
        FOREIGN KEY (conversation_id) 
        REFERENCES chat_conversations(id)
        ON DELETE CASCADE,
    CONSTRAINT fk_parent_message
        FOREIGN KEY (parent_message_id)
        REFERENCES chat_messages(id)
        ON DELETE SET NULL
);

CREATE INDEX idx_messages_conversation_id ON chat_messages(conversation_id);
CREATE INDEX idx_messages_created_at ON chat_messages(created_at);
CREATE INDEX idx_messages_parent_id ON chat_messages(parent_message_id);

COMMENT ON TABLE chat_messages IS 'Mensagens individuais com rastreamento completo de tokens e custos';

-- =====================================================
-- Tabela: tool_executions
-- Rastreamento DETALHADO de cada execução de tool
-- =====================================================
CREATE TABLE tool_executions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    message_id UUID NOT NULL,          -- Mensagem que gerou esta execução
    conversation_id UUID NOT NULL,     -- Denormalized para queries rápidas
    
    -- Identificação da tool
    tool_name VARCHAR(100) NOT NULL,   -- query_unit_revenue, etc
    tool_description TEXT,             -- Descrição da ferramenta
    
    -- Parâmetros e resultado
    tool_args JSONB NOT NULL,          -- Argumentos passados (ex: {start_date, end_date})
    tool_result JSONB,                 -- Resultado retornado pela tool
    
    -- SQL executado (se aplicável)
    sql_queries TEXT[],                -- Array de queries SQL executadas
    sql_filters JSONB,                 -- Filtros aplicados (ex: {unit: "Serrinha", date_range: ...})
    
    -- Endpoints/serviços consultados
    endpoints_called TEXT[],           -- Ex: ["/unidades/faturamento", "get_unit_revenue_data"]
    database_name VARCHAR(100),        -- Qual banco foi consultado
    
    -- Performance
    execution_time_ms INTEGER,         -- Tempo de execução em ms
    rows_returned INTEGER,             -- Quantas linhas foram retornadas
    data_size_bytes INTEGER,           -- Tamanho dos dados retornados
    
    -- Status e errors
    status VARCHAR(20) DEFAULT 'success' CHECK (status IN ('success', 'error', 'timeout')),
    error_message TEXT,                -- Mensagem de erro (se houver)
    error_traceback TEXT,              -- Stack trace (se houver)
    
    -- Auditoria
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT fk_tool_message
        FOREIGN KEY (message_id)
        REFERENCES chat_messages(id)
        ON DELETE CASCADE,
    CONSTRAINT fk_tool_conversation
        FOREIGN KEY (conversation_id)
        REFERENCES chat_conversations(id)
        ON DELETE CASCADE
);

CREATE INDEX idx_tool_executions_message_id ON tool_executions(message_id);
CREATE INDEX idx_tool_executions_conversation_id ON tool_executions(conversation_id);
CREATE INDEX idx_tool_executions_tool_name ON tool_executions(tool_name);
CREATE INDEX idx_tool_executions_status ON tool_executions(status);
CREATE INDEX idx_tool_executions_created_at ON tool_executions(created_at DESC);

COMMENT ON TABLE tool_executions IS 'Rastreamento detalhado de cada execução de ferramenta de IA';
COMMENT ON COLUMN tool_executions.sql_queries IS 'Array de queries SQL executadas pela tool';
COMMENT ON COLUMN tool_executions.sql_filters IS 'JSON com filtros aplicados (unidade, data, etc)';
COMMENT ON COLUMN tool_executions.endpoints_called IS 'Funções/endpoints internos chamados';

-- =====================================================
-- Trigger: Auto-update updated_at
-- =====================================================
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_conversations_updated_at
    BEFORE UPDATE ON chat_conversations
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- =====================================================
-- Trigger: Agregar custos na conversa
-- Atualiza total_tokens_used e total_cost_usd
-- =====================================================
CREATE OR REPLACE FUNCTION aggregate_conversation_costs()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE chat_conversations
    SET 
        total_tokens_used = (
            SELECT COALESCE(SUM(total_tokens), 0)
            FROM chat_messages
            WHERE conversation_id = NEW.conversation_id
        ),
        total_cost_usd = (
            SELECT COALESCE(SUM(cost_usd), 0.00)
            FROM chat_messages
            WHERE conversation_id = NEW.conversation_id
        )
    WHERE id = NEW.conversation_id;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_conversation_costs_on_message
    AFTER INSERT OR UPDATE ON chat_messages
    FOR EACH ROW
    WHEN (NEW.total_tokens IS NOT NULL OR NEW.cost_usd IS NOT NULL)
    EXECUTE FUNCTION aggregate_conversation_costs();

-- =====================================================
-- View: Análise Completa de Mensagem
-- Junta mensagem + tool executions para análise
-- =====================================================
CREATE OR REPLACE VIEW message_analysis AS
SELECT 
    m.id as message_id,
    m.conversation_id,
    c.session_id,
    c.user_id,
    m.role,
    m.content,
    m.model_name,
    m.total_tokens,
    m.cost_usd,
    m.response_time_ms,
    m.created_at,
    
    -- Tool executions agregados
    COALESCE(json_agg(
        json_build_object(
            'tool_name', t.tool_name,
            'tool_args', t.tool_args,
            'execution_time_ms', t.execution_time_ms,
            'rows_returned', t.rows_returned,
            'status', t.status,
            'endpoints_called', t.endpoints_called,
            'sql_filters', t.sql_filters
        ) ORDER BY t.created_at
    ) FILTER (WHERE t.id IS NOT NULL), '[]'::json) as tool_executions
    
FROM chat_messages m
LEFT JOIN chat_conversations c ON m.conversation_id = c.id
LEFT JOIN tool_executions t ON t.message_id = m.id
GROUP BY m.id, c.session_id, c.user_id;

COMMENT ON VIEW message_analysis IS 'View completa com mensagem + tool executions para análise';

-- =====================================================
-- View: Estatísticas de Custos por Usuário
-- =====================================================
CREATE OR REPLACE VIEW user_cost_stats AS
SELECT 
    c.user_id,
    COUNT(DISTINCT c.id) as total_conversations,
    COUNT(m.id) as total_messages,
    SUM(m.total_tokens) as total_tokens,
    SUM(m.cost_usd) as total_cost_usd,
    AVG(m.cost_usd) as avg_cost_per_message,
    MAX(c.created_at) as last_conversation_at
FROM chat_conversations c
LEFT JOIN chat_messages m ON c.id = m.conversation_id
WHERE c.user_id IS NOT NULL
GROUP BY c.user_id;

COMMENT ON VIEW user_cost_stats IS 'Estatísticas de uso e custo por usuário';

-- =====================================================
-- View: Estatísticas de Tools
-- Análise de performance das ferramentas
-- =====================================================
CREATE OR REPLACE VIEW tool_performance_stats AS
SELECT 
    tool_name,
    COUNT(*) as total_executions,
    COUNT(*) FILTER (WHERE status = 'success') as successful_executions,
    COUNT(*) FILTER (WHERE status = 'error') as failed_executions,
    ROUND(AVG(execution_time_ms)) as avg_execution_time_ms,
    ROUND(AVG(rows_returned)) as avg_rows_returned,
    MAX(execution_time_ms) as max_execution_time_ms,
    MIN(execution_time_ms) as min_execution_time_ms
FROM tool_executions
GROUP BY tool_name
ORDER BY total_executions DESC;

COMMENT ON VIEW tool_performance_stats IS 'Estatísticas de performance por ferramenta';

-- =====================================================
-- Função: Obter contexto completo de uma resposta
-- Retorna TUDO sobre como uma resposta foi gerada
-- =====================================================
CREATE OR REPLACE FUNCTION get_response_context(p_message_id UUID)
RETURNS JSON AS $$
DECLARE
    result JSON;
BEGIN
    SELECT json_build_object(
        'message', json_build_object(
            'id', m.id,
            'content', m.content,
            'model', m.model_name,
            'tokens', m.total_tokens,
            'cost_usd', m.cost_usd,
            'response_time_ms', m.response_time_ms,
            'created_at', m.created_at
        ),
        'conversation', json_build_object(
            'id', c.id,
            'session_id', c.session_id,
            'user_id', c.user_id,
            'title', c.title
        ),
        'tool_executions', COALESCE(json_agg(
            json_build_object(
                'tool_name', t.tool_name,
                'args', t.tool_args,
                'result_summary', json_build_object(
                    'rows', t.rows_returned,
                    'size_bytes', t.data_size_bytes,
                    'execution_time_ms', t.execution_time_ms
                ),
                'sql_queries', t.sql_queries,
                'sql_filters', t.sql_filters,
                'endpoints_called', t.endpoints_called,
                'database', t.database_name,
                'status', t.status,
                'error', t.error_message
            ) ORDER BY t.created_at
        ) FILTER (WHERE t.id IS NOT NULL), '[]'::json),
        'previous_context', (
            SELECT json_agg(
                json_build_object(
                    'role', pm.role,
                    'content', LEFT(pm.content, 200),
                    'created_at', pm.created_at
                ) ORDER BY pm.created_at DESC
            )
            FROM chat_messages pm
            WHERE pm.conversation_id = m.conversation_id
            AND pm.created_at < m.created_at
            LIMIT 5
        )
    ) INTO result
    FROM chat_messages m
    JOIN chat_conversations c ON m.conversation_id = c.id
    LEFT JOIN tool_executions t ON t.message_id = m.id
    WHERE m.id = p_message_id
    GROUP BY m.id, c.id;
    
    RETURN result;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION get_response_context(UUID) IS 'Retorna contexto COMPLETO de como uma resposta foi gerada';

-- =====================================================
-- Função: Limpeza de conversas antigas
-- =====================================================
CREATE OR REPLACE FUNCTION cleanup_old_conversations()
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    WITH deleted AS (
        DELETE FROM chat_conversations
        WHERE is_active = false
        AND created_at < CURRENT_TIMESTAMP - INTERVAL '30 days'
        RETURNING *
    )
    SELECT COUNT(*) INTO deleted_count FROM deleted;
    
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- Exemplo de Query: "Por que recebi esta resposta?"
-- =====================================================
-- SELECT get_response_context('message-uuid-aqui');

-- =====================================================
-- Exemplo: Custos do último mês
-- =====================================================
-- SELECT 
--     user_id,
--     total_conversations,
--     total_cost_usd,
--     ROUND(total_cost_usd / NULLIF(total_messages, 0), 4) as cost_per_message
-- FROM user_cost_stats
-- ORDER BY total_cost_usd DESC;

-- =====================================================
-- Exemplo: Performance das tools
-- =====================================================
-- SELECT * FROM tool_performance_stats;

-- ✅ Schema de auditoria completo criado!
