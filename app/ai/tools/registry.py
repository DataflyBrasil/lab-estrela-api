from app.ai.tools.unit_revenue import query_unit_revenue_tool
from app.ai.tools.exam_sla import query_exam_sla_tool
from app.ai.tools.doctor_ranking import query_doctor_ranking_tool
from app.ai.tools.budgets import query_budgets_tool
from app.ai.tools.strategic_finance import query_strategic_finance_tool
from app.ai.tools.detailed_finance import query_detailed_finance_tool
from app.ai.tools.client_analytics import query_client_analytics_tool
from app.ai.tools.patient_intelligence import query_patient_intelligence_tool

# Técnico / Operacional
from app.ai.tools.operational_sla import query_operational_sla_tool
from app.ai.tools.laudos_comparativo import query_laudos_comparativo_tool
from app.ai.tools.query_exam_detail import query_exam_detail_tool

# Orçamentos granulares
from app.ai.tools.orcamentos_granular import query_orcamentos_pacientes_tool, query_orcamentos_unidade_tool

# Management / Metas
from app.ai.tools.management_indicators import query_management_indicators_tool, list_units_tool
from app.ai.tools.metas_execucao import query_metas_execucao_tool

# Analytical tools
from app.ai.tools.compare_periods import compare_periods, COMPARE_PERIODS_DECLARATION
from app.ai.tools.growth_drivers import identify_growth_drivers, IDENTIFY_GROWTH_DRIVERS_DECLARATION
from app.ai.tools.statistical_analysis import calculate_statistics, CALCULATE_STATISTICS_DECLARATION
from app.ai.tools.breakdown_analysis import breakdown_analysis, BREAKDOWN_ANALYSIS_DECLARATION

from typing import List, Dict, Callable

class ToolRegistry:
    def __init__(self):
        self.tools = {
            # ── Financeiro ──────────────────────────────────────────────────────────
            'query_unit_revenue': query_unit_revenue_tool.execute,
            'query_strategic_finance': query_strategic_finance_tool.execute,
            'query_detailed_finance': query_detailed_finance_tool.execute,

            # ── Exames / SLA ────────────────────────────────────────────────────────
            'query_exam_sla': query_exam_sla_tool.execute,
            'query_operational_sla': query_operational_sla_tool.execute,
            'query_exam_detail': query_exam_detail_tool.execute,

            # ── Técnico ─────────────────────────────────────────────────────────────
            'query_laudos_comparativo': query_laudos_comparativo_tool.execute,

            # ── Comercial / Orçamentos ───────────────────────────────────────────────
            'query_doctor_ranking': query_doctor_ranking_tool.execute,
            'query_budgets': query_budgets_tool.execute,
            'query_orcamentos_pacientes': query_orcamentos_pacientes_tool.execute,
            'query_orcamentos_unidade': query_orcamentos_unidade_tool.execute,

            # ── Clientes / Pacientes ─────────────────────────────────────────────────
            'query_client_analytics': query_client_analytics_tool.execute,
            'query_patient_intelligence': query_patient_intelligence_tool.execute,

            # ── Management / Metas ───────────────────────────────────────────────────
            'query_management_indicators': query_management_indicators_tool.execute,
            'list_units': list_units_tool.execute,
            'query_metas_execucao': query_metas_execucao_tool.execute,

            # ── Analytical tools ─────────────────────────────────────────────────────
            'compare_periods': compare_periods,
            'identify_growth_drivers': identify_growth_drivers,
            'calculate_statistics': calculate_statistics,
            'breakdown_analysis': breakdown_analysis
        }

    def get_tool_definitions(self) -> List[Dict]:
        """
        Returns the tool definitions in the format expected by Gemini API (Function Declarations).
        Todos os tools de dados reutilizam os services e algoritmos já existentes no sistema,
        garantindo consistência com os endpoints padrão.
        """
        return [
            {
                "name": "query_unit_revenue",
                "description": "Retorna o faturamento total, faturamento por convênio e o número de atendimentos (exames) realizados por cada unidade do laboratório em um período específico. Use esta ferramenta quando o usuário perguntar sobre: faturamento geral, quanto faturou no mês/período, receita total, vendas, número de exames realizados, atendimentos, ou performance por unidade. É a ferramenta principal para qualquer pergunta sobre faturamento ou receita.",
                "parameters": {
                    "type": "OBJECT",
                    "properties": {
                        "start_date": {
                            "type": "STRING",
                            "description": "Data inicial do período no formato YYYY-MM-DD. Se não fornecido, usa últimos 14 dias."
                        },
                        "end_date": {
                            "type": "STRING",
                            "description": "Data final do período no formato YYYY-MM-DD. Se não fornecido, usa data atual."
                        }
                    },
                    "required": []
                }
            },
            {
                "name": "query_exam_sla",
                "description": "Retorna métricas de SLA (Service Level Agreement) dos exames, incluindo percentual de exames entregues no prazo, número de exames atrasados, e prazo médio de entrega. Pode filtrar por tipo: exames particulares, convênio, ou ambos. Use quando o usuário perguntar sobre prazo de entrega, atrasos, ou SLA de exames.",
                "parameters": {
                    "type": "OBJECT",
                    "properties": {
                        "sla_type": {
                            "type": "STRING",
                            "description": "Tipo de SLA a consultar: 'particular' (exames particulares), 'convenio' (exames de convênio), ou 'all' (ambos). Default: 'all'.",
                            "enum": ["particular", "convenio", "all"]
                        },
                        "start_date": {
                            "type": "STRING",
                            "description": "Data inicial do período no formato YYYY-MM-DD. Se não fornecido, usa últimos 30 dias."
                        },
                        "end_date": {
                            "type": "STRING",
                            "description": "Data final do período no formato YYYY-MM-DD. Se não fornecido, usa data atual."
                        }
                    },
                    "required": []
                }
            },
            {
                "name": "query_doctor_ranking",
                "description": "Retorna o ranking de médicos solicitantes ordenado por produção (faturamento e número de exames). Use quando o usuário perguntar sobre médicos que mais encaminham pacientes, produtividade de médicos, ou análise comercial de solicitantes.",
                "parameters": {
                    "type": "OBJECT",
                    "properties": {
                        "start_date": {
                            "type": "STRING",
                            "description": "Data inicial do período no formato YYYY-MM-DD. Se não fornecido, usa últimos 30 dias."
                        },
                        "end_date": {
                            "type": "STRING",
                            "description": "Data final do período no formato YYYY-MM-DD. Se não fornecido, usa data atual."
                        }
                    },
                    "required": []
                }
            },
            {
                "name": "query_budgets",
                "description": "Retorna métricas de orçamentos incluindo quantidade total, valor, orçamentos convertidos, abertos, e taxa de conversão. Também fornece breakdown por unidade e por usuário. Use quando o usuário perguntar sobre orçamentos, propostas, taxa de conversão, ou pipeline comercial.",
                "parameters": {
                    "type": "OBJECT",
                    "properties": {
                        "start_date": {
                            "type": "STRING",
                            "description": "Data inicial do período no formato YYYY-MM-DD. Se não fornecido, usa últimos 30 dias."
                        },
                        "end_date": {
                            "type": "STRING",
                            "description": "Data final do período no formato YYYY-MM-DD. Se não fornecido, usa data atual."
                        }
                    },
                    "required": []
                }
            },
            {
                "name": "query_strategic_finance",
                "description": "Retorna métricas financeiras estratégicas incluindo faturamento bruto, faturamento líquido, margem, caixa recebido, contas a receber, e outras métricas de alto nível. Use quando o usuário perguntar sobre visão financeira geral, margens, ou performance financeira estratégica.",
                "parameters": {
                    "type": "OBJECT",
                    "properties": {
                        "start_date": {
                            "type": "STRING",
                            "description": "Data inicial do período no formato YYYY-MM-DD. Se não fornecido, usa últimos 30 dias."
                        },
                        "end_date": {
                            "type": "STRING",
                            "description": "Data final do período no formato YYYY-MM-DD. Se não fornecido, usa data atual."
                        }
                    },
                    "required": []
                }
            },
            {
                "name": "query_detailed_finance",
                "description": "Retorna detalhamento financeiro incluindo produtos vendidos, formas de pagamento, e análise de pacientes. Use quando o usuário precisar de breakdown detalhado de receitas por produto, meio de pagamento, ou segmentação de clientes.",
                "parameters": {
                    "type": "OBJECT",
                    "properties": {
                        "start_date": {
                            "type": "STRING",
                            "description": "Data inicial do período no formato YYYY-MM-DD. Se não fornecido, usa últimos 30 dias."
                        },
                        "end_date": {
                            "type": "STRING",
                            "description": "Data final do período no formato YYYY-MM-DD. Se não fornecido, usa data atual."
                        }
                    },
                    "required": []
                }
            },
            {
                "name": "query_client_analytics",
                "description": "Retorna análise de clientes/pacientes incluindo pacientes novos, recorrentes, inativos, e dados demográficos. Use quando o usuário perguntar sobre aquisição de clientes, retenção, churn, ou análise de base de clientes.",
                "parameters": {
                    "type": "OBJECT",
                    "properties": {
                        "start_date": {
                            "type": "STRING",
                            "description": "Data inicial do período no formato YYYY-MM-DD. Se não fornecido, usa últimos 30 dias."
                        },
                        "end_date": {
                            "type": "STRING",
                            "description": "Data final do período no formato YYYY-MM-DD. Se não fornecido, usa data atual."
                        }
                    },
                    "required": []
                }
            },
            {
                "name": "query_patient_intelligence",
                "description": "Retorna inteligência de pacientes incluindo demografia (idade, gênero, localização), perfil socioeconômico (fonte de pagamento), e analytics avançado (LTV, frequência). Use quando o usuário perguntar sobre perfil de pacientes, personas, segmentação demográfica, ou análise de lifetime value.",
                "parameters": {
                    "type": "OBJECT",
                    "properties": {
                        "start_date": {
                            "type": "STRING",
                            "description": "Data inicial do período no formato YYYY-MM-DD. Se não fornecido, usa último ano para ter volume relevante."
                        },
                        "end_date": {
                            "type": "STRING",
                            "description": "Data final do período no formato YYYY-MM-DD. Se não fornecido, usa data atual."
                        }
                    },
                    "required": []
                }
            },
            
            # ========================================
            # TÉCNICO / OPERACIONAL
            # ========================================

            {
                "name": "query_operational_sla",
                "description": "Retorna o SLA operacional de liberação de resultados por unidade, bancada e aparelho. Mostra percentual no prazo, quantidade de exames atrasados e distribuição de faixas de atraso (menos de 1h, 1-2h, 3-5h, etc.). Use quando o usuário perguntar sobre tempo de liberação de laudos, atrasos por bancada, desempenho operacional do laboratório ou retrabalho de amostras.",
                "parameters": {
                    "type": "OBJECT",
                    "properties": {
                        "start_date": {"type": "STRING", "description": "Data inicial (YYYY-MM-DD). Default: últimos 30 dias."},
                        "end_date": {"type": "STRING", "description": "Data final (YYYY-MM-DD). Default: hoje."}
                    },
                    "required": []
                }
            },
            {
                "name": "query_laudos_comparativo",
                "description": "Retorna laudos liberados dia a dia no período comparando com o mesmo período do ano anterior. Inclui quantidade, valor faturado e SLA (no prazo vs. atrasado) para cada dia. Use quando o usuário perguntar sobre evolução de laudos, comparação com ano anterior, tendência de produção técnica ou crescimento de laudos.",
                "parameters": {
                    "type": "OBJECT",
                    "properties": {
                        "start_date": {"type": "STRING", "description": "Data inicial (YYYY-MM-DD). Default: primeiro dia do mês atual."},
                        "end_date": {"type": "STRING", "description": "Data final (YYYY-MM-DD). Default: hoje."}
                    },
                    "required": []
                }
            },
            {
                "name": "query_exam_detail",
                "description": "Retorna análise aprofundada de um exame específico: resumo financeiro, ranking dos médicos que mais solicitaram, ranking de unidades, ranking de convênios e últimos pacientes atendidos. Use quando o usuário perguntar sobre um exame específico pelo código (ex: 'TSH', 'GLICO', 'HEM') ou quiser saber quem solicita um determinado exame.",
                "parameters": {
                    "type": "OBJECT",
                    "properties": {
                        "exame_cod": {"type": "STRING", "description": "Código do exame (ex: 'HEM', 'TSH', 'GLICO', 'URINA'). Obrigatório."},
                        "start_date": {"type": "STRING", "description": "Data inicial (YYYY-MM-DD). Default: últimos 30 dias."},
                        "end_date": {"type": "STRING", "description": "Data final (YYYY-MM-DD). Default: hoje."},
                        "tpcod": {"type": "STRING", "description": "Tipo do exame. Default: 'LB' (laboratório)."}
                    },
                    "required": ["exame_cod"]
                }
            },

            # ========================================
            # ORÇAMENTOS GRANULARES
            # ========================================

            {
                "name": "query_orcamentos_pacientes",
                "description": "Retorna a lista detalhada de orçamentos do período com dados de cada paciente: nome, categoria (VIP/Fiel/Recorrente/Novo), status, se converteu em OS, valor, unidade e usuário que emitiu. Use quando o usuário quiser ver os orçamentos por paciente, identificar pacientes com orçamentos em aberto, ou analisar quais pacientes não converteram.",
                "parameters": {
                    "type": "OBJECT",
                    "properties": {
                        "start_date": {"type": "STRING", "description": "Data inicial (YYYY-MM-DD). Default: últimos 30 dias."},
                        "end_date": {"type": "STRING", "description": "Data final (YYYY-MM-DD). Default: hoje."}
                    },
                    "required": []
                }
            },
            {
                "name": "query_orcamentos_unidade",
                "description": "Retorna os orçamentos emitidos para uma unidade específica com flag de conversão em OS. Use quando o usuário quiser analisar a performance de orçamentos de uma unidade específica ou comparar conversão entre unidades.",
                "parameters": {
                    "type": "OBJECT",
                    "properties": {
                        "unidade": {"type": "STRING", "description": "Nome da unidade (ex: 'SERRINHA', 'PAULO AFONSO'). Obrigatório."},
                        "start_date": {"type": "STRING", "description": "Data inicial (YYYY-MM-DD). Default: últimos 30 dias."},
                        "end_date": {"type": "STRING", "description": "Data final (YYYY-MM-DD). Default: hoje."}
                    },
                    "required": ["unidade"]
                }
            },

            # ========================================
            # MANAGEMENT / METAS
            # ========================================

            {
                "name": "query_management_indicators",
                "description": "Retorna os indicadores estratégicos completos de gestão: mix particular/convênio, crescimento vs. período anterior, operacional (pacientes, exames, ticket médio), novos pacientes, conversão de orçamentos, descontos, fluxo financeiro (faturado vs. recebido), ranking de médicos e ranking de recepcionistas. É o painel executivo principal. Use quando o usuário pedir indicadores gerais, KPIs de gestão, visão executiva ou performance global.",
                "parameters": {
                    "type": "OBJECT",
                    "properties": {
                        "start_date": {"type": "STRING", "description": "Data inicial (YYYY-MM-DD). Default: primeiro dia do mês atual."},
                        "end_date": {"type": "STRING", "description": "Data final (YYYY-MM-DD). Default: hoje."},
                        "unidade": {"type": "STRING", "description": "Nome da unidade para filtrar (opcional). Se omitido, retorna consolidado de todas as unidades."}
                    },
                    "required": []
                }
            },
            {
                "name": "list_units",
                "description": "Lista todas as unidades disponíveis no banco de dados ativo. Use quando o usuário perguntar quais são as unidades do laboratório, quiser saber os nomes exatos das unidades ou antes de filtrar dados por unidade.",
                "parameters": {
                    "type": "OBJECT",
                    "properties": {},
                    "required": []
                }
            },
            {
                "name": "query_metas_execucao",
                "description": "Retorna a execução real do laboratório no ano atual com faturamento, número de pacientes e ticket médio. Granularity 'mensal' retorna um ponto por mês do ano atual; 'diario' retorna um ponto por dia do mês atual. Use quando o usuário perguntar sobre evolução mensal do ano, execução do mês, como está indo comparado aos meses anteriores, ou quiser ver a progressão diária do mês.",
                "parameters": {
                    "type": "OBJECT",
                    "properties": {
                        "granularity": {
                            "type": "STRING",
                            "description": "Granularidade dos dados: 'mensal' (meses do ano atual, default) ou 'diario' (dias do mês atual).",
                            "enum": ["mensal", "diario"]
                        },
                        "unidade": {"type": "STRING", "description": "Nome da unidade para filtrar (opcional). Se omitido, retorna consolidado."}
                    },
                    "required": []
                }
            },

            # ========================================
            # ANALYTICAL TOOLS
            # ========================================

            COMPARE_PERIODS_DECLARATION,
            IDENTIFY_GROWTH_DRIVERS_DECLARATION,
            CALCULATE_STATISTICS_DECLARATION,
            BREAKDOWN_ANALYSIS_DECLARATION
        ]

    def get_callable_tools(self) -> Dict[str, Callable]:
        return self.tools

tool_registry = ToolRegistry()
