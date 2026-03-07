from app.ai.tools.query_metrics import query_metrics_tool
from app.ai.tools.unit_revenue import query_unit_revenue_tool
from app.ai.tools.exam_sla import query_exam_sla_tool
from app.ai.tools.doctor_ranking import query_doctor_ranking_tool
from app.ai.tools.budgets import query_budgets_tool
from app.ai.tools.strategic_finance import query_strategic_finance_tool
from app.ai.tools.detailed_finance import query_detailed_finance_tool
from app.ai.tools.client_analytics import query_client_analytics_tool
from app.ai.tools.patient_intelligence import query_patient_intelligence_tool

# Analytical tools
from app.ai.tools.compare_periods import compare_periods, COMPARE_PERIODS_DECLARATION
from app.ai.tools.growth_drivers import identify_growth_drivers, IDENTIFY_GROWTH_DRIVERS_DECLARATION
from app.ai.tools.statistical_analysis import calculate_statistics, CALCULATE_STATISTICS_DECLARATION
from app.ai.tools.breakdown_analysis import breakdown_analysis, BREAKDOWN_ANALYSIS_DECLARATION

from typing import List, Dict, Callable

class ToolRegistry:
    def __init__(self):
        self.tools = {
            # Data retrieval tools
            'query_metrics': query_metrics_tool.execute,
            'query_unit_revenue': query_unit_revenue_tool.execute,
            'query_exam_sla': query_exam_sla_tool.execute,
            'query_doctor_ranking': query_doctor_ranking_tool.execute,
            'query_budgets': query_budgets_tool.execute,
            'query_strategic_finance': query_strategic_finance_tool.execute,
            'query_detailed_finance': query_detailed_finance_tool.execute,
            'query_client_analytics': query_client_analytics_tool.execute,
            'query_patient_intelligence': query_patient_intelligence_tool.execute,
            
            # Analytical tools
            'compare_periods': compare_periods,
            'identify_growth_drivers': identify_growth_drivers,
            'calculate_statistics': calculate_statistics,
            'breakdown_analysis': breakdown_analysis
        }
        
    def get_tool_definitions(self) -> List[Dict]:
        """
        Returns the tool definitions in the format expected by Gemini API (Function Declarations).
        """
        return [
            {
                "name": "query_metrics",
                "description": "Queries business metrics from the database based on a metric name, optional dimension, and optional filters.",
                "parameters": {
                    "type": "OBJECT",
                    "properties": {
                        "metric": {
                            "type": "STRING",
                            "description": "The name of the metric to query. Available metrics: total_revenue, exam_count, patient_count, ticket_average.",
                            "enum": ["total_revenue", "exam_count", "patient_count", "ticket_average"]
                        },
                        "dimension": {
                            "type": "STRING",
                            "description": "The dimension to group results by. Available dimensions: unit, date, month, exam_name, doctor, insurance.",
                            "enum": ["unit", "date", "month", "exam_name", "doctor", "insurance"]
                        },
                        "start_date": {
                            "type": "STRING",
                            "description": "Start date for filtering in YYYY-MM-DD format."
                        },
                        "end_date": {
                            "type": "STRING",
                            "description": "End date for filtering in YYYY-MM-DD format."
                        }
                    },
                    "required": ["metric"]
                }
            },
            {
                "name": "query_unit_revenue",
                "description": "Retorna o faturamento total e o número de atendimentos (exames) realizados por cada unidade do laboratório em um período específico. Use esta ferramenta quando o usuário perguntar sobre faturamento, vendas, receita, ou performance de unidades específicas.",
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
