"""
Cache TTL simples para resultados de analytics.

Justificativa: dashboards são read-only e o dado de laboratório tem latência
natural de alguns minutos entre coleta e liberação. Um TTL de 2 minutos elimina
queries duplicadas em refreshes consecutivos sem impactar a atualidade dos dados.

Uso:
    from .cache import analytics_cache

    key = f"financeiro_{start_date}_{end_date}"
    if key in analytics_cache:
        return analytics_cache[key]

    result = ...compute...
    analytics_cache[key] = result
    return result
"""
from cachetools import TTLCache

# maxsize=100: até 100 combinações de (endpoint, datas) em memória
# ttl=120: expiração em 2 minutos
analytics_cache: TTLCache = TTLCache(maxsize=100, ttl=120)
