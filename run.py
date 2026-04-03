import os
import uvicorn

# ---------------------------------------------------------------------------
# Modo de execução:
#   RUN_ENV=development (padrão): 1 worker + reload automático (dev local)
#   RUN_ENV=production            : 4 workers + sem reload (simulação real)
#
# Nota: workers > 1 é incompatível com reload=True no uvicorn.
# Para simular produção localmente:
#   RUN_ENV=production python3 run.py
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    env = os.getenv("RUN_ENV", "development")
    is_prod = env == "production"

    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=not is_prod,        # reload apenas em dev
        workers=4 if is_prod else 1,  # 4 processes em prod para contornar o GIL
        log_level="info",
    )
