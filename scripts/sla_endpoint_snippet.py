@app.get("/operacional/sla", response_model=SLAResponse)
def get_sla_operacional(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None
):
    """
    Endpoint para análise de SLA (Service Level Agreement) de exames.
    Retorna métricas de atraso de liberação de resultados por unidade, bancada e aparelho.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor(as_dict=True)
        
        # Default: último mês
        if not start_date:
            start_date = (datetime.now() - timedelta(days=30)).date()
        if not end_date:
            end_date = datetime.now().date()
        
        # Import SLA utilities
        from sla_utils import get_sla_data, process_sla_operational
        
        # Fetch raw data
        df_sla, df_amostras = get_sla_data(cursor, str(start_date), str(end_date))
        conn.close()
        
        # Process aggregations
        analytics_result = process_sla_operational(df_sla, df_amostras)
        
        return SLAResponse(
            success=True,
            data=SLAMetrics(**analytics_result)
        )
        
    except Exception as e:
        return SLAResponse(success=False, error=str(e))
