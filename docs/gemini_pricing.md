# Tabela de Preços do Gemini API

**Fonte:** https://ai.google.dev/gemini-api/docs/pricing  
**Atualizado:** 15/02/2026

## 💰 Tabela Comparativa de Custos (por 1M tokens)

### Modelos Flash (Rápidos e Econômicos)

| Modelo | Input (Standard) | Output (Standard) | Input (Batch) | Output (Batch) |
|--------|-----------------|-------------------|---------------|----------------|
| **Gemini 3 Flash** | $0.50 | $3.00 | $0.25 | $1.50 |
| **Gemini 2.5 Flash** | $0.30 | $2.50 | $0.15 | $1.25 |

*Nota: Input de áudio custa 2x mais nos modelos Flash*

---

### Modelos Pro (Avançados)

| Modelo | Input ≤200k | Output ≤200k | Input >200k | Output >200k |
|--------|-------------|--------------|-------------|--------------|
| **Gemini 3 Pro (Standard)** | $2.00 | $12.00 | $4.00 | $18.00 |
| **Gemini 3 Pro (Batch)** | $1.00 | $6.00 | $2.00 | $9.00 |
| **Gemini 2.5 Pro (Standard)** | $1.25 | $10.00 | $2.50 | $15.00 |
| **Gemini 2.5 Pro (Batch)** | $0.625 | $5.00 | $1.25 | $7.50 |

*Nota: Modelos Pro têm preço diferente baseado no tamanho do prompt*

---

## 📊 Comparação de Custos (500 input + 300 output tokens)

| Modelo | Standard | Batch | Economia |
|--------|----------|-------|----------|
| **Gemini 3 Pro** | $0.004600 | $0.002300 | 50% |
| **Gemini 3 Flash** | $0.001150 | $0.000575 | 50% |
| **Gemini 2.5 Pro** | $0.003625 | $0.001812 | 50% |
| **Gemini 2.5 Flash** ✅ | **$0.000900** | **$0.000450** | **50%** |

**Recomendação:** Gemini 2.5 Flash oferece o melhor custo-benefício!

---

## 💡 Estimativa Mensal

**Cenário: 100 mensagens/dia** (3.000/mês)

| Modelo | Custo Mensal (Standard) | Custo Mensal (Batch) |
|--------|------------------------|----------------------|
| Gemini 3 Pro | $13.80 USD (~R$ 77) | $6.90 USD (~R$ 38) |
| Gemini 3 Flash | $3.45 USD (~R$ 19) | $1.72 USD (~R$ 9) |
| Gemini 2.5 Pro | $10.87 USD (~R$ 61) | $5.44 USD (~R$ 30) |
| **Gemini 2.5 Flash** ✅ | **$2.70 USD (~R$ 15)** | **$1.35 USD (~R$ 7)** |

---

## 📌 Observações Importantes

1. **Batch Mode:** Desconto de 50% em TODOS os modelos
2. **Pro Models:** Preço aumenta quando prompt > 200k tokens
3. **Audio Input:** Custa 2x mais nos modelos Flash
4. **Thinking Tokens:** Incluídos no preço de output
5. **Context Caching:** $0.03 por 1M tokens (texto/vídeo)

---

## 🔧 Implementação no Sistema

O módulo `cost_calculator.py` calcula automaticamente:
- ✅ Custo por mensagem
- ✅ Token cutoff para Pro models
- ✅ Diferença entre Standard/Batch
- ✅ Ajuste para input de áudio
- ✅ Comparação entre modelos

**Uso:**
```python
from app.ai.utils.cost_calculator import calculate_cost

cost = calculate_cost(
    input_tokens=500,
    output_tokens=300,
    model="gemini-2.5-flash",
    mode="standard"
)

print(f"Custo: ${cost['total_cost']:.6f}")
```
