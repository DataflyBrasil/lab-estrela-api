"""
Cálculo de custos para uso do Gemini API.
Baseado nos preços oficiais extraídos de: https://ai.google.dev/gemini-api/docs/pricing
Data: 15/02/2026
"""

from typing import Dict, Optional, Literal
from decimal import Decimal

# =====================================================
# PREÇOS OFICIAIS DO GEMINI API
# =====================================================

# Gemini 3.0 Pro Preview
GEMINI_3_PRO_PRICING = {
    "standard": {
        "input_small": Decimal("2.00"),    # ≤ 200k tokens
        "input_large": Decimal("4.00"),    # > 200k tokens
        "output_small": Decimal("12.00"),  # ≤ 200k tokens
        "output_large": Decimal("18.00"),  # > 200k tokens
    },
    "batch": {
        "input_small": Decimal("1.00"),    # ≤ 200k tokens (50% desconto)
        "input_large": Decimal("2.00"),    # > 200k tokens (50% desconto)
        "output_small": Decimal("6.00"),   # ≤ 200k tokens (50% desconto)
        "output_large": Decimal("9.00"),   # > 200k tokens (50% desconto)
    }
}

# Gemini 3.0 Flash Preview
GEMINI_3_FLASH_PRICING = {
    "standard": {
        "input": Decimal("0.50"),          # texto/imagem/vídeo
        "input_audio": Decimal("1.00"),    # áudio
        "output": Decimal("3.00"),
    },
    "batch": {
        "input": Decimal("0.25"),          # 50% desconto
        "input_audio": Decimal("0.50"),    # 50% desconto
        "output": Decimal("1.50"),         # 50% desconto
    }
}

# Gemini 2.5 Pro
GEMINI_2_5_PRO_PRICING = {
    "standard": {
        "input_small": Decimal("1.25"),    # ≤ 200k tokens
        "input_large": Decimal("2.50"),    # > 200k tokens
        "output_small": Decimal("10.00"),  # ≤ 200k tokens
        "output_large": Decimal("15.00"),  # > 200k tokens
    },
    "batch": {
        "input_small": Decimal("0.625"),   # ≤ 200k tokens (50% desconto)
        "input_large": Decimal("1.25"),    # > 200k tokens (50% desconto)
        "output_small": Decimal("5.00"),   # ≤ 200k tokens (50% desconto)
        "output_large": Decimal("7.50"),   # > 200k tokens (50% desconto)
    }
}

# Gemini 2.5 Flash
GEMINI_2_5_FLASH_PRICING = {
    "standard": {
        "input": Decimal("0.30"),          # texto/imagem/vídeo
        "input_audio": Decimal("1.00"),    # áudio
        "output": Decimal("2.50"),         # inclui thinking tokens
    },
    "batch": {
        "input": Decimal("0.15"),          # 50% desconto
        "input_audio": Decimal("0.50"),    # 50% desconto
        "output": Decimal("1.25"),         # 50% desconto
    }
}

# Context caching pricing (por 1M tokens)
CONTEXT_CACHING_PRICING = {
    "text_video": Decimal("0.03"),  # $0.03 per 1M tokens
    "audio": Decimal("0.10"),       # $0.10 per 1M tokens
}

# Token limit for Pro models pricing tiers
PRO_TOKEN_CUTOFF = 200_000

# =====================================================
# FUNÇÕES DE CÁLCULO
# =====================================================

def calculate_cost(
    input_tokens: int,
    output_tokens: int,
    model: Literal[
        "gemini-3-pro", "gemini-3-pro-preview",
        "gemini-3-flash", "gemini-3-flash-preview",
        "gemini-2.5-pro",
        "gemini-2.5-flash"
    ] = "gemini-2.5-flash",
    mode: Literal["standard", "batch"] = "standard",
    is_audio: bool = False
) -> Dict[str, Decimal]:
    """
    Calcula o custo de uma requisição ao Gemini.
    
    Args:
        input_tokens: Número de tokens de entrada
        output_tokens: Número de tokens de saída
        model: Modelo usado
        mode: Modo de execução (standard, batch)
        is_audio: Se os tokens de entrada são de áudio (apenas Flash models)
    
    Returns:
        Dict com:
            - input_cost: Custo dos tokens de entrada
            - output_cost: Custo dos tokens de saída
            - total_cost: Custo total
            - currency: Moeda (USD)
            - breakdown: Detalhamento do cálculo
    """
    
    # Normalizar nome do modelo
    model = model.lower()
    if "3-pro" in model or "3.0-pro" in model:
        pricing = GEMINI_3_PRO_PRICING
        is_pro = True
    elif "3-flash" in model or "3.0-flash" in model:
        pricing = GEMINI_3_FLASH_PRICING
        is_pro = False
    elif "2.5-pro" in model:
        pricing = GEMINI_2_5_PRO_PRICING
        is_pro = True
    elif "2.5-flash" in model:
        pricing = GEMINI_2_5_FLASH_PRICING
        is_pro = False
    else:
        raise ValueError(f"Modelo desconhecido: {model}")
    
    if mode not in pricing:
        raise ValueError(f"Modo desconhecido: {mode}. Use 'standard' ou 'batch'")
    
    # Calcular custos
    breakdown = {}
    
    # Pro models tem preços diferentes baseados no tamanho do prompt
    if is_pro:
        total_prompt_tokens = input_tokens
        
        # Determinar qual tier de preço usar
        if total_prompt_tokens <= PRO_TOKEN_CUTOFF:
            input_price = pricing[mode]["input_small"]
            output_price = pricing[mode]["output_small"]
            breakdown["tier"] = f"≤ {PRO_TOKEN_CUTOFF:,} tokens"
        else:
            input_price = pricing[mode]["input_large"]
            output_price = pricing[mode]["output_large"]
            breakdown["tier"] = f"> {PRO_TOKEN_CUTOFF:,} tokens"
        
        breakdown["input_price_per_1m"] = float(input_price)
        breakdown["output_price_per_1m"] = float(output_price)
    
    # Flash models tem preço fixo, mas diferente para áudio
    else:
        if is_audio:
            input_price = pricing[mode].get("input_audio", pricing[mode]["input"])
            breakdown["input_type"] = "audio"
        else:
            input_price = pricing[mode]["input"]
            breakdown["input_type"] = "text/image/video"
        
        output_price = pricing[mode]["output"]
        breakdown["input_price_per_1m"] = float(input_price)
        breakdown["output_price_per_1m"] = float(output_price)
    
    # Preço é por 1 milhão de tokens
    input_cost = (Decimal(input_tokens) / Decimal("1000000")) * input_price
    output_cost = (Decimal(output_tokens) / Decimal("1000000")) * output_price
    total_cost = input_cost + output_cost
    
    return {
        "input_cost": input_cost,
        "output_cost": output_cost,
        "total_cost": total_cost,
        "currency": "USD",
        "breakdown": breakdown
    }

def format_cost_summary(
    input_tokens: int,
    output_tokens: int,
    model: str = "gemini-2.5-flash",
    mode: str = "standard",
    is_audio: bool = False
) -> str:
    """
    Formata um resumo legível do custo.
    
    Returns:
        String formatada com resumo do custo
    """
    costs = calculate_cost(input_tokens, output_tokens, model, mode, is_audio)
    
    total_tokens = input_tokens + output_tokens
    
    summary = f"""
💰 Custo da Requisição:
   📥 Input: {input_tokens:,} tokens → ${costs['input_cost']:.6f}
   📤 Output: {output_tokens:,} tokens → ${costs['output_cost']:.6f}"""
    
    if "tier" in costs["breakdown"]:
        summary += f"\n   📊 Tier: {costs['breakdown']['tier']}"
    
    summary += f"""
   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   💵 Total: {total_tokens:,} tokens → ${costs['total_cost']:.6f} USD
   
   Modelo: {model} ({mode})"""
    
    if is_audio:
        summary += " [áudio]"
    
    return summary.strip()

def compare_models(
    input_tokens: int,
    output_tokens: int,
    mode: str = "standard"
) -> Dict[str, Dict]:
    """
    Compara o custo entre todos os modelos disponíveis.
    
    Returns:
        Dict com o custo de cada modelo
    """
    models = [
        "gemini-3-pro",
        "gemini-3-flash",
        "gemini-2.5-pro",
        "gemini-2.5-flash"
    ]
    
    results = {}
    for model in models:
        try:
            cost = calculate_cost(input_tokens, output_tokens, model, mode)
            results[model] = {
                "total_cost": float(cost["total_cost"]),
                "input_cost": float(cost["input_cost"]),
                "output_cost": float(cost["output_cost"]),
                "breakdown": cost.get("breakdown", {})
            }
        except Exception as e:
            results[model] = {"error": str(e)}
    
    return results

# =====================================================
# EXEMPLOS DE USO
# =====================================================

if __name__ == "__main__":
    print("=" * 60)
    print("🧮 CALCULADORA DE CUSTOS DO GEMINI API")
    print("=" * 60)
    
    # Exemplo 1: Comparação entre modelos
    print("\n📝 Exemplo 1: Comparação de Modelos (500 input + 300 output)")
    comparison = compare_models(500, 300, "standard")
    
    print("\nStandard Mode:")
    for model, data in comparison.items():
        if "error" not in data:
            print(f"  {model:20s} → ${data['total_cost']:.6f}")
    
    # Exemplo 2: Gemini 2.5 Flash (nosso modelo atual)
    print("\n📝 Exemplo 2: Gemini 2.5 Flash (atual)")
    print(format_cost_summary(500, 300, "gemini-2.5-flash", "standard"))
    
    # Exemplo 3: Batch pricing (50% desconto)
    print("\n📝 Exemplo 3: Batch Pricing (50% desconto)")
    standard = calculate_cost(1000, 800, "gemini-2.5-flash", "standard")
    batch = calculate_cost(1000, 800, "gemini-2.5-flash", "batch")
    savings = standard['total_cost'] - batch['total_cost']
    savings_pct = (savings / standard['total_cost']) * 100
    
    print(f"""
   Standard: ${standard['total_cost']:.6f}
   Batch:    ${batch['total_cost']:.6f}
   ━━━━━━━━━━━━━━━━━━━━━━━━━
   Economia: ${savings:.6f} ({savings_pct:.1f}%)
""")
    
    # Exemplo 4: Pro model com token cutoff
    print("\n📝 Exemplo 4: Gemini 3 Pro - Token Cutoff")
    small_prompt = calculate_cost(100000, 50000, "gemini-3-pro", "standard")
    large_prompt = calculate_cost(250000, 50000, "gemini-3-pro", "standard")
    
    print(f"""
   Prompt pequeno (100k tokens): ${small_prompt['total_cost']:.4f}
   Prompt grande (250k tokens):  ${large_prompt['total_cost']:.4f}
   
   Diferença por causa do cutoff de 200k tokens
""")
    
    # Exemplo 5: Audio input
    print("\n📝 Exemplo 5: Gemini 3 Flash - Audio Input")
    text_cost = calculate_cost(1000, 500, "gemini-3-flash", "standard", is_audio=False)
    audio_cost = calculate_cost(1000, 500, "gemini-3-flash", "standard", is_audio=True)
    
    print(f"""
   Texto: ${text_cost['total_cost']:.6f}
   Áudio: ${audio_cost['total_cost']:.6f}
   
   Áudio custa {float(audio_cost['total_cost'] / text_cost['total_cost']):.1f}x mais
""")
