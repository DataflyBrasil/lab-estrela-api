"""
=============================================================================
SCRIPT DE GERAÇÃO DE DADOS - BANCO DE DADOS DE VENDAS
=============================================================================
Gera dados realistas com:
  - Sazonalidade (Black Friday, Natal, volta às aulas, etc.)
  - Tendência de crescimento anual
  - Variação regional de desempenho
  - Diferenças de performance entre produtos e vendedores
  - Motivos de devolução e canais de venda

Período: 01/01/2023 a 01/01/2026
=============================================================================
"""

import sqlite3
import random
import os
from datetime import datetime, timedelta
from pathlib import Path

# ========================= CONFIGURAÇÕES =========================
DB_NAME = "vendas_empresa.db"
SEED = 42
random.seed(SEED)

DATA_INICIO = datetime(2023, 1, 1)
DATA_FIM = datetime(2026, 1, 1)
TOTAL_VENDAS_MINIMO = 110_000

# ========================= DADOS BASE =========================

CATEGORIAS_PRODUTOS = {
    "Eletrônicos": [
        ("Smartphone Premium", 3499.90, 2100.00),
        ("Smartphone Básico", 999.90, 600.00),
        ("Notebook Gamer", 5999.90, 3800.00),
        ("Notebook Corporativo", 3299.90, 2000.00),
        ("Tablet 10 polegadas", 1899.90, 1100.00),
        ("Fone Bluetooth", 299.90, 120.00),
        ("Smartwatch", 799.90, 400.00),
        ("Caixa de Som Portátil", 349.90, 160.00),
    ],
    "Eletrodomésticos": [
        ("Geladeira Frost Free", 3199.90, 1900.00),
        ("Máquina de Lavar 12kg", 2499.90, 1500.00),
        ("Micro-ondas 30L", 599.90, 300.00),
        ("Aspirador Robô", 1299.90, 700.00),
        ("Air Fryer 5L", 449.90, 200.00),
        ("Cafeteira Expresso", 899.90, 450.00),
        ("Ventilador Torre", 299.90, 130.00),
        ("Purificador de Água", 699.90, 350.00),
    ],
    "Móveis": [
        ("Sofá 3 Lugares", 2799.90, 1400.00),
        ("Mesa de Escritório", 899.90, 400.00),
        ("Cadeira Ergonômica", 1199.90, 550.00),
        ("Estante Modular", 649.90, 280.00),
        ("Cama Box Queen", 1999.90, 950.00),
        ("Rack para TV", 499.90, 220.00),
        ("Guarda-Roupa 6 Portas", 1899.90, 900.00),
        ("Mesa de Jantar 6 Lugares", 1499.90, 700.00),
    ],
    "Esporte e Lazer": [
        ("Bicicleta Aro 29", 1899.90, 950.00),
        ("Esteira Elétrica", 2499.90, 1300.00),
        ("Kit Halteres", 399.90, 180.00),
        ("Barraca Camping 4P", 499.90, 220.00),
        ("Tênis de Corrida", 499.90, 200.00),
        ("Mochila Trilha 50L", 349.90, 150.00),
        ("Patinete Elétrico", 1999.90, 1000.00),
        ("Raquete de Tênis Pro", 599.90, 250.00),
    ],
    "Informática e Acessórios": [
        ("Monitor 27\" 4K", 1999.90, 1100.00),
        ("Teclado Mecânico RGB", 399.90, 170.00),
        ("Mouse Gamer", 249.90, 100.00),
        ("Webcam Full HD", 299.90, 130.00),
        ("SSD 1TB NVMe", 449.90, 220.00),
        ("Roteador Wi-Fi 6", 399.90, 180.00),
        ("Hub USB-C 7 em 1", 199.90, 80.00),
        ("Impressora Multifuncional", 799.90, 400.00),
    ],
}

REGIOES_UNIDADES = {
    "Sul": [
        ("Porto Alegre", "RS"), ("Curitiba", "PR"), ("Florianópolis", "SC"),
        ("Caxias do Sul", "RS"), ("Londrina", "PR"),
    ],
    "Sudeste": [
        ("São Paulo - Paulista", "SP"), ("São Paulo - Pinheiros", "SP"),
        ("Rio de Janeiro - Centro", "RJ"), ("Rio de Janeiro - Barra", "RJ"),
        ("Belo Horizonte", "MG"), ("Campinas", "SP"),
        ("Vitória", "ES"), ("Niterói", "RJ"),
    ],
    "Nordeste": [
        ("Salvador", "BA"), ("Recife", "PE"), ("Fortaleza", "CE"),
        ("Natal", "RN"), ("São Luís", "MA"),
    ],
    "Centro-Oeste": [
        ("Brasília", "DF"), ("Goiânia", "GO"), ("Campo Grande", "MS"),
    ],
    "Norte": [
        ("Manaus", "AM"), ("Belém", "PA"), ("Palmas", "TO"),
    ],
}

# Peso de vendas por região (Sudeste vende mais)
PESO_REGIAO = {
    "Sudeste": 1.4,
    "Sul": 1.1,
    "Nordeste": 0.9,
    "Centro-Oeste": 0.8,
    "Norte": 0.7,
}

CANAIS_VENDA = ["Loja Física", "E-commerce", "Televendas", "Marketplace"]
PESO_CANAL = [0.35, 0.40, 0.10, 0.15]

FORMAS_PAGAMENTO = [
    "Cartão de Crédito", "Cartão de Débito", "PIX",
    "Boleto Bancário", "Crediário",
]
PESO_PAGAMENTO = [0.35, 0.15, 0.30, 0.10, 0.10]

STATUS_VENDA = ["Concluída", "Cancelada", "Devolvida"]
PESO_STATUS = [0.92, 0.05, 0.03]

NOMES = [
    "Lucas", "Gabriel", "Rafael", "Matheus", "Pedro", "Gustavo", "Felipe",
    "Bruno", "Leonardo", "Thiago", "Daniel", "Marcos", "André", "João",
    "Carlos", "Fernando", "Ricardo", "Eduardo", "Diego", "Vinícius",
    "Ana", "Maria", "Juliana", "Fernanda", "Camila", "Beatriz", "Larissa",
    "Amanda", "Patrícia", "Mariana", "Bruna", "Carolina", "Letícia",
    "Aline", "Vanessa", "Gabriela", "Raquel", "Tatiana", "Renata", "Débora",
]

SOBRENOMES = [
    "Silva", "Santos", "Oliveira", "Souza", "Rodrigues", "Ferreira",
    "Almeida", "Pereira", "Lima", "Gomes", "Costa", "Ribeiro", "Martins",
    "Carvalho", "Araújo", "Melo", "Barbosa", "Rocha", "Dias", "Nascimento",
    "Moreira", "Monteiro", "Correia", "Mendes", "Nunes", "Teixeira",
    "Vieira", "Cardoso", "Pinto", "Batista",
]


# ========================= FUNÇÕES AUXILIARES =========================

def fator_sazonalidade(data: datetime) -> float:
    """
    Retorna um multiplicador de sazonalidade baseado no mês/período.
    Simula padrões reais de consumo brasileiro.
    """
    mes = data.month
    dia = data.day

    # Black Friday (última semana de novembro)
    if mes == 11 and dia >= 20:
        return 2.2

    fatores_mes = {
        1: 0.75,   # Janeiro - pós-festas, queda
        2: 0.70,   # Fevereiro - carnaval, baixo consumo
        3: 0.85,   # Março - volta às aulas (final)
        4: 0.90,   # Abril
        5: 1.15,   # Maio - Dia das Mães
        6: 1.10,   # Junho - Dia dos Namorados
        7: 0.95,   # Julho - férias
        8: 1.10,   # Agosto - Dia dos Pais
        9: 0.95,   # Setembro
        10: 1.00,  # Outubro - Dia das Crianças
        11: 1.30,  # Novembro - Black Friday (mês geral)
        12: 1.60,  # Dezembro - Natal
    }

    # Dia das Crianças (primeira quinzena de outubro)
    if mes == 10 and dia <= 15:
        return 1.25

    return fatores_mes.get(mes, 1.0)


def fator_tendencia_anual(data: datetime) -> float:
    """
    Simula crescimento de faturamento ao longo dos anos.
    2023: base | 2024: +12% | 2025: +25% (acumulado)
    """
    ano = data.year
    if ano == 2023:
        return 1.0
    elif ano == 2024:
        return 1.12
    elif ano == 2025:
        return 1.25
    return 1.0


def fator_dia_semana(data: datetime) -> float:
    """Fins de semana vendem mais em loja física."""
    dia = data.weekday()
    if dia == 5:   # Sábado
        return 1.15
    elif dia == 6: # Domingo
        return 0.90
    elif dia == 0: # Segunda
        return 0.85
    return 1.0


def gerar_cpf_ficticio() -> str:
    """Gera um CPF fictício formatado (apenas para dados fake)."""
    nums = [random.randint(0, 9) for _ in range(11)]
    return f"{nums[0]}{nums[1]}{nums[2]}.{nums[3]}{nums[4]}{nums[5]}.{nums[6]}{nums[7]}{nums[8]}-{nums[9]}{nums[10]}"


def gerar_email(nome: str, sobrenome: str) -> str:
    domínios = ["email.com", "outlook.com", "empresa.com.br", "mail.com"]
    return f"{nome.lower()}.{sobrenome.lower()}@{random.choice(domínios)}"


# ========================= CRIAÇÃO DO BANCO =========================

def criar_banco(conn: sqlite3.Connection):
    """Cria todas as tabelas do banco de dados."""
    cursor = conn.cursor()

    cursor.executescript("""
    -- Tabela de Categorias
    CREATE TABLE IF NOT EXISTS categorias (
        id_categoria    INTEGER PRIMARY KEY AUTOINCREMENT,
        nome_categoria  TEXT NOT NULL,
        descricao       TEXT
    );

    -- Tabela de Produtos
    CREATE TABLE IF NOT EXISTS produtos (
        id_produto      INTEGER PRIMARY KEY AUTOINCREMENT,
        nome_produto    TEXT NOT NULL,
        id_categoria    INTEGER NOT NULL,
        preco_venda     REAL NOT NULL,
        custo_produto   REAL NOT NULL,
        margem_lucro    REAL NOT NULL,
        ativo           INTEGER DEFAULT 1,
        FOREIGN KEY (id_categoria) REFERENCES categorias(id_categoria)
    );

    -- Tabela de Regiões
    CREATE TABLE IF NOT EXISTS regioes (
        id_regiao       INTEGER PRIMARY KEY AUTOINCREMENT,
        nome_regiao     TEXT NOT NULL
    );

    -- Tabela de Unidades
    CREATE TABLE IF NOT EXISTS unidades (
        id_unidade      INTEGER PRIMARY KEY AUTOINCREMENT,
        nome_unidade    TEXT NOT NULL,
        cidade          TEXT NOT NULL,
        estado          TEXT NOT NULL,
        id_regiao       INTEGER NOT NULL,
        data_abertura   TEXT NOT NULL,
        ativa           INTEGER DEFAULT 1,
        FOREIGN KEY (id_regiao) REFERENCES regioes(id_regiao)
    );

    -- Tabela de Vendedores
    CREATE TABLE IF NOT EXISTS vendedores (
        id_vendedor     INTEGER PRIMARY KEY AUTOINCREMENT,
        nome_vendedor   TEXT NOT NULL,
        cpf             TEXT NOT NULL,
        email           TEXT,
        id_unidade      INTEGER NOT NULL,
        data_admissao   TEXT NOT NULL,
        salario_base    REAL NOT NULL,
        nivel           TEXT NOT NULL DEFAULT 'Júnior',
        ativo           INTEGER DEFAULT 1,
        FOREIGN KEY (id_unidade) REFERENCES unidades(id_unidade)
    );

    -- Tabela de Vendas (tabela fato)
    CREATE TABLE IF NOT EXISTS vendas (
        id_venda        INTEGER PRIMARY KEY AUTOINCREMENT,
        data_venda      TEXT NOT NULL,
        id_produto      INTEGER NOT NULL,
        id_vendedor     INTEGER NOT NULL,
        id_unidade      INTEGER NOT NULL,
        quantidade      INTEGER NOT NULL,
        preco_unitario  REAL NOT NULL,
        desconto        REAL DEFAULT 0,
        valor_total     REAL NOT NULL,
        custo_total     REAL NOT NULL,
        lucro           REAL NOT NULL,
        canal_venda     TEXT NOT NULL,
        forma_pagamento TEXT NOT NULL,
        status_venda    TEXT NOT NULL DEFAULT 'Concluída',
        FOREIGN KEY (id_produto)   REFERENCES produtos(id_produto),
        FOREIGN KEY (id_vendedor)  REFERENCES vendedores(id_vendedor),
        FOREIGN KEY (id_unidade)   REFERENCES unidades(id_unidade)
    );

    -- Tabela de Metas Mensais (para análises comparativas)
    CREATE TABLE IF NOT EXISTS metas_mensais (
        id_meta         INTEGER PRIMARY KEY AUTOINCREMENT,
        ano             INTEGER NOT NULL,
        mes             INTEGER NOT NULL,
        id_unidade      INTEGER NOT NULL,
        meta_faturamento REAL NOT NULL,
        FOREIGN KEY (id_unidade) REFERENCES unidades(id_unidade)
    );

    -- Índices para performance
    CREATE INDEX IF NOT EXISTS idx_vendas_data ON vendas(data_venda);
    CREATE INDEX IF NOT EXISTS idx_vendas_produto ON vendas(id_produto);
    CREATE INDEX IF NOT EXISTS idx_vendas_vendedor ON vendas(id_vendedor);
    CREATE INDEX IF NOT EXISTS idx_vendas_unidade ON vendas(id_unidade);
    CREATE INDEX IF NOT EXISTS idx_vendas_status ON vendas(status_venda);
    """)

    conn.commit()
    print("✅ Tabelas criadas com sucesso!")


# ========================= POPULAR DADOS =========================

def popular_categorias(conn: sqlite3.Connection) -> dict:
    cursor = conn.cursor()
    cat_map = {}
    for cat_nome in CATEGORIAS_PRODUTOS:
        cursor.execute(
            "INSERT INTO categorias (nome_categoria, descricao) VALUES (?, ?)",
            (cat_nome, f"Categoria de {cat_nome.lower()}")
        )
        cat_map[cat_nome] = cursor.lastrowid
    conn.commit()
    print(f"✅ {len(cat_map)} categorias inseridas")
    return cat_map


def popular_produtos(conn: sqlite3.Connection, cat_map: dict) -> list:
    cursor = conn.cursor()
    produtos = []
    for cat_nome, itens in CATEGORIAS_PRODUTOS.items():
        id_cat = cat_map[cat_nome]
        for nome, preco, custo in itens:
            margem = round((preco - custo) / preco * 100, 2)
            cursor.execute(
                """INSERT INTO produtos
                   (nome_produto, id_categoria, preco_venda, custo_produto, margem_lucro)
                   VALUES (?, ?, ?, ?, ?)""",
                (nome, id_cat, preco, custo, margem)
            )
            produtos.append({
                "id": cursor.lastrowid,
                "nome": nome,
                "preco": preco,
                "custo": custo,
                "categoria": cat_nome,
            })
    conn.commit()
    print(f"✅ {len(produtos)} produtos inseridos")
    return produtos


def popular_regioes_e_unidades(conn: sqlite3.Connection) -> list:
    cursor = conn.cursor()
    unidades = []
    for regiao, cidades in REGIOES_UNIDADES.items():
        cursor.execute(
            "INSERT INTO regioes (nome_regiao) VALUES (?)", (regiao,)
        )
        id_regiao = cursor.lastrowid
        peso = PESO_REGIAO[regiao]

        for cidade, estado in cidades:
            nome_unidade = f"Loja {cidade}"
            # Unidades abertas entre 2020 e 2023
            abertura = datetime(
                random.randint(2020, 2022),
                random.randint(1, 12),
                random.randint(1, 28)
            ).strftime("%Y-%m-%d")

            cursor.execute(
                """INSERT INTO unidades
                   (nome_unidade, cidade, estado, id_regiao, data_abertura)
                   VALUES (?, ?, ?, ?, ?)""",
                (nome_unidade, cidade, estado, id_regiao, abertura)
            )
            unidades.append({
                "id": cursor.lastrowid,
                "nome": nome_unidade,
                "regiao": regiao,
                "peso": peso,
            })

    conn.commit()
    print(f"✅ {len(unidades)} unidades inseridas em {len(REGIOES_UNIDADES)} regiões")
    return unidades


def popular_vendedores(conn: sqlite3.Connection, unidades: list) -> list:
    cursor = conn.cursor()
    vendedores = []
    niveis = ["Júnior", "Pleno", "Sênior"]
    pesos_nivel = [0.50, 0.35, 0.15]
    salarios = {"Júnior": 2200.00, "Pleno": 3500.00, "Sênior": 5500.00}
    performance = {"Júnior": 0.8, "Pleno": 1.0, "Sênior": 1.3}

    nomes_usados = set()
    for i in range(100):
        while True:
            nome = random.choice(NOMES)
            sobrenome = f"{random.choice(SOBRENOMES)} {random.choice(SOBRENOMES)}"
            nome_completo = f"{nome} {sobrenome}"
            if nome_completo not in nomes_usados:
                nomes_usados.add(nome_completo)
                break

        nivel = random.choices(niveis, weights=pesos_nivel, k=1)[0]
        unidade = random.choice(unidades)
        admissao = datetime(
            random.randint(2020, 2023),
            random.randint(1, 12),
            random.randint(1, 28)
        ).strftime("%Y-%m-%d")

        cursor.execute(
            """INSERT INTO vendedores
               (nome_vendedor, cpf, email, id_unidade, data_admissao,
                salario_base, nivel)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                nome_completo, gerar_cpf_ficticio(),
                gerar_email(nome, sobrenome.split()[0]),
                unidade["id"], admissao,
                salarios[nivel], nivel
            )
        )
        vendedores.append({
            "id": cursor.lastrowid,
            "nome": nome_completo,
            "id_unidade": unidade["id"],
            "performance": performance[nivel],
            "regiao_peso": unidade["peso"],
        })

    conn.commit()
    print(f"✅ {len(vendedores)} vendedores inseridos")
    return vendedores


def popular_vendas(conn: sqlite3.Connection, produtos: list,
                   vendedores: list, unidades: list):
    """
    Gera registros de vendas com padrões realistas:
    - Sazonalidade mensal
    - Tendência de crescimento anual
    - Variação por dia da semana
    - Descontos variáveis (Black Friday tem descontos maiores)
    - Diferentes canais e formas de pagamento
    """
    cursor = conn.cursor()

    # Pré-computar: mapa de vendedores por unidade
    vendedores_por_unidade = {}
    for v in vendedores:
        uid = v["id_unidade"]
        if uid not in vendedores_por_unidade:
            vendedores_por_unidade[uid] = []
        vendedores_por_unidade[uid].append(v)

    # Pesos dos produtos (alguns vendem mais que outros)
    pesos_produtos = []
    for p in produtos:
        if p["preco"] < 500:
            pesos_produtos.append(3.0)   # Baratos vendem mais
        elif p["preco"] < 1500:
            pesos_produtos.append(2.0)
        elif p["preco"] < 3000:
            pesos_produtos.append(1.2)
        else:
            pesos_produtos.append(0.7)   # Caros vendem menos

    total_dias = (DATA_FIM - DATA_INICIO).days
    vendas_por_dia_base = TOTAL_VENDAS_MINIMO / total_dias  # ~100/dia

    registros = []
    total_inserido = 0
    batch_size = 5000

    print(f"\n⏳ Gerando {TOTAL_VENDAS_MINIMO}+ registros de vendas...")
    print(f"   Período: {DATA_INICIO.strftime('%d/%m/%Y')} a {DATA_FIM.strftime('%d/%m/%Y')}")
    print(f"   Total de dias: {total_dias}\n")

    dia_atual = DATA_INICIO
    while dia_atual < DATA_FIM:
        # Calcular quantidade de vendas neste dia
        fator_s = fator_sazonalidade(dia_atual)
        fator_t = fator_tendencia_anual(dia_atual)
        fator_d = fator_dia_semana(dia_atual)

        vendas_dia = int(
            vendas_por_dia_base * fator_s * fator_t * fator_d
            * random.uniform(0.85, 1.15)  # Variação aleatória
        )
        vendas_dia = max(vendas_dia, 20)  # Mínimo de 20 vendas/dia

        for _ in range(vendas_dia):
            # Selecionar produto
            produto = random.choices(produtos, weights=pesos_produtos, k=1)[0]

            # Selecionar unidade (com peso regional)
            pesos_u = [u["peso"] for u in unidades]
            unidade = random.choices(unidades, weights=pesos_u, k=1)[0]

            # Selecionar vendedor da unidade
            vendedores_unid = vendedores_por_unidade.get(unidade["id"])
            if not vendedores_unid:
                vendedor = random.choice(vendedores)
            else:
                vendedor = random.choice(vendedores_unid)

            # Quantidade (maioria compra 1, alguns compram mais)
            qtd = random.choices(
                [1, 2, 3, 4, 5],
                weights=[0.65, 0.20, 0.08, 0.04, 0.03],
                k=1
            )[0]

            # Desconto
            is_black_friday = (dia_atual.month == 11 and dia_atual.day >= 20)
            is_natal = (dia_atual.month == 12 and dia_atual.day >= 15)

            if is_black_friday:
                desconto = random.choices(
                    [0, 5, 10, 15, 20, 25, 30],
                    weights=[0.05, 0.10, 0.20, 0.25, 0.20, 0.15, 0.05],
                    k=1
                )[0]
            elif is_natal:
                desconto = random.choices(
                    [0, 5, 10, 15],
                    weights=[0.30, 0.30, 0.25, 0.15],
                    k=1
                )[0]
            else:
                desconto = random.choices(
                    [0, 5, 10, 15, 20],
                    weights=[0.50, 0.25, 0.15, 0.07, 0.03],
                    k=1
                )[0]

            # Leve variação de preço (promoções pontuais)
            preco_praticado = round(
                produto["preco"] * random.uniform(0.95, 1.0), 2
            )

            # Cálculos financeiros
            valor_bruto = round(preco_praticado * qtd, 2)
            valor_desconto = round(valor_bruto * desconto / 100, 2)
            valor_total = round(valor_bruto - valor_desconto, 2)
            custo_total = round(produto["custo"] * qtd, 2)
            lucro = round(valor_total - custo_total, 2)

            # Canal e pagamento
            canal = random.choices(CANAIS_VENDA, weights=PESO_CANAL, k=1)[0]
            pagamento = random.choices(
                FORMAS_PAGAMENTO, weights=PESO_PAGAMENTO, k=1
            )[0]
            status = random.choices(
                STATUS_VENDA, weights=PESO_STATUS, k=1
            )[0]

            # Horário aleatório
            hora = random.randint(8, 21)
            minuto = random.randint(0, 59)
            segundo = random.randint(0, 59)
            data_venda = dia_atual.replace(
                hour=hora, minute=minuto, second=segundo
            ).strftime("%Y-%m-%d %H:%M:%S")

            registros.append((
                data_venda, produto["id"], vendedor["id"],
                unidade["id"], qtd, preco_praticado, desconto,
                valor_total, custo_total, lucro, canal, pagamento, status
            ))

            # Inserir em batches
            if len(registros) >= batch_size:
                cursor.executemany(
                    """INSERT INTO vendas
                       (data_venda, id_produto, id_vendedor, id_unidade,
                        quantidade, preco_unitario, desconto, valor_total,
                        custo_total, lucro, canal_venda, forma_pagamento,
                        status_venda)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    registros
                )
                total_inserido += len(registros)
                registros = []
                print(f"   📊 {total_inserido:,} registros inseridos...")

        dia_atual += timedelta(days=1)

    # Inserir registros restantes
    if registros:
        cursor.executemany(
            """INSERT INTO vendas
               (data_venda, id_produto, id_vendedor, id_unidade,
                quantidade, preco_unitario, desconto, valor_total,
                custo_total, lucro, canal_venda, forma_pagamento,
                status_venda)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            registros
        )
        total_inserido += len(registros)

    conn.commit()
    print(f"\n✅ {total_inserido:,} vendas inseridas com sucesso!")
    return total_inserido


def popular_metas(conn: sqlite3.Connection, unidades: list):
    """Gera metas mensais para cada unidade."""
    cursor = conn.cursor()
    count = 0
    for ano in [2023, 2024, 2025]:
        for mes in range(1, 13):
            if ano == 2025 and mes == 12:
                # Último mês só vai até dia 1
                continue
            for unidade in unidades:
                # Meta proporcional ao peso da região
                meta_base = random.uniform(80_000, 200_000)
                meta = round(meta_base * unidade["peso"], 2)
                # Crescimento anual nas metas também
                if ano == 2024:
                    meta *= 1.10
                elif ano == 2025:
                    meta *= 1.20

                cursor.execute(
                    """INSERT INTO metas_mensais
                       (ano, mes, id_unidade, meta_faturamento)
                       VALUES (?, ?, ?, ?)""",
                    (ano, mes, unidade["id"], round(meta, 2))
                )
                count += 1

    conn.commit()
    print(f"✅ {count:,} metas mensais inseridas")


# ========================= QUERIES DE VALIDAÇÃO =========================

def validar_dados(conn: sqlite3.Connection):
    """Executa queries de validação para confirmar a integridade dos dados."""
    cursor = conn.cursor()

    print("\n" + "=" * 60)
    print("📋 VALIDAÇÃO DOS DADOS GERADOS")
    print("=" * 60)

    # Contagem de registros
    tabelas = ["categorias", "produtos", "regioes", "unidades",
               "vendedores", "vendas", "metas_mensais"]
    print("\n📊 Contagem de registros:")
    for t in tabelas:
        cursor.execute(f"SELECT COUNT(*) FROM {t}")
        count = cursor.fetchone()[0]
        print(f"   {t:20s} → {count:>10,} registros")

    # Faturamento por ano
    print("\n💰 Faturamento por ano (vendas concluídas):")
    cursor.execute("""
        SELECT strftime('%Y', data_venda) AS ano,
               SUM(valor_total) AS faturamento,
               COUNT(*) AS num_vendas,
               AVG(valor_total) AS ticket_medio
        FROM vendas
        WHERE status_venda = 'Concluída'
        GROUP BY ano
        ORDER BY ano
    """)
    for row in cursor.fetchall():
        print(f"   {row[0]}: R$ {row[1]:>14,.2f}  |  "
              f"{row[2]:>6,} vendas  |  "
              f"Ticket médio: R$ {row[3]:>8,.2f}")

    # Top 5 produtos
    print("\n🏆 Top 5 produtos por faturamento:")
    cursor.execute("""
        SELECT p.nome_produto, SUM(v.valor_total) AS fat,
               SUM(v.lucro) AS lucro_total
        FROM vendas v
        JOIN produtos p ON v.id_produto = p.id_produto
        WHERE v.status_venda = 'Concluída'
        GROUP BY p.nome_produto
        ORDER BY fat DESC
        LIMIT 5
    """)
    for i, row in enumerate(cursor.fetchall(), 1):
        print(f"   {i}. {row[0]:35s} → "
              f"R$ {row[1]:>12,.2f}  (Lucro: R$ {row[2]:>12,.2f})")

    # Vendas por canal
    print("\n📡 Distribuição por canal de venda:")
    cursor.execute("""
        SELECT canal_venda, COUNT(*) as qtd,
               ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM vendas), 1) AS pct
        FROM vendas
        GROUP BY canal_venda
        ORDER BY qtd DESC
    """)
    for row in cursor.fetchall():
        print(f"   {row[0]:20s} → {row[1]:>7,} vendas ({row[2]}%)")

    # Status
    print("\n📋 Status das vendas:")
    cursor.execute("""
        SELECT status_venda, COUNT(*) as qtd,
               ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM vendas), 1) AS pct
        FROM vendas
        GROUP BY status_venda
        ORDER BY qtd DESC
    """)
    for row in cursor.fetchall():
        print(f"   {row[0]:15s} → {row[1]:>7,} ({row[2]}%)")

    print("\n" + "=" * 60)


# ========================= QUERIES DE EXEMPLO =========================

def imprimir_queries_exemplo():
    """Imprime queries SQL úteis para análise."""
    print("\n" + "=" * 60)
    print("🔍 QUERIES SQL DE EXEMPLO PARA ANÁLISE")
    print("=" * 60)

    queries = {
        "Faturamento no mês X": """
SELECT strftime('%Y-%m', data_venda) AS mes,
       SUM(valor_total) AS faturamento
FROM vendas
WHERE status_venda = 'Concluída'
  AND strftime('%Y-%m', data_venda) = '2025-06'
GROUP BY mes;""",

        "Melhor produto (por faturamento)": """
SELECT p.nome_produto, c.nome_categoria,
       SUM(v.valor_total) AS faturamento,
       SUM(v.lucro) AS lucro_total,
       SUM(v.quantidade) AS unidades_vendidas
FROM vendas v
JOIN produtos p ON v.id_produto = p.id_produto
JOIN categorias c ON p.id_categoria = c.id_categoria
WHERE v.status_venda = 'Concluída'
GROUP BY p.id_produto
ORDER BY faturamento DESC
LIMIT 10;""",

        "Evolução mensal do faturamento": """
SELECT strftime('%Y-%m', data_venda) AS mes,
       SUM(valor_total) AS faturamento,
       SUM(lucro) AS lucro,
       COUNT(*) AS num_vendas,
       AVG(valor_total) AS ticket_medio
FROM vendas
WHERE status_venda = 'Concluída'
GROUP BY mes
ORDER BY mes;""",

        "Comparativo anual (crescimento)": """
SELECT strftime('%Y', data_venda) AS ano,
       SUM(valor_total) AS faturamento,
       SUM(lucro) AS lucro,
       COUNT(*) AS total_vendas,
       ROUND(AVG(desconto), 2) AS desconto_medio
FROM vendas
WHERE status_venda = 'Concluída'
GROUP BY ano
ORDER BY ano;""",

        "Desempenho produto X vs Y": """
SELECT p.nome_produto,
       strftime('%Y', v.data_venda) AS ano,
       SUM(v.valor_total) AS faturamento,
       SUM(v.quantidade) AS unidades,
       ROUND(AVG(v.desconto), 1) AS desc_medio,
       SUM(v.lucro) AS lucro
FROM vendas v
JOIN produtos p ON v.id_produto = p.id_produto
WHERE v.status_venda = 'Concluída'
  AND p.nome_produto IN ('Smartphone Premium', 'Notebook Gamer')
GROUP BY p.nome_produto, ano
ORDER BY p.nome_produto, ano;""",

        "Ranking de vendedores": """
SELECT ve.nome_vendedor, ve.nivel,
       u.nome_unidade,
       SUM(v.valor_total) AS faturamento,
       COUNT(*) AS num_vendas
FROM vendas v
JOIN vendedores ve ON v.id_vendedor = ve.id_vendedor
JOIN unidades u ON ve.id_unidade = u.id_unidade
WHERE v.status_venda = 'Concluída'
GROUP BY ve.id_vendedor
ORDER BY faturamento DESC
LIMIT 10;""",

        "Performance por região": """
SELECT r.nome_regiao,
       SUM(v.valor_total) AS faturamento,
       SUM(v.lucro) AS lucro,
       COUNT(*) AS num_vendas
FROM vendas v
JOIN unidades u ON v.id_unidade = u.id_unidade
JOIN regioes r ON u.id_regiao = r.id_regiao
WHERE v.status_venda = 'Concluída'
GROUP BY r.nome_regiao
ORDER BY faturamento DESC;""",
    }

    for titulo, query in queries.items():
        print(f"\n-- 📌 {titulo}")
        print(query)

    print("\n" + "=" * 60)


# ========================= MAIN =========================

def main():
    print("=" * 60)
    print("🚀 GERADOR DE DADOS - BANCO DE VENDAS")
    print("=" * 60)

    # Remover banco existente
    db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), DB_NAME)
    if os.path.exists(db_path):
        os.remove(db_path)
        print(f"🗑️  Banco anterior removido: {db_path}")

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    try:
        # 1. Criar estrutura
        print("\n📦 Criando tabelas...")
        criar_banco(conn)

        # 2. Popular dados
        print("\n📥 Populando dados...")
        cat_map = popular_categorias(conn)
        produtos = popular_produtos(conn, cat_map)
        unidades = popular_regioes_e_unidades(conn)
        vendedores = popular_vendedores(conn, unidades)
        total_vendas = popular_vendas(conn, produtos, vendedores, unidades)
        popular_metas(conn, unidades)

        # 3. Validação
        validar_dados(conn)

        # 4. Queries de exemplo
        imprimir_queries_exemplo()

        print(f"\n✅ Banco de dados gerado com sucesso!")
        print(f"📁 Arquivo: {db_path}")
        print(f"📊 Total de vendas: {total_vendas:,}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()