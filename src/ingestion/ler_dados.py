from pathlib import Path
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, current_date, col, lower, trim
import re



process = "ReadDataJob"

def create_spark_session():
    spark= (
        SparkSession.builder
        .appName(f"{process}")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def log_info(message, layout):
    print(
        f"{datetime.now().strftime('%Y/%m/%d %H:%M:%S')} "
        f"INFO Process{layout.title()}{pr}: {message}"
    )



def filter_irrelevant_jobs(df):

    PALAVRAS_PROIBIDAS = [
    "atendente", "lider", "coordenador", "subgerente", "Consultor(a)", "Eletromecânico",
    "recepcionista", "auxiliar", "médico", "especialista", "gerente", "designer",
    "entregador", "vendedor", "instrutor", "vendas", "consultor", "SUPERVISOR",
    "Porteiro", "estagiária", "motorista", "Paulínia/SP", "Líder", "cozinheiro",
    "enfermeiro", "operador de caixa", "auxiliar de limpeza", "telemarketing", "garçom", "barista",
    "jovem aprendiz", "estágio", "estagiário", "aprendiz", "consultor de vendas", "promotor",
    "caixa", "balconista", "farmacêutico", "assistente", "chefe", "nutricionista",
    "psicólogo", "advogado", "atendente", "recepcionista", "vendedor", "vendas",
    "caixa", "balconista", "operador de loja", "operador(a) de loja", "operador loja", "operador de caixa",
    "shopping", "sac", "customer success", "atendimento", "atendimento ao cliente", "chefe atendimento",
    "limpeza", "faxineiro", "copeiro", "garçom", "cumim", "pizzaiolo",
    "cozinheiro", "padeiro", "barista", "estoque", "armazém", "conferente",
    "coleta", "entregador", "cd ", "centro de distribuição", "enfermagem", "enfermeiro",
    "técnico de enfermagem", "psicólogo", "nutricionista", "farmacêutico", "administrativo", "secretariado",
    "rh", "recrutamento", "folha de pagamento", "gente e gestão", "dp ", "depto pessoal",
    "jurídico", "advogado", "contábil", "fiscal", "compliance", "controladoria",
    "financeiro", "mecânico", "eletromecânico", "telecom", "field", "automotiva",
    "instrumentação", "marketing", "trade marketing", "crm", "comercial", "account executive",
    "sdr", "pricing", "estágio", "estagiário", "estagiária", "aprendiz",
    "jovem aprendiz", "moda", "estilista", "professor", "guia de ferias", "laser",
    "auditor", "lead", "administrdor", "processos", "qualidade", "qa",
    "plataforma", "jornada", "produto", "manager", "cx", ".net",
    "front-end", "front", "executivo", "conteúdo", "arquiteto", "quality",
    "gestor", "ux", "controles", "projetos", "farmer", "prevenção de perdas",
    "front", "compras", "loja", "jr", "junior", "experiência",
    "planejamento", "testes", "supervisor", "porteiro", "Manutenção", "php", "encarregado(a)", 
    "encarregada", "encaregado", "product owner", "instrutor", "instrutora", "instrutor(a)",
    "mudanças", "Segurança da Informação", "cto", "MuleSoft", "Orçamentista", "Advogada", "Analista de Receitas",
    "Coordenação", "ajudante", "Acompanhante", "Revenue","CONTABIL"
]

    # normaliza titulo
    df = df.withColumn(
        "titulo_normalizado",
        trim(lower(col("titulo")))
    )

    #normaliza as palavras
    palavras_proibidas = [p.strip().lower() for p in PALAVRAS_PROIBIDAS]


    # cria regex
    regex = "(" + "|".join([re.escape(p) for p in palavras_proibidas]) + ")"
    #regex = r"\b(" + "|".join(trim(lower(PALAVRAS_PROIBIDAS))) + r")\b"

    # aplica filtro
    df = df.filter(
        ~col("titulo_normalizado").rlike(regex)
    )

    df = df.filter(
        ~(
            (col("tipo_trabalho") == "hybrid") & 
            (col("estado") != "São Paulo")
        )
    )

    return df


def read_csv_to_dataframe(spark):

    path_root = Path(__file__).resolve().parents[2]
    path= (f"{path_root}/data/other/")
    df = spark.read.option("header", True).option("inferSchema", True).csv(path)


    df.printSchema()
    df = filter_irrelevant_jobs(df)
    #df = df.select("titulo_normalizado").distinct()
    #df.show(n=500, truncate=False)
    return df
def export_to_html(df):
    output_path="/app/output/vagas.html"
    pandas_df = df.toPandas()

    # começa em 1 ao invés de 0
    pandas_df.index = pandas_df.index + 1

    html_table = pandas_df.to_html(
        index=True,
        escape=False
    )

    html = f"""
    <html>
    <head>
        <meta charset="utf-8">
        <style>

            body {{
                font-family: Arial;
                padding: 20px;
                background: #f5f5f5;
            }}

            table {{
                border-collapse: collapse;
                width: 100%;
                background: white;
            }}

            th, td {{
                border: 1px solid #ccc;
                padding: 8px;
                font-size: 13px;
                text-align: left;
            }}

            th {{
                background: #222;
                color: white;
            }}

            tr:nth-child(even) {{
                background: #f2f2f2;
            }}

        </style>
    </head>

    <body>

        <h2>Vagas filtradas</h2>

        {html_table}

    </body>
    </html>
    """

    with open(output_path, "w", encoding="utf-8") as file:
        file.write(html)

    print(f"HTML gerado: {output_path}")

if __name__ == "__main__":
    spark = create_spark_session()

    try:
        df = read_csv_to_dataframe(spark)
        export_to_html(df)
    except Exception as exc:
        print(f"Pipeline failed: {exc}")
        raise
    finally:
        spark.stop()