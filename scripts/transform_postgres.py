"""
Módulo de transformação: carrega CSV extraído para tabelas PostgreSQL.

O script espera encontrar arquivos CSV em `data/raw/` e insere as linhas
na tabela `dengue_cases` do banco Postgres definido no docker-compose.

Uso:
    python transform_postgres.py [--file path] [--table name] [--if-exists append]

Se nenhum arquivo for informado, ele usa o CSV mais recente no diretório
`data/raw/`.
"""

import argparse
import logging
import os
from pathlib import Path
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine

# configurar logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


PG_USER = os.environ.get("PG_USER", "admin")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "admin")
PG_HOST = os.environ.get("PG_HOST", "localhost")
PG_PORT = os.environ.get("PG_PORT", "5442")
PG_DB = os.environ.get("PG_DB", "admin")


def get_engine():
    url = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    logger.debug(f"Criando engine com URL {url}")
    return create_engine(url)


def find_latest_csv(directory: Path) -> Optional[Path]:
    """Retorna o arquivo CSV mais recente no diretório."""
    csv_files = sorted(directory.glob("*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
    return csv_files[0] if csv_files else None


def load_csv_to_postgres(
    csv_path: Path,
    table_name: str = "dengue_cases",
    if_exists: str = "append"
):
    """Carrega o CSV no banco PostgreSQL."""
    logger.info(f"Lendo CSV {csv_path}")
    df = pd.read_csv(csv_path, dtype=str)

    # substituir strings "nan" por NaN reais
    df = df.replace({"nan": pd.NA})

    engine = get_engine()
    logger.info(f"Escrevendo tabela '{table_name}' (if_exists={if_exists})")
    df.to_sql(table_name, engine, if_exists=if_exists, index=False)
    logger.info("Carga concluída com sucesso")


def main():
    parser = argparse.ArgumentParser(description="Carrega CSV para PostgreSQL")
    parser.add_argument("--file", type=Path, help="Caminho para o CSV (opcional)")
    parser.add_argument("--table", default="dengue_cases", help="Nome da tabela destino")
    parser.add_argument(
        "--if-exists",
        choices=["fail", "replace", "append"],
        default="append",
        help="Comportamento se a tabela já existir"
    )
    args = parser.parse_args()

    if args.file:
        csv_path = args.file
    else:
        csv_path = find_latest_csv(Path("data/raw"))
        if not csv_path:
            logger.error("Nenhum arquivo CSV encontrado em data/raw/")
            return 1

    load_csv_to_postgres(csv_path, table_name=args.table, if_exists=args.if_exists)
    return 0


if __name__ == "__main__":
    exit(main())
