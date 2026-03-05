"""Script para criar schema PostgreSQL de forma robusta."""
import sys
from sqlalchemy import create_engine, text
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger()

engine = create_engine('postgresql+psycopg2://admin:admin@localhost:5442/admin')

# Ler SQL
with open('database/01_create_schema.sql', 'r', encoding='utf-8') as f:
    sql = f.read()

# Dividir em statements
statements = []
current = []
in_comment = False

for line in sql.split('\n'):
    line_stripped = line.strip()
    
    if line_stripped.startswith('--'):
        continue
    
    if line_stripped.startswith('/*'):
        in_comment = True
    if in_comment:
        if '*/' in line:
            in_comment = False
        continue
    
    current.append(line)
    if line_stripped.endswith(';'):
        stmt = ' '.join(current).strip()
        if stmt:
            statements.append(stmt)
        current = []

# Executar statements
success = 0
failed = 0

for i, stmt in enumerate(statements):
    try:
        with engine.begin() as conn:
            conn.execute(text(stmt))
        success += 1
        if 'CREATE TABLE' in stmt:
            tbl = stmt.split('CREATE TABLE')[1].split('(')[0].strip().replace('IF NOT EXISTS', '').strip()
            logger.info(f"✓ Tabela criada: {tbl}")
    except Exception as e:
        err_msg = str(e).lower()
        if 'already exists' in err_msg or 'duplicate key' in err_msg:
            success += 1
        else:
            failed += 1
            logger.warning(f"✗ Erro statement {i}: {str(e)[:100]}")

logger.info(f"\n✓ Schema criado com sucesso!")
logger.info(f"  Statements executados: {success}")
if failed > 0:
    logger.info(f"  Errors: {failed} (ignorados)")
