"""Gera relatório de análise dos dados de dengue."""

import psycopg2
import json
from pathlib import Path
from datetime import datetime
from decimal import Decimal

def convert_decimals(obj):
    """Converte Decimal para float para JSON serialization."""
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError

conn_config = {
    'host': 'localhost',
    'port': 5442,
    'database': 'postgres',
    'user': 'admin',
    'password': 'admin'
}

conn = psycopg2.connect(**conn_config)
cur = conn.cursor()

# Relatório
report = {
    'titulo': 'RELATÓRIO DE ANÁLISE - DENGUE DATASUS',
    'data_geracao': datetime.now().isoformat(),
    'resumo_geral': {},
    'analises': {}
}

# ========== RESUMO GERAL ==========
queries_resumo = {
    'Total de Casos': 'SELECT COUNT(*) FROM cases',
    'Total de Pacientes Únicos': 'SELECT COUNT(DISTINCT id_paciente) FROM cases',
    'Total de Municípios': 'SELECT COUNT(DISTINCT municipio_resi) FROM localizacoes WHERE municipio_resi IS NOT NULL',
    'Estados Afetados': "SELECT COUNT(DISTINCT uf_residencia) FROM localizacoes WHERE uf_residencia IS NOT NULL",
    'Período de Notificação': "SELECT MIN(dt_notificacao)::text || ' a ' || MAX(dt_notificacao)::text FROM cases",
    'Óbitos Registrados': "SELECT COUNT(*) FROM cases WHERE evolucao = 2",
    'Taxa de Óbito (%)': "SELECT ROUND(100.0 * SUM(CASE WHEN evolucao = 2 THEN 1 ELSE 0 END) / COUNT(*), 2) FROM cases",
    'Hospitalizados': "SELECT COUNT(*) FROM cases WHERE hospitalizado = 1",
    'Taxa de Hospitalização (%)': "SELECT ROUND(100.0 * SUM(CASE WHEN hospitalizado = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) FROM cases",
}

print('='*70)
print('RELATÓRIO DE ANÁLISE - DENGUE DATASUS')
print('='*70)
print(f'Gerado em: {datetime.now().strftime("%d/%m/%Y às %H:%M:%S")}')
print('')

for label, query in queries_resumo.items():
    cur.execute(query)
    result = cur.fetchone()[0]
    report['resumo_geral'][label] = result
    print(f'{label:<35} {str(result):>20}')

# ========== ANÁLISE POR FAIXA ETÁRIA ==========
print('\n' + '='*70)
print('CASOS POR FAIXA ETÁRIA')
print('='*70)

cur.execute('''
    WITH faixas AS (
        SELECT 
            p.idade,
            CASE 
                WHEN p.idade < 5 THEN '0-4 anos'
                WHEN p.idade < 18 THEN '5-17 anos'
                WHEN p.idade < 30 THEN '18-29 anos'
                WHEN p.idade < 60 THEN '30-59 anos'
                ELSE '60+ anos'
            END as faixa,
            CASE 
                WHEN p.idade < 5 THEN 1
                WHEN p.idade < 18 THEN 2
                WHEN p.idade < 30 THEN 3
                WHEN p.idade < 60 THEN 4
                ELSE 5
            END as ordem
        FROM cases c
        JOIN pacientes p ON c.id_paciente = p.id_paciente
    )
    SELECT 
        faixa,
        COUNT(*) as casos,
        ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM pacientes), 2) as percentual
    FROM faixas
    GROUP BY faixa, ordem
    ORDER BY ordem
''')

faixas_etarias = []
for row in cur.fetchall():
    faixas_etarias.append({'faixa': row[0], 'casos': row[1], 'percentual': row[2]})
    print(f"{row[0]:<15} {row[1]:>6} casos ({row[2]:>6}%)")

report['analises']['faixas_etarias'] = faixas_etarias

# ========== ANÁLISE POR SEXO ==========
print('\n' + '='*70)
print('CASOS POR SEXO')
print('='*70)

cur.execute('''
    SELECT 
        CASE WHEN p.sexo = 'M' THEN 'Masculino' ELSE 'Feminino' END as sexo,
        COUNT(*) as casos,
        ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM pacientes), 2) as percentual
    FROM cases c
    JOIN pacientes p ON c.id_paciente = p.id_paciente
    GROUP BY p.sexo
    ORDER BY casos DESC
''')

for row in cur.fetchall():
    print(f"{row[0]:<15} {row[1]:>6} casos ({row[2]:>6}%)")

# ========== SINTOMAS MAIS FREQUENTES ==========
print('\n' + '='*70)
print('SINTOMAS MAIS FREQUENTES (por manifestação)')
print('='*70)

cur.execute('''
    SELECT 
        s.nome,
        COUNT(*) as registros,
        ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM cases), 2) as percentual
    FROM casos_sintomas cs
    JOIN sintomas s ON cs.id_sintoma = s.id_sintoma
    WHERE cs.presente = 1
    GROUP BY s.nome
    ORDER BY registros DESC
    LIMIT 10
''')

sintomas_freq = []
result = cur.fetchall()
if result:
    for row in result:
        sintomas_freq.append({'nome': row[0], 'registros': row[1], 'percentual': row[2]})
        print(f"{row[0]:<25} {row[1]:>3} casos ({row[2]:>6}%)")
else:
    print('  (Nenhum registro de associação sintomas)')

report['analises']['sintomas_frequentes'] = sintomas_freq

# ========== FATORES DE RISCO ==========
print('\n' + '='*70)
print('FATORES DE RISCO MAIS COMUNS')
print('='*70)

cur.execute('''
    SELECT 
        f.nome,
        COUNT(*) as registros,
        ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM cases), 2) as percentual
    FROM casos_fatores_risco cf
    JOIN fatores_risco f ON cf.id_fator = f.id_fator
    WHERE cf.presente = 1
    GROUP BY f.nome
    ORDER BY registros DESC
''')

fatores_freq = []
result = cur.fetchall()
if result:
    for row in result:
        fatores_freq.append({'nome': row[0], 'registros': row[1], 'percentual': row[2]})
        print(f"{row[0]:<30} {row[1]:>3} casos ({row[2]:>6}%)")
else:
    print('  (Nenhum registro de associação fatores de risco)')

report['analises']['fatores_risco'] = fatores_freq

# ========== EVOLUÇÃO ==========
print('\n' + '='*70)
print('EVOLUÇÃO DOS CASOS')
print('='*70)

cur.execute('''
    SELECT 
        CASE WHEN evolucao = 1 THEN 'Cura' WHEN evolucao = 2 THEN 'Óbito' ELSE 'Desconhecido' END as evolucao,
        COUNT(*) as casos,
        ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM cases), 2) as percentual
    FROM cases
    GROUP BY evolucao
    ORDER BY casos DESC
''')

for row in cur.fetchall():
    print(f"{row[0]:<15} {row[1]:>6} casos ({row[2]:>6}%)")

# ========== Estados/UFs ==========
print('\n' + '='*70)
print('DISTRIBUIÇÃO GEOGRÁFICA (UF de Residência)')
print('='*70)

cur.execute('''
    SELECT 
        COALESCE(l.uf_residencia, 'Desconhecido') as uf,
        COUNT(*) as casos,
        ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM cases), 2) as percentual
    FROM cases c
    JOIN localizacoes l ON c.id_localizacao = l.id_localizacao
    GROUP BY l.uf_residencia
    ORDER BY casos DESC
    LIMIT 15
''')

for row in cur.fetchall():
    print(f"{row[0]:<5} {row[1]:>6} casos ({row[2]:>6}%)")

# ========== Salvar relatório JSON ==========
report_path = Path('reports/analise_dengue.json')
report_path.parent.mkdir(parents=True, exist_ok=True)

with open(report_path, 'w', encoding='utf-8') as f:
    json.dump(report, f, indent=2, ensure_ascii=False, default=convert_decimals)

print('\n' + '='*70)
print(f'✓ Relatório salvo em: {report_path}')
print('='*70)

conn.close()
