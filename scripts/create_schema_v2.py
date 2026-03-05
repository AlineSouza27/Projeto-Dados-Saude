"""Script para executar schema PostgreSQL com melhor controle de erro."""

import psycopg2
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# Configuração de conexão
conn_config = {
    'host': 'localhost',
    'port': 5442,
    'database': 'postgres',
    'user': 'admin',
    'password': 'admin'
}

# Fases de execução
phases = {
    'Tabelas Base': [
        'CREATE TABLE IF NOT EXISTS pacientes (id_paciente SERIAL PRIMARY KEY, idade INT, sexo CHAR(1), raca VARCHAR(50), escolaridade VARCHAR(10), ocupacao INT, gestante INT, ano_nascimento INT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);',
        'CREATE TABLE IF NOT EXISTS localizacoes (id_localizacao SERIAL PRIMARY KEY, uf_notificacao VARCHAR(2), municipio_notif INT, regiao_notif INT, unidade_saude INT, uf_residencia VARCHAR(2), municipio_resi INT, regiao_resi INT, pais INT DEFAULT 1, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);',
        'CREATE TABLE IF NOT EXISTS diagnosticos (id_diagnostico SERIAL PRIMARY KEY, data_soro_1 DATE, resultado_soro_1 INT, data_soro_2 DATE, resultado_soro_2 INT, igm_s1 INT, igg_s1 INT, igm_s2 INT, igg_s2 INT, data_pcr DATE, resultado_pcr INT, amostra_pcr VARCHAR(50), data_ns1 DATE, resultado_ns1 INT, data_prnt DATE, resultado_prnt INT, data_viral DATE, resultado_viral INT, sorotipo INT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);',
        'CREATE TABLE IF NOT EXISTS sintomas (id_sintoma SERIAL PRIMARY KEY, nome VARCHAR(100) UNIQUE NOT NULL, descricao TEXT, categoria VARCHAR(50));',
        'CREATE TABLE IF NOT EXISTS fatores_risco (id_fator SERIAL PRIMARY KEY, nome VARCHAR(100) UNIQUE NOT NULL, descricao TEXT, categoria VARCHAR(50));',
    ],
    'Dimensões': [
        '''INSERT INTO sintomas (nome, descricao, categoria) VALUES 
        ('Febre', 'Temperatura corporal elevada', 'Sintoma geral'),
        ('Mialgia', 'Dor muscular', 'Dor'),
        ('Cefaleia', 'Dor de cabeça', 'Dor'),
        ('Exantema', 'Erupção cutânea', 'Pele'),
        ('Vômito', 'Vômito/emese', 'GI'),
        ('Náusea', 'Sensação de náusea', 'GI'),
        ('Dor nas costas', 'Dor na coluna vertebral', 'Dor'),
        ('Conjuntivite', 'Conjuntivite viral', 'Ocular'),
        ('Artrite', 'Inflamação articular', 'Articulações'),
        ('Artralgia', 'Dor articular', 'Articulações'),
        ('Dor retro-orbitária', 'Dor atrás dos olhos', 'Dor'),
        ('Leucopenia', 'Redução de leucócitos', 'Laboratorial'),
        ('Trombocitopenia', 'Redução de plaquetas', 'Laboratorial')
        ON CONFLICT (nome) DO NOTHING;''',
        
        '''INSERT INTO fatores_risco (nome, descricao, categoria) VALUES 
        ('Diabetes mellitus', 'Diabético', 'Doença crônica'),
        ('Hematológico', 'Doença hematológica', 'Doença crônica'),
        ('Hepatopatia', 'Doença hepática', 'Doença crônica'),
        ('Renal', 'Doença renal', 'Doença crônica'),
        ('Hipertensão', 'Pressão arterial elevada', 'Cardiovascular'),
        ('Ácido péptico', 'Úlcera / gastrite', 'GI'),
        ('Auto-imune', 'Doença auto-imune', 'Imunológica'),
        ('Obesidade', 'IMC >= 30', 'Metabólica'),
        ('Imunossupressão', 'Paciente imunossuprimido', 'Imunológica')
        ON CONFLICT (nome) DO NOTHING;''',
    ],
    'Tabela Central': [
        '''CREATE TABLE IF NOT EXISTS cases (
    id_case SERIAL PRIMARY KEY,
    id_notificacao VARCHAR(50) UNIQUE,
    id_paciente INT NOT NULL REFERENCES pacientes(id_paciente) ON DELETE CASCADE,
    id_localizacao INT NOT NULL REFERENCES localizacoes(id_localizacao) ON DELETE CASCADE,
    id_diagnostico INT REFERENCES diagnosticos(id_diagnostico) ON DELETE SET NULL,
    dt_notificacao DATE NOT NULL,
    dt_sin_pri DATE,
    dt_encerra DATE,
    dt_digita DATE,
    dt_investiga DATE,
    sem_notif INT,
    ano INT NOT NULL,
    tp_notif INT,
    id_agravo VARCHAR(10) DEFAULT 'A90',
    criterio INT,
    classificacao_final INT,
    evolucao INT CHECK(evolucao IN (1, 2, NULL)),
    dt_obito DATE,
    hospitalizado INT CHECK(hospitalizado IN (1, 2, NULL)),
    data_internacao DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);''',
    ],
    'Tabelas Associativas': [
        '''CREATE TABLE IF NOT EXISTS casos_sintomas (
    id_caso_sintoma SERIAL PRIMARY KEY,
    id_case INT NOT NULL REFERENCES cases(id_case) ON DELETE CASCADE,
    id_sintoma INT NOT NULL REFERENCES sintomas(id_sintoma) ON DELETE CASCADE,
    presente INT CHECK(presente IN (1, 0, NULL)),
    data_inicio DATE,
    UNIQUE(id_case, id_sintoma)
);''',
        
        '''CREATE TABLE IF NOT EXISTS casos_fatores_risco (
    id_caso_fator SERIAL PRIMARY KEY,
    id_case INT NOT NULL REFERENCES cases(id_case) ON DELETE CASCADE,
    id_fator INT NOT NULL REFERENCES fatores_risco(id_fator) ON DELETE CASCADE,
    presente INT CHECK(presente IN (1, 0, NULL)),
    observacao TEXT,
    UNIQUE(id_case, id_fator)
);''',
        
        '''CREATE TABLE IF NOT EXISTS manifestacoes_hemor (
    id_manifestacao SERIAL PRIMARY KEY,
    id_case INT NOT NULL REFERENCES cases(id_case) ON DELETE CASCADE,
    petequias INT,
    epistaxe INT,
    gengivorragia INT,
    metrorragia INT,
    hematuria INT,
    sangramento_gi INT,
    data_manifestacao DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);''',
        
        '''CREATE TABLE IF NOT EXISTS alertas_graves (
    id_alerta SERIAL PRIMARY KEY,
    id_case INT NOT NULL REFERENCES cases(id_case) ON DELETE CASCADE,
    hipotensao INT,
    plaquetas_criticas INT,
    vomitos_persistentes INT,
    sangramento INT,
    hematocrito_alt INT,
    dor_abdominal INT,
    letargia INT,
    hepatomegalia INT,
    data_alerta DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);''',
    ],
    'Índices': [
        'CREATE INDEX IF NOT EXISTS idx_pacientes_sexo ON pacientes(sexo);',
        'CREATE INDEX IF NOT EXISTS idx_pacientes_idade ON pacientes(idade);',
        'CREATE INDEX IF NOT EXISTS idx_localizacoes_uf_residencia ON localizacoes(uf_residencia);',
        'CREATE INDEX IF NOT EXISTS idx_localizacoes_municipio_resi ON localizacoes(municipio_resi);',
        'CREATE INDEX IF NOT EXISTS idx_diagnosticos_sorotipo ON diagnosticos(sorotipo);',
        'CREATE INDEX IF NOT EXISTS idx_cases_dt_notificacao ON cases(dt_notificacao);',
        'CREATE INDEX IF NOT EXISTS idx_cases_ano ON cases(ano);',
        'CREATE INDEX IF NOT EXISTS idx_cases_id_paciente ON cases(id_paciente);',
        'CREATE INDEX IF NOT EXISTS idx_cases_classificacao ON cases(classificacao_final);',
        'CREATE INDEX IF NOT EXISTS idx_cases_evolucao ON cases(evolucao);',
        'CREATE INDEX IF NOT EXISTS idx_casos_sintomas_id_case ON casos_sintomas(id_case);',
        'CREATE INDEX IF NOT EXISTS idx_casos_sintomas_id_sintoma ON casos_sintomas(id_sintoma);',
        'CREATE INDEX IF NOT EXISTS idx_casos_fatores_id_case ON casos_fatores_risco(id_case);',
        'CREATE INDEX IF NOT EXISTS idx_casos_fatores_id_fator ON casos_fatores_risco(id_fator);',
        'CREATE INDEX IF NOT EXISTS idx_manifestacoes_id_case ON manifestacoes_hemor(id_case);',
        'CREATE INDEX IF NOT EXISTS idx_alertas_id_case ON alertas_graves(id_case);',
    ]
}

def execute_phase(conn, phase_name, statements):
    """Executa uma fase de DDL com tratamento de erro."""
    logger.info(f"\n{'='*60}")
    logger.info(f"Executando: {phase_name}")
    logger.info('='*60)
    
    success_count = 0
    error_count = 0
    
    with conn.cursor() as cur:
        for i, stmt in enumerate(statements, 1):
            try:
                cur.execute(stmt)
                conn.commit()
                logger.info(f"  ✓ [{i}/{len(statements)}] Executado")
                success_count += 1
            except Exception as e:
                conn.rollback()
                logger.error(f"  ✗ [{i}/{len(statements)}] Erro: {str(e)[:80]}")
                error_count += 1
    
    logger.info(f"Resultado: {success_count} sucesso, {error_count} erro(s)")
    return success_count, error_count

# Executar
try:
    conn = psycopg2.connect(**conn_config)
    logger.info("✓ Conectado ao PostgreSQL")
    
    total_success = 0
    total_error = 0
    
    for phase_name, statements in phases.items():
        success, error = execute_phase(conn, phase_name, statements)
        total_success += success
        total_error += error
    
    logger.info(f"\n{'='*60}")
    logger.info(f"RESUMO FINAL")
    logger.info('='*60)
    logger.info(f"Total executado: {total_success} com sucesso")
    logger.info(f"Total com erro: {total_error}")
    logger.info("✓ Schema criado com sucesso!")
    
    conn.close()
    
except psycopg2.Error as e:
    logger.error(f"Erro de conexão: {e}")
except Exception as e:
    logger.error(f"Erro: {e}")
