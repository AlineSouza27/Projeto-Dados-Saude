-- ============================================================================
-- Script de criação do schema normalizado para dados de DENGUE DATASUS
-- ============================================================================

-- Habilitar extensões se necessário (comentadas para evitar erro)
-- CREATE EXTENSION IF NOT EXISTS uuid-ossp;
-- CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- ============================================================================
-- 1. TABELA: pacientes
-- ============================================================================
CREATE TABLE IF NOT EXISTS pacientes (
    id_paciente SERIAL PRIMARY KEY,
    idade INT,
    sexo CHAR(1) CHECK(sexo IN ('M', 'F')),
    raca VARCHAR(50),
    escolaridade VARCHAR(10),
    ocupacao INT,
    gestante INT CHECK(gestante IN (1, 5, NULL)),
    ano_nascimento INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_pacientes_sexo ON pacientes(sexo);
CREATE INDEX idx_pacientes_idade ON pacientes(idade);

-- ============================================================================
-- 2. TABELA: localizacoes
-- ============================================================================
CREATE TABLE IF NOT EXISTS localizacoes (
    id_localizacao SERIAL PRIMARY KEY,
    -- Notificação
    uf_notificacao VARCHAR(2),
    municipio_notif INT,
    regiao_notif INT,
    unidade_saude INT,
    -- Residência
    uf_residencia VARCHAR(2),
    municipio_resi INT,
    regiao_resi INT,
    pais INT DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_localizacoes_uf_residencia ON localizacoes(uf_residencia);
CREATE INDEX idx_localizacoes_municipio_resi ON localizacoes(municipio_resi);

-- ============================================================================
-- 3. TABELA: diagnosticos
-- ============================================================================
CREATE TABLE IF NOT EXISTS diagnosticos (
    id_diagnostico SERIAL PRIMARY KEY,
    -- Sorologia
    data_soro_1 DATE,
    resultado_soro_1 INT,
    data_soro_2 DATE,
    resultado_soro_2 INT,
    igm_s1 INT,
    igg_s1 INT,
    igm_s2 INT,
    igg_s2 INT,
    -- RT-PCR
    data_pcr DATE,
    resultado_pcr INT,
    amostra_pcr VARCHAR(50),
    -- NS1
    data_ns1 DATE,
    resultado_ns1 INT,
    -- PRNT (Teste de neutralização)
    data_prnt DATE,
    resultado_prnt INT,
    -- Isolamento viral
    data_viral DATE,
    resultado_viral INT,
    -- Sorotipo
    sorotipo INT CHECK(sorotipo IN (1, 2, 3, 4, NULL)),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_diagnosticos_sorotipo ON diagnosticos(sorotipo);

-- ============================================================================
-- 4. TABELA DIMENSÃO: sintomas
-- ============================================================================
CREATE TABLE IF NOT EXISTS sintomas (
    id_sintoma SERIAL PRIMARY KEY,
    nome VARCHAR(100) UNIQUE NOT NULL,
    descricao TEXT,
    categoria VARCHAR(50)
);

-- Popular tabela com sintomas padrão
INSERT INTO sintomas (nome, descricao, categoria) VALUES
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
ON CONFLICT (nome) DO NOTHING;

-- ============================================================================
-- 5. TABELA DIMENSÃO: fatores_risco
-- ============================================================================
CREATE TABLE IF NOT EXISTS fatores_risco (
    id_fator SERIAL PRIMARY KEY,
    nome VARCHAR(100) UNIQUE NOT NULL,
    descricao TEXT,
    categoria VARCHAR(50)
);

-- Popular com fatores padrão
INSERT INTO fatores_risco (nome, descricao, categoria) VALUES
    ('Diabetes mellitus', 'Diabético', 'Doença crônica'),
    ('Hematológico', 'Doença hematológica', 'Doença crônica'),
    ('Hepatopatia', 'Doença hepática', 'Doença crônica'),
    ('Renal', 'Doença renal', 'Doença crônica'),
    ('Hipertensão', 'Pressão arterial elevada', 'Cardiovascular'),
    ('Ácido péptico', 'Úlcera / gastrite', 'GI'),
    ('Auto-imune', 'Doença auto-imune', 'Imunológica'),
    ('Obesidade', 'IMC >= 30', 'Metabólica'),
    ('Imunossupressão', 'Paciente imunossuprimido', 'Imunológica')
ON CONFLICT (nome) DO NOTHING;

-- ============================================================================
-- 6. TABELA CENTRAL: cases
-- ============================================================================
CREATE TABLE IF NOT EXISTS cases (
    id_case SERIAL PRIMARY KEY,
    id_notificacao VARCHAR(50) UNIQUE,
    id_paciente INT NOT NULL REFERENCES pacientes(id_paciente) ON DELETE CASCADE,
    id_localizacao INT NOT NULL REFERENCES localizacoes(id_localizacao) ON DELETE CASCADE,
    id_diagnostico INT REFERENCES diagnosticos(id_diagnostico) ON DELETE SET NULL,
    -- Datas
    dt_notificacao DATE NOT NULL,
    dt_sin_pri DATE,
    dt_encerra DATE,
    dt_digita DATE,
    dt_investiga DATE,
    -- Códigos
    sem_notif INT,
    ano INT NOT NULL,
    tp_notif INT,
    id_agravo VARCHAR(10) DEFAULT 'A90',
    -- Diagnóstico e classificação
    criterio INT,
    classificacao_final INT,
    -- Evolução
    evolucao INT CHECK(evolucao IN (1, 2, NULL)), -- 1=cura, 2=óbito
    dt_obito DATE,
    -- Internação
    hospitalizado INT CHECK(hospitalizado IN (1, 2, NULL)), -- 1=sim, 2=não
    data_internacao DATE,
    -- Metadados
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_cases_dt_notificacao ON cases(dt_notificacao);
CREATE INDEX idx_cases_ano ON cases(ano);
CREATE INDEX idx_cases_id_paciente ON cases(id_paciente);
CREATE INDEX idx_cases_classificacao ON cases(classificacao_final);
CREATE INDEX idx_cases_evolucao ON cases(evolucao);

-- ============================================================================
-- 7. TABELA ASSOCIATIVA: casos_sintomas
-- ============================================================================
CREATE TABLE IF NOT EXISTS casos_sintomas (
    id_caso_sintoma SERIAL PRIMARY KEY,
    id_case INT NOT NULL REFERENCES cases(id_case) ON DELETE CASCADE,
    id_sintoma INT NOT NULL REFERENCES sintomas(id_sintoma) ON DELETE CASCADE,
    presente INT CHECK(presente IN (1, 0, NULL)), -- 1=sim, 0=não, NULL=não informado
    data_inicio DATE,
    UNIQUE(id_case, id_sintoma)
);

CREATE INDEX idx_casos_sintomas_id_case ON casos_sintomas(id_case);
CREATE INDEX idx_casos_sintomas_id_sintoma ON casos_sintomas(id_sintoma);

-- ============================================================================
-- 8. TABELA ASSOCIATIVA: casos_fatores_risco
-- ============================================================================
CREATE TABLE IF NOT EXISTS casos_fatores_risco (
    id_caso_fator SERIAL PRIMARY KEY,
    id_case INT NOT NULL REFERENCES cases(id_case) ON DELETE CASCADE,
    id_fator INT NOT NULL REFERENCES fatores_risco(id_fator) ON DELETE CASCADE,
    presente INT CHECK(presente IN (1, 0, NULL)), -- 1=sim, 0=não, NULL=não informado
    observacao TEXT,
    UNIQUE(id_case, id_fator)
);

CREATE INDEX idx_casos_fatores_id_case ON casos_fatores_risco(id_case);
CREATE INDEX idx_casos_fatores_id_fator ON casos_fatores_risco(id_fator);

-- ============================================================================
-- 9. TABELA: manifestacoes_hemor
-- ============================================================================
CREATE TABLE IF NOT EXISTS manifestacoes_hemor (
    id_manifestacao SERIAL PRIMARY KEY,
    id_case INT NOT NULL REFERENCES cases(id_case) ON DELETE CASCADE,
    petequias INT CHECK(petequias IN (1, 0, NULL)),
    epistaxe INT CHECK(epistaxe IN (1, 0, NULL)),
    gengivorragia INT CHECK(gengivorragia IN (1, 0, NULL)),
    metrorragia INT CHECK(metrorragia IN (1, 0, NULL)),
    hematuria INT CHECK(hematuria IN (1, 0, NULL)),
    sangramento_gi INT CHECK(sangramento_gi IN (1, 0, NULL)),
    data_manifestacao DATE,
    observacao TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_manifestacoes_id_case ON manifestacoes_hemor(id_case);

-- ============================================================================
-- 10. TABELA: alertas_graves (Sinais de alerta)
-- ============================================================================
CREATE TABLE IF NOT EXISTS alertas_graves (
    id_alerta SERIAL PRIMARY KEY,
    id_case INT NOT NULL REFERENCES cases(id_case) ON DELETE CASCADE,
    hipotensao INT CHECK(hipotensao IN (1, 0, NULL)),
    plaquetas_criticas INT CHECK(plaquetas_criticas IN (1, 0, NULL)),
    vomitos_persistentes INT CHECK(vomitos_persistentes IN (1, 0, NULL)),
    sangramento INT CHECK(sangramento IN (1, 0, NULL)),
    hematocrito_alt INT CHECK(hematocrito_alt IN (1, 0, NULL)),
    dor_abdominal INT CHECK(dor_abdominal IN (1, 0, NULL)),
    letargia INT CHECK(letargia IN (1, 0, NULL)),
    hepatomegalia INT CHECK(hepatomegalia IN (1, 0, NULL)),
    encefalopatia INT CHECK(encefalopatia IN (1, 0, NULL)),
    data_alerta DATE,
    observacao TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_alertas_id_case ON alertas_graves(id_case);

-- ============================================================================
-- VIEWS ÚTEIS PARA ANÁLISE
-- ============================================================================

-- View: Resumo de casos por UF
CREATE OR REPLACE VIEW vw_casos_por_uf AS
SELECT 
    l.uf_residencia,
    COUNT(c.id_case) as total_casos,
    COUNT(CASE WHEN c.evolucao = 2 THEN 1 END) as obitos,
    COUNT(CASE WHEN c.hospitalizado = 1 THEN 1 END) as hospitalizados,
    ROUND(100.0 * COUNT(CASE WHEN c.evolucao = 2 THEN 1 END) / NULLIF(COUNT(c.id_case), 0), 2) as taxa_mortalidade
FROM cases c
JOIN localizacoes l ON c.id_localizacao = l.id_localizacao
GROUP BY l.uf_residencia
ORDER BY total_casos DESC;

-- View: Sintomas mais frequentes
CREATE OR REPLACE VIEW vw_sintomas_frequentes AS
SELECT 
    s.nome,
    COUNT(cs.id_case) as frequencia,
    ROUND(100.0 * COUNT(cs.id_case) / (SELECT COUNT(*) FROM cases), 2) as percentual
FROM casos_sintomas cs
JOIN sintomas s ON cs.id_sintoma = s.id_sintoma
WHERE cs.presente = 1
GROUP BY s.nome
ORDER BY frequencia DESC;

-- View: Fatores de risco mais comuns
CREATE OR REPLACE VIEW vw_fatores_frequentes AS
SELECT 
    f.nome,
    COUNT(cf.id_case) as frequencia,
    ROUND(100.0 * COUNT(cf.id_case) / (SELECT COUNT(*) FROM cases), 2) as percentual
FROM casos_fatores_risco cf
JOIN fatores_risco f ON cf.id_fator = f.id_fator
WHERE cf.presente = 1
GROUP BY f.nome
ORDER BY frequencia DESC;

-- ============================================================================
-- Comentários nas tabelas
-- ============================================================================
COMMENT ON TABLE cases IS 'Tabela central com informações dos casos de dengue';
COMMENT ON TABLE pacientes IS 'Dados demográficos dos pacientes';
COMMENT ON TABLE localizacoes IS 'Informações geográficas de notificação e residência';
COMMENT ON TABLE diagnosticos IS 'Resultados de testes laboratoriais';
COMMENT ON TABLE casos_sintomas IS 'Sintomas apresentados por caso';
COMMENT ON TABLE casos_fatores_risco IS 'Fatores de risco associados a cada caso';
