# Proposta de Normalização - Dengue DATASUS

## 📊 Análise estrutural dos dados

Após análise do JSON extraído, identifico aproximadamente **130 campos** organizáveis em **8 tabelas** relacionadas por chaves estrangeiras.

---

## 🗂️ Modelo de Dados Proposto

### 1. **cases** (Casos de Dengue)
**Tabela central com informações gerais do caso**

```
id_case (PK)           → Identificador único do caso
id_notificacao         → ID único DATASUS
id_paciente (FK)       → Referência para tabela pacientes
id_localizacao (FK)    → Referência para tabela localizacoes
id_diagnostico (FK)    → Referência para diagnósticos
dt_notificacao         → Data da notificação
dt_sin_pri             → Data do primeiro sintoma
dt_encerra             → Data de encerramento
sem_notif              → Semana epidemiológica
ano                    → Ano da notificação
tp_notif               → Tipo de notificação (2=Notificação)
id_agravo              → Código do agravo (A90)
criterio               → Critério diagnóstico
classificacao_final    → Classificação final
evolucao               → Evolução (1=cura, 2=óbito)
dt_obito               → Data do óbito (se aplicável)
hospitalizado          → Sim/Não
data_internacao        → Data de internação
```

**Relacionamentos:**
- 1→N com `casos_sintomas`
- 1→N com `casos_testes`
- 1→N com `casos_fatores_risco`
- 1→N com `casos_manifestacoes_hemor`

---

### 2. **pacientes**
**Informações demográficas do paciente**

```
id_paciente (PK)      → ID único
idade                 → Idade em anos
sexo                  → F/M
raca                  → Categoria racial
escolaridade          → Nível educacional
ocupacao              → ID ocupação
gestante              → Código gestação (1=sim, 5=não)
ano_nascimento        → Ano de nascimento
```

---

### 3. **localizacoes**
**Dados geográficos de notificação e residência**

```
id_localizacao (PK)   → ID único
uf_notificacao        → UF onde foi notificado
municipio_notif       → Código município notificação
regiao_notif          → Região notificação
unidade_saude         → ID unidade de saúde notificadora
---
uf_residencia         → UF de residência
municipio_resi        → Código município residência
regiao_resi           → Região residência
pais                  → País (1=Brasil)
```

---

### 4. **diagnosticos**
**Resultados de testes laboratoriais**

```
id_diagnostico (PK)   → ID único
id_case (FK)          → Caso relacionado
```

**Testes sorológicos:**
```
data_soro_1           → Data coleta sorologia 1
resultado_soro_1      → Resultado sorologia 1
data_soro_2           → Data coleta sorologia 2
resultado_soro_2      → Resultado sorologia 2
igm_s1                → IgM sorologia 1
igg_s1                → IgG sorologia 1
igm_s2                → IgM sorologia 2
igg_s2                → IgG sorologia 2
```

**RT-PCR:**
```
data_pcr              → Data coleta PCR
resultado_pcr         → Resultado PCR
amostra_pcr           → Tipo amostra PCR
```

**NS1:**
```
data_ns1              → Data coleta NS1
resultado_ns1         → Resultado NS1
```

**Outros:**
```
data_prnt             → Data PRNT
resultado_prnt        → Resultado PRNT
data_viral            → Data isolamento viral
resultado_viral       → Resultado isolamento
sorotipo              → Sorotipo identificado (1-4)
```

---

### 5. **sintomas**
**Tabela dimensão de sintomas**

```
id_sintoma (PK)       → ID do sintoma
nome                  → Nome do sintoma
descricao             → Descrição
```

**Sintomas catalogados:**
- Febre
- Mialgia
- Cefaleia
- Exantema
- Vômito
- Náusea
- Dor nas costas
- Conjuntivite
- Artrite
- Artralgia
- Dor retro-orbitária

---

### 6. **casos_sintomas** (Associativa)
**Vincula casos aos sintomas apresentados**

```
id_caso_sintoma (PK)  → ID único
id_case (FK)          → Caso
id_sintoma (FK)       → Sintoma
presente               → Bool (1=sim, 0=não)
data_inicio           → Data de início
```

---

### 7. **fatores_risco**
**Tabela dimensão com fatores de risco**

```
id_fator (PK)         → ID do fator
nome                  → Nome do fator
descricao             → Descrição
```

**Fatores catalogados:**
- Diabetes mellitus
- Hematológico
- Hepatopatia
- Renal
- Hipertensão
- Ácido péptico
- Auto-imune

---

### 8. **casos_fatores_risco** (Associativa)
**Vincula casos aos fatores de risco**

```
id_caso_fator (PK)    → ID único
id_case (FK)          → Caso
id_fator (FK)         → Fator de risco
presente               → Bool (1=sim, 0=não)
```

---

### 9. **manifestacoes_hemor**
**Manifestações hemorrágicas por caso**

```
id_manifestacao (PK)  → ID único
id_case (FK)          → Caso
petequias             → Sim/Não
epistaxe              → Sim/Não
gengivorragia         → Sim/Não
metrorragia           → Sim/Não
hematúria             → Sim/Não
sangramento_gi        → Sim/Não
data_manifestacao     → Data da manifestação
```

---

### 10. **alertas_graves** (Opcional)
**Sinais de alerta e gravidade**

```
id_alerta (PK)        → ID único
id_case (FK)          → Caso
hipotensao            → Presente?
plaquetas_criticas    → Contagem plaquetas < limite
vomitos_persistentes   → Sim/Não
sangramento            → Sim/Não
hematocrito_alt       → Sim/Não
abdominal_dolor       → Sim/Não
letargia              → Sim/Não
hepatomegalia         → Sim/Não
```

---

## 🔑 Chaves e Relacionamentos

```
cases
├─── FK: id_paciente → pacientes.id_paciente
├─── FK: id_localizacao → localizacoes.id_localizacao
├─── FK: id_diagnostico → diagnosticos.id_diagnostico
├─── 1:N → casos_sintomas.id_case
├─── 1:N → casos_fatores_risco.id_case
├─── 1:N → manifestacoes_hemor.id_case
└─── 1:N → alertas_graves.id_case

casos_sintomas
├─── FK: id_case → cases.id_case
└─── FK: id_sintoma → sintomas.id_sintoma

casos_fatores_risco
├─── FK: id_case → cases.id_case
└─── FK: id_fator → fatores_risco.id_fator
```

---

## 📈 Vantagens desta normalização

| Aspecto | Benefício |
|---------|-----------|
| **Integridade** | Dados sem redundância |
| **Performance** | Queries otimizadas por contexto |
| **Manutenção** | Fácil atualizar informações |
| **Analytics** | Joins específicos para análises |
| **Escalabilidade** | Pronto para milhões de registros |

---

## 💾 Próximos passos

1. Você quer que eu crie o **script SQL** para criar essas tabelas?
2. Quer um script **Python/Pandas** para normalizar o CSV e popular essas tabelas?
3. Quer adicionar/remover alguma tabela?
4. Quer criar **views** para relatórios agregados?
