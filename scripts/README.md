# Scripts ELT - Pipeline de Dengue DATASUS

Pipeline completo de **Extração, Carregamento e Preparação** de dados de dengue da API de Dados Abertos do DATASUS para MinIO.

## 📋 Arquivos

### 1. `extract_datasus_dengue.py` - Extração
**Responsabilidade:** Consumir a API DATASUS e salvar dados localmente

```python
from extract_datasus_dengue import DengueDatasusAPI

# Instanciar API
api = DengueDatasusAPI(output_dir="data/raw")

# Extrair dados com limite de registros
api.fetch_data(limit=100, max_records=1000)

# Salvar formatos
json_path = api.save_to_json()     # Salva JSON
csv_path = api.save_to_csv()       # Salva CSV

# Obter dados
data = api.get_data()              # Retorna lista de dicts
summary = api.get_summary()        # Retorna resumo
```

**Executar diretamente:**
```bash
python extract_datasus_dengue.py
```

---

### 2. `load_to_minio.py` - Carregamento
**Responsabilidade:** Gerenciar uploads para MinIO

```python
from load_to_minio import MinIOUploader

# Conectar ao MinIO
uploader = MinIOUploader(
    endpoint="localhost:9050",
    access_key="datalake",
    secret_key="datalake",
    bucket_name="datalake",
    secure=False
)

# Upload de dados
data = [{"col1": "val1", "col2": "val2"}]

# JSON
json_url = uploader.upload_json(
    data,
    "raw/dengue/2026/03/dados.json",
    metadata={"source": "datasus_api"}
)

# CSV
csv_url = uploader.upload_csv(
    data,
    "raw/dengue/2026/03/dados.csv",
    metadata={"source": "datasus_api"}
)

# Arquivo local
file_url = uploader.upload_from_file(
    "local/path.json",
    "raw/dengue/2026/03/dados.json"
)

# Listar arquivos
objects = uploader.list_objects("raw/dengue/")
```

**Executar diretamente:**
```bash
python load_to_minio.py
```

---

### 3. `elt_pipeline.py` - Orquestração Completa
**Responsabilidade:** Orquestrar extração + carregamento em um único pipeline

```python
from elt_pipeline import DengueELTPipeline

# Criar pipeline
pipeline = DengueELTPipeline(
    local_output_dir="data/raw",
    minio_endpoint="localhost:9050",
    minio_access_key="datalake",
    minio_secret_key="datalake",
    minio_bucket="datalake",
    minio_secure=False
)

# Executar pipeline completo
success = pipeline.run(
    limit=100,              # Registros por página
    max_records=None,       # None = sem limite
    formats=["json", "csv"] # Salvar ambos os formatos
)
```

**Executar diretamente:**
```bash
python elt_pipeline.py
```

---

## 🚀 Modo de Uso

### Opção 1: Pipeline Completo (Recomendado)
```bash
cd scripts/
python elt_pipeline.py
```

**Fluxo:**
1. Extrai dados da API DATASUS
2. Salva localmente em `data/raw/`
3. Carrega no MinIO em `raw/dengue/YYYY/MM/`

**Saída esperada:**
```
╔════════════════════════════════════════════════════════╗
║  PIPELINE ELT - DADOS DE DENGUE DATASUS               ║
╚════════════════════════════════════════════════════════╝

✓ Status: SUCCESS
  Registros extraídos: 20

📁 Arquivos locais:
  JSON: data\raw\dengue_20260303_093100.json
  CSV: data\raw\dengue_20260303_093101.csv

☁️  Arquivos no MinIO:
  JSON: s3://datalake/raw/dengue/2026/03/dengue_20260303_093100.json
  CSV: s3://datalake/raw/dengue/2026/03/dengue_20260303_093100.csv
```

---

### Opção 2: Extração Isolada
Se você só quer extrair dados:

```bash
python extract_datasus_dengue.py
```

Salva em: `data/raw/dengue_TIMESTAMP.{json,csv}`

---

### Opção 3: Carregamento Isolado
Se você tem arquivos locais e quer carregar no MinIO:

```python
from load_to_minio import MinIOUploader

uploader = MinIOUploader()
uploader.upload_from_file(
    "data/raw/dengue_20260303_093100.json",
    "raw/dengue/2026/03/dengue_20260303_093100.json"
)
```

---

## 🔧 Configuração

### MinIO Local
Está rodando em `localhost:9050` conforme `docker-compose.yml`:
- **Access Key:** `datalake`
- **Secret Key:** `datalake`
- **Bucket:** `datalake`

### Estrutura no MinIO
```
datalake/
  └─ raw/
    └─ dengue/
      └─ 2026/
        └─ 03/
          ├─ dengue_20260303_093100.json
          └─ dengue_20260303_093100.csv
```

---

## 📊 API DATASUS

**URL Base:** https://apidadosabertos.saude.gov.br/arboviroses/dengue

**Parâmetros:**
- `limit=100` - Registros por página
- `offset=0` - Deslocamento

**Resposta:**
```json
{
  "parametros": [
    {
      "tp_not": "2",
      "id_agravo": "A90",
      "dt_notific": "2023-12-14",
      ...
    }
  ]
}
```

---

## 🔍 Dependências

Certifique-se de ter instalado:
```bash
pip install requests minio
```

---

## 💡 Dicas

1. **Para grandes volumes:** Ajuste `limit` e `max_records` no pipeline
2. **Sem MinIO:** Pipeline continua funcionando apenas com salvamento local
3. **Logs:** Todos os arquivos registram tudo em console (nível INFO)
4. **Metadados:** Cada arquivo no MinIO carrega `source`, `extracted_at`, `records`

---

## 📝 Próximas Etapas

### Phase 3: Transform (Normalização)

**Arquivos criados:**

1. [database/SCHEMA_NORMALIZACAO.md] - Proposta de divisão em 10 tabelas normalizadas
2. [database/01_create_schema.sql] - Script SQL para criar schema
3. [scripts/normalize_and_load.py] - Script que normaliza CSV e popula tabelas

**Passo 1: Criar schema PostgreSQL**
```bash
# Conectar ao banco postgres e executar o SQL
psql -h localhost -p 5442 -U admin -d admin -f database/01_create_schema.sql
```

**Passo 2: Normalizar e carregar dados**
```bash
cd scripts/
python normalize_and_load.py
# ou especificar arquivo
python normalize_and_load.py --file ../data/raw/dengue_TIMESTAMP.csv
```

**Resultado:**
- ✅ `pacientes` - Dados demográficos
- ✅ `localizacoes` - Informações geográficas (notificação + residência)
- ✅ `diagnosticos` - Resultados laboratoriais (sorologia, PCR, NS1, etc)
- ✅ `cases` - Tabela central com casos
- ✅ `casos_sintomas` - Sintomas por caso
- ✅ `casos_fatores_risco` - Fatores de risco por caso
- ✅ `manifestacoes_hemor` - Manifestações hemorrágicas
- ✅ `alertas_graves` - Sinais de alerta grave
- ✅ `sintomas` (dimensão)
- ✅ `fatores_risco` (dimensão)
- ✅ Views para análise agregada

**Pipeline completo:**
```bash
# 1. Extrair e carregar no MinIO
python elt_pipeline.py

# 2. Criar schema PostgreSQL
psql -h localhost -p 5442 -U admin -d admin -f database/01_create_schema.sql

# 3. Normalizar e popular tabelas
python normalize_and_load.py
```

Quer que eu crie análises SQL ou dashboards com os dados normalizados?
