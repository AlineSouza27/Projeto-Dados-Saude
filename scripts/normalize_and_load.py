"""
Script de normalização e carregamento de dados no PostgreSQL normalizado.

Lê o CSV bruto do dengue e popula as tabelas normalizadas do banco.

Uso:
    python normalize_and_load.py --file path/to/file.csv
    python normalize_and_load.py  # usa o CSV mais recente
"""

import argparse
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import os

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURAÇÕES POSTGRESQL
# ============================================================================
PG_USER = os.environ.get("PG_USER", "admin")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "admin")
PG_HOST = os.environ.get("PG_HOST", "localhost")
PG_PORT = os.environ.get("PG_PORT", "5442")
PG_DB = os.environ.get("PG_DB", "postgres")

DB_URL = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"


def get_engine():
    """Cria engine SQLAlchemy para PostgreSQL."""
    return create_engine(DB_URL, echo=False)


def clean_value(val):
    """Converte valores inválidos para None."""
    if pd.isna(val):
        return None
    if isinstance(val, str):
        if val.lower() in ('nan', 'none', ''):
            return None
    return val


def find_latest_csv(directory: Path) -> Optional[Path]:
    """Retorna o CSV mais recente."""
    csv_files = sorted(
        directory.glob("*.csv"),
        key=lambda p: p.stat().st_mtime,
        reverse=True
    )
    return csv_files[0] if csv_files else None


# ============================================================================
# MAPEAMENTO DE CAMPOS
# ============================================================================

FIELD_MAPPING = {
    # PACIENTE
    'nu_idade': 'age',
    'nu_idade_n': 'age_code',
    'cs_sexo': 'sex',
    'cs_raca': 'race',
    'cs_escol_n': 'education',
    'cs_escolar': 'education',
    'ocupacao': 'occupation',
    'id_ocupa_n': 'occupation_code',
    'cs_gestant': 'pregnant_code',
    'ano_nasc': 'birth_year',
    
    # LOCALIZAÇÃO
    'sg_uf_not': 'uf_notification',
    'id_municip': 'municipality_notif',
    'id_regiona': 'region_notif',
    'id_unidade': 'health_unit',
    'sg_uf': 'uf_residencia',
    'id_mn_resi': 'municipality_resi',
    'id_rg_resi': 'region_resi',
    'id_pais': 'country',
    
    # DATAS
    'dt_notific': 'date_notification',
    'dt_sin_pri': 'date_first_symptom',
    'dt_digita': 'date_typed',
    'dt_invest': 'date_investigation',
    'dt_encerra': 'date_close',
    'dt_obito': 'date_death',
    'dt_interna': 'date_internation',
    
    # NOTIFICAÇÃO
    'sem_not': 'week_notification',
    'sem_pri': 'week_first_symptom',
    'nu_ano': 'year',
    'tp_not': 'type_notification',
    'id_agravo': 'id_disease',
    
    # DIAGNÓSTICO
    'dt_soro': 'date_serology',
    'resul_soro': 'result_serology',
    'dt_viral': 'date_viral',
    'resul_vi_n': 'result_viral',
    'dt_pcr': 'date_pcr',
    'resul_pcr_': 'result_pcr',
    'sorotipo': 'serotype',
    'dt_ns1': 'date_ns1',
    'resul_ns1': 'result_ns1',
    'dt_prnt': 'date_prnt',
    'resul_prnt': 'result_prnt',
    
    # CLASSIFICAÇÃO
    'classi_fin': 'classification_final',
    'criterio': 'criterion',
    'evolucao': 'evolution',
    'hospitaliz': 'hospitalized',
}

# Sintomas
SYMPTOM_FIELDS = {
    'febre': 'Febre',
    'mialgia': 'Mialgia',
    'cefaleia': 'Cefaleia',
    'exantema': 'Exantema',
    'vomito': 'Vômito',
    'nausea': 'Náusea',
    'dor_costas': 'Dor nas costas',
    'conjuntvit': 'Conjuntivite',
    'artrite': 'Artrite',
    'artralgia': 'Artralgia',
    'dor_retro': 'Dor retro-orbitária',
}

# Fatores de risco
RISK_FACTOR_FIELDS = {
    'diabetes': 'Diabetes mellitus',
    'hematolog': 'Hematológico',
    'hepatopat': 'Hepatopatia',
    'renal': 'Renal',
    'hipertensa': 'Hipertensão',
    'acido_pept': 'Ácido péptico',
    'auto_imune': 'Auto-imune',
}

# Manifestações hemorrágicas
HEMORRHAGE_FIELDS = {
    'mani_hemor': 'any_hemorrhage',
    'epistaxe': 'epistaxis',
    'gengivo': 'gingival_bleeding',
    'metro': 'metrorragia',
    'hematura': 'hematuria',
    'sangram': 'gi_bleeding',
}


class DengueNormalizer:
    """Classe para normalizar e carregar dados de dengue."""
    
    def __init__(self):
        self.engine = get_engine()
        self.df = None
        
    def load_csv(self, filepath: Path):
        """Carrega CSV original."""
        logger.info(f"Lendo CSV: {filepath}")
        self.df = pd.read_csv(filepath, dtype=str)
        logger.info(f"Total de linhas: {len(self.df)}")
        
    def normalize_and_insert(self):
        """Normaliza dados e insere nas tabelas."""
        if self.df is None:
            raise ValueError("Nenhum CSV carregado")
        
        session_maker = sessionmaker(bind=self.engine)
        
        logger.info("Iniciando inserção normalizada...")
        
        # Resetar sequências
        with self.engine.connect() as conn:
            conn.execute(text("DELETE FROM cases CASCADE;"))
            conn.execute(text("DELETE FROM pacientes CASCADE;"))
            conn.execute(text("DELETE FROM localizacoes CASCADE;"))
            conn.execute(text("DELETE FROM diagnosticos CASCADE;"))
            conn.commit()
        
        # Processar cada linha
        for idx, row in self.df.iterrows():
            try:
                # 1. Inserir paciente
                paciente_id = self._insert_paciente(row)
                
                # 2. Inserir localização
                localizacao_id = self._insert_localizacao(row)
                
                # 3. Inserir diagnóstico
                diagnostico_id = self._insert_diagnostico(row)
                
                # 4. Inserir case
                case_id = self._insert_case(row, paciente_id, localizacao_id, diagnostico_id)
                
                # 5. Inserir sintomas
                self._insert_sintomas(row, case_id)
                
                # 6. Inserir fatores de risco
                self._insert_fatores_risco(row, case_id)
                
                # 7. Inserir manifestações hemorrágicas
                self._insert_manifestacoes_hemor(row, case_id)
                
                if (idx + 1) % 10 == 0:
                    logger.info(f"Processadas {idx + 1} linhas")
                    
            except Exception as e:
                logger.error(f"Erro na linha {idx}: {e}")
                continue
        
        logger.info("✓ Carregamento concluído com sucesso")
    
    def _insert_paciente(self, row) -> int:
        """Insere paciente e retorna ID."""
        query = """
            INSERT INTO pacientes (idade, sexo, raca, escolaridade, ocupacao, gestante, ano_nascimento)
            VALUES (:idade, :sexo, :raca, :escolaridade, :ocupacao, :gestante, :ano_nascimento)
            RETURNING id_paciente;
        """
        
        with self.engine.connect() as conn:
            result = conn.execute(text(query), {
                'idade': self._clean_int(row.get('nu_idade_n')),
                'sexo': clean_value(row.get('cs_sexo')),
                'raca': clean_value(row.get('cs_raca')),
                'escolaridade': clean_value(row.get('cs_escol_n')),
                'ocupacao': self._clean_int(row.get('id_ocupa_n')),
                'gestante': self._clean_int(row.get('cs_gestant')),
                'ano_nascimento': self._clean_int(row.get('ano_nasc')),
            })
            conn.commit()
            return result.scalar()
    
    def _insert_localizacao(self, row) -> int:
        """Insere localização."""
        query = """
            INSERT INTO localizacoes (
                uf_notificacao, municipio_notif, regiao_notif, unidade_saude,
                uf_residencia, municipio_resi, regiao_resi, pais
            ) VALUES (:uf_notif, :mun_notif, :reg_notif, :unit, :uf_resi, :mun_resi, :reg_resi, :pais)
            RETURNING id_localizacao;
        """
        
        with self.engine.connect() as conn:
            result = conn.execute(text(query), {
                'uf_notif': clean_value(row.get('sg_uf_not')),
                'mun_notif': self._clean_int(row.get('id_municip')),
                'reg_notif': self._clean_int(row.get('id_regiona')),
                'unit': self._clean_int(row.get('id_unidade')),
                'uf_resi': clean_value(row.get('sg_uf')),
                'mun_resi': self._clean_int(row.get('id_mn_resi')),
                'reg_resi': self._clean_int(row.get('id_rg_resi')),
                'pais': self._clean_int(row.get('id_pais')) or 1,
            })
            conn.commit()
            return result.scalar()
    
    def _insert_diagnostico(self, row) -> int:
        """Insere diagnóstico."""
        query = """
            INSERT INTO diagnosticos (
                data_soro_1, resultado_soro_1,
                data_pcr, resultado_pcr,
                data_ns1, resultado_ns1,
                data_viral, resultado_viral,
                sorotipo
            ) VALUES (:dt_soro, :res_soro, :dt_pcr, :res_pcr, :dt_ns1, :res_ns1, :dt_viral, :res_viral, :ser)
            RETURNING id_diagnostico;
        """
        
        with self.engine.connect() as conn:
            result = conn.execute(text(query), {
                'dt_soro': self._clean_date(row.get('dt_soro')),
                'res_soro': self._clean_int(row.get('resul_soro')),
                'dt_pcr': self._clean_date(row.get('dt_pcr')),
                'res_pcr': self._clean_int(row.get('resul_pcr_')),
                'dt_ns1': self._clean_date(row.get('dt_ns1')),
                'res_ns1': self._clean_int(row.get('resul_ns1')),
                'dt_viral': self._clean_date(row.get('dt_viral')),
                'res_viral': self._clean_int(row.get('resul_vi_n')),
                'ser': self._clean_int(row.get('sorotipo')),
            })
            conn.commit()
            return result.scalar()
    
    def _insert_case(self, row, pac_id: int, loc_id: int, diag_id: int) -> int:
        """Insere case principal."""
        query = """
            INSERT INTO cases (
                id_paciente, id_localizacao, id_diagnostico,
                dt_notificacao, dt_sin_pri, dt_encerra, dt_digita, dt_investiga,
                sem_notif, ano, tp_notif, id_agravo,
                criterio, classificacao_final, evolucao, dt_obito,
                hospitalizado, data_internacao
            ) VALUES (
                :pac, :loc, :diag,
                :dt_not, :dt_sem, :dt_enc, :dt_dig, :dt_inv,
                :sem, :ano, :tp, :agr,
                :crit, :class, :evol, :dt_ob,
                :hosp, :dt_hos
            )
            RETURNING id_case;
        """
        
        with self.engine.connect() as conn:
            result = conn.execute(text(query), {
                'pac': pac_id,
                'loc': loc_id,
                'diag': diag_id,
                'dt_not': self._clean_date(row.get('dt_notific')),
                'dt_sem': self._clean_date(row.get('dt_sin_pri')),
                'dt_enc': self._clean_date(row.get('dt_encerra')),
                'dt_dig': self._clean_date(row.get('dt_digita')),
                'dt_inv': self._clean_date(row.get('dt_invest')),
                'sem': self._clean_int(row.get('sem_not')),
                'ano': self._clean_int(row.get('nu_ano')),
                'tp': self._clean_int(row.get('tp_not')),
                'agr': clean_value(row.get('id_agravo')),
                'crit': self._clean_int(row.get('criterio')),
                'class': self._clean_int(row.get('classi_fin')),
                'evol': self._clean_int(row.get('evolucao')),
                'dt_ob': self._clean_date(row.get('dt_obito')),
                'hosp': self._clean_int(row.get('hospitaliz')),
                'dt_hos': self._clean_date(row.get('dt_interna')),
            })
            conn.commit()
            return result.scalar()
    
    def _insert_sintomas(self, row, case_id: int):
        """Insere sintomas associados ao caso."""
        for csv_field, symptom_name in SYMPTOM_FIELDS.items():
            value = row.get(csv_field)
            if value and str(value).strip().lower() not in ('nan', 'none'):
                try:
                    present = int(float(value)) if pd.notna(value) else None
                    if present in (0, 1):
                        query = """
                            INSERT INTO casos_sintomas (id_case, id_sintoma, presente)
                            SELECT :case, id_sintoma, :pres
                            FROM sintomas WHERE nome = :nome
                            ON CONFLICT DO NOTHING;
                        """
                        with self.engine.connect() as conn:
                            conn.execute(text(query), {
                                'case': case_id,
                                'pres': present,
                                'nome': symptom_name
                            })
                            conn.commit()
                except (ValueError, TypeError):
                    pass
    
    def _insert_fatores_risco(self, row, case_id: int):
        """Insere fatores de risco."""
        for csv_field, factor_name in RISK_FACTOR_FIELDS.items():
            value = row.get(csv_field)
            if value and str(value).strip().lower() not in ('nan', 'none'):
                try:
                    present = int(float(value)) if pd.notna(value) else None
                    if present in (0, 1):
                        query = """
                            INSERT INTO casos_fatores_risco (id_case, id_fator, presente)
                            SELECT :case, id_fator, :pres
                            FROM fatores_risco WHERE nome = :nome
                            ON CONFLICT DO NOTHING;
                        """
                        with self.engine.connect() as conn:
                            conn.execute(text(query), {
                                'case': case_id,
                                'pres': present,
                                'nome': factor_name
                            })
                            conn.commit()
                except (ValueError, TypeError):
                    pass
    
    def _insert_manifestacoes_hemor(self, row, case_id: int):
        """Insere manifestações hemorrágicas."""
        query = """
            INSERT INTO manifestacoes_hemor (
                id_case, petequias, epistaxe, gengivorragia, metrorragia, hematuria, sangramento_gi
            ) VALUES (:case, :pet, :epis, :geng, :metro, :hema, :sang);
        """
        
        with self.engine.connect() as conn:
            conn.execute(text(query), {
                'case': case_id,
                'pet': self._clean_int(row.get('petequia_n')),
                'epis': self._clean_int(row.get('epistaxe')),
                'geng': self._clean_int(row.get('gengivo')),
                'metro': self._clean_int(row.get('metro')),
                'hema': self._clean_int(row.get('hematura')),
                'sang': self._clean_int(row.get('sangram')),
            })
            conn.commit()
    
    @staticmethod
    def _clean_int(val) -> Optional[int]:
        """Converte para int ou None."""
        val = clean_value(val)
        if val is None:
            return None
        try:
            return int(float(val))
        except (ValueError, TypeError):
            return None
    
    @staticmethod
    def _clean_date(val) -> Optional[str]:
        """Converte para data ou None."""
        val = clean_value(val)
        if val is None:
            return None
        try:
            # Tentar parsear como data
            return pd.to_datetime(val).strftime('%Y-%m-%d')
        except:
            return None


def main():
    parser = argparse.ArgumentParser(description="Normaliza e carrega dados de dengue")
    parser.add_argument("--file", type=Path, help="Caminho do CSV (opcional)")
    args = parser.parse_args()
    
    if args.file:
        csv_path = args.file
    else:
        csv_path = find_latest_csv(Path("data/raw"))
        if not csv_path:
            csv_path = find_latest_csv(Path("../data/raw"))
        
        if not csv_path:
            logger.error("Nenhum CSV encontrado em data/raw/")
            return 1
    
    try:
        normalizer = DengueNormalizer()
        normalizer.load_csv(csv_path)
        normalizer.normalize_and_insert()
        
        print(f"\n✓ Normalização completa!")
        print(f"  Arquivo processado: {csv_path}")
        
        return 0
    except Exception as e:
        logger.error(f"Erro: {e}")
        return 1


if __name__ == "__main__":
    exit(main())
