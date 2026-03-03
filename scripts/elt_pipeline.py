"""
Pipeline ELT (Extract, Load, Transform) para dados de dengue do DATASUS.

Este módulo orquestra a extração de dados da API do DATASUS,
carregamento no MinIO e preparação para transformação.
"""

import json
import logging
from datetime import datetime
from typing import Dict, Optional, Tuple

from extract_datasus_dengue import DengueDatasusAPI
from load_to_minio import MinIOUploader

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DengueELTPipeline:
    """Classe para orquestrar o pipeline ELT de dados de dengue."""
    
    def __init__(
        self,
        local_output_dir: Optional[str] = None,
        minio_endpoint: str = "localhost:9050",
        minio_access_key: str = "datalake",
        minio_secret_key: str = "datalake",
        minio_bucket: str = "datalake",
        minio_secure: bool = False
    ):
        """
        Inicializa o pipeline.
        
        Args:
            local_output_dir: Diretório local para salvar dados
                              (se None usa ../data/raw)
            minio_endpoint: Endpoint do MinIO
            minio_access_key: Chave de acesso MinIO
            minio_secret_key: Chave secreta MinIO
            minio_bucket: Nome do bucket MinIO
            minio_secure: Usar HTTPS no MinIO
        """
        if local_output_dir:
            self.local_output_dir = local_output_dir
        else:
            self.local_output_dir = Path(__file__).parent.parent / "data" / "raw"
        
        self.api = DengueDatasusAPI(output_dir=str(self.local_output_dir))
        
        # Inicializar MinIO (opcional)
        self.minio = None
        self.use_minio = True
        
        try:
            self.minio = MinIOUploader(
                endpoint=minio_endpoint,
                access_key=minio_access_key,
                secret_key=minio_secret_key,
                bucket_name=minio_bucket,
                secure=minio_secure
            )
        except Exception as e:
            logger.warning(f"MinIO não disponível: {e}")
            logger.warning("Pipeline continuará apenas com salvamento local")
            self.use_minio = False
        
        self.execution_summary = {
            "start_time": None,
            "end_time": None,
            "status": "pending",
            "extracted_records": 0,
            "local_files": {},
            "minio_files": {}
        }
    
    def extract(self, limit: int = 100, max_records: Optional[int] = None) -> bool:
        """
        Extrai dados da API.
        
        Args:
            limit: Registros por requisição
            max_records: Limite máximo de registros
        
        Returns:
            True se bem-sucedido
        """
        logger.info("═" * 60)
        logger.info("ETAPA 1: EXTRAÇÃO")
        logger.info("═" * 60)
        
        try:
            success = self.api.fetch_data(limit=limit, max_records=max_records)
            
            if success:
                self.execution_summary["extracted_records"] = len(self.api.all_data)
                logger.info(f"✓ Extração concluída: {len(self.api.all_data)} registros")
                return True
            else:
                logger.error("✗ Falha na extração")
                return False
        except Exception as e:
            logger.error(f"✗ Erro na extração: {e}")
            return False
    
    def load(self, formats: list = ["json", "csv"]) -> bool:
        """
        Carrega dados localmente e no MinIO.
        
        Args:
            formats: Formatos a salvar (json, csv)
        
        Returns:
            True se bem-sucedido
        """
        logger.info("\n" + "═" * 60)
        logger.info("ETAPA 2: CARREGAMENTO")
        logger.info("═" * 60)
        
        try:
            now = datetime.now()
            data = self.api.get_data()
            
            if not data:
                logger.warning("Nenhum dado para carregar")
                return False
            
            # Salvar JSON
            if "json" in formats:
                logger.info("Salvando JSON...")
                json_path = self.api.save_to_json()
                self.execution_summary["local_files"]["json"] = str(json_path)
                
                if self.use_minio and self.minio:
                    try:
                        minio_json_path = f"raw/dengue/{now.year}/{now.month:02d}/dengue_{now.strftime('%Y%m%d_%H%M%S')}.json"
                        minio_url = self.minio.upload_json(
                            data,
                            minio_json_path,
                            metadata={
                                "source": "datasus_api",
                                "extracted_at": now.isoformat(),
                                "records": str(len(data))
                            }
                        )
                        self.execution_summary["minio_files"]["json"] = minio_url
                        logger.info(f"✓ JSON carregado: {minio_url}")
                    except Exception as e:
                        logger.warning(f"Erro ao carregar JSON no MinIO: {e}")
                else:
                    logger.info(f"✓ JSON salvo localmente: {json_path}")
            
            # Salvar CSV
            if "csv" in formats:
                logger.info("Salvando CSV...")
                csv_path = self.api.save_to_csv()
                if csv_path:
                    self.execution_summary["local_files"]["csv"] = str(csv_path)
                    
                    if self.use_minio and self.minio:
                        try:
                            minio_csv_path = f"raw/dengue/{now.year}/{now.month:02d}/dengue_{now.strftime('%Y%m%d_%H%M%S')}.csv"
                            minio_url = self.minio.upload_csv(
                                data,
                                minio_csv_path,
                                metadata={
                                    "source": "datasus_api",
                                    "extracted_at": now.isoformat(),
                                    "records": str(len(data))
                                }
                            )
                            self.execution_summary["minio_files"]["csv"] = minio_url
                            logger.info(f"✓ CSV carregado: {minio_url}")
                        except Exception as e:
                            logger.warning(f"Erro ao carregar CSV no MinIO: {e}")
                    else:
                        logger.info(f"✓ CSV salvo localmente: {csv_path}")
            
            logger.info("✓ Carregamento concluído")
            return True
        
        except Exception as e:
            logger.error(f"✗ Erro no carregamento: {e}")
            return False
    
    def run(
        self,
        limit: int = 100,
        max_records: Optional[int] = None,
        formats: list = ["json", "csv"]
    ) -> bool:
        """
        Executa o pipeline completo.
        
        Args:
            limit: Registros por requisição
            max_records: Limite máximo de registros
            formats: Formatos a salvar
        
        Returns:
            True se bem-sucedido
        """
        self.execution_summary["start_time"] = datetime.now().isoformat()
        
        logger.info("\n")
        logger.info("╔" + "═" * 58 + "╗")
        logger.info("║" + " " * 58 + "║")
        logger.info("║" + "  PIPELINE ELT - DADOS DE DENGUE DATASUS".center(58) + "║")
        logger.info("║" + " " * 58 + "║")
        logger.info("╚" + "═" * 58 + "╝")
        
        # Executar extração
        if not self.extract(limit=limit, max_records=max_records):
            self.execution_summary["status"] = "failed"
            self.execution_summary["end_time"] = datetime.now().isoformat()
            return False
        
        # Executar carregamento
        if not self.load(formats=formats):
            self.execution_summary["status"] = "failed"
            self.execution_summary["end_time"] = datetime.now().isoformat()
            return False
        
        self.execution_summary["status"] = "success"
        self.execution_summary["end_time"] = datetime.now().isoformat()
        
        # Exibir resumo
        self._print_summary()
        return True
    
    def _print_summary(self):
        """Exibe um resumo da execução."""
        logger.info("\n" + "═" * 60)
        logger.info("RESUMO DA EXECUÇÃO")
        logger.info("═" * 60)
        
        summary = self.execution_summary
        
        print(f"\n✓ Status: {summary['status'].upper()}")
        print(f"  Registros extraídos: {summary['extracted_records']}")
        
        if summary['local_files']:
            print(f"\n📁 Arquivos locais:")
            for fmt, path in summary['local_files'].items():
                print(f"  {fmt.upper()}: {path}")
        
        if summary['minio_files']:
            print(f"\n☁️  Arquivos no MinIO:")
            for fmt, url in summary['minio_files'].items():
                print(f"  {fmt.upper()}: {url}")
        
        print("\n" + "═" * 60 + "\n")


def main():
    """Função principal para executar o pipeline."""
    
    # Criar e executar pipeline
    pipeline = DengueELTPipeline(
        local_output_dir="data/raw",
        minio_endpoint="localhost:9050",
        minio_access_key="datalake",
        minio_secret_key="datalake",
        minio_bucket="datalake",
        minio_secure=False
    )
    
    # Executar pipeline completo
    success = pipeline.run(limit=100, max_records=None, formats=["json", "csv"])
    
    return 0 if success else 1


if __name__ == "__main__":
    exit(main())
