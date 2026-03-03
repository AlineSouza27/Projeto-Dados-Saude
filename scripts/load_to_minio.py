"""
Módulo para carregar dados no MinIO.

Este módulo gerencia uploads de dados para o MinIO data lake,
com suporte a metadados e estrutura de pastas organizada.
"""

import io
import json
import csv
from datetime import datetime
from typing import Dict, List, Optional
import logging
from minio import Minio
from minio.error import S3Error

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MinIOUploader:
    """Classe para gerenciar uploads no MinIO."""
    
    def __init__(
        self,
        endpoint: str = "localhost:9050",
        access_key: str = "datalake",
        secret_key: str = "datalake",
        bucket_name: str = "datalake",
        secure: bool = False
    ):
        """
        Inicializa a conexão com MinIO.
        
        Args:
            endpoint: Endpoint do MinIO (host:port)
            access_key: Chave de acesso
            secret_key: Chave secreta
            bucket_name: Nome do bucket
            secure: Usar HTTPS
        """
        self.client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure
        )
        self.bucket_name = bucket_name
        
        # Garantir que o bucket existe
        self._ensure_bucket_exists()
    
    def _ensure_bucket_exists(self):
        """Cria o bucket se não existir."""
        try:
            if not self.client.bucket_exists(self.bucket_name):
                self.client.make_bucket(self.bucket_name)
                logger.info(f"Bucket '{self.bucket_name}' criado")
            else:
                logger.info(f"Bucket '{self.bucket_name}' existe")
        except S3Error as e:
            logger.error(f"Erro ao verificar/criar bucket: {e}")
            raise
    
    def upload_json(
        self,
        data: List[Dict],
        file_path: str,
        metadata: Optional[Dict] = None
    ) -> str:
        """
        Faz upload de dados JSON.
        
        Args:
            data: Dados em formato lista de dicionários
            file_path: Caminho no MinIO (ex: raw/dengue/2024/03/dados.json)
            metadata: Metadados adicionais
        
        Returns:
            Caminho completo no MinIO
        """
        try:
            json_data = json.dumps(data, ensure_ascii=False, indent=2).encode('utf-8')
            
            self.client.put_object(
                self.bucket_name,
                file_path,
                io.BytesIO(json_data),
                len(json_data),
                content_type="application/json",
                metadata=metadata
            )
            
            logger.info(f"Upload JSON: s3://{self.bucket_name}/{file_path}")
            return f"s3://{self.bucket_name}/{file_path}"
        except S3Error as e:
            logger.error(f"Erro ao fazer upload JSON: {e}")
            raise
    
    def upload_csv(
        self,
        data: List[Dict],
        file_path: str,
        metadata: Optional[Dict] = None
    ) -> Optional[str]:
        """
        Faz upload de dados CSV.
        
        Args:
            data: Dados em formato lista de dicionários
            file_path: Caminho no MinIO
            metadata: Metadados adicionais
        
        Returns:
            Caminho completo no MinIO ou None se não houver dados
        """
        try:
            if not data:
                logger.warning("Nenhum dado para fazer upload CSV")
                return None
            
            # Criar CSV em memória
            output = io.StringIO()
            all_keys = set()
            for record in data:
                if isinstance(record, dict):
                    all_keys.update(record.keys())
            
            fieldnames = sorted(list(all_keys))
            writer = csv.DictWriter(output, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)
            
            csv_data = output.getvalue().encode('utf-8')
            
            self.client.put_object(
                self.bucket_name,
                file_path,
                io.BytesIO(csv_data),
                len(csv_data),
                content_type="text/csv",
                metadata=metadata
            )
            
            logger.info(f"Upload CSV: s3://{self.bucket_name}/{file_path}")
            return f"s3://{self.bucket_name}/{file_path}"
        except S3Error as e:
            logger.error(f"Erro ao fazer upload CSV: {e}")
            raise
    
    def upload_from_file(
        self,
        file_path: str,
        minio_path: str,
        metadata: Optional[Dict] = None
    ) -> str:
        """
        Faz upload de um arquivo local.
        
        Args:
            file_path: Caminho do arquivo local
            minio_path: Caminho de destino no MinIO
            metadata: Metadados adicionais
        
        Returns:
            Caminho completo no MinIO
        """
        try:
            self.client.fput_object(
                self.bucket_name,
                minio_path,
                file_path,
                metadata=metadata
            )
            
            logger.info(f"Upload arquivo: s3://{self.bucket_name}/{minio_path}")
            return f"s3://{self.bucket_name}/{minio_path}"
        except S3Error as e:
            logger.error(f"Erro ao fazer upload do arquivo: {e}")
            raise
    
    def list_objects(self, prefix: str = "") -> List[str]:
        """
        Lista objetos no bucket.
        
        Args:
            prefix: Prefixo para filtrar objetos
        
        Returns:
            Lista de objetos encontrados
        """
        try:
            objects = []
            for obj in self.client.list_objects(self.bucket_name, prefix=prefix, recursive=True):
                objects.append(obj.object_name)
            return objects
        except S3Error as e:
            logger.error(f"Erro ao listar objetos: {e}")
            raise


def main():
    """Função principal para teste do MinIOUploader."""
    
    logger.info("Teste de conexão com MinIO")
    
    try:
        uploader = MinIOUploader(
            endpoint="localhost:9050",
            access_key="datalake",
            secret_key="datalake",
            bucket_name="datalake",
            secure=False
        )
        
        print("✓ Conexão com MinIO estabelecida com sucesso!")
        
        # Listar objetos existentes
        objects = uploader.list_objects("raw/dengue/")
        print(f"\nObjetos em 'raw/dengue/': {len(objects)}")
        for obj in objects[:5]:  # Mostrar apenas os primeiros 5
            print(f"  - {obj}")
        
        return 0
    except Exception as e:
        logger.error(f"Erro: {e}")
        return 1


if __name__ == "__main__":
    exit(main())
