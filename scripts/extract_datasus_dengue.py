"""
Módulo para extração de dados de dengue da API de Dados Abertos do DATASUS.

Este módulo consome a API de arboviroses do DATASUS e permite salvar
os dados localmente em diferentes formatos.
"""

import requests
import json
import csv
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
import logging

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DengueDatasusAPI:
    """Classe para consumir a API de dados de dengue do DATASUS."""
    
    BASE_URL = "https://apidadosabertos.saude.gov.br/arboviroses/dengue"
    TIMEOUT = 30
    DEFAULT_LIMIT = 100
    
    def __init__(self, output_dir: Optional[str] = None):
        """
        Inicializa a classe.
        
        Args:
            output_dir: Diretório para salvar os dados extraídos localmente.
                        Se None, usa `../data/raw` baseado no arquivo atual.
        """
        if output_dir:
            self.output_dir = Path(output_dir)
        else:
            # path relative ao diretório do módulo (project root)
            self.output_dir = Path(__file__).parent.parent / "data" / "raw"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.session = requests.Session()
        self.all_data = []
    
    def fetch_data(
        self,
        limit: int = DEFAULT_LIMIT,
        offset: int = 0,
        max_records: Optional[int] = None
    ) -> bool:
        """
        Extrai dados da API com paginação automática.
        
        Args:
            limit: Número de registros por requisição
            offset: Deslocamento inicial
            max_records: Número máximo de registros a extrair (None = todos)
        
        Returns:
            True se bem-sucedido, False caso contrário
        """
        total_records = 0
        current_offset = offset
        
        while True:
            try:
                params = {
                    "limit": limit,
                    "offset": current_offset
                }
                
                logger.info(f"Requisitando dados: offset={current_offset}, limit={limit}")
                
                response = self.session.get(
                    self.BASE_URL,
                    params=params,
                    timeout=self.TIMEOUT
                )
                response.raise_for_status()
                
                data = response.json()
                
                # Extrair registros
                if isinstance(data, dict) and "parametros" in data:
                    records = data.get("parametros", [])
                elif isinstance(data, dict) and "results" in data:
                    records = data.get("results", [])
                elif isinstance(data, list):
                    records = data
                else:
                    logger.warning(f"Formato inesperado na resposta: {type(data)}")
                    logger.debug(f"Conteúdo: {data}")
                    break
                
                if not records:
                    logger.info("Nenhum registro adicional encontrado")
                    break
                
                self.all_data.extend(records)
                total_records += len(records)
                
                logger.info(f"Extraídos {len(records)} registros (Total: {total_records})")
                
                # Verificar se atingiu o limite máximo
                if max_records and total_records >= max_records:
                    logger.info(f"Limite de {max_records} registros atingido")
                    self.all_data = self.all_data[:max_records]
                    break
                
                # Próxima página
                current_offset += limit
                
                # Se menos registros que o solicitado, é a última página
                if len(records) < limit:
                    logger.info("Última página atingida")
                    break
                    
            except requests.exceptions.Timeout:
                logger.error("Timeout na requisição à API")
                return False
            except requests.exceptions.ConnectionError:
                logger.error("Erro de conexão com a API")
                return False
            except requests.exceptions.HTTPError as e:
                logger.error(f"Erro HTTP: {e.response.status_code} - {e.response.text}")
                return False
            except json.JSONDecodeError:
                logger.error("Erro ao decodificar JSON da resposta")
                return False
            except Exception as e:
                logger.error(f"Erro inesperado: {e}")
                return False
        
        logger.info(f"Extração concluída com {total_records} registros no total")
        return True
    
    def save_to_json(self, filename: Optional[str] = None) -> Path:
        """
        Salva os dados em arquivo JSON localmente.
        
        Args:
            filename: Nome do arquivo (padrão: dengue_TIMESTAMP.json)
        
        Returns:
            Caminho do arquivo salvo
        """
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"dengue_{timestamp}.json"
        
        filepath = self.output_dir / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(self.all_data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Dados salvos em JSON: {filepath}")
        return filepath
    
    def save_to_csv(self, filename: Optional[str] = None) -> Optional[Path]:
        """
        Salva os dados em arquivo CSV localmente.
        
        Args:
            filename: Nome do arquivo (padrão: dengue_TIMESTAMP.csv)
        
        Returns:
            Caminho do arquivo salvo ou None se não houver dados
        """
        if not self.all_data:
            logger.warning("Nenhum dado para salvar em CSV")
            return None
        
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"dengue_{timestamp}.csv"
        
        filepath = self.output_dir / filename
        
        # Extrair todas as chaves únicas
        all_keys = set()
        for record in self.all_data:
            if isinstance(record, dict):
                all_keys.update(record.keys())
        
        fieldnames = sorted(list(all_keys))
        
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(self.all_data)
        
        logger.info(f"Dados salvos em CSV: {filepath}")
        return filepath
    
    def get_data(self) -> List[Dict]:
        """Retorna os dados extraídos."""
        return self.all_data
    
    def get_summary(self) -> Dict:
        """
        Retorna um resumo dos dados extraídos.
        
        Returns:
            Dicionário com informações sobre os dados
        """
        return {
            "total_records": len(self.all_data),
            "timestamp": datetime.now().isoformat(),
            "sample": self.all_data[:1] if self.all_data else None
        }


def main():
    """Função principal para extrair dados de dengue."""
    
    logger.info("Iniciando extração de dados de dengue do DATASUS")
    
    # Criar instância da API
    api = DengueDatasusAPI(output_dir="data/raw")
    
    # Extrair dados
    success = api.fetch_data(limit=100, max_records=None)
    
    if success:
        # Salvar em ambos os formatos
        json_path = api.save_to_json()
        csv_path = api.save_to_csv()
        
        # Exibir resumo
        summary = api.get_summary()
        logger.info(f"Resumo: {json.dumps(summary, ensure_ascii=False, indent=2)}")
        
        print(f"\n✓ Extração concluída com sucesso!")
        print(f"  JSON: {json_path}")
        if csv_path:
            print(f"  CSV: {csv_path}")
        print(f"\n📊 Total de registros: {len(api.all_data)}")
        
        return 0
    else:
        logger.error("Falha na extração de dados")
        return 1


if __name__ == "__main__":
    exit(main())
