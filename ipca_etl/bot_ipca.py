import requests
import pandas as pd
from pathlib import Path

# url da api do ibge pro ipca
URL_IPCA = "https://sidra.ibge.gov.br/Ajax/JSon/Tabela/1/1737?versao=-1"

# nome do arquivo parquet de saida
ARQUIVO_SAIDA = Path(__file__).parent / "data" / "ipca_dados.parquet"

def get_dados_ipca(url: str):
    """
    Faz a chamada à API do IBGE e devolve os dados do IPCA em formato JSON.
    Parâmetros de url, endereço da API do IBGE.
    Retorno de dados brutos vindos da API.
    Se houver falha de conexão ou resposta inválida, a função levanta uma exceção.
    """
    try:
        resposta = requests.get(url, timeout=30)
        resposta.raise_for_status()
        return resposta.json()
    except requests.exceptions.RequestException as erro:
        raise Exception(f"Erro ao pegar dados da URL {url}: {erro}")


def processa_dados_ipca(dados_brutos: dict) -> pd.DataFrame:
    """
    Organiza os dados retornados pela API em um DataFrame do pandas.
    Dados brutos da estrutura JSON com os dados do IPCA.
    Tabela organizada, com colunas renomeadas e datas tratadas.

    Detalhes:
        - renomeia colunas para nomes mais claros
        - converte a coluna de data para formato datetime
        - levanta erro caso os períodos não sejam encontrados
    """
    try:
        periodos = dados_brutos.get("Periodos", {}).get("Periodos", [])

        if not periodos:
            raise ValueError("Periodos não retornados")

        tabela = pd.DataFrame(periodos).rename(columns={
            "Id": "id_periodo",
            "Codigo": "codigo_periodo",
            "Nome": "nome_periodo",
            "Disponivel": "disponivel",
            "DataLiberacao": "data_liberacao"
        })

        if "data_liberacao" in tabela.columns:
            tabela["data_liberacao"] = pd.to_datetime(tabela["data_liberacao"], errors="coerce")

        print(f"[INFO] DataFrame criado: {tabela.shape[0]} linhas x {tabela.shape[1]} colunas")
        print(tabela.head())

        return tabela.reset_index(drop=True)

    except Exception as erro:
        raise Exception(f"Erro ao processar os dados: {erro}")


def salva_parquet(tabela: pd.DataFrame, caminho: Path):
    """
    Salva a tabela em formato Parquet no caminho especificado.
    Tabela com dados já tratados do IPCA.
    Caminho local onde o arquivo será gravado.
    Cria automaticamente a pasta 'data' se ela não existir.
    """
    try:
        caminho.parent.mkdir(parents=True, exist_ok=True)
        tabela.to_parquet(caminho, index=False)
        print(f"[OK] Arquivo salvo em: {caminho.resolve()}")
    except Exception as erro:
        raise Exception(f"Erro ao salvar o arquivo Parquet: {erro}")


def main():
    """
    Executa o fluxo completo da aplicação:
    - captura os dados do IPCA na API do IBGE
    - organiza em DataFrame
    - grava em arquivo Parquet na pasta /data
    """
    print("Iniciando captura dos dados do IPCA...")
    try:
        dados = get_dados_ipca(URL_IPCA)
        print("[OK] Dados brutos pegos da API.")

        tabela_ipca = processa_dados_ipca(dados)
        salva_parquet(tabela_ipca, ARQUIVO_SAIDA)

    except Exception as erro:
        print(f"[ERRO] {erro}")


if __name__ == "__main__":
    main()
