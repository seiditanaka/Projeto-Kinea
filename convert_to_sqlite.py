import pandas as pd
import sqlite3

tabela = pd.DataFrame()

numero = len(pd.read_json(r"https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoDolarPeriodo(dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)?@dataInicial=%2701-01-2020%27&@dataFinalCotacao=%2711-01-2023%27&$top=10000&$format=json&$select=cotacaoCompra,cotacaoVenda,dataHoraCotacao", orient = 'columns')['value'])
for i in range(numero):
    oi = pd.read_json(r"https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoDolarPeriodo(dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)?@dataInicial=%2701-01-2020%27&@dataFinalCotacao=%2711-01-2023%27&$top=10000&$format=json&$select=cotacaoCompra,cotacaoVenda,dataHoraCotacao", orient = 'columns')['value'][i]
    x = pd.DataFrame([oi])
    tabela = pd.concat([tabela,x], ignore_index = True)
    
tabela['dataHoraCotacao'] = pd.to_datetime(tabela['dataHoraCotacao']).dt.date
tabela = tabela.rename(columns = {'cotacaoCompra' : 'Compra', 'cotacaoVenda': 'Venda', 'dataHoraCotacao': 'Data'})

conn = sqlite3.connect("cotacoes.db")
tabela.to_sql("cotacoes", conn, if_exists = 'replace')

conn.close()
