from flask import Flask, render_template, request, redirect, url_for
import sqlite3
from datetime import timedelta, datetime
import pandas as pd
from dateutil.relativedelta import relativedelta

app = Flask(__name__)

# Puxar data do front
@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        data_selecionada = request.form['data']
        return redirect(url_for('resultado', data=data_selecionada))
    return render_template('index.html')

@app.route('/resultado/<data>')
def resultado(data):
    #Abrir conexão no SQLITE
    conexao = sqlite3.connect('cotacoes.db')
    cursor = conexao.cursor()

    #Mudando a formatação da Data
    DataF = datetime.strptime(data, '%Y-%m-%d')
    DataF = DataF.strftime("%d/%m/%Y")
    
    #Puxar dados da base do sqlite para calcular as variações diárias
    cursor.execute("SELECT Data, Venda, Compra FROM cotacoes ORDER BY Data")
    df = pd.read_sql_query("SELECT Data, Venda, Compra FROM cotacoes ORDER BY Data", conexao)
    df['Data'] = pd.to_datetime(df['Data'])
    print(df)
    df.set_index('Data', inplace = True)
    df['var_diariaC'] =  df['Compra'].diff()
    df['var_diariaV'] =  df['Venda'].diff()
    dados = cursor.fetchall()
    
    #Definindo a função que puxa os valores da data anterior caso na data selecionada seja indisponível
    def get_most_recent_value_before_date(cursor, date):
        cursor.execute("""
        SELECT Venda, Compra 
        FROM cotacoes 
        WHERE Data < ?
        ORDER BY Data DESC
        LIMIT 1
        """, (date,))
        return cursor.fetchone()

    #Definindo a função que calcula a variação
    def percentual_variation(current, previous):
        return ((current - previous) / previous) * 100

    #Definindo a função que calcula a data anterior
    def subtract_period(data_string, days=0, months=0, years=0):
        date_obj = datetime.strptime(data_string, "%Y-%m-%d")
        # Subtract days using timedelta
        if days:
            date_obj -= timedelta(days=days)
        # Subtract months and years using relativedelta
        if months or years:
            date_obj -= relativedelta(months=months, years=years)
        return date_obj.strftime('%Y-%m-%d')

    # Obter valores do dia selecionado
    cursor.execute("SELECT Venda, Compra FROM cotacoes WHERE Data=?", (data,))
    valores_hoje = cursor.fetchone()
    
    #Se não houver o valor do dia selecionado na base de dados ele puxa do API
    if not valores_hoje:
        try:
            data2 = datetime.strptime(data, "%Y-%m-%d")
            data4 = data2 - timedelta(days = 370) 
            data_api = data2.strftime("%m-%d-%Y")
            data_api3 = data4.strftime("%m-%d-%Y")

            tabela = pd.DataFrame()
            numerot = len(pd.read_json(f"https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoDolarPeriodo(dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)?@dataInicial=%27{data_api3}%27&@dataFinalCotacao=%27{data_api}%27&$top=10000&$format=json&$select=cotacaoCompra,cotacaoVenda,dataHoraCotacao", orient = 'columns')['value'])
            for i in range(numerot):
                oi = pd.read_json(f"https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoDolarPeriodo(dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)?@dataInicial=%27{data_api3}%27&@dataFinalCotacao=%27{data_api}%27&$top=10000&$format=json&$select=cotacaoCompra,cotacaoVenda,dataHoraCotacao", orient = 'columns')['value'][i]
                x = pd.DataFrame([oi])
                tabela = pd.concat([tabela,x], ignore_index = True)

            tabela['dataHoraCotacao'] = pd.to_datetime(tabela['dataHoraCotacao']).dt.date
            tabela = tabela.rename(columns = {'cotacaoCompra' : 'Compra', 'cotacaoVenda': 'Venda', 'dataHoraCotacao': 'Data'})
            tabela.set_index('Data', inplace = True)
            tabela['variacaocompra'] = tabela['Compra'].diff()
            tabela['variacaovenda'] = tabela['Venda'].diff()
            mediamesdavardiaria = tabela['variacaocompra'].mean()
            mediamesdavardiariaV = tabela['variacaovenda'].mean()
            desvpadmesdavardiaria = tabela['variacaocompra'].std()
            desvpadmesdavardiariaV = tabela['variacaovenda'].std()
            api_url = f"https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoDolarPeriodo(dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)?@dataInicial=%27{data_api}%27&@dataFinalCotacao=%27{data_api}%27&$top=10000&$format=json&$select=cotacaoCompra,cotacaoVenda,dataHoraCotacao"
            pocas = pd.read_json(api_url, orient = 'columns')
            cotcompra = pocas['value'][0]['cotacaoCompra']
            cotvenda = pocas['value'][0]['cotacaoVenda']
            valores_hoje = cotvenda, cotcompra
            valores_dia_anterior = tabela['Venda'].iloc[-2], tabela['Compra'].iloc[-2]
            valores_ano_passado = tabela['Venda'].iloc[-252], tabela['Compra'].iloc[-252]
            valores_mes_passado = tabela['Venda'].iloc[-22], tabela['Compra'].iloc[-22]
        #Caso não tenha nem na base de dados nem no API
        except:
            return f"Nenhum resultado encontrado para a data: {DataF}"
    else:
        #Caso tenha o valor correspondente da data selecionada
        data_anterior = subtract_period(data, days=1)
        data_mes_passado = subtract_period(data, months=1)
        data_ano_passado = subtract_period(data, years=1)

        # Obter valores dos períodos: dia anterior, mês passado e ano passado
        cursor.execute("SELECT Venda, Compra FROM cotacoes WHERE Data=?", (data_anterior,))
        valores_dia_anterior = cursor.fetchone() or get_most_recent_value_before_date(cursor, data_anterior)

        cursor.execute("SELECT Venda, Compra FROM cotacoes WHERE Data=?", (data_mes_passado,))
        valores_mes_passado = cursor.fetchone() or get_most_recent_value_before_date(cursor, data_mes_passado)

        cursor.execute("SELECT Venda, Compra FROM cotacoes WHERE Data=?", (data_ano_passado,))
        valores_ano_passado = cursor.fetchone() or get_most_recent_value_before_date(cursor, data_ano_passado)
        
        date_index = df.index.get_loc(data)
        mediamesdavardiaria = df['var_diariaC'].iloc[date_index-22:date_index].mean()
        mediamesdavardiariaV = df['var_diariaV'].iloc[date_index-22:date_index].mean()
        desvpadmesdavardiaria = df['var_diariaC'].iloc[date_index-22:date_index].std()
        desvpadmesdavardiariaV = df['var_diariaV'].iloc[date_index-22:date_index].std()

    conexao.close()
    
    if valores_hoje and (valores_dia_anterior or valores_mes_passado or valores_ano_passado):
        venda_hoje, compra_hoje = valores_hoje

        # Calcula variações
        var_diaria_venda = percentual_variation(venda_hoje, valores_dia_anterior[0]) if valores_dia_anterior else None
        var_diaria_compra = percentual_variation(compra_hoje, valores_dia_anterior[1]) if valores_dia_anterior else None

        var_mensal_venda = percentual_variation(venda_hoje, valores_mes_passado[0]) if valores_mes_passado else None
        var_mensal_compra = percentual_variation(compra_hoje, valores_mes_passado[1]) if valores_mes_passado else None

        var_anual_venda = percentual_variation(venda_hoje, valores_ano_passado[0]) if valores_ano_passado else None
        var_anual_compra = percentual_variation(compra_hoje, valores_ano_passado[1]) if valores_ano_passado else None
        
        significanciacompra = 'Compra significante' if abs((var_diaria_compra/100) - mediamesdavardiaria) > 2 * desvpadmesdavardiaria else 'Compra insignificante'
        significanciavenda = 'Venda significante' if abs((var_diaria_venda/100) - mediamesdavardiariaV) > 2 * desvpadmesdavardiariaV else 'Venda insignificante'

        #Passar para o front
        return render_template('resultado.html', data=data, venda=venda_hoje, compra=compra_hoje,
                               var_diaria_venda=var_diaria_venda, var_diaria_compra=var_diaria_compra,
                               var_mensal_venda=var_mensal_venda, var_mensal_compra=var_mensal_compra,
                               var_anual_venda=var_anual_venda, var_anual_compra=var_anual_compra, 
                               significanciacompra=significanciacompra, significanciavenda=significanciavenda, DataF = DataF) 
    else:
        return f"Nenhum resultado encontrado para a data: {DataF}"


if __name__ == '__main__':
    app.run(debug=True)
    
