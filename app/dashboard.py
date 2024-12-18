import os
from dotenv import load_dotenv
import streamlit as st
import altair as alt
import psycopg2
import pandas as pd
from datetime import datetime
import locale

# Carrega as variáveis de ambiente
load_dotenv()

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")

# Define o locale para português do Brasil
locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')

def get_connection():
    """Conecta ao banco de dados e retorna os dados."""
    try:
        conn = psycopg2.connect(
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            host=POSTGRES_HOST,
            port=POSTGRES_PORT
        )
        query = "SELECT * FROM bitcoin_historical_price ORDER BY timestamp DESC"
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Error: {e}")
        return pd.DataFrame()

def format_currency(value):
    """Formata o valor para o formato '104.227,01 USD'"""
    formatted_value = f"{value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"{formatted_value} USD"

def main():
    st.set_page_config(
        page_title="Bitcoin Dashboard",
        page_icon="💲",
        layout="wide",
        initial_sidebar_state="expanded"
    )

    alt.themes.enable('dark')

    # Cabeçalho do Dashboard
    st.title("Bitcoin Dashboard")
    st.write("""
    O mercado de Bitcoin continua a surpreender. Descubra as tendências recentes, explore dados históricos e veja
    como os principais eventos impactam o preço da moeda digital. Este painel ajuda você a acompanhar as flutuações
    em tempo real e tomar decisões informadas.
    """)

    # Obtém os dados
    df = get_connection()
    if df.empty:
        st.warning("Nenhum dado disponível no momento.")
        return

    # Converte o campo de timestamp para datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # Configurações da barra lateral
    with st.sidebar:
        st.write("### Filtros de Visualização")
        st.write("Selecione o intervalo de datas e a granularidade para visualizar os dados históricos.")
        date_range = st.date_input(
            "Intervalo de Datas",
            [datetime(2024, 12, 18), datetime.now().date()]
        )
        start_date = datetime.combine(date_range[0], datetime.min.time())
        end_date = datetime.combine(date_range[1], datetime.max.time())

        st.write("Selecione a granularidade")
        granularity = st.radio("Granularidade", ['Hora', 'Dia', 'Mês', 'Ano'])

    # Filtra os dados pelo intervalo de datas
    df_filtered = df[(df['timestamp'] >= start_date) & (df['timestamp'] <= end_date)]

    if df_filtered.empty:
        max_date = df['timestamp'].max().date()
        st.warning(f"Nenhum dado disponível para a data selecionada. Dados disponíveis até {max_date}.")
        return

    # Ajusta os dados com base na granularidade
    if granularity == 'Hora':
        df_filtered['time_group'] = df_filtered['timestamp'].dt.floor('H')
    elif granularity == 'Dia':
        df_filtered['time_group'] = df_filtered['timestamp'].dt.floor('D')
    elif granularity == 'Mês':
        df_filtered['time_group'] = df_filtered['timestamp'].dt.to_period('M').dt.to_timestamp()
    elif granularity == 'Ano':
        df_filtered['time_group'] = df_filtered['timestamp'].dt.to_period('A').dt.to_timestamp()

    # Exibe as métricas em cards e o gráfico de linha
    st.markdown("### Estatísticas Gerais")
    col1, col2, col3 = st.columns(3)

    with col1:
        current_price = df_filtered['value'].iloc[0]
        st.metric(label="Preço Atual", value=format_currency(current_price))

    with col2:
       
        min_price = df_filtered['value'].min()
        st.metric(label="Preço Mínimo", value=format_currency(min_price))

    with col3:
       
        max_price = df_filtered['value'].max()
        st.metric(label="Preço Máximo", value=format_currency(max_price))

    # Gráfico de linha
    line_chart(df_filtered, start_date, end_date, granularity)

def line_chart(df_filtered, start_date, end_date, granularity):
    df_grouped = df_filtered.groupby('time_group').agg({'value': 'mean'}).reset_index()

    st.write(f"### Preço do Bitcoin de {start_date.date()} a {end_date.date()} ({granularity})")
    c = alt.Chart(df_grouped).mark_line(color='green').encode(
        x=alt.X('time_group:T', title='Tempo'),
        y=alt.Y('value:Q', title='Preço Médio'),
        tooltip=['time_group:T', 'value:Q']
    ).properties(
        width=800,
        height=400
    )
    st.altair_chart(c)

if __name__ == "__main__":
    main()
