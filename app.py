import streamlit as st
import pandas as pd
import duckdb
import plotly.express as px
import requests
import time
from io import BytesIO

# --- 1. CONFIGURAÇÃO E ESTILO ---
st.set_page_config(page_title="PRO COMEX | Dashboard", layout="wide")

st.markdown("""
<style>
    .stApp { background-color: #F8F9FC; }
    [data-testid="stSidebar"] { background-color: #134596; border-right: 1px solid #0D3370; }
    [data-testid="stSidebar"] .stMarkdown p, [data-testid="stSidebar"] label { color: white !important; }
    .metric-card {
        background-color: white; border-radius: 12px; padding: 20px;
        border: 1px solid #E2E8F0; box-shadow: 0 4px 6px rgba(0,0,0,0.02);
    }
    .metric-label { color: #64748B; font-size: 14px; font-weight: 500; margin-bottom: 5px; }
    .metric-value { color: #1E293B; font-size: 24px; font-weight: bold; margin: 0; }
</style>
""", unsafe_allow_html=True)

if 'con' not in st.session_state:
    st.session_state['con'] = duckdb.connect(database=':memory:')
con = st.session_state['con']

URF_MAP = {
    "0817800": "SANTOS", "0717700": "SÃO SEBASTIÃO", "0917800": "VITÓRIA",
    "0717600": "PARANAGUÁ", "1017700": "ITAPOÁ", "0817700": "VIRACOPOS",
    "0927800": "SÃO FRANCISCO SUL", "0227600": "URUGUAIANA", "0927700": "VITÓRIA (ADUANA)",
    "0610600": "CORUMBÁ", "0917900": "CARIACICA", "1010600": "FLORIANÓPOLIS"
}

# --- 2. FUNÇÕES DE SUPORTE ---

@st.cache_data(ttl=3600)
def carregar_nomes_paises():
    try:
        url = "https://api-comexstat.mdic.gov.br/auxiliary/country"
        r = requests.get(url, verify=False, timeout=5, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code == 200:
            return {int(i['id']): i['text'] for i in r.json()['data']}
    except:
        return None

def tratar_dataframe(df):
    df.columns = [c.strip().upper() for c in df.columns]
    
    # Mapeamento estrito para o seu Layout
    col_map = {
        'YEAR': 'CO_ANO', 'MONTH': 'CO_MES', 'NCM': 'CO_NCM', 
        'VALUE': 'VL_FOB', 'NET_WEIGHT': 'KG_LIQUIDO',
        'FREIGHT': 'VL_FRETE', 'INSURANCE': 'VL_SEGURO'
    }
    df = df.rename(columns=col_map)
    
    paises = carregar_nomes_paises()
    if paises is None:
        paises = {160: "China", 249: "EUA", 23: "Alemanha", 63: "Argentina", 105: "Brasil"}

    # Adiciona colunas se não existirem no CSV para evitar erro no SQL
    for col in ['CO_ANO', 'CO_MES', 'CO_UNID', 'CO_VIA', 'QT_ESTAT', 'VL_FRETE', 'VL_SEGURO']:
        if col not in df.columns: df[col] = 0

    df['NOME_URF'] = df['CO_URF'].apply(lambda x: URF_MAP.get(str(x).zfill(7), str(x))) if 'CO_URF' in df.columns else "NÃO INFORMADO"
    
    if 'CO_PAIS' in df.columns:
        df['NOME_PAIS'] = pd.to_numeric(df['CO_PAIS'], errors='coerce').fillna(0).astype(int).map(paises).fillna("Outros")
    else:
        df['NOME_PAIS'] = "NÃO INFORMADO"
    
    con.register('raw_data', df)
    return True

@st.cache_data(ttl=600)
def buscar_dados_api(ncm, fluxo):
    url = "https://api-comexstat.mdic.gov.br/general"
    ncm_limpo = ''.join(filter(str.isdigit, str(ncm)))
    headers = {"User-Agent": "Mozilla/5.0", "Content-Type": "application/json"}
    payload = {
        "flow": fluxo, "monthStart": "01", "monthEnd": "12",
        "yearStart": "2024", "yearEnd": "2026",
        "filters": [{"filter": "ncm", "values": [ncm_limpo]}],
        "details": ["ncm", "country", "uf", "urf", "year", "month"],
        "metrics": ["fob", "kg"]
    }
    try:
        response = requests.post(url, json=payload, headers=headers, verify=False, timeout=25)
        if response.status_code == 429:
            time.sleep(11)
            return buscar_dados_api(ncm, fluxo)
        if response.status_code == 200:
            data = response.json().get('data', {}).get('list', [])
            if not data: return None
            df = pd.DataFrame(data)
            return df.rename(columns={
                'coCountry': 'CO_PAIS', 'coUf': 'SG_UF_NCM', 'coUrf': 'CO_URF',
                'metricFob': 'VL_FOB', 'metricKg': 'KG_LIQUIDO', 'coYear': 'CO_ANO',
                'coMonth': 'CO_MES', 'coNcm': 'CO_NCM'
            })
    except: return None

# --- 3. SIDEBAR ---
with st.sidebar:
    st.markdown("<h2 style='text-align: center;'>PRO COMEX</h2>", unsafe_allow_html=True)
    ncm_input = st.text_input("Código NCM", "39199090")
    fluxo_api = st.radio("Operação", ["import", "export"], format_func=lambda x: "Importação" if x=="import" else "Exportação")
    st.markdown("---")
    arquivo_subido = st.file_uploader("📂 Upload CSV", type=["csv"])
    btn_api = st.button("🌐 Buscar via API")

    dados_carregados = False
    if arquivo_subido:
        df_csv = pd.read_csv(arquivo_subido, sep=';', engine='python')
        dados_carregados = tratar_dataframe(df_csv)
    
    if btn_api:
        with st.spinner('Consultando...'):
            df_api = buscar_dados_api(ncm_input, fluxo_api)
            if df_api is not None:
                dados_carregados = tratar_dataframe(df_api)
                st.success("Dados carregados!")

    pais_selecionado = "Todos"
    uf_selecionada = "Todos"
    if dados_carregados:
        lista_paises = con.execute("SELECT DISTINCT NOME_PAIS FROM raw_data ORDER BY NOME_PAIS").df()['NOME_PAIS'].tolist()
        lista_paises.insert(0, "Todos")
        pais_selecionado = st.selectbox("Filtrar por País", lista_paises)
        lista_ufs = con.execute("SELECT DISTINCT SG_UF_NCM FROM raw_data WHERE SG_UF_NCM IS NOT NULL ORDER BY SG_UF_NCM").df()['SG_UF_NCM'].tolist()
        lista_ufs.insert(0, "Todos")
        uf_selecionada = st.selectbox("Filtrar por Estado (UF)", lista_ufs)

# --- 4. DASHBOARD ---
if dados_carregados:
    ncm_query = ''.join(filter(str.isdigit, ncm_input))
    filtro_sql = f"WHERE CAST(CO_NCM AS VARCHAR) LIKE '{ncm_query}%'"
    if pais_selecionado != "Todos": filtro_sql += f" AND NOME_PAIS = '{pais_selecionado}'"
    if uf_selecionada != "Todos": filtro_sql += f" AND SG_UF_NCM = '{uf_selecionada}'"

    resumo = con.execute(f"SELECT SUM(VL_FOB), SUM(KG_LIQUIDO), COUNT(*) FROM raw_data {filtro_sql}").fetchone()
    
    if resumo and resumo[0] is not None:
        st.markdown(f"### 📈 Dashboard NCM: {ncm_query}")
        c1, c2, c3 = st.columns(3)
        c1.markdown(f'<div class="metric-card"><p class="metric-label">Valor Total (USD)</p><p class="metric-value">US$ {resumo[0]:,.2f}</p></div>', unsafe_allow_html=True)
        c2.markdown(f'<div class="metric-card"><p class="metric-label">Peso (KG)</p><p class="metric-value">{resumo[1]:,.2f} KG</p></div>', unsafe_allow_html=True)
        c3.markdown(f'<div class="metric-card"><p class="metric-label">Operações</p><p class="metric-value">{resumo[2]}</p></div>', unsafe_allow_html=True)

        st.write("---")
        
        # BUSCA SEGUINDO EXATAMENTE SEU LAYOUT
        df_final = con.execute(f"""
            SELECT 
                CO_ANO, CO_MES, CO_NCM, CO_UNID, NOME_PAIS, SG_UF_NCM, 
                CO_VIA, NOME_URF, QT_ESTAT, KG_LIQUIDO, VL_FOB, VL_FRETE, VL_SEGURO 
            FROM raw_data {filtro_sql}
            ORDER BY CO_ANO DESC, CO_MES DESC
        """).df()
        
        col_t1, col_t2 = st.columns([3, 1])
        col_t1.subheader("📋 Detalhamento")
        
        output = BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df_final.to_excel(writer, index=False, sheet_name='Relatorio')
        
        col_t2.download_button("📥 Exportar Excel", output.getvalue(), f"comex_{ncm_query}.xlsx")
        
        st.dataframe(
            df_final, use_container_width=True, hide_index=True,
            column_config={
                "CO_ANO": st.column_config.TextColumn("Ano"),
                "CO_MES": st.column_config.TextColumn("Mês"),
                "CO_NCM": st.column_config.TextColumn("NCM"),
                "NOME_PAIS": st.column_config.TextColumn("País"),
                "NOME_URF": st.column_config.TextColumn("Porto/Recinto"),
                "VL_FOB": st.column_config.NumberColumn("Valor FOB (USD)", format="US$ %.2f"),
                "VL_FRETE": st.column_config.NumberColumn("Frete (USD)", format="US$ %.2f"),
                "VL_SEGURO": st.column_config.NumberColumn("Seguro (USD)", format="US$ %.2f"),
                "KG_LIQUIDO": st.column_config.NumberColumn("Peso Líq. (KG)", format="%.2f")
            }
        )
else:
    st.info("💡 Insira o NCM e carregue os dados para começar.")