import streamlit as st
from streamlit_option_menu import option_menu
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import duckdb

# ─────────────────────────────────────────────
#  PAGE CONFIG  (must be FIRST streamlit call)
# ─────────────────────────────────────────────
st.set_page_config(
    page_icon="🚗",
    page_title="US Accident Dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────
#  GLOBAL CSS
# ─────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Bebas+Neue&family=DM+Sans:wght@300;400;500;600&family=JetBrains+Mono:wght@400;600&display=swap');

:root {
    --bg-primary:  #0A0D14;
    --bg-card:     #111520;
    --bg-card2:    #161B2E;
    --accent:      #E63946;
    --accent3:     #F4A261;
    --text:        #F0F4FF;
    --muted:       #7B8DB0;
    --border:      rgba(255,255,255,0.06);
    --glow:        rgba(230,57,70,0.25);
}
html, body, [data-testid="stAppViewContainer"] {
    background: var(--bg-primary) !important;
    color: var(--text) !important;
    font-family: 'DM Sans', sans-serif;
}
[data-testid="stAppViewContainer"]::before {
    content:''; position:fixed; top:-200px; left:-200px;
    width:600px; height:600px;
    background:radial-gradient(circle,rgba(230,57,70,0.08) 0%,transparent 70%);
    pointer-events:none; z-index:0;
}
[data-testid="stSidebar"] {
    background: linear-gradient(180deg,#0D1018 0%,#111520 100%) !important;
    border-right: 1px solid var(--border) !important;
}
[data-testid="stSidebar"]::before {
    content:''; display:block; height:3px;
    background:linear-gradient(90deg,var(--accent),var(--accent3));
    margin-bottom:12px;
}
h1 {
    font-family:'Bebas Neue',sans-serif !important;
    font-size:3.5rem !important; letter-spacing:0.06em !important;
    background:linear-gradient(90deg,var(--text) 0%,var(--muted) 100%);
    -webkit-background-clip:text; -webkit-text-fill-color:transparent;
    margin-bottom:0 !important;
}
[data-testid="stMetric"] {
    background:var(--bg-card) !important;
    border:1px solid var(--border) !important;
    border-radius:16px !important; padding:20px 24px !important;
    position:relative; overflow:hidden;
    transition:transform 0.2s,box-shadow 0.2s;
}
[data-testid="stMetric"]:hover { transform:translateY(-3px); box-shadow:0 8px 32px var(--glow); }
[data-testid="stMetric"]::before {
    content:''; position:absolute; top:0; left:0; right:0; height:2px;
    background:linear-gradient(90deg,var(--accent),var(--accent3));
}
[data-testid="stMetricLabel"] {
    font-size:11px !important; font-weight:600 !important;
    letter-spacing:0.12em !important; text-transform:uppercase !important;
    color:var(--muted) !important;
}
[data-testid="stMetricValue"] {
    font-family:'Bebas Neue',sans-serif !important;
    font-size:2.2rem !important; color:var(--text) !important;
}
hr { border:none !important; height:1px !important;
     background:linear-gradient(90deg,transparent,var(--border),transparent) !important;
     margin:32px 0 !important; }
[data-testid="stExpander"] {
    background:var(--bg-card) !important; border:1px solid var(--border) !important;
    border-radius:12px !important; overflow:hidden;
}
.hero {
    background:linear-gradient(135deg,var(--bg-card2) 0%,var(--bg-card) 100%);
    border:1px solid var(--border); border-radius:20px;
    padding:36px 40px; margin-bottom:32px; position:relative; overflow:hidden;
}
.hero::after { content:'🚗'; position:absolute; right:40px; top:50%;
               transform:translateY(-50%); font-size:7rem; opacity:0.07; }
.hero h2 { font-family:'Bebas Neue',sans-serif !important; font-size:2.6rem !important;
           letter-spacing:0.06em !important; color:var(--text) !important; margin:0 0 8px !important; }
.hero p  { font-size:15px; color:var(--muted); margin:0; line-height:1.7; }
.badge   { display:inline-block; background:var(--accent); color:white;
           font-family:'JetBrains Mono',monospace; font-size:10px; font-weight:600;
           letter-spacing:0.1em; padding:3px 10px; border-radius:20px; margin-bottom:12px; }
.section-label {
    font-family:'JetBrains Mono',monospace; font-size:11px; font-weight:600;
    letter-spacing:0.15em; text-transform:uppercase; color:var(--accent); margin-bottom:6px;
}

/* Clear button styling */
div[data-testid="stButton"] > button[kind="secondary"] {
    background: transparent !important;
    border: 1px solid rgba(230,57,70,0.4) !important;
    color: #E63946 !important;
    font-family: 'JetBrains Mono', monospace !important;
    font-size: 11px !important;
    letter-spacing: 0.1em !important;
    border-radius: 8px !important;
    width: 100% !important;
    transition: all 0.2s !important;
}
div[data-testid="stButton"] > button[kind="secondary"]:hover {
    background: rgba(230,57,70,0.1) !important;
    border-color: #E63946 !important;
}
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────
#  DB + CACHE
# ─────────────────────────────────────────────
@st.cache_resource
def get_conn():
    return duckdb.connect()

@st.cache_data(ttl=3600)
def query(sql: str) -> pd.DataFrame:
    return get_conn().execute(sql).df()

@st.cache_data(ttl=3600)
def query_one(sql: str):
    return get_conn().execute(sql).fetchone()[0]

# ─────────────────────────────────────────────
#  STATE MAP
# ─────────────────────────────────────────────
US_STATE_ABBREV = {
    'Alabama':'AL','Alaska':'AK','Arizona':'AZ','Arkansas':'AR',
    'California':'CA','Colorado':'CO','Connecticut':'CT','Delaware':'DE',
    'Florida':'FL','Georgia':'GA','Hawaii':'HI','Idaho':'ID',
    'Illinois':'IL','Indiana':'IN','Iowa':'IA','Kansas':'KS',
    'Kentucky':'KY','Louisiana':'LA','Maine':'ME','Maryland':'MD',
    'Massachusetts':'MA','Michigan':'MI','Minnesota':'MN','Mississippi':'MS',
    'Missouri':'MO','Montana':'MT','Nebraska':'NE','Nevada':'NV',
    'New Hampshire':'NH','New Jersey':'NJ','New Mexico':'NM','New York':'NY',
    'North Carolina':'NC','North Dakota':'ND','Ohio':'OH','Oklahoma':'OK',
    'Oregon':'OR','Pennsylvania':'PA','Rhode Island':'RI',
    'South Carolina':'SC','South Dakota':'SD','Tennessee':'TN',
    'Texas':'TX','Utah':'UT','Vermont':'VT','Virginia':'VA',
    'Washington':'WA','West Virginia':'WV','Wisconsin':'WI','Wyoming':'WY'
}

# ─────────────────────────────────────────────
#  CHART THEME
# ─────────────────────────────────────────────
CHART_LAYOUT = dict(
    paper_bgcolor='rgba(17,21,32,1)',
    plot_bgcolor='rgba(17,21,32,1)',
    font=dict(family='DM Sans', color='#7B8DB0', size=12),
    title_font=dict(family='Bebas Neue', size=22, color='#F0F4FF'),
    title_x=0.01,
    margin=dict(l=20, r=20, t=55, b=20),
    colorway=['#E63946','#F4A261','#2EC4B6','#9B5DE5','#44CF6C','#FFD166'],
    xaxis=dict(gridcolor='rgba(255,255,255,0.04)', zerolinecolor='rgba(255,255,255,0.08)'),
    yaxis=dict(gridcolor='rgba(255,255,255,0.04)', zerolinecolor='rgba(255,255,255,0.08)'),
    legend=dict(bgcolor='rgba(0,0,0,0)', bordercolor='rgba(255,255,255,0.06)'),
)

def T(fig):
    fig.update_layout(**CHART_LAYOUT)
    return fig

def make_choropleth(df, color_col, title):
    fig = px.choropleth(
        df, locations='State_Code', locationmode='USA-states',
        color=color_col, hover_name='State', scope='usa',
        color_continuous_scale='Reds', title=title,
    )
    fig.update_layout(**CHART_LAYOUT, height=520,
        geo=dict(bgcolor='rgba(17,21,32,1)', lakecolor='rgba(17,21,32,1)',
                 landcolor='rgba(30,36,54,1)', showframe=False))
    return fig

def prep_state(df):
    df = df.copy()
    df['State'] = df['State'].str.strip().str.title()
    df['State_Code'] = df['State'].map(US_STATE_ABBREV)
    return df

def table_fig(header_vals, cell_vals, header_color, title):
    n = len(cell_vals[0]) if cell_vals else 1
    fig = go.Figure(data=[go.Table(
        header=dict(values=[f'<b>{v}</b>' for v in header_vals],
                    fill_color=header_color,
                    font=dict(color='white', size=13, family='DM Sans'),
                    align='center', height=36),
        cells=dict(values=cell_vals,
                   fill_color=[['#111520','#161B2E']*n],
                   font=dict(color='#F0F4FF', size=12, family='DM Sans'),
                   align='center', height=30)
    )])
    fig.update_layout(title=title, **CHART_LAYOUT)
    return fig

# ─────────────────────────────────────────────
#  FILTER HELPER
# ─────────────────────────────────────────────
def build_where(years, states, severities):
    clauses = []
    if years and years != (2016, 2023):
        clauses.append(f"YEAR(CAST(Start_Time AS TIMESTAMP)) BETWEEN {years[0]} AND {years[1]}")
    if states:
        s = ",".join([f"'{x}'" for x in states])
        clauses.append(f"State IN ({s})")
    if severities:
        sv = ",".join([str(x) for x in severities])
        clauses.append(f"Severity IN ({sv})")
    return ("WHERE " + " AND ".join(clauses)) if clauses else ""

def and_where(base_where, extra):
    if base_where:
        return base_where + " AND " + extra
    return "WHERE " + extra

# ─────────────────────────────────────────────
#  SESSION STATE — initialize filter defaults
#  This must run BEFORE any widget is rendered
# ─────────────────────────────────────────────
DEFAULTS = {
    "f_year":   (2016, 2023),
    "f_states": [],
    "f_sev":    [],
}
for k, v in DEFAULTS.items():
    if k not in st.session_state:
        st.session_state[k] = v

# ── Clear callback — runs BEFORE widgets re-render ──
def clear_filters():
    st.session_state["f_year"]   = (2016, 2023)
    st.session_state["f_states"] = []
    st.session_state["f_sev"]    = []

# ─────────────────────────────────────────────
#  SIDEBAR
# ─────────────────────────────────────────────
with st.sidebar:
    st.markdown(
        "<div style='padding:16px 0 8px;text-align:center;'>"
        "<span style='font-family:Bebas Neue;font-size:1.5rem;letter-spacing:.1em;color:#E63946;'>US ACCIDENTS</span>"
        "<br><span style='font-size:11px;color:#7B8DB0;letter-spacing:.1em;'>INTELLIGENCE DASHBOARD</span>"
        "</div>", unsafe_allow_html=True
    )

    page = option_menu(
        menu_title=None,
        options=["Home","Geographic","Time","Weather","Road Features","Risk Report"],
        icons=["house-fill","map-fill","clock-fill","cloud-fill","sign-stop-fill","exclamation-triangle-fill"],
        default_index=0,
        styles={
            "container": {"padding":"0","background":"transparent"},
            "icon": {"color":"#E63946","font-size":"14px"},
            "nav-link": {"font-family":"DM Sans","font-size":"13px","color":"#7B8DB0",
                         "border-radius":"10px","margin":"2px 0","--hover-color":"rgba(230,57,70,0.1)"},
            "nav-link-selected": {"background":"linear-gradient(90deg,#E63946,#c1121f)",
                                  "color":"white","font-weight":"600"},
        },
    )

    st.markdown("---")
    st.markdown(
        "<p style='font-family:JetBrains Mono;font-size:10px;letter-spacing:.15em;"
        "color:#E63946;margin-bottom:12px;'>⚙ GLOBAL FILTERS</p>",
        unsafe_allow_html=True
    )

    # ── Widgets bound to session_state keys ──
    # Streamlit reads widget value FROM session_state[key] on every rerun,
    # so setting the key in session_state before rerun is what makes clear work.
    year_range = st.slider(
        "📅 Year Range", 2016, 2023,
        key="f_year"           # value = st.session_state["f_year"]
    )

    all_states = sorted(
        query("SELECT DISTINCT State FROM 'accidents.parquet' ORDER BY State")['State'].tolist()
    )
    sel_states = st.multiselect(
        "📍 States", all_states,
        placeholder="All States",
        key="f_states"         # value = st.session_state["f_states"]
    )

    sel_sev = st.multiselect(
        "⚠️ Severity", [1, 2, 3, 4],
        format_func=lambda x: {1:'1 — Low', 2:'2 — Mild', 3:'3 — High', 4:'4 — Very High'}[x],
        placeholder="All Severities",
        key="f_sev"            # value = st.session_state["f_sev"]
    )

    # ── Clear Filters button with on_click callback ──
    # on_click fires BEFORE the next rerun, so session_state is already
    # reset when widgets read their keys — this is why it actually works.
    st.button(
        "🔄 Clear Filters",
        on_click=clear_filters,
        use_container_width=True,
        type="secondary",
    )

    # Active filter indicator
    active = []
    if st.session_state["f_year"] != (2016, 2023):
        active.append(f"📅 {st.session_state['f_year'][0]}–{st.session_state['f_year'][1]}")
    if st.session_state["f_states"]:
        active.append(f"📍 {len(st.session_state['f_states'])} state(s)")
    if st.session_state["f_sev"]:
        active.append(f"⚠️ Sev {st.session_state['f_sev']}")

    if active:
        st.markdown(
            "<div style='margin-top:10px;padding:10px 12px;"
            "background:rgba(230,57,70,0.08);border:1px solid rgba(230,57,70,0.2);"
            "border-radius:8px;'>"
            "<p style='font-family:JetBrains Mono;font-size:9px;color:#E63946;"
            "margin:0 0 6px;letter-spacing:.1em;'>ACTIVE FILTERS</p>"
            + "".join([f"<p style='font-size:11px;color:#F0F4FF;margin:2px 0;'>{a}</p>" for a in active])
            + "</div>",
            unsafe_allow_html=True
        )
    else:
        st.markdown(
            "<div style='margin-top:10px;padding:8px 12px;"
            "background:rgba(68,207,108,0.06);border:1px solid rgba(68,207,108,0.15);"
            "border-radius:8px;'>"
            "<p style='font-family:JetBrains Mono;font-size:9px;color:#44CF6C;"
            "margin:0;letter-spacing:.1em;'>● NO FILTERS — SHOWING ALL DATA</p>"
            "</div>",
            unsafe_allow_html=True
        )

    WHERE = build_where(year_range, sel_states, sel_sev)

    st.markdown(
        "<div style='margin-top:24px;padding:12px;border-top:1px solid rgba(255,255,255,0.06);'>"
        "<p style='font-size:10px;color:#3a4260;text-align:center;margin:0;'>"
        "US ACCIDENTS ANALYSIS © 2024</p></div>",
        unsafe_allow_html=True
    )


# ═══════════════════════════════════════════════════════════════
#  HOME
# ═══════════════════════════════════════════════════════════════
if page == "Home":
    st.markdown("<h1>Overview</h1>", unsafe_allow_html=True)
    st.markdown("""<div class='hero'>
        <span class='badge'>LIVE ANALYSIS</span>
        <h2>US Road Accident Intelligence</h2>
        <p>Comprehensive analysis of traffic accidents across the United States —
        covering geography, time patterns, weather correlation, road features, and risk scoring.</p>
    </div>""", unsafe_allow_html=True)

    # KPIs
    total     = query_one(f"SELECT COUNT(*) FROM 'accidents.parquet' {WHERE}")
    avg_sev   = query_one(f"SELECT ROUND(AVG(Severity),1) FROM 'accidents.parquet' {WHERE}")
    top_state = query(f"SELECT State, COUNT(*) c FROM 'accidents.parquet' {WHERE} GROUP BY State ORDER BY c DESC LIMIT 1")
    top_city  = query(f"SELECT City,  COUNT(*) c FROM 'accidents.parquet' {WHERE} GROUP BY City  ORDER BY c DESC LIMIT 1")
    severe    = query_one(f"SELECT COUNT(*) FROM 'accidents.parquet' {and_where(WHERE,'Severity >= 3')}")
    sev_pct   = round((severe / total) * 100, 2) if total else 0
    peak_h    = query(f"SELECT HOUR(CAST(Start_Time AS TIMESTAMP)) h, COUNT(*) c FROM 'accidents.parquet' {WHERE} GROUP BY h ORDER BY c DESC")

    c1,c2,c3,c4,c5,c6 = st.columns(6)
    c1.metric("🚨 Total Accidents",     f"{total:,}")
    c2.metric("⚠️ Avg Severity",         avg_sev)
    c3.metric("📍 Most Dangerous State", top_state['State'].iloc[0] if not top_state.empty else "N/A")
    c4.metric("🏙️ Most Dangerous City",  top_city['City'].iloc[0]  if not top_city.empty  else "N/A")
    c5.metric("🔴 Severe (≥3) %",        f"{sev_pct}%")
    c6.metric("⏰ Peak Hour",            f"{peak_h['h'].iloc[0]}:00" if not peak_h.empty else "N/A")
    st.markdown("---")

    col_l, col_r = st.columns(2)
    with col_l:
        df = query(f"""
            SELECT YEAR(CAST(Start_Time AS TIMESTAMP)) AS Year, COUNT(*) AS Count
            FROM 'accidents.parquet' {WHERE} GROUP BY Year ORDER BY Year
        """)
        fig = px.line(df, x='Year', y='Count', title='Accidents Year over Year', markers=True, line_shape='spline')
        fig.update_traces(line_color='#E63946', line_width=2.5, marker=dict(color='#F4A261', size=8))
        st.plotly_chart(T(fig), use_container_width=True)

    with col_r:
        df = query(f"SELECT Severity, COUNT(*) AS Count FROM 'accidents.parquet' {WHERE} GROUP BY Severity ORDER BY Severity")
        df['Label'] = df['Severity'].map({1:'Low',2:'Mild',3:'High',4:'Very High'})
        fig = px.pie(df, values='Count', names='Label', title='Severity Distribution', hole=0.55)
        fig.update_traces(marker=dict(colors=['#44CF6C','#F4A261','#E63946','#9B5DE5'],
                                      line=dict(color='#111520', width=2)))
        st.plotly_chart(T(fig), use_container_width=True)

    st.markdown("---")

    col_l, col_r = st.columns(2)
    with col_l:
        t_all  = query_one(f"SELECT COUNT(*) FROM 'accidents.parquet' {WHERE}")
        t_sev2 = query_one(f"SELECT COUNT(*) FROM 'accidents.parquet' {and_where(WHERE,'Severity >= 2')}")
        t_sev3 = query_one(f"SELECT COUNT(*) FROM 'accidents.parquet' {and_where(WHERE,'Severity >= 3')}")
        t_sev4 = query_one(f"SELECT COUNT(*) FROM 'accidents.parquet' {and_where(WHERE,'Severity = 4')}")
        funnel_df = pd.DataFrame({
            'Stage': ['All Accidents','Mild+ (≥2)','High+ (≥3)','Very High (4)'],
            'Count': [t_all, t_sev2, t_sev3, t_sev4]
        })
        fig = px.funnel(funnel_df, x='Count', y='Stage', title='Accident Severity Funnel')
        fig.update_traces(marker_color=['#2EC4B6','#F4A261','#E63946','#9B5DE5'])
        st.plotly_chart(T(fig), use_container_width=True)

    with col_r:
        df = query(f"""
            SELECT MONTH(CAST(Start_Time AS TIMESTAMP)) AS Month,
                   MONTHNAME(CAST(Start_Time AS TIMESTAMP)) AS Month_Name,
                   COUNT(*) AS Count
            FROM 'accidents.parquet' {WHERE}
            GROUP BY Month, Month_Name ORDER BY Month
        """)
        fig = px.line(df, x='Month_Name', y='Count', title='Month-wise Trend', markers=True, line_shape='spline')
        fig.update_traces(line_color='#F4A261', line_width=2.5, marker=dict(color='#E63946', size=8))
        st.plotly_chart(T(fig), use_container_width=True)

    st.markdown("---")

    col_l, col_r = st.columns(2)
    with col_l:
        df = query(f"SELECT State, COUNT(*) AS Count FROM 'accidents.parquet' {WHERE} GROUP BY State ORDER BY Count DESC LIMIT 10")
        fig = px.bar(df, x='Count', y='State', orientation='h', title='Top 10 Most Dangerous States')
        fig.update_traces(marker_color='#E63946', marker_line_color='rgba(0,0,0,0)')
        st.plotly_chart(T(fig), use_container_width=True)

    with col_r:
        df = query(f"SELECT MONTH(CAST(Start_Time AS TIMESTAMP)) AS Month, COUNT(*) AS Count FROM 'accidents.parquet' {WHERE} GROUP BY Month")
        df['Season'] = df['Month'].map({1:'Winter',2:'Winter',12:'Winter',3:'Spring',4:'Spring',5:'Spring',
                                         6:'Summer',7:'Summer',8:'Summer',9:'Fall',10:'Fall',11:'Fall'})
        s_df = df.groupby('Season')['Count'].sum().reset_index()
        fig = px.bar(s_df, x='Season', y='Count', title='Accidents by Season', color='Season',
                     color_discrete_map={'Winter':'#2EC4B6','Spring':'#44CF6C','Summer':'#F4A261','Fall':'#E63946'})
        fig.update_layout(showlegend=False)
        st.plotly_chart(T(fig), use_container_width=True)


# ═══════════════════════════════════════════════════════════════
#  GEOGRAPHIC
# ═══════════════════════════════════════════════════════════════
elif page == "Geographic":
    st.markdown("<h1>Geographic</h1>", unsafe_allow_html=True)

    df = prep_state(query(f"SELECT State, COUNT(*) AS Count FROM 'accidents.parquet' {WHERE} GROUP BY State ORDER BY Count DESC"))
    st.plotly_chart(make_choropleth(df, 'Count', 'Accident Count by State'), use_container_width=True)
    with st.expander("📊 View Data"): st.dataframe(df.drop(columns='State_Code'))
    st.markdown("---")

    df = prep_state(query(f"SELECT State, ROUND(AVG(Severity),2) AS Avg_Severity FROM 'accidents.parquet' {WHERE} GROUP BY State"))
    st.plotly_chart(make_choropleth(df, 'Avg_Severity', 'Average Severity by State'), use_container_width=True)
    with st.expander("📊 View Data"): st.dataframe(df.drop(columns='State_Code'))
    st.markdown("---")

    st.markdown("<p class='section-label'>STATE → CITY DRILLDOWN (CLICK TO EXPLORE)</p>", unsafe_allow_html=True)
    df_tree = query(f"""
        SELECT State, City, COUNT(*) AS Count
        FROM 'accidents.parquet' {WHERE}
        GROUP BY State, City HAVING Count > 500
        ORDER BY Count DESC LIMIT 300
    """)
    fig = px.treemap(df_tree, path=[px.Constant('USA'), 'State', 'City'],
                     values='Count', color='Count', color_continuous_scale='Reds',
                     title='State → City Accident Treemap — Click any State to Drill Down')
    fig.update_traces(textinfo='label+value', hovertemplate='<b>%{label}</b><br>Accidents: %{value:,}')
    fig.update_layout(**CHART_LAYOUT, height=600)
    st.plotly_chart(fig, use_container_width=True)
    st.markdown("---")

    st.markdown("<p class='section-label'>ACCIDENT HOTSPOT MAP (LAT/LONG)</p>", unsafe_allow_html=True)
    sample_size = st.slider("Sample size (dots on map)", 1000, 20000, 5000, step=1000)
    df_geo = query(f"""
        SELECT Start_Lat, Start_Lng, Severity
        FROM 'accidents.parquet' {WHERE}
        USING SAMPLE {sample_size}
    """)
    fig = px.scatter_geo(df_geo, lat='Start_Lat', lon='Start_Lng',
                         color='Severity', scope='usa',
                         color_continuous_scale='RdYlGn_r',
                         title=f'Accident Hotspot Map — {sample_size:,} Sampled Points',
                         opacity=0.5)
    fig.update_traces(marker=dict(size=3))
    fig.update_layout(**CHART_LAYOUT, height=580,
                      geo=dict(bgcolor='rgba(17,21,32,1)', lakecolor='rgba(17,21,32,1)',
                               landcolor='rgba(30,36,54,1)', showframe=False,
                               showcoastlines=True, coastlinecolor='rgba(255,255,255,0.1)'))
    st.plotly_chart(fig, use_container_width=True)
    st.markdown("---")

    col_l, col_r = st.columns(2)
    with col_l:
        df = query(f"SELECT City, COUNT(*) AS Count FROM 'accidents.parquet' {WHERE} GROUP BY City ORDER BY Count DESC LIMIT 20")
        fig = px.bar(df, x='Count', y='City', orientation='h', title='Top 20 Cities by Accident Count')
        fig.update_traces(marker_color='#E63946')
        fig.update_layout(height=650)
        st.plotly_chart(T(fig), use_container_width=True)

    with col_r:
        df = query(f"""
            SELECT City, ROUND(AVG(Severity),2) AS Avg_Severity
            FROM 'accidents.parquet' {WHERE}
            GROUP BY City HAVING COUNT(*) > 35000
            ORDER BY Avg_Severity DESC LIMIT 20
        """)
        fig = px.bar(df, x='Avg_Severity', y='City', orientation='h', title='Cities by Avg Severity (min 35k accidents)')
        fig.update_traces(marker_color='#F4A261')
        fig.update_layout(height=650)
        st.plotly_chart(T(fig), use_container_width=True)

    st.markdown("---")
    st.markdown("<p class='section-label'>DAY / NIGHT SPLIT BY STATE</p>", unsafe_allow_html=True)
    col_l, col_r = st.columns(2)
    for col, tod, label in [(col_l,'Day','☀️ Day Accidents'), (col_r,'Night','🌙 Night Accidents')]:
        with col:
            tod_filter = f"Sunrise_Sunset = '{tod}'"
            df = prep_state(query(f"SELECT State, COUNT(*) AS Count FROM 'accidents.parquet' {and_where(WHERE, tod_filter)} GROUP BY State"))
            st.plotly_chart(make_choropleth(df, 'Count', label), use_container_width=True)
            with st.expander(f"📊 {label} Data"):
                st.dataframe(df.drop(columns='State_Code').sort_values('Count', ascending=False))


# ═══════════════════════════════════════════════════════════════
#  TIME
# ═══════════════════════════════════════════════════════════════
elif page == "Time":
    st.markdown("<h1>Time Analysis</h1>", unsafe_allow_html=True)

    hour_range = st.slider("⏰ Filter Hour Range", 0, 23, (0, 23))
    HWHERE = and_where(WHERE, f"HOUR(CAST(Start_Time AS TIMESTAMP)) BETWEEN {hour_range[0]} AND {hour_range[1]}")

    df = query(f"""
        SELECT HOUR(CAST(Start_Time AS TIMESTAMP)) AS Hour, COUNT(*) AS Count
        FROM 'accidents.parquet' {HWHERE} GROUP BY Hour ORDER BY Hour
    """)
    fig = px.bar(df, x='Hour', y='Count', title='Accidents by Hour of Day')
    fig.update_traces(marker_color='#E63946', marker_line_color='rgba(0,0,0,0)')
    st.plotly_chart(T(fig), use_container_width=True)
    with st.expander("📊 View Data"): st.dataframe(df)
    st.markdown("---")

    st.markdown("<p class='section-label'>ACCIDENT CLOCK — POLAR VIEW</p>", unsafe_allow_html=True)
    df_polar = query(f"""
        SELECT HOUR(CAST(Start_Time AS TIMESTAMP)) AS Hour, COUNT(*) AS Count
        FROM 'accidents.parquet' {WHERE} GROUP BY Hour ORDER BY Hour
    """)
    df_polar['Hour_Label'] = df_polar['Hour'].apply(lambda h: f"{h:02d}:00")
    fig = px.bar_polar(df_polar, r='Count', theta='Hour_Label',
                       color='Count', color_continuous_scale='Reds',
                       title='Accidents by Hour — Polar Clock')
    fig.update_layout(**CHART_LAYOUT,
                      polar=dict(bgcolor='rgba(17,21,32,1)',
                                 angularaxis=dict(color='#7B8DB0', gridcolor='rgba(255,255,255,0.05)'),
                                 radialaxis=dict(color='#7B8DB0', gridcolor='rgba(255,255,255,0.05)')))
    st.plotly_chart(fig, use_container_width=True)
    st.markdown("---")

    col_l, col_r = st.columns(2)
    with col_l:
        df = query(f"SELECT DAYNAME(CAST(Start_Time AS TIMESTAMP)) AS Day, COUNT(*) AS Count FROM 'accidents.parquet' {WHERE} GROUP BY Day ORDER BY Count DESC")
        fig = px.bar(df, x='Count', y='Day', orientation='h', title='Accidents by Day of Week')
        fig.update_traces(marker_color='#F4A261')
        st.plotly_chart(T(fig), use_container_width=True)
        with st.expander("📊 View Data"): st.dataframe(df)

    with col_r:
        df = query(f"SELECT MONTHNAME(CAST(Start_Time AS TIMESTAMP)) AS Month, COUNT(*) AS Count FROM 'accidents.parquet' {WHERE} GROUP BY Month ORDER BY Count DESC")
        fig = px.bar(df, x='Month', y='Count', title='Accidents by Month')
        fig.update_traces(marker_color='#2EC4B6')
        st.plotly_chart(T(fig), use_container_width=True)
        with st.expander("📊 View Data"): st.dataframe(df)

    st.markdown("---")

    st.markdown("<p class='section-label'>ROLLING AVERAGE — SPOT ACCIDENT SPIKES</p>", unsafe_allow_html=True)
    df_roll = query(f"""
        SELECT CAST(Start_Time AS DATE) AS Date, COUNT(*) AS Count
        FROM 'accidents.parquet' {WHERE} GROUP BY Date ORDER BY Date
    """)
    df_roll['Rolling_Avg'] = df_roll['Count'].rolling(window=30, min_periods=1).mean()
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df_roll['Date'], y=df_roll['Count'],
                             mode='lines', name='Daily Count',
                             line=dict(color='rgba(230,57,70,0.25)', width=1)))
    fig.add_trace(go.Scatter(x=df_roll['Date'], y=df_roll['Rolling_Avg'],
                             mode='lines', name='30-Day Rolling Avg',
                             line=dict(color='#F4A261', width=2.5)))
    fig.update_layout(title='Daily Accidents with 30-Day Rolling Average', **CHART_LAYOUT)
    st.plotly_chart(fig, use_container_width=True)
    with st.expander("📊 View Data"): st.dataframe(df_roll)
    st.markdown("---")

    df = query(f"""
        SELECT DAYNAME(CAST(Start_Time AS TIMESTAMP)) AS Dayname,
               HOUR(CAST(Start_Time AS TIMESTAMP)) AS Hour, COUNT(*) AS Count
        FROM 'accidents.parquet' {WHERE} GROUP BY Dayname, Hour
    """)
    day_order = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
    fig = px.density_heatmap(df, x='Dayname', y='Hour', z='Count',
                             category_orders={'Dayname':day_order},
                             color_continuous_scale='Inferno', title='Hour vs Day Heatmap')
    fig.update_traces(xgap=3, ygap=3)
    fig.update_layout(height=600)
    st.plotly_chart(T(fig), use_container_width=True)
    with st.expander("📊 View Data"): st.dataframe(df)
    st.markdown("---")

    col_l, col_r = st.columns(2)
    with col_l:
        df = query(f"SELECT HOUR(CAST(Start_Time AS TIMESTAMP)) AS Hour, COUNT(*) AS Count FROM 'accidents.parquet' {WHERE} GROUP BY Hour")
        df['Type'] = df['Hour'].apply(lambda h: 'Rush Hour' if (7<=h<=9 or 16<=h<=19) else 'Non-Rush Hour')
        agg = df.groupby('Type')['Count'].sum().reset_index()
        fig = px.pie(agg, values='Count', names='Type', title='Rush Hour vs Non-Rush Hour', hole=0.55)
        fig.update_traces(marker=dict(colors=['#E63946','#9B5DE5'], line=dict(color='#111520', width=2)))
        st.plotly_chart(T(fig), use_container_width=True)

    with col_r:
        df = query(f"SELECT DAYNAME(CAST(Start_Time AS TIMESTAMP)) AS Day, COUNT(*) AS Count FROM 'accidents.parquet' {WHERE} GROUP BY Day")
        df['Type'] = df['Day'].apply(lambda d: 'Weekend' if d in ['Saturday','Sunday'] else 'Weekday')
        agg = df.groupby('Type')['Count'].sum().reset_index()
        fig = px.pie(agg, values='Count', names='Type', title='Weekend vs Weekday', hole=0.55)
        fig.update_traces(marker=dict(colors=['#F4A261','#2EC4B6'], line=dict(color='#111520', width=2)))
        st.plotly_chart(T(fig), use_container_width=True)


# ═══════════════════════════════════════════════════════════════
#  WEATHER
# ═══════════════════════════════════════════════════════════════
elif page == "Weather":
    st.markdown("<h1>Weather</h1>", unsafe_allow_html=True)

    top_conditions = query("""
        SELECT Weather_Condition FROM 'accidents.parquet'
        GROUP BY Weather_Condition ORDER BY COUNT(*) DESC LIMIT 20
    """)['Weather_Condition'].dropna().tolist()
    sel_weather = st.multiselect("🌤️ Filter Weather Conditions", top_conditions, placeholder="All Conditions")

    WWHERE = WHERE
    if sel_weather:
        wc = ",".join([f"'{w}'" for w in sel_weather])
        WWHERE = and_where(WHERE, f"Weather_Condition IN ({wc})")

    col_l, col_r = st.columns(2)
    with col_l:
        df = query(f"SELECT Weather_Condition, COUNT(*) AS Count FROM 'accidents.parquet' {WWHERE} GROUP BY Weather_Condition ORDER BY Count DESC LIMIT 15")
        fig = px.bar(df, x='Count', y='Weather_Condition', orientation='h', title='Top 15 Weather Conditions')
        fig.update_traces(marker_color='#E63946')
        fig.update_layout(height=500)
        st.plotly_chart(T(fig), use_container_width=True)
        with st.expander("📊 View Data"): st.dataframe(df)

    with col_r:
        df = query(f'SELECT Severity, AVG("Visibility(mi)") AS Avg_Visibility FROM \'accidents.parquet\' {WWHERE} GROUP BY Severity ORDER BY Severity')
        fig = px.bar(df, x='Severity', y='Avg_Visibility', title='Avg Visibility by Severity')
        fig.update_traces(marker_color='#2EC4B6')
        st.plotly_chart(T(fig), use_container_width=True)
        with st.expander("📊 View Data"): st.dataframe(df)

    st.markdown("---")

    # ── FIX: pre-fetch top-8 to avoid nested WHERE conflict ──
    st.markdown("<p class='section-label'>SEVERITY DISTRIBUTION ACROSS WEATHER — BOX PLOT</p>", unsafe_allow_html=True)
    top8 = query("""
        SELECT Weather_Condition FROM 'accidents.parquet'
        WHERE Weather_Condition IS NOT NULL
        GROUP BY Weather_Condition ORDER BY COUNT(*) DESC LIMIT 8
    """)['Weather_Condition'].dropna().tolist()
    top8_sql = ",".join([f"'{w}'" for w in top8])

    df_box = query(f"""
        SELECT Weather_Condition, Severity
        FROM 'accidents.parquet'
        {and_where(WWHERE, f"Weather_Condition IN ({top8_sql})")}
        USING SAMPLE 50000
    """)
    fig = px.box(df_box, x='Weather_Condition', y='Severity', color='Weather_Condition',
                 title='Severity Distribution by Top 8 Weather Conditions',
                 color_discrete_sequence=['#E63946','#F4A261','#2EC4B6','#9B5DE5','#44CF6C','#FFD166','#FF6B6B','#A8DADC'])
    fig.update_layout(showlegend=False, height=500)
    st.plotly_chart(T(fig), use_container_width=True)
    st.markdown("---")

    st.markdown("<p class='section-label'>DOES LOW VISIBILITY = MORE SEVERE ACCIDENTS?</p>", unsafe_allow_html=True)
    df_sc = query(f"""
        SELECT "Visibility(mi)" AS Visibility, Severity, COUNT(*) AS Count
        FROM 'accidents.parquet' {WWHERE}
        WHERE "Visibility(mi)" <= 15
        GROUP BY Visibility, Severity
    """)
    fig = px.scatter(df_sc, x='Visibility', y='Severity', size='Count', color='Severity',
                     color_continuous_scale='Reds', opacity=0.7,
                     title='Visibility vs Severity — Bubble Size = Accident Count', size_max=40)
    st.plotly_chart(T(fig), use_container_width=True)
    st.markdown("---")

    df = query(f"""
        SELECT Weather_Condition, Severity, COUNT(*) AS Count FROM 'accidents.parquet' {WWHERE}
        GROUP BY Weather_Condition, Severity HAVING Count > 50000 ORDER BY Severity DESC LIMIT 40
    """)
    fig = px.density_heatmap(df, x='Severity', y='Weather_Condition', z='Count',
                             color_continuous_scale='OrRd', title='Weather Condition × Severity Heatmap')
    fig.update_traces(xgap=3, ygap=3)
    fig.update_layout(height=600)
    st.plotly_chart(T(fig), use_container_width=True)
    with st.expander("📊 View Data"): st.dataframe(df)
    st.markdown("---")

    col_l, col_r = st.columns(2)
    with col_l:
        df = query(f'SELECT "Visibility(mi)" FROM \'accidents.parquet\' {WWHERE}')
        df = df[df['Visibility(mi)'] <= 20]
        fig = px.histogram(df, x='Visibility(mi)', nbins=30, title='Visibility Distribution')
        fig.update_traces(marker_color='#9B5DE5', marker_line_color='rgba(0,0,0,0)')
        fig.update_layout(bargap=0.05)
        st.plotly_chart(T(fig), use_container_width=True)

    with col_r:
        df = query(f'SELECT "Temperature(F)" FROM \'accidents.parquet\' {WWHERE}')
        bins   = [-89, 0, 50, 100, 207]
        labels = ['Very Low (<0°F)','Low (0-50°F)','Medium (50-100°F)','High (>100°F)']
        df['Temp_Category'] = pd.cut(df['Temperature(F)'], bins=bins, labels=labels)
        agg = df.groupby('Temp_Category', observed=True)['Temperature(F)'].count().reset_index(name='Count')
        fig = px.pie(agg, values='Count', names='Temp_Category', title='Temperature Category Distribution', hole=0.55)
        fig.update_traces(marker=dict(colors=['#2EC4B6','#44CF6C','#F4A261','#E63946'],
                                      line=dict(color='#111520', width=2)))
        st.plotly_chart(T(fig), use_container_width=True)

    st.markdown("---")
    col_l, col_r = st.columns(2)
    with col_l:
        df = query(f'SELECT Severity, AVG("Wind_Speed(mph)") AS Avg_Wind_Speed FROM \'accidents.parquet\' {WWHERE} GROUP BY Severity ORDER BY Severity')
        fig = px.bar(df, x='Severity', y='Avg_Wind_Speed', title='Avg Wind Speed by Severity')
        fig.update_traces(marker_color='#F4A261')
        st.plotly_chart(T(fig), use_container_width=True)

    with col_r:
        df = query(f'SELECT "Wind_Speed(mph)" FROM \'accidents.parquet\' {WWHERE}')
        fig = px.histogram(df, x='Wind_Speed(mph)', nbins=40, title='Wind Speed Distribution')
        fig.update_traces(marker_color='#44CF6C', marker_line_color='rgba(0,0,0,0)')
        fig.update_layout(bargap=0.05)
        st.plotly_chart(T(fig), use_container_width=True)


# ═══════════════════════════════════════════════════════════════
#  ROAD FEATURES
# ═══════════════════════════════════════════════════════════════
elif page == "Road Features":
    st.markdown("<h1>Road Features</h1>", unsafe_allow_html=True)

    FEATURES = ['Amenity','Bump','Crossing','Give_Way','Junction',
                'No_Exit','Railway','Roundabout','Station','Stop',
                'Traffic_Calming','Traffic_Signal']

    df_feat = query(f"SELECT Severity, {', '.join(FEATURES)} FROM 'accidents.parquet' {WHERE}")

    freq_df = pd.DataFrame(
        [(col, int(df_feat[col].sum())) for col in FEATURES],
        columns=['Feature','Count']
    ).sort_values('Count', ascending=False)

    fig = px.bar(freq_df, x='Count', y='Feature', orientation='h', title='Road Feature Frequency')
    fig.update_traces(marker_color='#E63946')
    fig.update_layout(bargap=0.3)
    st.plotly_chart(T(fig), use_container_width=True)
    with st.expander("📊 View Data"): st.dataframe(freq_df)
    st.markdown("---")

    col_l, col_r = st.columns(2)
    with col_l:
        sev_df = pd.DataFrame(
            [(col, df_feat[df_feat[col]==True]['Severity'].mean()) for col in FEATURES],
            columns=['Feature','Avg_Severity']
        ).sort_values('Avg_Severity', ascending=False)
        fig = px.bar(sev_df, x='Avg_Severity', y='Feature', orientation='h', title='Road Feature vs Avg Severity')
        fig.update_traces(marker_color='#F4A261')
        st.plotly_chart(T(fig), use_container_width=True)

    with col_r:
        rows = [{'Feature':col,
                 'Present': int(df_feat[df_feat[col]==True]['Severity'].count()),
                 'Absent':  int(df_feat[df_feat[col]==False]['Severity'].count())}
                for col in FEATURES]
        grp_df = pd.DataFrame(rows)
        fig = go.Figure()
        fig.add_trace(go.Bar(name='Feature Present', x=grp_df['Feature'], y=grp_df['Present'], marker_color='#E63946'))
        fig.add_trace(go.Bar(name='Feature Absent',  x=grp_df['Feature'], y=grp_df['Absent'],  marker_color='#2EC4B6'))
        fig.update_layout(barmode='group', title='Feature Present vs Absent — Accident Count', **CHART_LAYOUT)
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    st.markdown("<p class='section-label'>RADAR — ROAD FEATURES PRESENCE BY SEVERITY LEVEL</p>", unsafe_allow_html=True)
    radar_rows = []
    for sev in [1,2,3,4]:
        sub = df_feat[df_feat['Severity']==sev]
        row = {'Severity': f'Severity {sev}'}
        for col in FEATURES:
            row[col] = sub[col].mean() * 100
        radar_rows.append(row)
    radar_df = pd.DataFrame(radar_rows)

    colors_r = ['#44CF6C','#F4A261','#E63946','#9B5DE5']
    fig = go.Figure()
    for i, row in radar_df.iterrows():
        vals = [row[c] for c in FEATURES] + [row[FEATURES[0]]]
        fig.add_trace(go.Scatterpolar(
            r=vals, theta=FEATURES+[FEATURES[0]],
            fill='toself', name=row['Severity'],
            line_color=colors_r[i], opacity=0.7
        ))
    fig.update_layout(
        polar=dict(bgcolor='rgba(17,21,32,1)',
                   angularaxis=dict(color='#7B8DB0', gridcolor='rgba(255,255,255,0.05)'),
                   radialaxis=dict(color='#7B8DB0', gridcolor='rgba(255,255,255,0.05)', ticksuffix='%')),
        title='Road Feature Presence % by Severity Level',
        **CHART_LAYOUT, height=550
    )
    st.plotly_chart(fig, use_container_width=True)
    st.markdown("---")

    col_l, col_r = st.columns(2)
    with col_l:
        df = query(f"SELECT Junction, COUNT(*) AS Count FROM 'accidents.parquet' {WHERE} GROUP BY Junction")
        fig = px.bar(df, x='Junction', y='Count', title='Junction vs Non-Junction')
        fig.update_traces(marker_color='#9B5DE5')
        fig.update_layout(bargap=0.5)
        st.plotly_chart(T(fig), use_container_width=True)

        df = query(f"SELECT Crossing, AVG(Severity) AS Avg_Severity FROM 'accidents.parquet' {WHERE} GROUP BY Crossing")
        fig = px.bar(df, x='Crossing', y='Avg_Severity', title='Crossing vs Avg Severity')
        fig.update_traces(marker_color='#2EC4B6')
        fig.update_layout(bargap=0.5)
        st.plotly_chart(T(fig), use_container_width=True)

    with col_r:
        df = query(f"SELECT Traffic_Signal, AVG(Severity) AS Avg_Severity FROM 'accidents.parquet' {WHERE} GROUP BY Traffic_Signal")
        fig = px.bar(df, x='Traffic_Signal', y='Avg_Severity', title='Traffic Signal vs Avg Severity')
        fig.update_traces(marker_color='#44CF6C')
        fig.update_layout(bargap=0.5)
        st.plotly_chart(T(fig), use_container_width=True)

        df = query(f"SELECT Railway, COUNT(*) AS Count FROM 'accidents.parquet' {WHERE} GROUP BY Railway")
        fig = px.pie(df, values='Count', names='Railway', title='Railway vs Non-Railway', hole=0.55)
        fig.update_traces(marker=dict(colors=['#E63946','#2EC4B6'], line=dict(color='#111520', width=2)))
        st.plotly_chart(T(fig), use_container_width=True)

    st.markdown("---")
    df = query(f"SELECT Street, COUNT(*) AS Count FROM 'accidents.parquet' {WHERE} GROUP BY Street ORDER BY Count DESC LIMIT 10")
    fig = px.bar(df, x='Count', y='Street', orientation='h', title='Top 10 Most Dangerous Streets')
    fig.update_traces(marker_color='#E63946')
    st.plotly_chart(T(fig), use_container_width=True)
    with st.expander("📊 View Data"): st.dataframe(df)


# ═══════════════════════════════════════════════════════════════
#  RISK REPORT
# ═══════════════════════════════════════════════════════════════
elif page == "Risk Report":
    st.markdown("<h1>Risk Report</h1>", unsafe_allow_html=True)

    top_n = st.slider("🎯 Show Top N States", 5, 50, 10)

    df_all = query(f"""
        SELECT State, COUNT(*) AS Count, ROUND(AVG(Severity),2) AS Avg_Severity
        FROM 'accidents.parquet' {WHERE}
        GROUP BY State ORDER BY Count DESC
    """)

    col_l, col_r = st.columns(2)
    with col_l:
        fig = px.bar(df_all.head(top_n), x='Count', y='State', orientation='h',
                     title=f'Top {top_n} States by Accident Count')
        fig.update_traces(marker_color='#E63946')
        st.plotly_chart(T(fig), use_container_width=True)
        with st.expander("📊 View Data"): st.dataframe(df_all.head(top_n))

    with col_r:
        st.plotly_chart(table_fig(
            ['State','Total Accidents','Avg Severity'],
            [df_all['State'], df_all['Count'], df_all['Avg_Severity']],
            '#E63946', 'State-wise Accident Summary'
        ), use_container_width=True)

    st.markdown("---")

    st.markdown("<p class='section-label'>MULTI-VARIABLE STATE RISK — BUBBLE CHART</p>", unsafe_allow_html=True)
    df_s4 = query(f"SELECT State, COUNT(*) AS Sev4_Count FROM 'accidents.parquet' {and_where(WHERE,'Severity = 4')} GROUP BY State")
    df_bubble = df_all.merge(df_s4, on='State', how='left').fillna(0)
    fig = px.scatter(df_bubble, x='Count', y='Avg_Severity',
                     size='Sev4_Count', color='Avg_Severity',
                     hover_name='State', color_continuous_scale='Reds',
                     size_max=60, opacity=0.85,
                     title='State Risk Bubble — X: Total Accidents | Y: Avg Severity | Size: Severity-4 Count')
    fig.update_layout(height=550)
    st.plotly_chart(T(fig), use_container_width=True)
    st.markdown("---")

    yoy = query(f"""
        SELECT State, YEAR(CAST(Start_Time AS TIMESTAMP)) AS Year, COUNT(*) AS Count
        FROM 'accidents.parquet' {WHERE} GROUP BY State, Year
    """)
    yoy = yoy.sort_values(['State','Year'])
    yoy['YoY_Percent'] = yoy.groupby('State')['Count'].pct_change() * 100
    yoy = yoy.dropna()
    latest  = yoy.groupby('State').tail(1)
    top10   = latest.nlargest(top_n, 'YoY_Percent')
    bot10   = latest.nsmallest(top_n, 'YoY_Percent')
    plot_df = pd.concat([top10, bot10]).sort_values('YoY_Percent')
    plot_df['YoY_Percent'] = plot_df['YoY_Percent'].clip(-100, 100)

    fig = px.bar(plot_df, x='YoY_Percent', y='State', orientation='h',
                 title='Year-over-Year Accident Change (Top ↑ & Bottom ↓)',
                 color='YoY_Percent', color_continuous_scale='RdYlGn_r')
    fig.update_layout(height=650, xaxis_title='YoY Change (%)')
    fig.update_traces(text=plot_df['YoY_Percent'].round(1),
                      textposition='outside', textfont_color='#F0F4FF')
    st.plotly_chart(T(fig), use_container_width=True)
    st.markdown("---")

    # ── FIX: Waterfall — use increasing/decreasing instead of marker_color ──
    st.markdown("<p class='section-label'>WATERFALL — IMPROVED vs WORSENED STATES</p>", unsafe_allow_html=True)
    wf_df = latest.sort_values('YoY_Percent').head(20).copy()
    fig = go.Figure(go.Waterfall(
        x=wf_df['State'].tolist(),
        y=wf_df['YoY_Percent'].tolist(),
        measure=['relative'] * len(wf_df),
        increasing=dict(marker=dict(color='#E63946')),   # worsened (positive = more accidents)
        decreasing=dict(marker=dict(color='#44CF6C')),   # improved (negative = fewer accidents)
        connector=dict(line=dict(color='rgba(255,255,255,0.08)')),
        text=[f"{v:.1f}%" for v in wf_df['YoY_Percent']],
        textposition='outside',
    ))
    fig.update_layout(
        title='YoY Waterfall — Green = Improved ↓  |  Red = Worsened ↑',
        yaxis_title='YoY % Change',
        **CHART_LAYOUT, height=500
    )
    st.plotly_chart(fig, use_container_width=True)
    st.markdown("---")

    national_avg = query_one(f"SELECT AVG(Severity) FROM 'accidents.parquet' {WHERE}")
    above_avg    = df_all[df_all['Avg_Severity'] > national_avg]

    col_l, col_r = st.columns(2)
    with col_l:
        st.markdown(f"""
        <div style='background:var(--bg-card);border:1px solid var(--border);
             border-radius:16px;padding:24px;border-left:4px solid #E63946;margin-top:8px;'>
            <p style='font-size:11px;color:#7B8DB0;letter-spacing:.1em;margin:0 0 6px;
                      font-family:JetBrains Mono;'>NATIONAL AVERAGE SEVERITY</p>
            <p style='font-family:Bebas Neue;font-size:3.5rem;color:#E63946;margin:0;
                      letter-spacing:.05em;'>{national_avg:.4f}</p>
            <p style='font-size:13px;color:#7B8DB0;margin:8px 0 0;'>
                {len(above_avg)} states exceed this threshold</p>
        </div>""", unsafe_allow_html=True)

    with col_r:
        st.plotly_chart(table_fig(
            ['State','Accidents','Avg Severity'],
            [above_avg['State'], above_avg['Count'], above_avg['Avg_Severity'].round(3)],
            '#F4A261', 'States Exceeding National Avg Severity'
        ), use_container_width=True)

    with st.expander("📊 View Data"): st.dataframe(above_avg)
    st.markdown("---")

    df_s4_bar = query(f"SELECT State, COUNT(*) AS Count FROM 'accidents.parquet' {and_where(WHERE,'Severity = 4')} GROUP BY State ORDER BY Count DESC")
    fig = px.bar(df_s4_bar, x='Count', y='State', orientation='h', title='Severity 4 Accidents per State')
    fig.update_traces(marker_color='#E63946')
    fig.update_layout(height=900)
    st.plotly_chart(T(fig), use_container_width=True)
    with st.expander("📊 View Data"): st.dataframe(df_s4_bar)
    st.markdown("---")

    col_l, col_r = st.columns(2)
    with col_l:
        df_m = query(f"SELECT State, MONTHNAME(CAST(Start_Time AS TIMESTAMP)) AS Month_Name, COUNT(*) AS Count FROM 'accidents.parquet' {WHERE} GROUP BY State, Month_Name")
        peak_month = df_m.loc[df_m.groupby('State')['Count'].idxmax()]
        st.plotly_chart(table_fig(
            ['State','Peak Month','Count'],
            [peak_month['State'], peak_month['Month_Name'], peak_month['Count']],
            '#44CF6C', 'Peak Month per State'
        ), use_container_width=True)

    with col_r:
        df_h = query(f"SELECT State, HOUR(CAST(Start_Time AS TIMESTAMP)) AS Hour, COUNT(*) AS Count FROM 'accidents.parquet' {WHERE} GROUP BY State, Hour")
        peak_hour_df = df_h.loc[df_h.groupby('State')['Count'].idxmax()]
        st.plotly_chart(table_fig(
            ['State','Peak Hour','Count'],
            [peak_hour_df['State'], peak_hour_df['Hour'], peak_hour_df['Count']],
            '#2EC4B6', 'Peak Hour per State'
        ), use_container_width=True)











































