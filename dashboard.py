"""Community Financial Health Index Dashboard"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import json
from urllib.request import urlopen, Request
from src import analytics

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Community Financial Health Index",
    page_icon="",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ---------------------------------------------------------------------------
# Theme
# ---------------------------------------------------------------------------

OLIVE = "#6B7A2F"
TERRACOTTA = "#B85042"
GREEN = "#4A7C59"
GOLD = "#D4A843"
ORANGE = "#D47A43"
BG = "#F5F4F0"
WHITE = "#FFFFFF"
TEXT = "#1A1F1C"
MUTED = "#6B6B6B"

QUADRANT_COLORS = {
    "well_served": GREEN,
    "thin_access": GOLD,
    "strained": ORANGE,
    "financial_desert": TERRACOTTA,
}

QUADRANT_LABELS = {
    "well_served": "Well Served",
    "thin_access": "Thin Access",
    "strained": "Strained",
    "financial_desert": "Financial Desert",
}

QUADRANT_DESCRIPTIONS = {
    "well_served": "Low distress, strong access. These communities have adequate banking infrastructure and lower financial stress indicators.",
    "thin_access": "Low distress but limited access. Economically stable communities that still lack sufficient banking infrastructure.",
    "strained": "High distress but some access. Banking exists but economic pressures are high: poverty, complaints, unbanked populations.",
    "financial_desert": "High distress and low access. The most underserved communities, where banking gaps compound economic hardship.",
}

PLOTLY_CONFIG = {"displayModeBar": False}


# ---------------------------------------------------------------------------
# Score interpretation helpers
# ---------------------------------------------------------------------------

def access_label(score):
    """Translate Access Score to plain language."""
    if score is None:
        return "N/A", MUTED
    if score >= 75:
        return "Strong access", GREEN
    elif score >= 50:
        return "Adequate access", OLIVE
    elif score >= 25:
        return "Below average", ORANGE
    else:
        return "Very limited", TERRACOTTA


def distress_label(score):
    """Translate Distress Score to plain language."""
    if score is None:
        return "N/A", MUTED
    if score >= 75:
        return "Severe distress", TERRACOTTA
    elif score >= 50:
        return "High distress", ORANGE
    elif score >= 25:
        return "Moderate distress", GOLD
    else:
        return "Low distress", GREEN


def exclusion_label(score):
    """Translate Exclusion Score to plain language."""
    if score is None:
        return "N/A", MUTED
    if score > 40:
        return "Severely underserved", TERRACOTTA
    elif score > 20:
        return "Significantly underserved", ORANGE
    elif score > 0:
        return "Moderately underserved", GOLD
    elif score > -20:
        return "Roughly balanced", OLIVE
    else:
        return "Well served", GREEN


# ---------------------------------------------------------------------------
# CSS
# ---------------------------------------------------------------------------

st.markdown("""
<style>
    #MainMenu, footer, header {visibility: hidden;}

    .kpi-card {
        background: #FFFFFF;
        border: 1px solid #E0DDD8;
        border-radius: 12px;
        padding: 20px 24px;
        text-align: center;
    }
    .kpi-value {
        font-size: 2.2rem;
        font-weight: 700;
        color: #1A1F1C;
        line-height: 1.2;
    }
    .kpi-label {
        font-size: 0.85rem;
        color: #6B6B6B;
        margin-top: 4px;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    .quadrant-card {
        background: #FFFFFF;
        border: 1px solid #E0DDD8;
        border-radius: 10px;
        padding: 16px 20px;
    }
    .quadrant-title {
        font-size: 1rem;
        font-weight: 600;
        margin-bottom: 4px;
    }
    .quadrant-count {
        font-size: 1.8rem;
        font-weight: 700;
    }
    .quadrant-desc {
        font-size: 0.8rem;
        color: #6B6B6B;
        margin-top: 4px;
    }
    .county-header {
        font-size: 1.5rem;
        font-weight: 700;
        color: #1A1F1C;
        margin-bottom: 4px;
    }
    .county-sub {
        font-size: 0.9rem;
        color: #6B6B6B;
    }
    .score-card {
        background: #FFFFFF;
        border: 1px solid #E0DDD8;
        border-radius: 10px;
        padding: 16px 20px;
        text-align: center;
    }
    .score-value {
        font-size: 2rem;
        font-weight: 700;
        line-height: 1.2;
    }
    .score-label {
        font-size: 0.8rem;
        color: #6B6B6B;
        margin-top: 2px;
    }
    .rec-card {
        background: #FFFFFF;
        border-left: 4px solid #6B7A2F;
        border-radius: 0 8px 8px 0;
        padding: 12px 16px;
        margin-bottom: 8px;
    }
    .rec-title {
        font-weight: 600;
        font-size: 0.95rem;
        color: #1A1F1C;
    }
    .rec-desc {
        font-size: 0.85rem;
        color: #6B6B6B;
        margin-top: 2px;
    }
    .section-title {
        font-size: 1.15rem;
        font-weight: 600;
        color: #1A1F1C;
        margin: 24px 0 12px 0;
    }
    .methodology-block {
        background: #FFFFFF;
        border: 1px solid #E0DDD8;
        border-radius: 10px;
        padding: 20px 24px;
        margin-bottom: 16px;
    }
</style>
""", unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# GeoJSON for county choropleth (cached)
# ---------------------------------------------------------------------------

@st.cache_resource
def load_county_geojson():
    """Load county GeoJSON from Plotly's GitHub repo."""
    url = "https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json"
    try:
        req = Request(url, headers={"User-Agent": "FinHealthIndex/1.0"})
        with urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode())
    except Exception as e:
        st.warning(f"Could not load county boundaries: {e}")
        return None


# ---------------------------------------------------------------------------
# Data loading (cached)
# ---------------------------------------------------------------------------

@st.cache_data(ttl=3600)
def load_overview():
    return analytics.get_overview_stats()

@st.cache_data(ttl=3600)
def load_all_counties():
    return analytics.get_all_counties()

@st.cache_data(ttl=3600)
def load_top_excluded(n=10):
    return analytics.get_top_excluded_counties(limit=n)

@st.cache_data(ttl=3600)
def load_quadrant_dist():
    return analytics.get_quadrant_distribution()

@st.cache_data(ttl=3600)
def load_state_summary():
    return analytics.get_state_summary()

@st.cache_data(ttl=3600)
def load_scatter():
    return analytics.get_scatter_data()

@st.cache_data(ttl=3600)
def load_national_avgs():
    return analytics.get_national_averages()

@st.cache_data(ttl=3600)
def load_banking_deserts():
    return analytics.get_banking_deserts()

@st.cache_data(ttl=3600)
def load_desert_by_state():
    return analytics.get_desert_count_by_state()

@st.cache_data(ttl=3600)
def load_border_comparison():
    return analytics.get_border_comparison()

@st.cache_data(ttl=3600)
def load_county_detail(fips):
    return analytics.get_county_detail(fips)

@st.cache_data(ttl=3600)
def load_county_profile(fips):
    return analytics.get_county_financial_profile(fips)


# ---------------------------------------------------------------------------
# Helper: intervention recommendations
# ---------------------------------------------------------------------------

def get_interventions(row):
    """Rule-based intervention recommendations for a county."""
    recs = []
    is_desert = bool(row.get("is_banking_desert", False))
    is_at_risk = bool(row.get("is_at_risk_desert", False))
    poverty = row.get("poverty_rate") or 0
    unbanked = row.get("state_unbanked_pct") or 0
    complaints = row.get("state_complaints_per_100k") or 0
    quadrant = row.get("quadrant", "")
    branches = row.get("bank_branches") or 0
    pop = row.get("population") or 0
    pct_black = row.get("pct_black") or 0
    pct_hispanic = row.get("pct_hispanic") or 0
    minority_pct = pct_black + pct_hispanic

    # Banking desert + high poverty
    if is_desert and poverty > 15:
        recs.append({
            "title": "CDFI or credit union branch expansion",
            "desc": "Community development financial institutions can fill the gap left by commercial banks. Mobile banking units can serve as an interim bridge.",
        })

    # High complaints + high unbanked
    if complaints > 300 and unbanked > 4:
        recs.append({
            "title": "Financial literacy and consumer protection outreach",
            "desc": "High complaint rates combined with unbanked populations suggest residents need both access and education about their rights.",
        })

    # Strained quadrant
    if quadrant == "strained":
        recs.append({
            "title": "Matched savings programs (IDAs) and VITA free tax prep sites",
            "desc": "Individual Development Accounts match low-income savings 2:1 or more. VITA sites provide free tax preparation, helping residents claim earned income credits.",
        })

    # Low access + high minority population
    if (is_desert or is_at_risk) and minority_pct > 40:
        recs.append({
            "title": "CRA-motivated branch placement and multilingual financial services",
            "desc": "Community Reinvestment Act obligations can incentivize banks to open branches in underserved areas. Bilingual services reduce barriers for non-English speakers.",
        })

    # Rural + low branches (population < 25K and few branches)
    if pop < 25000 and branches <= 2:
        recs.append({
            "title": "Digital banking adoption paired with broadband expansion",
            "desc": "Rural communities with few branches need reliable internet access to use online banking. Federal broadband programs can support this transition.",
        })

    # Financial desert quadrant (catch-all)
    if quadrant == "financial_desert" and len(recs) == 0:
        recs.append({
            "title": "Comprehensive financial access intervention",
            "desc": "This county faces both high economic distress and limited banking access. A multi-pronged approach combining branch expansion, financial literacy, and safety-net programs is needed.",
        })

    # Default if nothing triggered
    if len(recs) == 0:
        recs.append({
            "title": "Monitor and maintain current financial infrastructure",
            "desc": "This county does not trigger high-priority intervention flags. Continue monitoring branch counts and economic indicators.",
        })

    return recs


# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------

tab1, tab2, tab3, tab4 = st.tabs(["Overview", "Explore", "Your County", "Under the Hood"])

# ===== TAB 1: OVERVIEW =====
with tab1:
    st.markdown("# Community Financial Health Index")
    st.markdown(
        "Every US county scored on two questions: **How much economic pain exists?** (Distress) "
        "and **How much banking infrastructure is available?** (Access). "
        "The gap between them shows where people are financially underserved."
    )

    overview = load_overview()
    if len(overview) > 0:
        row = overview.iloc[0]
        total = int(row["total_counties"])
        deserts = int(row["banking_desert_count"])
        exc = row["avg_exclusion_score"]
        exc_sev, exc_col = exclusion_label(exc)
        most_excl = row["most_excluded_state"]

        # KPI cards with plain-language context
        k1, k2, k3, k4 = st.columns(4)
        with k1:
            st.markdown(
                f'<div class="kpi-card">'
                f'<div class="kpi-value">{total:,}</div>'
                f'<div class="kpi-label">Counties Analyzed</div>'
                f'<div style="font-size:0.72rem;color:#6B6B6B;margin-top:4px;">Every US county with available data</div>'
                f'</div>',
                unsafe_allow_html=True,
            )
        with k2:
            st.markdown(
                f'<div class="kpi-card">'
                f'<div class="kpi-value" style="color: {TERRACOTTA};">{deserts:,}</div>'
                f'<div class="kpi-label">Banking Deserts</div>'
                f'<div style="font-size:0.72rem;color:#6B6B6B;margin-top:4px;">Counties with zero bank branches</div>'
                f'</div>',
                unsafe_allow_html=True,
            )
        with k3:
            st.markdown(
                f'<div class="kpi-card">'
                f'<div class="kpi-value" style="color: {exc_col};">{exc:+.1f}</div>'
                f'<div class="kpi-label">Avg Exclusion Score</div>'
                f'<div style="font-size:0.8rem;font-weight:600;margin-top:2px;" style="color: {exc_col};">{exc_sev}</div>'
                f'<div style="font-size:0.72rem;color:#6B6B6B;margin-top:4px;">Positive = distress exceeds access</div>'
                f'</div>',
                unsafe_allow_html=True,
            )
        with k4:
            st.markdown(
                f'<div class="kpi-card">'
                f'<div class="kpi-value" style="color: {TERRACOTTA};">{most_excl}</div>'
                f'<div class="kpi-label">Most Excluded State</div>'
                f'<div style="font-size:0.72rem;color:#6B6B6B;margin-top:4px;">Highest avg exclusion score</div>'
                f'</div>',
                unsafe_allow_html=True,
            )

    # Choropleth map
    st.markdown('<div class="section-title">Financial Exclusion by County</div>', unsafe_allow_html=True)
    all_counties = load_all_counties()
    geojson = load_county_geojson()

    if geojson is not None and len(all_counties) > 0:
        fig_map = px.choropleth(
            all_counties,
            geojson=geojson,
            locations="fips",
            color="exclusion_score",
            color_continuous_scale=[
                [0, GREEN],
                [0.5, "#F5F4F0"],
                [1, TERRACOTTA],
            ],
            scope="usa",
            hover_name="name",
            hover_data={
                "state": True,
                "exclusion_score": ":.1f",
                "access_score": ":.1f",
                "distress_score": ":.1f",
                "population": ":,",
                "fips": False,
            },
            labels={
                "exclusion_score": "Exclusion Score",
                "access_score": "Access Score",
                "distress_score": "Distress Score",
            },
        )
        fig_map.update_layout(
            margin=dict(l=0, r=0, t=0, b=0),
            geo=dict(
                bgcolor="rgba(0,0,0,0)",
                lakecolor="#E8EEF3",
                landcolor="#F0F0ED",
            ),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            coloraxis_colorbar=dict(
                title="Exclusion",
                thickness=12,
                len=0.5,
            ),
            height=500,
        )
        st.plotly_chart(fig_map, use_container_width=True, config=PLOTLY_CONFIG)
    else:
        st.info("County map requires GeoJSON data. Check your internet connection.")

    # Quadrant summary (2x2)
    st.markdown('<div class="section-title">Quadrant Summary</div>', unsafe_allow_html=True)
    quad_df = load_quadrant_dist()
    if len(quad_df) > 0:
        quad_map = {}
        for _, qr in quad_df.iterrows():
            quad_map[qr["quadrant"]] = qr

        q1, q2 = st.columns(2)
        q3, q4 = st.columns(2)

        for col, qname in [(q1, "well_served"), (q2, "thin_access"), (q3, "strained"), (q4, "financial_desert")]:
            with col:
                qdata = quad_map.get(qname)
                count = int(qdata["count"]) if qdata is not None else 0
                color = QUADRANT_COLORS.get(qname, MUTED)
                label = QUADRANT_LABELS.get(qname, qname)
                desc = QUADRANT_DESCRIPTIONS.get(qname, "")
                st.markdown(
                    f'<div class="quadrant-card">'
                    f'<div class="quadrant-title" style="color: {color};">{label}</div>'
                    f'<div class="quadrant-count">{count:,}</div>'
                    f'<div class="quadrant-desc">{desc}</div>'
                    f'</div>',
                    unsafe_allow_html=True,
                )

    # Top 10 most excluded counties
    st.markdown('<div class="section-title">Top 10 Most Excluded Counties</div>', unsafe_allow_html=True)
    top10 = load_top_excluded(10)
    if len(top10) > 0:
        display_df = top10[["name", "state", "population", "exclusion_score", "access_score", "distress_score", "quadrant"]].copy()
        display_df.columns = ["County", "State", "Population", "Exclusion", "Access", "Distress", "Quadrant"]
        display_df["Population"] = display_df["Population"].apply(lambda x: f"{int(x):,}" if pd.notna(x) else "N/A")
        display_df["Quadrant"] = display_df["Quadrant"].map(QUADRANT_LABELS)
        st.dataframe(display_df, use_container_width=True, hide_index=True)

    # Banking desert count by state
    st.markdown('<div class="section-title">Banking Deserts by State</div>', unsafe_allow_html=True)
    desert_states = load_desert_by_state()
    if len(desert_states) > 0:
        fig_bar = px.bar(
            desert_states.head(20),
            y="state",
            x="desert_count",
            orientation="h",
            color_discrete_sequence=[TERRACOTTA],
            hover_data={"affected_population": ":,"},
            labels={"desert_count": "Banking Deserts", "state": "State", "affected_population": "Affected Pop."},
        )
        fig_bar.update_layout(
            margin=dict(l=0, r=20, t=10, b=0),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            yaxis=dict(autorange="reversed"),
            height=max(300, len(desert_states.head(20)) * 25),
        )
        st.plotly_chart(fig_bar, use_container_width=True, config=PLOTLY_CONFIG)


# ===== TAB 2: EXPLORE =====
with tab2:
    st.markdown("## Explore Counties")
    st.markdown(
        "Each dot is a county. **Right side** = more economic pain. **Bottom** = fewer banks. "
        "Counties in the **bottom-right** are the most underserved."
    )

    scatter_df = load_scatter()
    if len(scatter_df) == 0:
        st.warning("No data available. Run the data pipeline first.")
    else:
        # Filters
        f1, f2, f3 = st.columns(3)
        with f1:
            states_list = sorted(scatter_df["state"].unique().tolist())
            selected_states = st.multiselect("Filter by state", states_list, default=[])
        with f2:
            pop_range = st.slider(
                "Population range",
                min_value=0,
                max_value=int(scatter_df["population"].max()),
                value=(0, int(scatter_df["population"].max())),
                step=1000,
            )
        with f3:
            deserts_only = st.toggle("Show only banking deserts", value=False)

        # Apply filters
        filtered = scatter_df.copy()
        if selected_states:
            filtered = filtered[filtered["state"].isin(selected_states)]
        filtered = filtered[
            (filtered["population"] >= pop_range[0]) &
            (filtered["population"] <= pop_range[1])
        ]
        if deserts_only:
            filtered = filtered[filtered["is_banking_desert"] == True]

        # Scatter plot: Distress (x) vs Access (y)
        st.markdown('<div class="section-title">Distress vs Access</div>', unsafe_allow_html=True)
        if len(filtered) > 0:
            filtered["quadrant_label"] = filtered["quadrant"].map(QUADRANT_LABELS)
            fig_scatter = px.scatter(
                filtered,
                x="distress_score",
                y="access_score",
                color="quadrant",
                color_discrete_map=QUADRANT_COLORS,
                size="population",
                size_max=20,
                hover_name="name",
                hover_data={
                    "state": True,
                    "population": ":,",
                    "exclusion_score": ":.1f",
                    "quadrant": False,
                    "quadrant_label": True,
                },
                labels={
                    "distress_score": "Distress Score (higher = more distress)",
                    "access_score": "Access Score (higher = better access)",
                    "quadrant_label": "Quadrant",
                },
                opacity=0.65,
            )
            # Add quadrant divider lines
            fig_scatter.add_hline(y=50, line_dash="dash", line_color="#999", line_width=1)
            fig_scatter.add_vline(x=50, line_dash="dash", line_color="#999", line_width=1)

            # Quadrant labels
            # x = distress (higher = worse), y = access (higher = better)
            # well_served: low distress (left), high access (top)
            fig_scatter.add_annotation(x=25, y=95, text="Well Served", showarrow=False, font=dict(color=GREEN, size=11))
            # thin_access: low distress (left), low access (bottom)
            fig_scatter.add_annotation(x=25, y=5, text="Thin Access", showarrow=False, font=dict(color=GOLD, size=11))
            # strained: high distress (right), high access (top)
            fig_scatter.add_annotation(x=75, y=95, text="Strained", showarrow=False, font=dict(color=ORANGE, size=11))
            # financial_desert: high distress (right), low access (bottom)
            fig_scatter.add_annotation(x=75, y=5, text="Financial Desert", showarrow=False, font=dict(color=TERRACOTTA, size=11))

            fig_scatter.update_layout(
                margin=dict(l=0, r=0, t=20, b=0),
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                legend_title_text="Quadrant",
                height=550,
                xaxis=dict(range=[0, 100], gridcolor="#E0DDD8"),
                yaxis=dict(range=[0, 100], gridcolor="#E0DDD8"),
            )
            # Rename legend entries
            for trace in fig_scatter.data:
                if trace.name in QUADRANT_LABELS:
                    trace.name = QUADRANT_LABELS[trace.name]

            st.plotly_chart(fig_scatter, use_container_width=True, config=PLOTLY_CONFIG)
        else:
            st.info("No counties match your filters.")

        # Searchable data table
        st.markdown('<div class="section-title">County Data</div>', unsafe_allow_html=True)
        table_df = filtered[
            ["name", "state", "population", "access_score", "distress_score", "exclusion_score", "quadrant", "bank_branches", "branches_per_10k", "median_income", "poverty_rate"]
        ].copy()
        table_df["quadrant"] = table_df["quadrant"].map(QUADRANT_LABELS)
        table_df.columns = [
            "County", "State", "Population", "Access", "Distress", "Exclusion",
            "Quadrant", "Branches", "Branches/10K", "Income", "Poverty %",
        ]
        st.dataframe(table_df, use_container_width=True, hide_index=True, height=400)


# ===== TAB 3: YOUR COUNTY =====
with tab3:
    st.markdown("## Your County")

    all_data = load_all_counties()
    if len(all_data) == 0:
        st.warning("No data available. Run the data pipeline first.")
    else:
        # Build search options
        options = all_data.apply(
            lambda r: f"{r['name']}, {r['state']} ({r['fips']})", axis=1
        ).tolist()
        fips_list = all_data["fips"].tolist()

        selected = st.selectbox("Search for a county", options, index=None, placeholder="Type a county name...")

        if selected:
            sel_fips = selected.split("(")[-1].rstrip(")")
            detail = load_county_detail(sel_fips)
            natl = load_national_avgs()

            if len(detail) > 0:
                c = detail.iloc[0]
                natl_row = natl.iloc[0] if len(natl) > 0 else {}

                # County header
                quad_color = QUADRANT_COLORS.get(c.get("quadrant", ""), MUTED)
                quad_label = QUADRANT_LABELS.get(c.get("quadrant", ""), c.get("quadrant", ""))
                st.markdown(
                    f'<div class="county-header">{c["name"]}</div>'
                    f'<div class="county-sub">'
                    f'{c["state"]} | Pop. {int(c["population"]):,} | '
                    f'Income ${int(c["median_income"]):,} | '
                    f'Poverty {c["poverty_rate"]:.1f}% | '
                    f'<span style="color: {quad_color}; font-weight: 600;">{quad_label}</span>'
                    f'</div>',
                    unsafe_allow_html=True,
                ) if pd.notna(c.get("median_income")) else st.markdown(
                    f'<div class="county-header">{c["name"]}</div>'
                    f'<div class="county-sub">'
                    f'{c["state"]} | Pop. {int(c["population"]):,} | '
                    f'<span style="color: {quad_color}; font-weight: 600;">{quad_label}</span>'
                    f'</div>',
                    unsafe_allow_html=True,
                )

                st.markdown("")

                # Score cards with plain-language labels
                s1, s2, s3 = st.columns(3)
                with s1:
                    acc = c.get("access_score", 0) or 0
                    a_sev, a_col = access_label(acc)
                    st.markdown(
                        f'<div class="score-card">'
                        f'<div class="score-label">How much banking exists?</div>'
                        f'<div class="score-value" style="color: {a_col};">{acc:.0f} / 100</div>'
                        f'<div style="font-size:0.8rem;font-weight:600;margin-top:2px;" style="color: {a_col};">{a_sev}</div>'
                        f'<div style="font-size:0.72rem;color:#6B6B6B;margin-top:4px;">Based on branches per capita, deposits, unbanked rate</div>'
                        f'</div>',
                        unsafe_allow_html=True,
                    )
                with s2:
                    dist = c.get("distress_score", 0) or 0
                    d_sev, d_col = distress_label(dist)
                    st.markdown(
                        f'<div class="score-card">'
                        f'<div class="score-label">How much financial pain?</div>'
                        f'<div class="score-value" style="color: {d_col};">{dist:.0f} / 100</div>'
                        f'<div style="font-size:0.8rem;font-weight:600;margin-top:2px;" style="color: {d_col};">{d_sev}</div>'
                        f'<div style="font-size:0.72rem;color:#6B6B6B;margin-top:4px;">Based on poverty, complaints, unbanked, income</div>'
                        f'</div>',
                        unsafe_allow_html=True,
                    )
                with s3:
                    exclusion = c.get("exclusion_score", 0) or 0
                    e_sev, e_col = exclusion_label(exclusion)
                    st.markdown(
                        f'<div class="score-card">'
                        f'<div class="score-label">The gap</div>'
                        f'<div class="score-value" style="color: {e_col};">{exclusion:+.0f}</div>'
                        f'<div style="font-size:0.8rem;font-weight:600;margin-top:2px;" style="color: {e_col};">{e_sev}</div>'
                        f'<div style="font-size:0.72rem;color:#6B6B6B;margin-top:4px;">Positive = pain exceeds access. Negative = well served.</div>'
                        f'</div>',
                        unsafe_allow_html=True,
                    )

                st.markdown("")

                # Banking profile
                st.markdown('<div class="section-title">Banking Profile</div>', unsafe_allow_html=True)

                bp1, bp2 = st.columns(2)
                with bp1:
                    branches = int(c.get("bank_branches", 0) or 0)
                    bper10k = c.get("branches_per_10k", 0) or 0
                    natl_bper10k = natl_row.get("avg_branches_per_10k", 3) or 3
                    deposits = c.get("total_deposits", 0) or 0

                    desert_status = ""
                    if c.get("is_banking_desert"):
                        desert_status = '<span style="color: #B85042; font-weight: 600;">Banking Desert</span>'
                    elif c.get("is_at_risk_desert"):
                        desert_status = '<span style="color: #D47A43; font-weight: 600;">At-Risk Desert</span>'
                    else:
                        desert_status = '<span style="color: #4A7C59;">Adequate</span>'

                    st.markdown(f"**Branches:** {branches}")
                    st.markdown(f"**Branches per 10K pop:** {bper10k:.1f} (national avg: {natl_bper10k:.1f})")
                    st.markdown(f"**Total deposits:** ${deposits:,.0f}")
                    st.markdown(f"**Desert status:** {desert_status}", unsafe_allow_html=True)

                with bp2:
                    # Bar chart comparing county vs national
                    metrics = ["Branches/10K", "Unbanked %", "Underbanked %"]
                    county_vals = [
                        bper10k,
                        c.get("state_unbanked_pct", 0) or 0,
                        c.get("state_underbanked_pct", 0) or 0,
                    ]
                    natl_vals = [
                        natl_row.get("avg_branches_per_10k", 3) or 3,
                        natl_row.get("avg_unbanked", 4.2) or 4.2,
                        natl_row.get("avg_underbanked", 14.2) or 14.2,
                    ]

                    fig_bank = go.Figure()
                    fig_bank.add_trace(go.Bar(
                        name=c["name"],
                        x=metrics,
                        y=county_vals,
                        marker_color=OLIVE,
                    ))
                    fig_bank.add_trace(go.Bar(
                        name="National Avg",
                        x=metrics,
                        y=natl_vals,
                        marker_color="#C0C0C0",
                    ))
                    fig_bank.update_layout(
                        barmode="group",
                        margin=dict(l=0, r=0, t=10, b=0),
                        paper_bgcolor="rgba(0,0,0,0)",
                        plot_bgcolor="rgba(0,0,0,0)",
                        legend=dict(orientation="h", yanchor="bottom", y=1.02),
                        height=250,
                        yaxis=dict(gridcolor="#E0DDD8"),
                    )
                    st.plotly_chart(fig_bank, use_container_width=True, config=PLOTLY_CONFIG)

                # Economic profile
                st.markdown('<div class="section-title">Economic Profile</div>', unsafe_allow_html=True)

                ep1, ep2 = st.columns(2)
                with ep1:
                    income = c.get("median_income")
                    poverty = c.get("poverty_rate")
                    natl_income = natl_row.get("avg_income")
                    natl_poverty = natl_row.get("avg_poverty")

                    if pd.notna(income):
                        st.markdown(f"**Median income:** ${int(income):,} (national avg: ${int(natl_income):,})" if pd.notna(natl_income) else f"**Median income:** ${int(income):,}")
                    if pd.notna(poverty):
                        st.markdown(f"**Poverty rate:** {poverty:.1f}% (national avg: {natl_poverty:.1f}%)" if pd.notna(natl_poverty) else f"**Poverty rate:** {poverty:.1f}%")

                    compl = c.get("state_complaints_per_100k")
                    natl_compl = natl_row.get("avg_complaints_per_100k")
                    if pd.notna(compl):
                        st.markdown(f"**Complaints per 100K (state):** {compl:.1f} (national avg: {natl_compl:.1f})" if pd.notna(natl_compl) else f"**Complaints per 100K (state):** {compl:.1f}")

                    top_compl = c.get("state_top_complaint")
                    if pd.notna(top_compl) and top_compl != "Unknown":
                        st.markdown(f"**Top complaint type (state):** {top_compl}")

                with ep2:
                    # Economic comparison bars
                    econ_metrics = ["Poverty %", "Complaints/100K"]
                    econ_county = [
                        c.get("poverty_rate", 0) or 0,
                        (c.get("state_complaints_per_100k", 0) or 0) / 10,  # scale down for display
                    ]
                    econ_natl = [
                        natl_row.get("avg_poverty", 13) or 13,
                        (natl_row.get("avg_complaints_per_100k", 200) or 200) / 10,
                    ]
                    fig_econ = go.Figure()
                    fig_econ.add_trace(go.Bar(
                        name=c["name"],
                        x=econ_metrics,
                        y=econ_county,
                        marker_color=OLIVE,
                    ))
                    fig_econ.add_trace(go.Bar(
                        name="National Avg",
                        x=econ_metrics,
                        y=econ_natl,
                        marker_color="#C0C0C0",
                    ))
                    fig_econ.update_layout(
                        barmode="group",
                        margin=dict(l=0, r=0, t=10, b=0),
                        paper_bgcolor="rgba(0,0,0,0)",
                        plot_bgcolor="rgba(0,0,0,0)",
                        legend=dict(orientation="h", yanchor="bottom", y=1.02),
                        height=250,
                        yaxis=dict(gridcolor="#E0DDD8"),
                    )
                    st.plotly_chart(fig_econ, use_container_width=True, config=PLOTLY_CONFIG)

                # Intervention recommendations
                st.markdown('<div class="section-title">Intervention Recommendations</div>', unsafe_allow_html=True)
                recs = get_interventions(c.to_dict())
                for rec in recs:
                    st.markdown(
                        f'<div class="rec-card">'
                        f'<div class="rec-title">{rec["title"]}</div>'
                        f'<div class="rec-desc">{rec["desc"]}</div>'
                        f'</div>',
                        unsafe_allow_html=True,
                    )


# ===== TAB 4: UNDER THE HOOD =====
with tab4:
    st.markdown("## Under the Hood")

    # Methodology
    st.markdown(
        '<div class="methodology-block">'
        '<h4 style="margin-top: 0;">Methodology</h4>'
        "<p>The Community Financial Health Index combines four public data sources "
        "to measure how well American counties are served by the financial system. "
        "Each county receives three scores:</p>"
        "<ul>"
        "<li><strong>Access Score (0-100, higher = better):</strong> "
        "Weighted composite of bank branches per 10K population (50%), "
        "total deposits (20%), and inverse unbanked rate (30%). "
        "Computed using percentile ranks across all counties.</li>"
        "<li><strong>Distress Score (0-100, higher = worse):</strong> "
        "Weighted composite of poverty rate (40%), consumer complaints per 100K (25%), "
        "unbanked rate (20%), and inverse income (15%). "
        "Computed using percentile ranks.</li>"
        "<li><strong>Exclusion Score:</strong> Distress minus Access. "
        "Positive values indicate communities where economic distress outpaces "
        "financial access. Negative values indicate well-served communities.</li>"
        "</ul>"
        "</div>",
        unsafe_allow_html=True,
    )

    # Data sources
    st.markdown(
        '<div class="methodology-block">'
        '<h4 style="margin-top: 0;">Data Sources</h4>'
        "<ul>"
        '<li><strong>FDIC Summary of Deposits:</strong> Bank branch locations and deposit data. '
        '<a href="https://banks.data.fdic.gov/api/locations">banks.data.fdic.gov</a></li>'
        '<li><strong>CFPB Consumer Complaint Database:</strong> Consumer financial complaints by state. '
        '<a href="https://www.consumerfinance.gov/data-research/consumer-complaints/">consumerfinance.gov</a></li>'
        '<li><strong>Census ACS 5-Year Estimates:</strong> Population, income, poverty, and race by county. '
        '<a href="https://data.census.gov">data.census.gov</a></li>'
        '<li><strong>FDIC National Survey of Unbanked/Underbanked Households (2023):</strong> '
        "State and regional unbanked/underbanked rates from the published \"How America Banks\" report. "
        '<a href="https://www.fdic.gov/analysis/household-survey">fdic.gov/analysis/household-survey</a></li>'
        "</ul>"
        "</div>",
        unsafe_allow_html=True,
    )

    # Limitations
    st.markdown(
        '<div class="methodology-block">'
        '<h4 style="margin-top: 0;">Known Limitations</h4>'
        "<ul>"
        "<li><strong>CFPB ZIP codes are truncated to 3 digits.</strong> "
        "County-level complaint rates are impossible, so we use state-level aggregates. "
        "This means all counties in a state share the same complaint rate.</li>"
        "<li><strong>FDIC unbanked/underbanked rates are state-level.</strong> "
        "The FDIC survey publishes state and regional estimates, not county-level data. "
        "All counties in a state share the same unbanked rate.</li>"
        "<li><strong>Credit unions are not included.</strong> "
        "FDIC data covers only FDIC-insured bank branches. Credit unions (NCUA-regulated) "
        "are a significant source of financial access in many communities but are not in this dataset.</li>"
        "<li><strong>No HMDA mortgage data.</strong> "
        "Home Mortgage Disclosure Act data would add lending patterns and denial rates "
        "but is not yet integrated.</li>"
        "<li><strong>Banking desert definition is simple.</strong> "
        "A county with 0 FDIC branches is flagged as a banking desert. "
        "This does not account for proximity to branches in adjacent counties.</li>"
        "</ul>"
        "</div>",
        unsafe_allow_html=True,
    )

    # Quadrant definitions
    st.markdown(
        '<div class="methodology-block">'
        '<h4 style="margin-top: 0;">Quadrant Definitions</h4>'
        "<table>"
        "<tr><th>Quadrant</th><th>Criteria</th><th>Interpretation</th></tr>"
        f'<tr><td style="color: {GREEN}; font-weight: 600;">Well Served</td>'
        "<td>Distress &le; 50, Access &ge; 50</td>"
        "<td>Low financial stress with adequate banking infrastructure</td></tr>"
        f'<tr><td style="color: {GOLD}; font-weight: 600;">Thin Access</td>'
        "<td>Distress &le; 50, Access &lt; 50</td>"
        "<td>Economically stable but underserved by banks</td></tr>"
        f'<tr><td style="color: {ORANGE}; font-weight: 600;">Strained</td>'
        "<td>Distress &gt; 50, Access &ge; 50</td>"
        "<td>Banks present but economic conditions are stressed</td></tr>"
        f'<tr><td style="color: {TERRACOTTA}; font-weight: 600;">Financial Desert</td>'
        "<td>Distress &gt; 50, Access &lt; 50</td>"
        "<td>Both high economic distress and limited financial access</td></tr>"
        "</table>"
        "</div>",
        unsafe_allow_html=True,
    )

    # Border comparison
    st.markdown('<div class="section-title">Border vs Non-Border Comparison</div>', unsafe_allow_html=True)
    border_df = load_border_comparison()
    if len(border_df) > 0:
        st.dataframe(
            border_df.rename(columns={
                "category": "Category",
                "county_count": "Counties",
                "avg_access": "Avg Access",
                "avg_distress": "Avg Distress",
                "avg_exclusion": "Avg Exclusion",
                "avg_income": "Avg Income",
                "avg_poverty": "Avg Poverty %",
                "avg_branches_per_10k": "Avg Branches/10K",
                "avg_unbanked": "Avg Unbanked %",
                "banking_deserts": "Banking Deserts",
            }),
            use_container_width=True,
            hide_index=True,
        )

    # Future expansion
    st.markdown(
        '<div class="methodology-block">'
        '<h4 style="margin-top: 0;">Future Expansion</h4>'
        "<ul>"
        "<li><strong>HMDA:</strong> Home Mortgage Disclosure Act data for lending patterns and denial rates</li>"
        "<li><strong>CRA:</strong> Community Reinvestment Act ratings for bank performance in underserved areas</li>"
        "<li><strong>NCUA:</strong> Credit union branch locations to complete the financial access picture</li>"
        "<li><strong>FCC Broadband:</strong> Internet access data, since digital banking requires connectivity</li>"
        "<li><strong>Fed Banking Deserts:</strong> Federal Reserve research on banking desert definitions and impacts</li>"
        "<li><strong>County-level CFPB:</strong> If CFPB restores full ZIP codes, enable county-level complaint mapping</li>"
        "</ul>"
        "</div>",
        unsafe_allow_html=True,
    )

    # Footer
    st.markdown("---")
    st.markdown(
        '<p style="color: #6B6B6B; font-size: 0.8rem; text-align: center;">'
        "Community Financial Health Index | "
        "Built by Sebastian Becerra | "
        "Data: FDIC, CFPB, Census ACS, FDIC Household Survey"
        "</p>",
        unsafe_allow_html=True,
    )
