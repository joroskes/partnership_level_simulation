import streamlit as st
import pandas as pd
import io

st.set_page_config(page_title="Pharmacy Partnership Analyzer", layout="wide")

st.title("Pharmacy Partnership Analyzer")

# --- Initialize session state for runs ---
if "runs" not in st.session_state:
    st.session_state["runs"] = {}

# --- Upload file ---
uploaded_file = st.file_uploader("Upload your Excel, CSV or Parquet file", type=["csv", "xlsx", "parquet"])

if uploaded_file:
    # --- Load data ---
    if uploaded_file.name.endswith(".csv"):
        df = pd.read_csv(uploaded_file)
    elif uploaded_file.name.endswith(".xlsx"):
        df = pd.read_excel(uploaded_file)
    elif uploaded_file.name.endswith(".parquet"):
        df = pd.read_parquet(uploaded_file)
    else:
        st.error("Unsupported file format")
        st.stop()

    st.success("File loaded successfully!")

    # --- Filters ---
    st.header("Filters (optional)")
    channels = products = []
    if 'Channel' in df.columns:
        channels = st.multiselect("Channel", sorted(df['Channel'].unique()))
    if 'Product_Type' in df.columns:
        products = st.multiselect("Product_Type", sorted(df['Product_Type'].unique()))
    if 'Causale' in df.columns:
        causale = st.multiselect("Causale", sorted(df['Causale'].unique()))
    if 'Canale' in df.columns:
        canale = st.multiselect("Canale", sorted(df['Canale'].unique()))
    if 'Out of Scope \nFilter' in df.columns:
        scope_filter = st.multiselect("Out of Scope \nFilter", sorted(df['Out of Scope \nFilter'].unique()))


    # Apply filters
    df_filtered = df.copy()
    if channels:
        df_filtered = df_filtered[df_filtered['Channel'].isin(channels)]
    if products:
        df_filtered = df_filtered[df_filtered['Product_Type'].isin(products)]
    if causale:
        df_filtered = df_filtered[df_filtered['Causale'].isin(causale)]
    if canale:
        df_filtered = df_filtered[df_filtered['Canale'].isin(canale)]
    if scope_filter:
        df_filtered = df_filtered[df_filtered['Out of Scope \nFilter'].isin(scope_filter)]

    st.write(f"Rows used for this run: **{len(df_filtered)}**")

    # --- Threshold inputs ---
    st.sidebar.header("Set Revenue Thresholds")
    silver_min = st.sidebar.number_input("Min total revenue for Silver", value=1000)
    gold_min = st.sidebar.number_input("Min total revenue for Gold", value=1000)
    gold_max = st.sidebar.number_input("Max total revenue for Gold", value=2000)
    platinum_min = st.sidebar.number_input("Min total revenue for Platinum", value=2000)

    # --- Filtering merged pharmacies ---
    merged_pharmacies = df_filtered[
        (df_filtered["Channel"] == "Independent Pharmacies") &
        (df_filtered["Causale"] == "Vendita") &
        (df_filtered["Out of Scope \nFilter"] == "In scope") &
        (df_filtered[" Cluster Check "].isin([" 1.EL ", " 2.L "]))
    ].copy()

    # --- Revenue calculations ---
    all_pharmacy_revenue = (
        merged_pharmacies.groupby('Cod CRM', observed=False)['Net Price 1 Revenue (Imponibile)']
        .sum().reset_index(name="total_net1rev_imponibile")
    )

    rev_tier23 = (
        merged_pharmacies[merged_pharmacies["tier"].isin(['Tier 2', 'Tier 3'])]
        .groupby('Cod CRM', observed=False)['Net Price 1 Revenue (Imponibile)']
        .sum().reset_index(name="tier23_net1rev_imponibile")
    )

    tier_counts = (
        merged_pharmacies.groupby(['Cod CRM', 'tier'])['Brand']
        .nunique().reset_index(name='num_products')
    )
    tier_counts_pivot_use = tier_counts.pivot(index='Cod CRM', columns='tier', values='num_products').fillna(
        0).reset_index()

    threshold_data = all_pharmacy_revenue.merge(tier_counts_pivot_use, on="Cod CRM", how="left").merge(
        rev_tier23, on="Cod CRM", how="left")
    threshold_data["Tier 2 & 3"] = threshold_data.get("Tier 2", 0) + threshold_data.get("Tier 3", 0)

    # --- Assign categories ---
    order = ["Silver", "Gold", "Platinum", "Unassigned"]
    all_pharmacy_revenue['partnership_category'] = "Unassigned"
    all_pharmacy_revenue.loc[
        all_pharmacy_revenue["total_net1rev_imponibile"] > silver_min, 'partnership_category'] = "Silver"
    all_pharmacy_revenue.loc[
        (all_pharmacy_revenue["total_net1rev_imponibile"] >= gold_min) &
        (all_pharmacy_revenue["total_net1rev_imponibile"] < gold_max),
        'partnership_category'
    ] = "Gold"
    all_pharmacy_revenue.loc[
        all_pharmacy_revenue["total_net1rev_imponibile"] >= platinum_min, 'partnership_category'] = "Platinum"

    all_pharmacy_revenue['partnership_category'] = pd.Categorical(
        all_pharmacy_revenue['partnership_category'],
        categories=order,
        ordered=True
    )

    # --- Create tables ---
    category_table = all_pharmacy_revenue.groupby('partnership_category')['Cod CRM'].apply(list)
    category_series = {cat: pd.Series(ids) for cat, ids in category_table.items()}
    category_table_pivot = pd.concat(category_series, axis=1).reset_index(drop=True)

    summary_table = all_pharmacy_revenue.groupby('partnership_category').agg(
        num_pharmacies=('Cod CRM', 'count'),
        total_revenue=('total_net1rev_imponibile', 'sum')
    ).reset_index()

    # Add total row (excluding Unassigned)
    no_unassigned = summary_table.query("partnership_category != 'Unassigned'")
    total_row = no_unassigned.agg({
        'num_pharmacies': 'sum',
        'total_revenue': 'sum'
    }).to_frame().T
    total_row['num_pharmacies'] = total_row['num_pharmacies'].astype(int)
    total_row.insert(0, 'partnership_category', 'Total Net 1 Rev Imponibile (ex-Unassigned)')
    summary_table = pd.concat([summary_table, total_row], ignore_index=True)

    # --- Display tables ---
    st.subheader("All Pharmacy Revenue")
    st.dataframe(all_pharmacy_revenue.style.format({"total_net1rev_imponibile": "€{:,.2f}"}))

    st.subheader("Category Table (IDs)")
    st.dataframe(category_table_pivot)

    st.subheader("Summary Table")
    st.dataframe(summary_table.style.format({"total_revenue": "€{:,.2f}"}))

    # --- Save each run in session state ---
    if st.button("Store this run"):
        import datetime
        run_id = f"run_{len(st.session_state['runs']) + 1}"
        st.session_state['runs'][run_id] = {
            "timestamp": datetime.datetime.now(),
            "filters": {"Channel": channels, "Product_Type": products},
            "thresholds": {"silver_min": silver_min, "gold_min": gold_min, "gold_max": gold_max, "platinum_min": platinum_min},
            "all_pharmacy_revenue": all_pharmacy_revenue,
            "category_table_pivot": category_table_pivot,
            "summary_table": summary_table
        }
        st.success(f"Run stored as {run_id}!")

    # --- Display stored runs ---
    if st.session_state['runs']:
        st.subheader("Stored Runs")
        runs_list = []
        for rid, r in st.session_state['runs'].items():
            runs_list.append({
                "run_id": rid,
                "timestamp": r['timestamp'],
                "filters": r['filters'],
                "thresholds": r['thresholds']
            })
        runs_df = pd.DataFrame(runs_list)
        st.dataframe(runs_df)

        # --- Download stored runs ---
        st.subheader("Download stored runs")
        export_format = st.selectbox("Choose format", ['parquet', 'excel', 'csv', 'json'])

        if st.button("Generate download file"):
            buf = io.BytesIO()
            fname = f"net_revenue_cooper.{export_format}"
            if export_format == 'parquet':
                runs_df.to_parquet(buf, index=False)
            elif export_format == 'excel':
                runs_df.to_excel(buf, index=False)
            elif export_format == 'csv':
                runs_df.to_csv(buf, index=False)
            elif export_format == 'json':
                runs_df.to_json(buf, orient='records', date_format='iso')
            buf.seek(0)
            st.download_button(
                label=f"Download runs as {export_format.upper()}",
                data=buf,
                file_name=fname,
                mime="application/octet-stream"
            )
    else:
        st.info("No runs yet. Run a simulation to store runs.")

    # --- Clear stored runs ---
    if st.button("Clear all stored runs"):
        st.session_state['runs'] = {}
        st.success("Cleared stored runs.")
