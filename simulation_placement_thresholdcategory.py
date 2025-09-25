import streamlit as st
import pandas as pd

st.set_page_config(page_title="Pharmacy Partnership Analyzer", layout="wide")

st.title("Pharmacy Partnership Analyzer")

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

    # --- Threshold inputs ---
    st.sidebar.header("Set Revenue Thresholds")
    silver_min = st.sidebar.number_input("Min total revenue for Silver", value=1000)
    gold_min = st.sidebar.number_input("Min total revenue for Gold", value=1000)
    gold_max = st.sidebar.number_input("Max total revenue for Gold", value=2000)
    platinum_min = st.sidebar.number_input("Min total revenue for Platinum", value=2000)

    # --- Filtering merged pharmacies ---
    merged_pharmacies = df[
        (df["Channel"] == "Independent Pharmacies") &
        (df["Causale"] == "Vendita") &
        (df["Out of Scope \nFilter"] == "In scope") &
        (df[" Cluster Check "].isin([" 1.EL ", " 2.L "]))
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

    threshold_data = all_pharmacy_revenue.merge(tier_counts_pivot_use, on="Cod CRM", how="left").merge(rev_tier23,
                                                                                                       on="Cod CRM",
                                                                                                       how="left")
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

    # make it an ordered Categorical so every later operation keeps Silver→Gold→Platinum
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

    # --- Calculating the total values based on partnership group placement ---
    no_unassigned = summary_table.query("partnership_category != 'Unassigned'")

    # Create a one-row DataFrame with sums
    total_row = no_unassigned.agg({
        'num_pharmacies': 'sum',
        'total_revenue': 'sum'
    }).to_frame().T

    # Give it a label for the category column
    total_row['num_pharmacies'] = total_row['num_pharmacies'].astype(int)
    total_row.insert(0, 'partnership_category', 'Total Net 1 Rev Imponibile (ex-Unassigned)')

    # Append to the original table
    summary_table = pd.concat([summary_table, total_row], ignore_index=True)

    # --- Display tables ---
    st.subheader("All Pharmacy Revenue")
    st.dataframe(
        all_pharmacy_revenue.style.format({"total_net1rev_imponibile": "€{:,.2f}"})
    )

    st.subheader("Category Table (IDs)")
    st.dataframe(category_table_pivot)

    st.subheader("Summary Table")
    st.dataframe(
        summary_table.style.format({"total_revenue": "€{:,.2f}"})
    )

    # --- Save tables ---
    # Download buttons instead of writing to disk
    st.markdown("### Download Results")
    csv1 = category_table_pivot.to_csv(index=False).encode('utf-8')
    csv2 = summary_table.to_csv(index=False).encode('utf-8')

    st.download_button("Download Category IDs CSV",
                       data=csv1,
                       file_name="store_categories_ids.csv",
                       mime="text/csv")
    st.download_button("Download Summary CSV",
                       data=csv2,
                       file_name="store_categories_summary.csv",
                       mime="text/csv")