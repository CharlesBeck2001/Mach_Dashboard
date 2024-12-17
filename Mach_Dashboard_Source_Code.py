#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Dec 17 00:34:39 2024

@author: charlesbeck
"""

import requests
import pandas as pd
import matplotlib.pyplot as plt
import streamlit as st
import numpy as np
import altair as alt
import plotly.graph_objects as go

st.set_page_config(layout="wide")
st.title("Mach Dashboard")

page = st.selectbox("Choose a page", ["Home", "Trading Data Visualizations", "Volume Distribution", "Volume Flow Chart", "Fill Time", "New Users", "CCTP Data"])

if page == "Home":
    st.write("Welcome to the dashboard for Mach, a world class cryptocurrency exchange!")
elif page == "Trading Data Visualizations":
    
    # Supabase credentials
    supabase_url = "https://fzkeftdzgseugijplhsh.supabase.co"
    supabase_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZ6a2VmdGR6Z3NldWdpanBsaHNoIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTczMjcxMzk3NCwiZXhwIjoyMDQ4Mjg5OTc0fQ.Og46ddAeoybqUavWBAUbUoj8HJiZrfAQZi-6gRP46i4"

    sql_query1 = """  
    SELECT op.*
    FROM order_placed op
    INNER JOIN match_executed me
    ON op.order_uuid = me.order_uuid
    """

    sql_query2 = """
    SELECT 
      TO_CHAR(
        TO_TIMESTAMP(hour_series || ':00:00', 'HH24:MI:SS'),
        'FMHH AM'  -- Format hour as "1 AM", "2 AM", etc.
      ) AS hour_of_day,
      COALESCE(SUM(svt.total_volume), 0) AS total_hourly_volume
    FROM generate_series(0, 23) AS hour_series  -- Generate hours from 0 to 23
    LEFT JOIN overall_volume_table svt
      ON EXTRACT(HOUR FROM svt.block_timestamp) = hour_series  -- Match the hour of the trade to the generated hours
    GROUP BY hour_series
    ORDER BY hour_series
    """

    sql_query3 = """
    SELECT 
      TO_CHAR(DATE_TRUNC('day', svt.block_timestamp), 'FMMonth FMDD, YYYY') AS day,
      COALESCE(SUM(svt.total_volume), 0) AS total_daily_volume
    FROM overall_volume_table svt
    GROUP BY DATE_TRUNC('day', svt.block_timestamp)
    ORDER BY day
    """

    sql_query4 = """
    SELECT 
      TO_CHAR(DATE_TRUNC('week', svt.block_timestamp), 'FMMonth FMDD, YYYY') AS week_starting,
      COALESCE(SUM(svt.total_volume), 0) AS total_weekly_volume
    FROM overall_volume_table svt
    GROUP BY DATE_TRUNC('week', svt.block_timestamp)
    ORDER BY week_starting
    """

    sql_query5 = """
    SELECT 
        TO_CHAR(
            TO_TIMESTAMP(DATE_PART('hour', op.block_timestamp) || ':00:00', 'HH24:MI:SS'),
            'FMHH:MI AM'
        ) AS hour_of_day,
        COUNT(*) AS total_trades
    FROM order_placed op
    INNER JOIN match_executed me
    ON op.order_uuid = me.order_uuid
    GROUP BY DATE_PART('hour', op.block_timestamp)
    ORDER BY DATE_PART('hour', op.block_timestamp)
    """

    sql_query6 = """
    SELECT 
        DATE(op.block_timestamp) AS trade_date,
        COUNT(*) AS total_trades
    FROM order_placed op
    INNER JOIN match_executed me
    ON op.order_uuid = me.order_uuid
    GROUP BY DATE(op.block_timestamp)
    ORDER BY trade_date
    """

    sql_query7 = """
    SELECT 
        DATE_TRUNC('week', op.block_timestamp) AS week_start_date,
        COUNT(*) AS total_trades
    FROM order_placed op
    INNER JOIN match_executed me
    ON op.order_uuid = me.order_uuid
    GROUP BY DATE_TRUNC('week', op.block_timestamp)
    ORDER BY week_start_date
    """

    def execute_sql(query):
        headers = {
            "apikey": supabase_key,
            "Authorization": f"Bearer {supabase_key}",
            "Content-Type": "application/json"
        }
        # Endpoint for the RPC function
        rpc_endpoint = f"{supabase_url}/rest/v1/rpc/execute_sql"
        
        # Payload with the SQL query
        payload = {"query": query}
        
        # Make the POST request to the RPC function
        response = requests.post(rpc_endpoint, headers=headers, json=payload)
        
        # Handle response
        if response.status_code == 200:
            data = response.json()
            
            df = pd.DataFrame(data)
            
            print("Query executed successfully, returning DataFrame.")
            return(df)
        else:
            print("Error executing query:", response.status_code, response.json())

    # Call the function
    df_hourly_volume = execute_sql(sql_query2)
    df_daily_volume = execute_sql(sql_query3)
    df_weekly_volume = execute_sql(sql_query4)

    df_hourly_trades = execute_sql(sql_query5)
    df_daily_trades = execute_sql(sql_query6)
    df_weekly_trades = execute_sql(sql_query7)


    # Dictionary holding the DataFrames
    dfs = {
        "hourly_volume": df_hourly_volume,
        "daily_volume": df_daily_volume,
        "weekly_volume": df_weekly_volume,
        "hourly_trades": df_hourly_trades,
        "daily_trades": df_daily_trades,
        "weekly_trades": df_weekly_trades,
    }

    for key in dfs:
        dfs[key] = pd.json_normalize(dfs[key]['result'])

    # Convert date columns explicitly to datetime
    #dfs["daily_volume"]["day"] = pd.to_datetime(dfs["daily_volume"]["day"], format='%m-%d-%Y', errors='coerce')
    #dfs["weekly_volume"]["week_starting"] = pd.to_datetime(dfs["weekly_volume"]["week_starting"], format='%m-%d-%Y', errors='coerce')
    dfs["daily_trades"]["trade_date"] = pd.to_datetime(dfs["daily_trades"]["trade_date"], format='%Y-%m-%d', errors='coerce')
    dfs["weekly_trades"]["week_start_date"] = pd.to_datetime(dfs["weekly_trades"]["week_start_date"], format='%Y-%m-%dT%H:%M:%S', errors='coerce')

    # Add missing hours to hourly_trades (zero padding)
    all_hours = [f"{i}:00 AM" if i < 12 else f"{i-12}:00 PM" if i > 12 else f"{i}:00 PM" for i in range(24)]
    dfs["hourly_trades"] = dfs["hourly_trades"].set_index("hour_of_day").reindex(all_hours, fill_value=0).reset_index()
    dfs["hourly_trades"].rename(columns={"index": "hour_of_day"}, inplace=True)

    # Drop rows with invalid datetime or missing values in important columns
    for df_name in ["hourly_volume", "daily_volume", "weekly_volume", "hourly_trades", "daily_trades", "weekly_trades"]:
        # Check if the columns exist before dropping NaNs
        if "trade_date" in dfs[df_name].columns:
            dfs[df_name].dropna(subset=["trade_date"], inplace=True)
        if "week_start" in dfs[df_name].columns:
            dfs[df_name].dropna(subset=["week_start"], inplace=True)
        if "week_start_date" in dfs[df_name].columns:
            dfs[df_name].dropna(subset=["week_start_date"], inplace=True)
        if "hour_of_day" in dfs[df_name].columns:
            dfs[df_name].dropna(subset=["hour_of_day"], inplace=True)
        dfs[df_name] = dfs[df_name].reset_index(drop=True)

    # Convert columns with large numbers to float64 to handle overflow issues
    dfs["hourly_volume"]["total_hourly_volume"] = dfs["hourly_volume"]["total_hourly_volume"].astype('float64')
    dfs["daily_volume"]["total_daily_volume"] = dfs["daily_volume"]["total_daily_volume"].astype('float64')
    dfs["weekly_volume"]["total_weekly_volume"] = dfs["weekly_volume"]["total_weekly_volume"].astype('float64')


    # Ensure the 'day' column is converted to a proper datetime format
    dfs["daily_volume"]['day'] = pd.to_datetime(dfs["daily_volume"]['day'], format='%B %d, %Y')
    dfs["weekly_volume"]["week_starting"] = pd.to_datetime(dfs["weekly_volume"]["week_starting"], format='%B %d, %Y')

    # Sort the DataFrame by the 'day' column in ascending order
    dfs["daily_volume"] = dfs["daily_volume"].sort_values(by='day')
    dfs["weekly_volume"] = dfs["weekly_volume"].sort_values(by = 'week_starting')

    # Convert 'day' column back to the original string format after sorting
    dfs["daily_volume"]['day'] = dfs["daily_volume"]['day'].dt.strftime('%B %d, %Y')
    dfs["weekly_volume"]["week_starting"] = dfs["weekly_volume"]["week_starting"].dt.strftime('%B %d, %Y')

    # Set page layout options to give more space for columns

    st.title("Trading Data Visualizations")

    # Create a container for the volume plots
    with st.container():
        st.subheader("Volume Data Plots")
        col1, col2, col3 = st.columns([1, 1, 1])  # Equal width for each
        with col1:
            st.write("Hourly Volume")
            hourly_data = dfs["hourly_volume"].set_index("hour_of_day")["total_hourly_volume"]
            st.line_chart(hourly_data, use_container_width=True)
            # Add interactivity with select slider
            selected_hour = st.select_slider("Select Hour", options=hourly_data.index)
            st.write(f"Volume at {selected_hour}: {hourly_data[selected_hour]:.2f}")
        
        with col2:
            st.write("Daily Volume")
            daily_data = dfs["daily_volume"].set_index("day")["total_daily_volume"]
        
            # Convert the daily data index to datetime without time
            daily_data.index = pd.to_datetime(daily_data.index).date  # Keep only the date part

            st.line_chart(daily_data, use_container_width=True)
        
            # Add interactivity with select slider
            selected_date = st.select_slider("Select Date", options=daily_data.index)
        
            # Display the selected date and corresponding volume
            st.write(f"Volume on {selected_date}: {daily_data[selected_date]:.2f}")
        
        with col3:
            st.write("Weekly Volume")
        
            # Set index to 'week_starting' and extract the total weekly volume
            weekly_data = dfs["weekly_volume"].set_index("week_starting")["total_weekly_volume"]
            
            
            weekly_data.index = pd.to_datetime(weekly_data.index).date
        
            # Plot the line chart
            st.line_chart(weekly_data, use_container_width=True)
        
            # Add interactivity with select slider for weeks
            selected_week = st.select_slider("Select Week", options=weekly_data.index)
        
            # Ensure selected_week is in datetime format
            selected_week = pd.to_datetime(selected_week)
        
            # Round the selected week to the start of the week (Monday)
            selected_week = selected_week - pd.Timedelta(days=selected_week.weekday())
        
            # Display the volume for the selected week
            try:
                st.write(f"Volume during week starting {selected_week.date()}: {weekly_data[selected_week]:.2f}")
            except KeyError:
                st.write(f"No data available for the selected week starting {selected_week.date()}")

    # Create a container for the logarithmic volume data plots
    with st.container():
        st.subheader("Volume Data Plots")
        col1, col2, col3 = st.columns([1, 1, 1])  # Equal width for each

        with col1:
            
            st.write("Hourly Volume")
            hourly_data = dfs["hourly_volume"].set_index("hour_of_day")["total_hourly_volume"]
        
            # Filter out zeros before applying log10 transformation
            hourly_data_nonzero = hourly_data[hourly_data > 0]
        
            # Apply log10 transformation to the non-zero hourly data
            hourly_log_data = np.log10(hourly_data_nonzero)
            st.line_chart(hourly_log_data, use_container_width=True)
        
            # Add interactivity with select slider
            selected_hour = st.select_slider(
                "Select Hour", options=hourly_log_data.index, key="hour_slider"
                )
            st.write(f"Log10 Volume at {selected_hour}: {hourly_log_data[selected_hour]:.2f}")
        
        with col2:
            st.write("Daily Volume")
            daily_data = dfs["daily_volume"].set_index("day")["total_daily_volume"]
        
            # Convert the daily data index to datetime without time
            daily_data.index = pd.to_datetime(daily_data.index).date  # Keep only the date part
            
            # Apply log10 transformation to daily data
            daily_log_data = np.log10(daily_data)
            st.line_chart(daily_log_data, use_container_width=True)
        
            # Add interactivity with select slider
            selected_date = st.select_slider(
                "Select Date", options=daily_log_data.index, key="date_slider"
            )
            st.write(f"Log10 Volume on {selected_date}: {daily_log_data[selected_date]:.2f}")
        
        with col3:
            st.write("Weekly Volume")
        
            # Set index to 'week_starting' and extract the total weekly volume
            weekly_data = dfs["weekly_volume"].set_index("week_starting")["total_weekly_volume"]
            
            weekly_data.index = pd.to_datetime(weekly_data.index).date
            
            # Apply log10 transformation to weekly data
            weekly_log_data = np.log10(weekly_data)
        
            # Plot the line chart
            st.line_chart(weekly_log_data, use_container_width=True)
        
            # Add interactivity with select slider for weeks
            selected_week = st.select_slider(
                "Select Week", options=weekly_log_data.index, key="week_slider"
            )
        
            # Ensure selected_week is in datetime format
            selected_week = pd.to_datetime(selected_week)
        
            # Round the selected week to the start of the week (Monday)
            selected_week = selected_week - pd.Timedelta(days=selected_week.weekday())
        
            # Display the log10 volume for the selected week
            try:
                st.write(f"Log10 Volume during week starting {selected_week.date()}: {weekly_log_data[selected_week]:.2f}")
            except KeyError:
                st.write(f"No data available for the selected week starting {selected_week.date()}")
                
                
    # Create a container for the trade plots
    with st.container():
        st.subheader("Trade Data Plots")
        col4, col5, col6 = st.columns([1, 1, 1])  # Equal width for each
        with col4:
            st.write("Hourly Trades")
            hourly_trade_data = dfs["hourly_trades"].set_index("hour_of_day")["total_trades"]
            st.line_chart(hourly_trade_data, use_container_width=True)
            # Add interactivity with select slider
            selected_hour_trade = st.select_slider("Select Hour for Trades", options=hourly_trade_data.index)
            st.write(f"Trades at {selected_hour_trade}: {hourly_trade_data[selected_hour_trade]:.2f}")
        
        with col5:
            st.write("Daily Trades")
            daily_trade_data = dfs["daily_trades"].set_index("trade_date")["total_trades"]
            st.line_chart(daily_trade_data, use_container_width=True)
            # Add interactivity with select slider
            selected_date_trade = st.select_slider("Select Date for Trades", options=daily_trade_data.index)
            st.write(f"Trades on {selected_date_trade.date()}: {daily_trade_data[selected_date_trade]:.2f}")
        
        with col6:
            st.write("Weekly Trades")
            weekly_trade_data = dfs["weekly_trades"].set_index("week_start_date")["total_trades"]
            st.line_chart(weekly_trade_data, use_container_width=True)
            # Add interactivity with select slider
            selected_week_trade = st.select_slider("Select Week for Trades", options=weekly_trade_data.index)
            st.write(f"Trades during week starting {selected_week_trade.date()}: {weekly_trade_data[selected_week_trade]:.2f}")
            
            
            
    # Calculate total volume across the dataset
    total_volume = dfs["weekly_volume"]["total_weekly_volume"].sum()

    # Output the total volume
    st.subheader("Total Volume in Dataset")
    st.write(f"Total Volume is: **{total_volume:,.2f}**")
    
elif page == "Volume Distribution":
    
    # Supabase credentials
    supabase_url = "https://fzkeftdzgseugijplhsh.supabase.co"
    supabase_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZ6a2VmdGR6Z3NldWdpanBsaHNoIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTczMjcxMzk3NCwiZXhwIjoyMDQ4Mjg5OTc0fQ.Og46ddAeoybqUavWBAUbUoj8HJiZrfAQZi-6gRP46i4"

    sql_query1 = """
    SELECT 
        source_chain, 
        source_id, 
        SUM(source_volume) AS source_volume
    FROM 
        overall_volume_table
    GROUP BY 
        source_chain, 
        source_id
    ORDER BY 
        source_chain, 
        source_id
    """

    sql_query2 = """
    SELECT 
        dest_chain, 
        dest_id, 
        SUM(dest_volume) AS dest_volume
    FROM 
        overall_volume_table
    GROUP BY 
        dest_chain, 
        dest_id
    ORDER BY 
        dest_chain, 
        dest_id
    """

    sql_query3 = """SELECT 
        chain AS chain,
        asset AS asset,
        SUM(volume) AS total_volume
    FROM (
        -- Treat source_chain/source_id as one group
        SELECT 
            source_chain AS chain, 
            source_id AS asset, 
            source_volume AS volume
        FROM 
            overall_volume_table

        UNION ALL

        -- Treat dest_chain/dest_id as another group
        SELECT 
            dest_chain AS chain, 
            dest_id AS asset, 
            dest_volume AS volume
        FROM 
            overall_volume_table
    ) AS combined_data
    GROUP BY 
        chain, 
        asset
    ORDER BY 
        chain, 
        asset
    """

    def execute_sql(query):
        headers = {
            "apikey": supabase_key,
            "Authorization": f"Bearer {supabase_key}",
            "Content-Type": "application/json"
        }
        # Endpoint for the RPC function
        rpc_endpoint = f"{supabase_url}/rest/v1/rpc/execute_sql"
        
        # Payload with the SQL query
        payload = {"query": query}
        
        # Make the POST request to the RPC function
        response = requests.post(rpc_endpoint, headers=headers, json=payload)
        
        # Handle response
        if response.status_code == 200:
            data = response.json()
            
            df = pd.DataFrame(data)
            
            print("Query executed successfully, returning DataFrame.")
            return(df)
        else:
            print("Error executing query:", response.status_code, response.json())
            
    # Call the function
    df_source_chain_volume = execute_sql(sql_query1)
    df_dest_chain_volume = execute_sql(sql_query2)
    df_total_chain_volume = execute_sql(sql_query3)


    df_source_chain_volume = pd.json_normalize(df_source_chain_volume['result'])
    df_dest_chain_volume = pd.json_normalize(df_dest_chain_volume['result'])        
    df_total_chain_volume = pd.json_normalize(df_total_chain_volume['result'])


    # Streamlit title
    st.title("Volume Distribution")

    with st.container():
        
        col1, col2, col3 = st.columns([1, 1, 1])
        
        with col1:
            
            st.write("Source Volume")
            # Group data by 'dest_chain' and 'dest_id', summing up 'dest_volume'
            grouped_df = df_source_chain_volume.groupby(["source_chain", "source_id"], as_index=False)["source_volume"].sum()

            # Sort data by 'dest_chain' and 'dest_id'
            grouped_df = grouped_df.sort_values(by=["source_chain", "source_id"])
            
            # Sort data by 'source_chain' and sum of 'source_volume'
            chain_order = grouped_df.groupby('source_chain')['source_volume'].sum().sort_values(ascending=False).index

            
            # Define a selection for highlighting on hover
            highlight = alt.selection_single(
                on="mouseover",  # Trigger selection on hover
                fields=["source_chain", "source_id"],  # Selection is based on these fields
                nearest=True,
                empty="none"
                )

            # Altair Stacked Bar Chart with Diagonal Labels and Hover Highlight
            base = (
                alt.Chart(grouped_df)
                .mark_bar()
                .encode(
                    x=alt.X(
                        "source_chain:N", 
                        title="Source Chain",
                        sort = chain_order,
                        axis=alt.Axis(labelAngle=-45)  # Set diagonal angle for x-axis labels
                        ),
                    y=alt.Y("source_volume:Q", title="Total Volume"),
                    color=alt.Color("source_id:N", title="Source ID", scale=alt.Scale(scheme="category20")),
                    tooltip=["source_chain", "source_id", "source_volume"]
                    )
                )

            # Add conditional opacity for highlighting
            highlighted_chart = base.encode(
                opacity=alt.condition(highlight, alt.value(1), alt.value(0.6))
                ).add_selection(highlight)

            #st.subheader("Source Volume")

            # Display the interactive chart in Streamlit
            st.altair_chart(highlighted_chart, use_container_width=True)

        
        with col2:
            
            st.write("Destination Volume")
            #Group data by 'dest_chain' and 'dest_id', summing up 'dest_volume'
            grouped_df = df_dest_chain_volume.groupby(["dest_chain", "dest_id"], as_index=False)["dest_volume"].sum()

            # Sort data by 'dest_chain' and 'dest_id'
            grouped_df = grouped_df.sort_values(by=["dest_chain", "dest_id"])
            
            # Sort data by 'source_chain' and sum of 'source_volume'
            chain_order = grouped_df.groupby('dest_chain')['dest_volume'].sum().sort_values(ascending=False).index
            
            # Define a selection for highlighting on hover
            highlight = alt.selection_single(
                on="mouseover",  # Trigger selection on hover
                fields=["dest_chain", "dest_id"],  # Selection is based on these fields
                nearest=True,
                empty="none"
                )

            # Altair Stacked Bar Chart with Diagonal Labels and Hover Highlight
            base = (
                alt.Chart(grouped_df)
                .mark_bar()
                .encode(
                    x=alt.X(
                        "dest_chain:N", 
                        title="Destination Chain",
                        sort = chain_order,
                        axis=alt.Axis(labelAngle=-45)  # Set diagonal angle for x-axis labels
                        ),
                    y=alt.Y("dest_volume:Q", title="Total Volume"),
                    color=alt.Color("dest_id:N", title="Destination ID", scale=alt.Scale(scheme="category20")),
                    tooltip=["dest_chain", "dest_id", "dest_volume"]
                    )
                )

            # Add conditional opacity for highlighting
            highlighted_chart = base.encode(
                opacity=alt.condition(highlight, alt.value(1), alt.value(0.6))
                ).add_selection(highlight)

            #st.subheader("Destination Volume")

            # Display the interactive chart in Streamlit
            st.altair_chart(highlighted_chart, use_container_width=True)

        
        with col3:
        
            st.write("Total Volume")
            # Group data by 'dest_chain' and 'dest_id', summing up 'dest_volume'
            grouped_df = df_total_chain_volume.groupby(["chain", "asset"], as_index=False)["total_volume"].sum()

            # Sort data by 'dest_chain' and 'dest_id'
            grouped_df = grouped_df.sort_values(by=["chain", "asset"])
            
            # Sort data by 'source_chain' and sum of 'source_volume'
            chain_order = grouped_df.groupby('chain')['total_volume'].sum().sort_values(ascending=False).index
            
            # Define a selection for highlighting on hover
            highlight = alt.selection_single(
                on="mouseover",  # Trigger selection on hover
                fields=["chain", "asset"],  # Selection is based on these fields
                nearest=True,
                empty="none"
                )

            # Altair Stacked Bar Chart with Diagonal Labels and Hover Highlight
            base = (
                alt.Chart(grouped_df)
                .mark_bar()
                .encode(
                    x=alt.X(
                        "chain:N", 
                        title="Chain",
                        sort = chain_order,
                        axis=alt.Axis(labelAngle=-45)  # Set diagonal angle for x-axis labels
                        ),
                    y=alt.Y("total_volume:Q", title="Total Volume"),
                    color=alt.Color("asset:N", title="Asset ID", scale=alt.Scale(scheme="category20")),
                    tooltip=["chain", "asset", "total_volume"]
                    )
                )

            # Add conditional opacity for highlighting
            highlighted_chart = base.encode(
                opacity=alt.condition(highlight, alt.value(1), alt.value(0.6))
                ).add_selection(highlight)

            #st.subheader("Overall Volume")

            # Display the interactive chart in Streamlit
            st.altair_chart(highlighted_chart, use_container_width=True)

    # Calculate total volume by asset
    asset_volume = df_total_chain_volume.groupby('asset')['total_volume'].sum().reset_index()
    asset_volume['percent'] = 100 * asset_volume['total_volume'] / asset_volume['total_volume'].sum()

    # Create the first pie chart for asset distribution using Altair
    pie_asset = alt.Chart(asset_volume).mark_arc().encode(
        theta=alt.Theta(field="total_volume", type="quantitative"),
        color=alt.Color(field="asset", type="nominal"),
        tooltip=['asset', 'total_volume', 'percent']
    ).properties(
        title="Volume by Asset"
    )

    # Calculate total volume by chain
    chain_volume = df_total_chain_volume.groupby('chain')['total_volume'].sum().reset_index()
    chain_volume['percent'] = 100 * chain_volume['total_volume'] / chain_volume['total_volume'].sum()

    # Create the second pie chart for chain distribution using Altair
    pie_chain = alt.Chart(chain_volume).mark_arc().encode(
        theta=alt.Theta(field="total_volume", type="quantitative"),
        color=alt.Color(field="chain", type="nominal"),
        tooltip=['chain', 'total_volume', 'percent']
    ).properties(
        title="Volume by Chain"
    )

    # Streamlit layout
    st.title('Volume Distribution Analysis')
    st.altair_chart(pie_asset, use_container_width=True)
    st.altair_chart(pie_chain, use_container_width=True)

elif page == "Volume Flow Chart":
    
    # Supabase credentials
    supabase_url = "https://fzkeftdzgseugijplhsh.supabase.co"
    supabase_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZ6a2VmdGR6Z3NldWdpanBsaHNoIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTczMjcxMzk3NCwiZXhwIjoyMDQ4Mjg5OTc0fQ.Og46ddAeoybqUavWBAUbUoj8HJiZrfAQZi-6gRP46i4"

    sql_query1 = """  
    SELECT 
        source_chain,
        source_id,
        dest_chain,
        dest_id,
        SUM(source_volume) AS total_source_volume,
        SUM(dest_volume) AS total_dest_volume
    FROM 
        overall_volume_table
    GROUP BY 
        source_chain, source_id, dest_chain, dest_id
    ORDER BY 
        total_source_volume DESC
    LIMIT 15
    """

    def execute_sql(query):
        headers = {
            "apikey": supabase_key,
            "Authorization": f"Bearer {supabase_key}",
            "Content-Type": "application/json"
        }
        # Endpoint for the RPC function
        rpc_endpoint = f"{supabase_url}/rest/v1/rpc/execute_sql"
        
        # Payload with the SQL query
        payload = {"query": query}
        
        # Make the POST request to the RPC function
        response = requests.post(rpc_endpoint, headers=headers, json=payload)
        
        # Handle response
        if response.status_code == 200:
            data = response.json()
            df = pd.DataFrame(data)
            return df
        else:
            print("Error executing query:", response.status_code, response.json())

    # Call the function
    df_volume_flow_chart = execute_sql(sql_query1)

    df_volume_flow_chart = pd.json_normalize(df_volume_flow_chart['result'])

    # Set up the Streamlit page without sidebar
   
    st.title("Flow Chart for 15 Most Significant Pairs")

    # Create source and target columns by combining source_id with source_chain, and similarly for dest
    df_volume_flow_chart['source'] = df_volume_flow_chart['source_id'] + " - " + df_volume_flow_chart['source_chain']
    df_volume_flow_chart['target'] = df_volume_flow_chart['dest_id'] + " - " + df_volume_flow_chart['dest_chain']
    df_volume_flow_chart['value'] = df_volume_flow_chart['total_source_volume']

    # Sample structure of df_volume_flow_chart, assuming it's already loaded
    # df_volume_flow_chart = pd.DataFrame({
    #     'source': ['A1', 'A2', 'A1', 'B1', 'B2', 'B2'],
    #     'target': ['B1', 'B2', 'B2', 'C1', 'C1', 'C2'],
    #     'value': [8, 4, 2, 8, 4, 2]
    # })

    # Step 1: Identify duplicate nodes between source and target
    nodes = set(df_volume_flow_chart['source']).union(set(df_volume_flow_chart['target']))  # Get all unique nodes

    # Step 2: Create a mapping for nodes that appear in both source and target
    node_label_mapping = {}
    for node in nodes:
        # If the node is in both source and target, modify the target name by adding "(D)"
        if node in df_volume_flow_chart['source'].values and node in df_volume_flow_chart['target'].values:
            node_label_mapping[node] = {'source': node + " (S)", 'target': node + " (D)"}
        else:
            node_label_mapping[node] = {'source': node, 'target': node}

    # Step 3: Prepare the list of unique labels for nodes
    label_names = []
    for node in node_label_mapping:
        label_names.append(node_label_mapping[node]['source'])
        if node_label_mapping[node]['target'] != node_label_mapping[node]['source']:  # Avoid duplicates
            label_names.append(node_label_mapping[node]['target'])

    # Step 4: Map the source and target columns to the updated labels
    df_volume_flow_chart['source'] = df_volume_flow_chart['source'].map(lambda x: node_label_mapping[x]['source'])
    df_volume_flow_chart['target'] = df_volume_flow_chart['target'].map(lambda x: node_label_mapping[x]['target'])

    # Step 5: Prepare Sankey diagram indices
    source_indices = [label_names.index(source) for source in df_volume_flow_chart['source']]
    target_indices = [label_names.index(target) for target in df_volume_flow_chart['target']]
    values = df_volume_flow_chart['value']

    # Step 6: Create Sankey diagram using Plotly
    fig = go.Figure(data=[go.Sankey(
        node=dict(
            pad=15,
            thickness=20,
            line=dict(color="black", width=0.5),
            label=label_names,  # Use the correct label names
            color="blue"
        ),
        link=dict(
            source=source_indices,
            target=target_indices,
            value=values,
            color="rgba(255, 0, 0, 0.4)"  # Set link color
        )
    )])

    # Step 7: Display the Sankey diagram using Streamlit
    st.plotly_chart(fig)

elif page == "Fill Time":
    
    # Supabase credentials
    supabase_url = "https://fzkeftdzgseugijplhsh.supabase.co"
    supabase_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZ6a2VmdGR6Z3NldWdpanBsaHNoIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTczMjcxMzk3NCwiZXhwIjoyMDQ4Mjg5OTc0fQ.Og46ddAeoybqUavWBAUbUoj8HJiZrfAQZi-6gRP46i4"

    sql_query1 = """
    WITH deduplicated AS (
        SELECT 
            op.order_uuid,
            cal.chain,
            op.block_timestamp as time_order_made,
            EXTRACT(EPOCH FROM (me.block_timestamp - op.block_timestamp))::FLOAT AS fill_time,
            ROW_NUMBER() OVER (PARTITION BY op.order_uuid ORDER BY me.block_timestamp) AS rn
        FROM order_placed op
        INNER JOIN match_executed me
          ON op.order_uuid = me.order_uuid
        INNER JOIN coingecko_assets_list cal
          ON op.source_asset = cal.address
    ),
    fill_table AS (
      SELECT order_uuid, chain, time_order_made, fill_time
      FROM deduplicated
      WHERE rn = 1
    ),
    median_time_fill_table AS (
        SELECT
            DATE(time_order_made) AS order_date,  -- Extract the date from time_order_made
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY fill_time) AS median_fill_time
        FROM fill_table
        GROUP BY order_date  -- Group by the extracted date
        ORDER BY order_date
    )
    SELECT * 
    FROM median_time_fill_table
    """

    sql_query2 = """
    WITH deduplicated AS (
        SELECT 
            op.order_uuid,
            cal.chain,
            op.block_timestamp as time_order_made,
            EXTRACT(EPOCH FROM (me.block_timestamp - op.block_timestamp))::FLOAT AS fill_time,
            ROW_NUMBER() OVER (PARTITION BY op.order_uuid ORDER BY me.block_timestamp) AS rn
        FROM order_placed op
        INNER JOIN match_executed me
          ON op.order_uuid = me.order_uuid
        INNER JOIN coingecko_assets_list cal
          ON op.source_asset = cal.address
    ),
    fill_table AS (
      SELECT order_uuid, chain, time_order_made, fill_time
      FROM deduplicated
      WHERE rn = 1
    ),
    median_source_chain_fill_table AS (
    SELECT 
        chain,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY fill_time) AS median_fill_time
    FROM fill_table
    GROUP BY chain
    ORDER BY median_fill_time
    )
    SELECT * FROM median_source_chain_fill_table
    """

    sql_query3 = """
    WITH deduplicated AS (
        SELECT 
            op.order_uuid,
            cal.chain AS source_chain,
            cal2.chain AS dest_chain,
            op.block_timestamp as time_order_made,
            EXTRACT(EPOCH FROM (me.block_timestamp - op.block_timestamp))::FLOAT AS fill_time,
            ROW_NUMBER() OVER (PARTITION BY op.order_uuid ORDER BY me.block_timestamp) AS rn
        FROM order_placed op
        INNER JOIN match_executed me
          ON op.order_uuid = me.order_uuid
        INNER JOIN coingecko_assets_list cal
          ON op.source_asset = cal.address
        INNER JOIN coingecko_assets_list cal2
          ON op.dest_asset = cal2.address
    ),
    fill_table AS (
      SELECT order_uuid, source_chain, dest_chain, time_order_made, fill_time
      FROM deduplicated
      WHERE rn = 1
    ),
    median_chain_fill_table AS (
    SELECT 
        source_chain,
        dest_chain,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY fill_time) AS median_fill_time
    FROM fill_table
    GROUP BY source_chain, dest_chain
    ORDER BY median_fill_time
    )
    SELECT * FROM median_chain_fill_table
    """

    sql_query4 = """
    WITH deduplicated AS (
        SELECT 
            op.order_uuid,
            op.source_asset as source_address,
            op.dest_asset as dest_address,
            cal.chain AS source_chain,
            cal2.chain AS dest_chain,
            op.block_timestamp as time_order_made,
            EXTRACT(EPOCH FROM (me.block_timestamp - op.block_timestamp))::FLOAT AS fill_time,
            ROW_NUMBER() OVER (PARTITION BY op.order_uuid ORDER BY me.block_timestamp) AS rn
        FROM order_placed op
        INNER JOIN match_executed me
          ON op.order_uuid = me.order_uuid
        INNER JOIN coingecko_assets_list cal
          ON op.source_asset = cal.address
        INNER JOIN coingecko_assets_list cal2
          ON op.dest_asset = cal2.address
    ),
    fill_table AS (
      SELECT order_uuid, source_chain, dest_chain, source_address, dest_address, time_order_made, fill_time
      FROM deduplicated
      WHERE rn = 1
    )
    SELECT * 
    FROM fill_table
    ORDER BY fill_time DESC
    LIMIT 10
    """

    sql_query5 = """
    WITH deduplicated AS (
        SELECT 
            op.order_uuid,
            op.source_asset as source_address,
            op.dest_asset as dest_address,
            cal.chain AS source_chain,
            cal2.chain AS dest_chain,
            op.block_timestamp as time_order_made,
            EXTRACT(EPOCH FROM (me.block_timestamp - op.block_timestamp))::FLOAT AS fill_time,
            ROW_NUMBER() OVER (PARTITION BY op.order_uuid ORDER BY me.block_timestamp) AS rn
        FROM order_placed op
        INNER JOIN match_executed me
          ON op.order_uuid = me.order_uuid
        INNER JOIN coingecko_assets_list cal
          ON op.source_asset = cal.address
        INNER JOIN coingecko_assets_list cal2
          ON op.dest_asset = cal2.address
    ),
    fill_table AS (
      SELECT order_uuid, source_chain, dest_chain, source_address, dest_address, time_order_made, fill_time
      FROM deduplicated
      WHERE rn = 1
    )
    SELECT * 
    FROM fill_table
    ORDER BY fill_time ASC
    LIMIT 10
    """



    def execute_sql(query):
        headers = {
            "apikey": supabase_key,
            "Authorization": f"Bearer {supabase_key}",
            "Content-Type": "application/json"
        }
        # Endpoint for the RPC function
        rpc_endpoint = f"{supabase_url}/rest/v1/rpc/execute_sql"
        
        # Payload with the SQL query
        payload = {"query": query}
        
        # Make the POST request to the RPC function
        response = requests.post(rpc_endpoint, headers=headers, json=payload)
        
        # Handle response
        if response.status_code == 200:
            data = response.json()
            df = pd.DataFrame(data)
            return df
        else:
            print("Error executing query:", response.status_code, response.json())

    # Call the function
    df_fill_time_date = execute_sql(sql_query1)
    df_fill_time_s_chain = execute_sql(sql_query2)
    df_fill_time_chain = execute_sql(sql_query3)
    df_fill_time_highest = execute_sql(sql_query4)
    df_fill_time_lowest = execute_sql(sql_query5)

    df_fill_time_date = pd.json_normalize(df_fill_time_date['result'])
    df_fill_time_s_chain = pd.json_normalize(df_fill_time_s_chain['result'])
    df_fill_time_chain = pd.json_normalize(df_fill_time_chain['result'])
    df_fill_time_highest = pd.json_normalize(df_fill_time_highest['result'])
    df_fill_time_lowest = pd.json_normalize(df_fill_time_lowest['result'])


    # Create chain pair column for better visualization
    df_fill_time_chain['chain_pair'] = df_fill_time_chain['source_chain'] + ' to ' + df_fill_time_chain['dest_chain']

    # Sorting the median fill times in descending order for better visualization
    df_fill_time_chain = df_fill_time_chain.sort_values(by='median_fill_time', ascending=False)

#    st.set_page_config(layout="wide")

    # Plotting the first chart (Chain Pair vs Median Fill Time) using Altair
    st.title('Fill Time Visualizations')

    # Create two columns to place the charts next to each other
    col1, col2, col3 = st.columns([3, 3, 2])

    # First chart (Chain Pair vs Median Fill Time)
    with col1:
        st.subheader('Median Fill Time by Chain Pair')
        chart_chain = alt.Chart(df_fill_time_chain).mark_bar().encode(
            x=alt.X('chain_pair:N', sort=None),  # Chain pair on x-axis
            y='median_fill_time:Q',  # Median fill time on y-axis
            color='median_fill_time:Q',  # Color by median fill time
            tooltip=['chain_pair:N', 'median_fill_time:Q']  # Tooltip with chain pair and median fill time
        )
        st.altair_chart(chart_chain, use_container_width=True)

    # Second chart (Fill Time by Date) as a line chart
    with col2:
        st.subheader('Median Fill Time by Date')
        # Convert 'date' column to datetime for line chart
        df_fill_time_date['order_date'] = pd.to_datetime(df_fill_time_date['order_date'])

        # Display the line chart using Streamlit's st.line_chart
        st.line_chart(df_fill_time_date.set_index('order_date')['median_fill_time'])
        
    # Third container (Table) displaying df_fill_time_s_chain, sorted by median_fill_time
    with col3:
        st.subheader('Source Chain Median Fill Time')
        # Sort df_fill_time_s_chain by median_fill_time in descending order
        df_fill_time_s_chain_sorted = df_fill_time_s_chain.sort_values(by='median_fill_time', ascending=False)

        # Display the sorted table
        st.dataframe(df_fill_time_s_chain_sorted[['chain', 'median_fill_time']])

    # Centering the dataframe using columns
    col1, col2, col3 = st.columns([0.5, 7, 0.5])  # Use a ratio of 1:2:1 to center the dataframe

    with col2:  # This column will be centered
        st.subheader("Orders with the Ten Lowest Fill Times")
        st.dataframe(df_fill_time_lowest[['order_uuid', 'source_chain', 'dest_chain', 'source_address', 'dest_address', 'time_order_made', 'fill_time']])

    # Centering the dataframe using columns
    col1, col2, col3 = st.columns([0.5, 7, 0.5])  # Use a ratio of 1:2:1 to center the dataframe

    with col2:  # This column will be centered
        st.subheader("Orders with the Ten Highest Fill Times")
        st.dataframe(df_fill_time_highest[['order_uuid', 'source_chain', 'dest_chain', 'source_address', 'dest_address', 'time_order_made', 'fill_time']])
        

elif page == "New Users":
    
    # Supabase credentials
    supabase_url = "https://fzkeftdzgseugijplhsh.supabase.co"
    supabase_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZ6a2VmdGR6Z3NldWdpanBsaHNoIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTczMjcxMzk3NCwiZXhwIjoyMDQ4Mjg5OTc0fQ.Og46ddAeoybqUavWBAUbUoj8HJiZrfAQZi-6gRP46i4"

    sql_query1 = """
    WITH ranked_orders AS (
        SELECT
            op.order_uuid,
            op.sender_address,
            DATE(op.block_timestamp) AS order_date,  -- Assuming there's a timestamp column in the op table
            ROW_NUMBER() OVER (PARTITION BY op.sender_address ORDER BY op.block_timestamp) AS first_seen
        FROM order_placed op
        INNER JOIN match_executed me
        ON op.order_uuid = me.order_uuid
    )
    SELECT
        order_date,
        COUNT(DISTINCT sender_address) AS new_sender_addresses
    FROM ranked_orders
    WHERE first_seen = 1  -- Only count the first occurrence of each sender_address
    GROUP BY order_date
    ORDER BY order_date
    """


    sql_query2 = """
    WITH date_sender_counts AS (
        SELECT
            DATE(op.block_timestamp) AS order_date,
            op.sender_address
        FROM order_placed op
        INNER JOIN match_executed me
        ON op.order_uuid = me.order_uuid
        GROUP BY order_date, op.sender_address
    )

    SELECT
        ds1.order_date,
        COUNT(DISTINCT ds2.sender_address) AS cumulative_distinct_sender_addresses
    FROM date_sender_counts ds1
    JOIN date_sender_counts ds2
        ON ds2.order_date <= ds1.order_date
    GROUP BY ds1.order_date
    ORDER BY ds1.order_date
    """

    def execute_sql(query):
        headers = {
            "apikey": supabase_key,
            "Authorization": f"Bearer {supabase_key}",
            "Content-Type": "application/json"
        }
        # Endpoint for the RPC function
        rpc_endpoint = f"{supabase_url}/rest/v1/rpc/execute_sql"
        
        # Payload with the SQL query
        payload = {"query": query}
        
        # Make the POST request to the RPC function
        response = requests.post(rpc_endpoint, headers=headers, json=payload)
        
        # Handle response
        if response.status_code == 200:
            data = response.json()
            df = pd.DataFrame(data)
            return df
        else:
            print("Error executing query:", response.status_code, response.json())

    # Call the function
    df_new_addresses = execute_sql(sql_query1)
    df_cumulative_address = execute_sql(sql_query2)

    df_new_addresses = pd.json_normalize(df_new_addresses['result'])
    df_cumulative_address = pd.json_normalize(df_cumulative_address['result'])
    #st.set_page_config(layout="wide")
    st.title('New Users Visualization')
    # Create Streamlit layout with two columns
    col1, col2 = st.columns(2)


    # Plotting the first chart (Chain Pair vs Median Fill Time) using Altair

    # First chart: Cumulative distinct sender addresses
    with col2:
        st.subheader('Cumulative Distinct Sender Addresses')
        chart_cumulative = alt.Chart(df_cumulative_address).mark_line().encode(
            x='order_date:T',
            y='cumulative_distinct_sender_addresses:Q'
        ).properties(width=500, height=300)
        st.altair_chart(chart_cumulative)

    # Second chart: New sender addresses
    with col1:
        st.subheader('New Sender Addresses')
        chart_new = alt.Chart(df_new_addresses).mark_line().encode(
            x='order_date:T',
            y='new_sender_addresses:Q'
        ).properties(width=500, height=300)
        st.altair_chart(chart_new)

elif page == "CCTP Data":
    
    supabase_url_2 = "https://lgujolalhjnsrfuhzlkq.supabase.co"
    supabase_key_2 = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImxndWpvbGFsaGpuc3JmdWh6bGtxIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTcyNDU0NTgwMCwiZXhwIjoyMDQwMTIxODAwfQ.f9g2UA0-aGbbwWujrFooC8O7RXGQt5nz-eTxTvtUDqk"


    sql_query1 = """  
    WITH cctp_table AS (
    SELECT DATE(created_at) AS transfer_date, SUM(CAST(amount AS FLOAT)) AS total_amount
    FROM cctp_transfers
    GROUP BY DATE(created_at)
    ORDER BY transfer_date
    )
    SELECT * FROM cctp_table
    """

    def execute_sql_2(query):
        headers = {
            "apikey": supabase_key_2,
            "Authorization": f"Bearer {supabase_key_2}",
            "Content-Type": "application/json"
        }
        # Endpoint for the RPC function
        rpc_endpoint = f"{supabase_url_2}/rest/v1/rpc/execute_sql"
        
        # Payload with the SQL query
        payload = {"query": query}
        
        # Make the POST request to the RPC function
        response = requests.post(rpc_endpoint, headers=headers, json=payload)
        
        # Handle response
        if response.status_code == 200:
            data = response.json()
            
            df = pd.DataFrame(data)
            
            print("Query executed successfully, returning DataFrame.")
            return(df)
        else:
            print("Error executing query:", response.status_code, response.json())

    df_cctp = execute_sql_2(sql_query1)
    df_cctp = pd.json_normalize(df_cctp['result'])

    # Convert 'transfer_date' to datetime
    df_cctp['transfer_date'] = pd.to_datetime(df_cctp['transfer_date'], format='%Y-%m-%d')
    #st.set_page_config(layout="wide")
    st.title('CCTP Data')
    # Set 'transfer_date' as the index for the plot
    df_cctp.set_index('transfer_date', inplace=True)
    col1, col2 = st.columns(2)
    with col1:
        # Plot the data using Streamlit
        st.subheader('CCTP Volume Graph')
        st.line_chart(df_cctp['total_amount'])

    with col2:
        # Display the DataFrame with formatted dates for reference (optional)
        st.subheader('CCTP Volume Data')
        st.write(df_cctp)







    
    
    
    
    
    
    
    
    
    