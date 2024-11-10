import pandas as pd
import json
import os
import pymysql
import matplotlib.pyplot as plt
import plotly.express as px
import subprocess
import streamlit as st
from streamlit_option_menu import option_menu
import time
import plotly.graph_objects as go

# The command to clone the repository
if os.path.isdir('pulse') is not True:
    phonepe_url = "https://github.com/PhonePe/pulse.git"
    subprocess.run(["git", "clone", phonepe_url])

def collection_table():
    #Aggregated Insurance Data
    df_agg_ins=pd.read_csv('data/Aggregated_insurance.csv')

    #Table Creation-1
    connection = pymysql.connect(
            host='localhost',
            user='root',
            password='Tamizhini@04',
            port=3306,
            autocommit=True
            )

    mycursor = connection.cursor()

    mycursor.execute('CREATE DATABASE IF NOT EXISTS Phonepe_Data')
    mycursor.execute('USE Phonepe_Data')

    query = '''CREATE TABLE IF NOT EXISTS Aggregated_Insurance
                                                    (id INT AUTO_INCREMENT PRIMARY KEY,
                                                        State VARCHAR(50),
                                                        Year BIGINT,
                                                        Quarter INT,
                                                        `Transaction Type` VARCHAR(50),
                                                        `Transaction Count` BIGINT,
                                                        `Transaction Amount` BIGINT,
                                                        UNIQUE(State, Year, Quarter, `Transaction Type`,`Transaction Count`,`Transaction Amount`)
                                                        )'''

    mycursor.execute(query)

    for index,row in df_agg_ins.iterrows():
        query1='''INSERT INTO Aggregated_Insurance
                                                    (State,
                                                    Year,
                                                    Quarter,
                                                    `Transaction Type`,
                                                    `Transaction Count`,
                                                    `Transaction Amount`)
                                                    
                                                    values(%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE
                                            State=values(State),
                                            Year=values(Year),
                                            Quarter=values(Quarter),
                                            `Transaction Type`=values(`Transaction Type`),
                                            `Transaction Count`=values(`Transaction Count`),
                                            `Transaction Amount`=values(`Transaction Amount`)'''
        
        
        values=(row['State'],
                row['Year'],
                row['Quarter'],
                row['Transaction_Type'],
                row['Transaction_Count'],
                row['Transaction_Amount'])
        
        mycursor.execute(query1,values)

    #Aggregated Transaction Data
    df_agg_trans=pd.read_csv('data/Aggregated_transaction.csv')

    #Table Creation-2
    query = '''CREATE TABLE IF NOT EXISTS Aggregated_Transaction
                                                    (id INT AUTO_INCREMENT PRIMARY KEY,
                                                        State VARCHAR(50),
                                                        Year BIGINT,
                                                        Quarter INT,
                                                        `Transaction Type` VARCHAR(50),
                                                        `Transaction Count` BIGINT,
                                                        `Transaction Amount` BIGINT,
                                                        UNIQUE(State, Year, Quarter, `Transaction Type`,`Transaction Count`,`Transaction Amount`)
                                                    )'''

    mycursor.execute(query)

    for index,row in df_agg_trans.iterrows():
        query1='''INSERT INTO Aggregated_Transaction
                                                    (State,
                                                    Year,
                                                    Quarter,
                                                    `Transaction Type`,
                                                    `Transaction Count`,
                                                    `Transaction Amount`)
                                                    
                                                    values(%s,%s,%s,%s,%s,%s)
                                                    
                    ON DUPLICATE KEY UPDATE
                                            State=values(State),
                                            Year=values(Year),
                                            Quarter=values(Quarter),
                                            `Transaction Type`=values(`Transaction Type`),
                                            `Transaction Count`=values(`Transaction Count`),
                                            `Transaction Amount`=values(`Transaction Amount`)'''
        
        values=(row['State'],
                row['Year'],
                row['Quarter'],
                row['Transaction_Type'],
                row['Transaction_Count'],
                row['Transaction_Amount'])
        
        mycursor.execute(query1, values)

    #Aggregated User Data
    df_agg_user=pd.read_csv('data/Aggregated_user.csv')

    #Table Creation-3
    query = '''CREATE TABLE IF NOT EXISTS Aggregated_User
                                                    (id INT AUTO_INCREMENT PRIMARY KEY,
                                                    State VARCHAR(50),
                                                        Year BIGINT,
                                                        Quarter INT,
                                                        Brand VARCHAR(20),
                                                        `Transaction Count` BIGINT,
                                                        Percentage DECIMAL(10,6),
                                                        UNIQUE (State, Year, Quarter, Brand, `Transaction Count`, Percentage)
                                                        )'''

    mycursor.execute(query)

    for index,row in df_agg_user.iterrows():
        query1='''INSERT INTO Aggregated_User
                                            (State,
                                            Year,
                                            Quarter,
                                            Brand,
                                            `Transaction Count`,
                                            Percentage)
                                            
                                            values(%s,%s,%s,%s,%s,%s)
                                            
                    ON DUPLICATE KEY UPDATE
                                            State=values(State),
                                            Year=values(Year),
                                            Quarter=values(Quarter),
                                            Brand=values(Brand),
                                            `Transaction Count`=values(`Transaction Count`),
                                            Percentage=values(Percentage)'''
        
        values=(row['State'],
                row['Year'],
                row['Quarter'],
                row['Brand'],
                row['Transaction_Count'],
                row['Percentage'])
        
        mycursor.execute(query1,values)

    #Map Insurance Data
    df_map_ins=pd.read_csv('data/Map_insurance.csv')

    #Table Creation-4
    query = '''CREATE TABLE IF NOT EXISTS Map_Insurance
                                                    (id INT AUTO_INCREMENT PRIMARY KEY,
                                                    State VARCHAR(200),
                                                        Year BIGINT,
                                                        Quarter INT,
                                                        District VARCHAR(500),
                                                        `Transaction Count` BIGINT,
                                                        `Transaction Amount` BIGINT,
                                                        UNIQUE (State, Year, Quarter, District, `Transaction Count`, `Transaction Amount`)
                                                        )'''

    mycursor.execute(query)

    for index,row in df_map_ins.iterrows():
        query1='''INSERT INTO Map_Insurance
                                            (State,
                                            Year,
                                            Quarter,
                                            District,
                                            `Transaction Count`,
                                            `Transaction Amount`)
                                            
                                            values(%s,%s,%s,%s,%s,%s)
                                            
                    ON DUPLICATE KEY UPDATE
                                            State=values(State),
                                            Year=values(Year),
                                            Quarter=values(Quarter),
                                            District=values(District),
                                            `Transaction Count`=values(`Transaction Count`),
                                            `Transaction Amount`=values(`Transaction Amount`)'''
        
        values=(row['State'],
                row['Year'],
                row['Quarter'],
                row['District'],
                row['Transaction_Count'],
                row['Transaction_Amount'])
        
        mycursor.execute(query1,values)

    #Map Transaction Data
    df_map_trans=pd.read_csv('data/Map_transaction.csv')

    #Table Creation-5
    query = '''CREATE TABLE IF NOT EXISTS Map_Transaction
                                                    (id INT AUTO_INCREMENT PRIMARY KEY,
                                                    State VARCHAR(200),
                                                        Year BIGINT,
                                                        Quarter INT,
                                                        District VARCHAR(100),
                                                        `Transaction Count` BIGINT,
                                                        `Transaction Amount` BIGINT,
                                                        UNIQUE (State, Year, Quarter, District, `Transaction Count`, `Transaction Amount`)
                                                        )'''

    mycursor.execute(query)

    for index,row in df_map_trans.iterrows():
        query1='''INSERT INTO Map_Transaction
                                            (State,
                                            Year,
                                            Quarter,
                                            District,
                                            `Transaction Count`,
                                            `Transaction Amount`)
                                            
                                            values(%s,%s,%s,%s,%s,%s)
                                            
                    ON DUPLICATE KEY UPDATE
                                            State=values(State),
                                            Year=values(Year),
                                            Quarter=values(Quarter),
                                            District=values(District),
                                            `Transaction Count`=values(`Transaction Count`),
                                            `Transaction Amount`=values(`Transaction Amount`)'''
        
        values=(row['State'],
                row['Year'],
                row['Quarter'],
                row['District'],
                row['Transaction_Count'],
                row['Transaction_Amount'])
        
        
        mycursor.execute(query1, values)

    #Map User Data
    df_map_user=pd.read_csv('data/Map_user.csv')

    #Table Creation-6
    query = '''CREATE TABLE IF NOT EXISTS Map_User
                                                    (id INT AUTO_INCREMENT PRIMARY KEY,
                                                    State VARCHAR(200),
                                                        Year BIGINT,
                                                        Quarter INT,
                                                        District VARCHAR(100),
                                                        `Registered Users` BIGINT,
                                                        `App Opens` BIGINT,
                                                        UNIQUE (State, Year, Quarter, District, `Registered Users`, `App Opens`)
                                                        )'''

    mycursor.execute(query)

    for index,row in df_map_user.iterrows():
        query1='''INSERT INTO Map_User
                                            (State,
                                            Year,
                                            Quarter,
                                            District,
                                            `Registered Users`,
                                            `App Opens`)
                                            
                                            values(%s,%s,%s,%s,%s,%s)
                                            
                    ON DUPLICATE KEY UPDATE
                                            State=values(State),
                                            Year=values(Year),
                                            Quarter=values(Quarter),
                                            District=values(District),
                                            `Registered Users`=values(`Registered Users`),
                                            `App Opens`=values(`App Opens`)'''
        
        values=(row['State'],
                row['Year'],
                row['Quarter'],
                row['District'],
                row['Registered_Users'],
                row['App_Opens'])
        
        mycursor.execute(query1, values)

    #Top Insurance Data
    df_top_ins=pd.read_csv('data/Top_insurance.csv')

    #Table Creation-7
    query = '''CREATE TABLE IF NOT EXISTS Top_Insurance
                                                    (id INT AUTO_INCREMENT PRIMARY KEY,
                                                    State VARCHAR(200),
                                                        Year BIGINT,
                                                        Quarter INT,
                                                        Pincode VARCHAR(6),
                                                        `Transaction Count` BIGINT,
                                                        `Transaction Amount` BIGINT,
                                                        UNIQUE (State, Year, Quarter, Pincode, `Transaction Count`, `Transaction Amount`)
                                                        )'''

    mycursor.execute(query)

    for index,row in df_top_ins.iterrows():
        query1='''INSERT INTO Top_Insurance
                                            (State,
                                            Year,
                                            Quarter,
                                            Pincode,
                                            `Transaction Count`,
                                            `Transaction Amount`)
                                            
                                            values(%s,%s,%s,%s,%s,%s)
                                            
                    ON DUPLICATE KEY UPDATE
                                            State=values(State),
                                            Year=values(Year),
                                            Quarter=values(Quarter),
                                            Pincode=values(Pincode),
                                            `Transaction Count`=values(`Transaction Count`),
                                            `Transaction Amount`=values(`Transaction Amount`)'''
        
        values=(row['State'],
                row['Year'],
                row['Quarter'],
                row['Pincode'],
                row['Transaction_Count'],
                row['Transaction_Amount'])

        mycursor.execute(query1, values)

    #Top Transaction Data
    df_top_trans=pd.read_csv('data/Top_transaction.csv')
    
    #Table Creation-8
    query = '''CREATE TABLE IF NOT EXISTS Top_Transaction
                                                    (id INT AUTO_INCREMENT PRIMARY KEY,
                                                    State VARCHAR(200),
                                                        Year BIGINT,
                                                        Quarter INT,
                                                        Pincode VARCHAR(6),
                                                        `Transaction Count` BIGINT,
                                                        `Transaction Amount` BIGINT,
                                                        UNIQUE (State, Year, Quarter, Pincode, `Transaction Count`, `Transaction Amount`)
                                                        )'''

    mycursor.execute(query)

    for index,row in df_top_trans.iterrows():
        query1='''INSERT INTO Top_Transaction
                                            (State,
                                            Year,
                                            Quarter,
                                            Pincode,
                                            `Transaction Count`,
                                            `Transaction Amount`)
                                            
                                            values(%s,%s,%s,%s,%s,%s)
                                            
                    ON DUPLICATE KEY UPDATE
                                            State=values(State),
                                            Year=values(Year),
                                            Quarter=values(Quarter),
                                            Pincode=values(Pincode),
                                            `Transaction Count`=values(`Transaction Count`),
                                            `Transaction Amount`=values(`Transaction Amount`)'''
        
        values=(row['State'],
                row['Year'],
                row['Quarter'],
                row['Pincode'],
                row['Transaction_Count'],
                row['Transaction_Amount'])
        
        mycursor.execute(query1, values)

    #Top User Data
    df_top_user=pd.read_csv('data/Top_user.csv')
    
    #Table Creation-9
    query = '''CREATE TABLE IF NOT EXISTS Top_User
                                                    (id INT AUTO_INCREMENT PRIMARY KEY,
                                                    State VARCHAR(200),
                                                        Year BIGINT,
                                                        Quarter INT,
                                                        Pincode VARCHAR(6),
                                                        `Registered Users` BIGINT,
                                                        UNIQUE (State, Year, Quarter, Pincode, `Registered Users`)
                                                        )'''

    mycursor.execute(query)

    for index,row in df_top_user.iterrows():
        query1='''INSERT INTO Top_User
                                            (State,
                                            Year,
                                            Quarter,
                                            Pincode,
                                            `Registered Users`)
                                            
                                            values(%s,%s,%s,%s,%s)
                                            
                    ON DUPLICATE KEY UPDATE
                                            State=values(State),
                                            Year=values(Year),
                                            Quarter=values(Quarter),
                                            Pincode=values(Pincode),
                                            `Registered Users`=values(`Registered Users`)'''
        
        values=(row['State'],
                row['Year'],
                row['Quarter'],
                row['Pincode'],
                row['Registered_Users'])
        
        mycursor.execute(query1, values)

#Extracting from SQL

def Extract_sql():
    connection = pymysql.connect(
            host='localhost',
            user='root',
            password='Tamizhini@04',
            port=3306,
            autocommit=True
            )
    mycursor = connection.cursor()

    mycursor.execute('USE Phonepe_Data')

    query1='''SELECT * from Aggregated_insurance'''

    mycursor.execute(query1)
    table1=mycursor.fetchall()
    df1=pd.DataFrame(table1,columns=['ID','State','Year','Quarter','Transaction Type','Transaction Count','Transaction Amount'])

    query2='''SELECT * from Aggregated_Transaction'''

    mycursor.execute(query2)
    table2=mycursor.fetchall()
    df2=pd.DataFrame(table2,columns=['ID','State','Year','Quarter','Transaction Type','Transaction Count','Transaction Amount'])

    query3='''SELECT * from Aggregated_User'''

    mycursor.execute(query3)
    table3=mycursor.fetchall()
    df3=pd.DataFrame(table3,columns=['ID','State','Year','Quarter','Brand','Transaction Count','Percentage'])

    query4='''SELECT * from Map_Insurance'''

    mycursor.execute(query4)
    table4=mycursor.fetchall()
    df4=pd.DataFrame(table4,columns=['ID','State','Year','Quarter','District','Transaction Count','Transaction Amount'])

    query5='''SELECT * from Map_Transaction'''

    mycursor.execute(query5)
    table5=mycursor.fetchall()
    df5=pd.DataFrame(table5,columns=['ID','State','Year','Quarter','District','Transaction Count','Transaction Amount'])

    query6='''SELECT * from Map_User'''

    mycursor.execute(query6)
    table6=mycursor.fetchall()
    df6=pd.DataFrame(table6,columns=['ID','State','Year','Quarter','District','Registered Users','App Opens'])

    query7='''SELECT * from Top_Insurance'''

    mycursor.execute(query7)
    table7=mycursor.fetchall()
    df7=pd.DataFrame(table7,columns=['ID','State','Year','Quarter','Pincode','Transaction Count','Transaction Amount'])

    query8='''SELECT * from Top_Transaction'''

    mycursor.execute(query8)
    table8=mycursor.fetchall()
    df8=pd.DataFrame(table8,columns=['ID','State','Year','Quarter','Pincode','Transaction Count','Transaction Amount'])


    query9='''SELECT * from Top_User'''

    mycursor.execute(query9)
    table9=mycursor.fetchall()
    df9=pd.DataFrame(table9,columns=['ID','State','Year','Quarter','Pincode','Registered Users'])

    return df1, df2, df3, df4, df5, df6, df7, df8, df9



def agg_ins_tcount():    
# First plot: Transaction Count for Insurance data
    df1['Quarter'] = pd.Categorical(df1['Quarter'], categories=[1, 2, 3, 4], ordered=True)

# Sort the DataFrame by Year and Quarter to ensure correct order
    df = df1.sort_values(by=['Year', 'Quarter'])

# Plotting
    fig = go.Figure()
    for quarter in [1, 2, 3, 4]:                       # Loop through each quarter and add traces in order
        quarter_data = df[df['Quarter'] == quarter]
        if not quarter_data.empty:
            fig.add_trace(go.Bar(
                x=quarter_data['Year'],                # X-axis: Years
                y=quarter_data['Transaction Count'],   # Y-axis: Transaction Counts
                name=f'Q{quarter}'                     # Label for the legend
            ))

    fig.update_layout(
        barmode='group',             # Group bars by Year
        xaxis_title='Year',            # X-axis title
        yaxis_title='Transaction Count', # Y-axis title
        xaxis=dict(
            tickmode='linear',
            tickvals=df['Year'].unique(),
            ticktext=df['Year'].unique()
        )
)
    return fig

def agg_trans_tcount():  
    #Second plot: Transaction Count for Transaction data
    df2['Quarter'] = pd.Categorical(df2['Quarter'], categories=[1, 2, 3, 4], ordered=True)
    df = df2.sort_values(by=['Year', 'Quarter'])
    fig = go.Figure()
    
    for quarter in [1,2,3,4]:
        quarter_data = df[df['Quarter'] == quarter]
        if not quarter_data.empty:
            fig.add_trace(go.Bar(
            x=quarter_data['Year'],
            y=quarter_data['Transaction Count'],
            name=f'Q{quarter}' 
    ))
    fig.update_layout(
        barmode='group',
        xaxis_title='Year',      
        yaxis_title='Transaction Count', 
        xaxis=dict(
            tickmode='linear',
            tickvals=df['Year'].unique(),
            ticktext=df['Year'].unique()
        )
    )

    return fig

    
def agg_ins_tamount():
    # First plot: Transaction Amount for Insurance Data
    df1['Quarter']=pd.Categorical(df1['Quarter'],categories=[1,2,3,4], ordered=True)
    df=df1.sort_values(by=['Year', 'Quarter'])

    fig = go.Figure()
    
    for quarter in [1,2,3,4]:
        quarter_data = df[df['Quarter'] == quarter]
        if not quarter_data.empty:
            fig.add_trace(go.Bar(
            x=quarter_data['Year'],
            y=quarter_data['Transaction Amount'],
            name=f'Q{quarter}' 
    ))
    fig.update_layout(
        barmode='stack',
        xaxis_title='Year',      
        yaxis_title='Transaction Amount',
        xaxis=dict(
            tickmode='linear',
            tickvals=df['Year'].unique(),
            ticktext=df['Year'].unique()
        )
    )

    return fig

def agg_trans_tamount():
    # First plot: Transaction Amount for Transaction Data
    df2['Quarter']=pd.Categorical(df2['Quarter'],categories=[1,2,3,4], ordered=True)
    df=df2.sort_values(by=['Year', 'Quarter'])

    fig = go.Figure()
    
    for quarter in [1,2,3,4]:
        quarter_data = df[df['Quarter'] == quarter]
        if not quarter_data.empty:
            fig.add_trace(go.Bar(
            x=quarter_data['Year'],
            y=quarter_data['Transaction Amount'],
            name=f'Q{quarter}' 
    ))
    fig.update_layout(
        barmode='stack',
        xaxis_title='Year',      
        yaxis_title='Transaction Amount', 
        xaxis=dict(
            tickmode='linear',
            tickvals=df['Year'],
            ticktext=df['Year']
        )
    )

    return fig

def agg_ttype_tcount():
    df = df2.groupby('Transaction Type', as_index=False).sum()
    fig = go.Figure(data=go.Pie(
        values=df['Transaction Count'],
        labels=df['Transaction Type'],
        textinfo='label+percent',
        insidetextorientation='radial'
    ))
    fig.update_layout(
        width=600,
        height=600,
        paper_bgcolor='rgba(0,0,0,0)',  # Background color of the whole figure
        plot_bgcolor='rgba(0,0,0,0)'    # Background color of the plotting area
    )

    return fig

def agg_ttype_tamount():
    # Group the DataFrame by 'Transaction Type' and sum 'Transaction Amount'
    df = df2.groupby('Transaction Type', as_index=False).sum()

    # Create a Plotly pie chart
    fig = go.Figure(data=go.Pie(
        values=df['Transaction Amount'],
        labels=df['Transaction Type'],
        textinfo='label+percent',
        insidetextorientation='radial'
    ))

    # Update layout to set background colors
    fig.update_layout(
        paper_bgcolor='rgba(0,0,0,0)',  # Background color of the whole figure
        plot_bgcolor='rgba(0,0,0,0)'    # Background color of the plotting area
    )

    # Return the figure object for further use if needed
    return fig

def ttype_tcount_tamount():
    states=df2['State'].unique()
    options=st.selectbox('Select a State you would like to visualize',states)
    d_f=df2[df2['State']==options]
    d_f.reset_index(drop=True,inplace=True)

    dfg=d_f.groupby('Transaction Type')[['Transaction Count','Transaction Amount']].sum()
    dfg.reset_index(inplace=True)

    fig_count=px.pie(dfg,
                    names='Transaction Type',
                    values='Transaction Count',
                    title='Transaction Type Analysis with Transaction Count',
                    hole = 0.4,
                    width=600, height=600,hover_name='Transaction Type')
    st.plotly_chart(fig_count)


    fig_amount=px.pie(dfg,
                    names='Transaction Type',
                    values='Transaction Amount',
                    title='Transaction Type Analysis with Transaction Amount',
                    hole = 0.4,
                    width=600, height=600,hover_name='Transaction Type')
    st.plotly_chart(fig_amount)

    return ttype_tcount_tamount

def agg_ins_qwise_tcount():
    df1['Quarter'] = pd.Categorical(df1['Quarter'], categories=[1, 2, 3, 4], ordered=True)
# Sort the DataFrame by Year and Quarter to ensure correct order
    df = df1.sort_values(by=['Year', 'Quarter'])

# Plotting
    fig = go.Figure()
    for year in [2020, 2021, 2022, 2023, 2024]:                       # Loop through each quarter and add traces in order
        year_data = df[df['Year'] == year]
        if not year_data.empty:
            fig.add_trace(go.Bar(
                x=year_data['Quarter'],                # X-axis: Years
                y=year_data['Transaction Count'],   # Y-axis: Transaction Counts
                name=f'FY{year}'                     # Label for the legend
            ))

    fig.update_layout(
    barmode='group',               # Group bars by Year
    xaxis_title='Quarter',            # X-axis title
    yaxis_title='Transaction Count', # Y-axis title
    xaxis=dict(
        tickmode='linear',
        tickvals=df['Quarter'].unique(),
        ticktext=df['Quarter'].unique()
    )
)
    
    return fig
    
def agg_trans_qwise_tcount():
    df2['Quarter'] = pd.Categorical(df2['Quarter'], categories=[1, 2, 3, 4], ordered=True)
# Sort the DataFrame by Year and Quarter to ensure correct order
    df = df2.sort_values(by=['Year', 'Quarter'])

# Plotting
    fig = go.Figure()
    for year in [2020, 2021, 2022, 2023, 2024]:                       # Loop through each quarter and add traces in order
        year_data = df[df['Year'] == year]
        if not year_data.empty:
            fig.add_trace(go.Bar(
                x=year_data['Quarter'],                # X-axis: Years
                y=year_data['Transaction Count'],   # Y-axis: Transaction Counts
                name=f'FY{year}'                     # Label for the legend
            ))

    fig.update_layout(
    barmode='group',               # Group bars by Year
    xaxis_title='Quarter',            # X-axis title
    yaxis_title='Transaction Count', # Y-axis title
    xaxis=dict(
        tickmode='linear',
        tickvals=df['Quarter'].unique(),
        ticktext=df['Quarter'].unique()
    )
)
    
    return fig

def agg_ins_qwise_tamount():
    df1['Quarter'] = pd.Categorical(df1['Quarter'], categories=[1, 2, 3, 4], ordered=True)
# Sort the DataFrame by Year and Quarter to ensure correct order
    df = df1.sort_values(by=['Year', 'Quarter'])

# Plotting
    fig = go.Figure()
    for year in [2020, 2021, 2022, 2023, 2024]:                       # Loop through each quarter and add traces in order
        year_data = df[df['Year'] == year]
        if not year_data.empty:
            fig.add_trace(go.Bar(
                x=year_data['Quarter'],                # X-axis: Years
                y=year_data['Transaction Amount'],   # Y-axis: Transaction Counts
                name=f'FY{year}'                     # Label for the legend
            ))

    fig.update_layout(
    barmode='group',               # Group bars by Year
    xaxis_title='Quarter',            # X-axis title
    yaxis_title='Transaction Amount', # Y-axis title
    xaxis=dict(
        tickmode='linear',
        tickvals=df['Quarter'].unique(),
        ticktext=df['Quarter'].unique()
    )
)
    
    return fig

def agg_trans_qwise_tamount():
    df2['Quarter'] = pd.Categorical(df2['Quarter'], categories=[1, 2, 3, 4], ordered=True)
# Sort the DataFrame by Year and Quarter to ensure correct order
    df = df2.sort_values(by=['Year', 'Quarter'])

# Plotting
    fig = go.Figure()
    for year in [2020, 2021, 2022, 2023, 2024]:                       # Loop through each quarter and add traces in order
        year_data = df[df['Year'] == year]
        if not year_data.empty:
            fig.add_trace(go.Bar(
                x=year_data['Quarter'],                # X-axis: Years
                y=year_data['Transaction Amount'],   # Y-axis: Transaction Counts
                name=f'FY{year}'                     # Label for the legend
            ))

    fig.update_layout(
    barmode='group',               # Group bars by Year
    xaxis_title='Quarter',            # X-axis title
    yaxis_title='Transaction Amount', # Y-axis title
    xaxis=dict(
        tickmode='linear',
        tickvals=df['Quarter'].unique(),
        ticktext=df['Quarter'].unique()
    )
)
    
    return fig

def agg_brand_tcount():
    # Group by Brand and Year, then sum the Transaction Count
    df= df3.groupby(['Brand', 'Year'])['Transaction Count'].sum().reset_index()

    fig = px.line(df, x='Year', y='Transaction Count', color='Brand',
                  labels={'Transaction Count': 'Transaction Count'})
    
    fig.update_layout(
        paper_bgcolor='rgba(0,0,0,0)',  # Background color of the whole figure
        plot_bgcolor='rgba(0,0,0,0)'    # Background color of the plotting area
    )

    return fig

def agg_bpercent_over_yrs():
    #how each brand performs over the years, with the focus primarily on the brand
    df=df3.groupby(['Brand', 'Year'], as_index=False)['Percentage'].sum()

    # Create a stacked bar chart
    fig = px.treemap(df, path=['Brand'], values='Percentage',
                     labels={'Percentage': 'Percentage (%)'})

    # Update layout for background colors if needed
    fig.update_layout(
        paper_bgcolor='rgba(0,0,0,0)',  # Background color of the whole figure
        plot_bgcolor='rgba(0,0,0,0)'    # Background color of the plotting area
    )

    return fig

def agg_bpercent_each_yr():
    #how brands perform within each year, with the focus primarily on the year
    # Group the data by 'Year' and 'Brand', summing up the 'Percentage'
    df= df3.groupby(['Year', 'Brand'], as_index=False)['Percentage'].sum()

    # Create a stacked bar chart
    fig = px.line(df, x='Year', y='Percentage', color='Brand',
                 labels={'Percentage': 'Percentage (%)'})

    # Update layout to set background colors if needed
    fig.update_layout(
        paper_bgcolor='rgba(0,0,0,0)',  # Background color of the whole figure
        plot_bgcolor='rgba(0,0,0,0)',   # Background color of the plotting area
        xaxis_title='Year',
        yaxis_title='Percentage (%)'
    )

    return fig

def agg_state_tcount():
    df_pivot = df2.pivot_table(
    index='State', 
    columns='Year', 
    values='Transaction Count', 
    aggfunc='sum'  # You can use 'sum', 'mean', 'max', etc.
)
    
    # Create the heatmap
    fig = px.imshow(df_pivot,
                    labels=dict(x="Year", y="State", color="Transaction Count"),
                    x=df_pivot.columns,
                    y=df_pivot.index,
                    color_continuous_scale='Blues',
                    aspect='auto')

    # Update layout for background colors if needed
    fig.update_layout(
        paper_bgcolor='rgba(0,0,0,0)', 
        plot_bgcolor='rgba(0,0,0,0)' ,
        width=1500,
        height=800,
           xaxis=dict(
            tickmode='linear',
            tickvals=df2['Year'],
            ticktext=df2['Year']
        ) 
    )
    return fig

def agg_state_tamount():
    df_pivot = df2.pivot_table(
    index='State', 
    columns='Year', 
    values='Transaction Amount', 
    aggfunc='sum'  # You can use 'sum', 'mean', 'max', etc.
)
    
    # Create the heatmap
    fig = px.imshow(df_pivot,
                    labels=dict(x="Year", y="State", color="Transaction Amount"),
                    x=df_pivot.columns,
                    y=df_pivot.index,
                    color_continuous_scale='mint',
                    aspect='auto')

    # Update layout for background colors if needed
    fig.update_layout(
        paper_bgcolor='rgba(0,0,0,0)', 
        plot_bgcolor='rgba(0,0,0,0)' ,
        width=1500,
        height=800,
           xaxis=dict(
            tickmode='linear',
            tickvals=df2['Year'],
            ticktext=df2['Year']
        )
    ) 

    return fig

def agg_state_brand():
    df_pivot = df3.pivot_table(
    index='State', 
    columns='Brand', 
    values='Percentage', 
    aggfunc='sum'  # You can use 'sum', 'mean', 'max', etc.
)
    df_pivot = df_pivot.fillna(0)
    df_pivot = df_pivot/17
    
    # Create the heatmap
    fig = px.imshow(df_pivot,
                    labels=dict(x="Brand", y="State", color="Percentage %"),
                    x=df_pivot.columns,
                    y=df_pivot.index,
                    color_continuous_scale='bupu',
                    aspect='auto')

    return fig

def map_ins_state_wise():
    states = df4['State'].unique()
    selected_state=st.select_slider('States',options=states)
    df = df4[df4['State'] == selected_state]

    district_data = df.groupby('District').agg({'Transaction Count': 'sum','Transaction Amount': 'sum'}).reset_index()
    st.subheader(f"District-wise Transaction Analysis for {selected_state} from Map Insurance Data")

    fig_count = px.bar(district_data, 
                x='District', 
                y='Transaction Count',
                title=f"Transaction Count by Districts in {selected_state} from Map Insurance Data")

    fig_amount = px.bar(district_data, 
                x='District',
                y='Transaction Amount',
                title=f"Transaction Amount by Districts in {selected_state} from Map Insurance Data")
    
    return fig_count, fig_amount

def map_trans_stwise_tcount():
    states = df5['State'].unique()
    selected_state=st.select_slider('States',options=states)
    df = df5[df5['State'] == selected_state]

    district_data = df.groupby('District').agg({'Transaction Count': 'sum','Transaction Amount': 'sum'}).reset_index()
    st.subheader(f"District-wise Transaction Analysis for {selected_state} from Map Transaction Data")

    fig_count = px.treemap(district_data, 
                path=['District'], 
                values='Transaction Count',
                title=f"Transaction Count by Districts in {selected_state} from Map Transaction Data")
    
    return fig_count

def map_trans_stwise_tamount():
    states = df5['State'].unique()
    selected_state=st.select_slider('States',options=states)
    df = df5[df5['State'] == selected_state]

    district_data = df.groupby('District').agg({'Transaction Count': 'sum','Transaction Amount': 'sum'}).reset_index()
    st.subheader(f"District-wise Transaction Analysis for {selected_state} from Map Transaction Data")

    fig_amount = px.line(district_data, 
                x=['District'],
                y='Transaction Amount',
                title=f"Transaction Amount by Districts in {selected_state} from Map Transaction Data")
    
    return fig_amount

def map_user_yreg_user():
    df6['Quarter'] = pd.Categorical(df6['Quarter'], categories=[1, 2, 3, 4], ordered=True)
    df = df6.sort_values(by=['Year', 'Quarter'])

    fig = go.Figure()
    for quarter in [1, 2, 3, 4]:                       # Loop through each quarter and add traces in order
        quarter_data = df[df['Quarter'] == quarter]
        if not quarter_data.empty:
            fig.add_trace(go.Bar(
                x=quarter_data['Year'],                
                y=quarter_data['Registered Users'],
                name=f'Q{quarter}'
                ))

        fig.update_layout(
            barmode='group',             # Group bars by Year
            xaxis_title='Year',            # X-axis title
            yaxis_title='Registered Users', # Y-axis title
            xaxis=dict(
                tickmode='linear',
                tickvals=df['Year'].unique(),
                ticktext=df['Year'].unique()
            )
    )
    
    return fig

def map_user_yapp_opens():
    df6['Quarter'] = pd.Categorical(df6['Quarter'], categories=[1, 2, 3, 4], ordered=True)
    df = df6.sort_values(by=['Year', 'Quarter'])

    fig = go.Figure()
    for quarter in [1, 2, 3, 4]:                       # Loop through each quarter and add traces in order
        quarter_data = df[df['Quarter'] == quarter]
        if not quarter_data.empty:
            fig.add_trace(go.Bar(
                x=quarter_data['Year'],                
                y=quarter_data['App Opens'],
                name=f'Q{quarter}'
                ))

        fig.update_layout(
            barmode='group',             # Group bars by Year
            xaxis_title='Year',            # X-axis title
            yaxis_title='App Opens', # Y-axis title
            xaxis=dict(
                tickmode='linear',
                tickvals=df['Year'].unique(),
                ticktext=df['Year'].unique()
            )
    )
    
    return fig

def map_user_qreg_user():
    df6['Quarter'] = pd.Categorical(df6['Quarter'], categories=[1,2,3,4], ordered=True)
    # Sort the DataFrame by Year and Quarter to ensure correct order
    df = df6.sort_values(by=['Year', 'Quarter'])

    # Plotting
    fig = go.Figure()
    for year in [2018,2019,2020, 2021, 2022, 2023, 2024]:                       # Loop through each quarter and add traces in order
        year_data = df[df['Year'] == year]
        if not year_data.empty:
            fig.add_trace(go.Bar(
                x=year_data['Quarter'],                # X-axis: Years
                y=year_data['Registered Users'],   # Y-axis: Transaction Counts
                name=f'FY{year}'                     # Label for the legend
            ))

    fig.update_layout(
    barmode='group',               # Group bars by Year
    xaxis_title='Quarter',            # X-axis title
    yaxis_title='Registered Users', # Y-axis title
    xaxis=dict(
        tickmode='linear',
        tickvals=df['Quarter'].unique()
        
    )
    )
    
    return fig

def map_user_qapp_opens():
    df6['Quarter'] = pd.Categorical(df6['Quarter'], categories=[1,2,3,4], ordered=True)
    # Sort the DataFrame by Year and Quarter to ensure correct order
    df = df6.sort_values(by=['Year', 'Quarter'])

    # Plotting
    fig = go.Figure()
    for year in [2018,2019,2020, 2021, 2022, 2023, 2024]:                       # Loop through each quarter and add traces in order
        year_data = df[df['Year'] == year]
        if not year_data.empty:
            fig.add_trace(go.Bar(
                x=year_data['Quarter'],                # X-axis: Years
                y=year_data['App Opens'],   # Y-axis: Transaction Counts
                name=f'FY{year}'                     # Label for the legend
            ))

    fig.update_layout(
    barmode='group',               # Group bars by Year
    xaxis_title='Quarter',            # X-axis title
    yaxis_title='App Opens', # Y-axis title
    xaxis=dict(
        tickmode='linear',
        tickvals=df['Quarter'].unique()
        
    ))

    return fig
        
def map_high_reg_count():
    df_ten=df6.groupby(['State','District'])['Registered Users'].sum().reset_index().rename(
        columns={'Registered Users': 'Sum_reg_users'}).sort_values(by='Sum_reg_users', ascending=False).head(10)
    fig = px.scatter(df_ten, x='District', y='Sum_reg_users', size='Sum_reg_users', color='State',
                 title='Top 10 Districts by Registered Users', size_max=80)
    fig.update_traces(marker=dict(opacity=0.7, line=dict(width=1, color='DarkSlateGrey')))
    
    return fig
           
def map_high_app_opens():
    df_ten=df6.groupby(['State','District'])['App Opens'].sum().reset_index().rename(
        columns={'App Opens': 'Sum_app_opens'}).sort_values(by='Sum_app_opens', ascending=False).head(10)
    fig = px.scatter(df_ten, x='District', y='Sum_app_opens', size='Sum_app_opens', color='State',
                 title='Top 10 States and Districts by App Opens', size_max=80)
    
    return fig

def top_ins_tcount():
    df7['Pincode'] = df7['Pincode'].astype(str)
    df7['Pincode'] = df7['Pincode'].astype('category')
    df7['Pincode'] = df7['Pincode'].astype('category')
    gby_tcount=df7.groupby(['Pincode','State','Year'])['Transaction Count'].sum().reset_index()
    fig = px.scatter(gby_tcount, x='State', y='Transaction Count', 
                 size='Transaction Count', color='Pincode', 
                 title='Insurance Transaction Analysis by Pincode',
                 hover_data=['Transaction Count'])

    return fig

def top_ins_tamount():
    df7['Pincode'] = df7['Pincode'].astype(str)
    df7['Pincode'] = df7['Pincode'].astype('category')
    gby_tamount=df7.groupby(['Pincode','State','Year'])['Transaction Amount'].sum().reset_index()
    fig = px.scatter(gby_tamount, x='Pincode', y='Transaction Amount', 
                 size='Transaction Amount', color='State', 
                 title='Insurance Transaction Amount Analysis by Pincode',
                 hover_data=['Transaction Amount'])
    
    return fig

def top_ins_tc10():
    df7['Pincode'] = df7['Pincode'].astype(str)
    df7['Pincode'] = df7['Pincode'].astype('category')
    top_tc10=df7.groupby(['Pincode','State'])['Transaction Count'].sum().reset_index().rename(
        columns={'Transaction Count':'Sum_tcount'}).sort_values(by='Sum_tcount', ascending=False).head(10)
    fig = px.treemap(top_tc10, path=['State', 'Pincode'], values='Sum_tcount',
                 title='Top 10 Pincodes by Count of Insurance Transaction')

    return fig

def top_ins_ta10():
    df7['Pincode'] = df7['Pincode'].astype(str)
    df7['Pincode'] = df7['Pincode'].astype('category')
    top_ta10=df7.groupby(['Pincode','State'])['Transaction Amount'].sum().reset_index().rename(
        columns={'Transaction Amount':'Sum_tamount'}).sort_values(by='Sum_tamount', ascending=False).head(10)
    fig = px.treemap(top_ta10, path=['State', 'Pincode'], values='Sum_tamount',
                 title='Top 10 Pincodes by Insurance Transaction Amount')

    return fig

def top_trans_tcount():
    df8['Pincode'] = df8['Pincode'].astype(str)
    df8['Pincode'] = df8['Pincode'].astype('category')
    gby_tcount=df8.groupby(['Pincode','State','Year'])['Transaction Count'].sum().reset_index()
    fig = px.scatter(gby_tcount, x='Pincode', y='Transaction Count', 
                 size='Transaction Count', color='State', 
                 title='Transaction Count Analysis by Pincode',
                 hover_data=['Transaction Count'])
    fig.update_xaxes(type='category',tickformat="")

    return fig

def top_trans_tamount():
    df8['Pincode'] = df8['Pincode'].astype(str)
    df8['Pincode'] = df8['Pincode'].astype('category')
    gby_tamount=df8.groupby(['Pincode','State','Year'])['Transaction Amount'].sum().reset_index()
    fig = px.scatter(gby_tamount, x='Pincode', y='Transaction Amount', 
                 size='Transaction Amount', color='State', 
                 title='Transaction Amount Analysis by Pincode',
                 hover_data=['Transaction Amount'])

    return fig

def top_trans_tc10():
    df8['Pincode'] = df8['Pincode'].astype(str)
    df8['Pincode'] = df8['Pincode'].astype('category')
    top_tc10=df8.groupby(['Pincode','State'])['Transaction Count'].sum().reset_index().rename(
        columns={'Transaction Count':'Sum_tcount'}).sort_values(by='Sum_tcount', ascending=False).head(10)
    fig = px.scatter(top_tc10, x='Pincode', y='Sum_tcount', 
                    size='Sum_tcount', color='State', 
                    title='Top 10 Pincodes by Transaction Count',
                    hover_name='State')  

    return fig

def top_trans_ta10():
    df8['Pincode'] = df8['Pincode'].astype(str)
    df8['Pincode'] = df8['Pincode'].astype('category')
    top_tc10=df8.groupby(['Pincode','State'])['Transaction Amount'].sum().reset_index().rename(
        columns={'Transaction Amount':'Sum_tamount'}).sort_values(by='Sum_tamount', ascending=False).head(10)

    fig = px.scatter(top_tc10, x='Pincode', y='Sum_tamount', 
                    size='Sum_tamount', color='State', 
                    title='Top 10 Pincodes by Transaction Amount',
                    hover_name='State')  

    return fig

if __name__ == "__main__":
    collection_table()
    df1, df2, df3, df4, df5, df6, df7, df8, df9=Extract_sql()

    st.markdown("<h1 style='text-align: center; color: gold;'>Phonepe Data Visualisation</h1>", unsafe_allow_html=True)
    with st.spinner('Loading! Please wait'):
        time.sleep(5)

    with st.sidebar:
        selected = option_menu("Main Menu", ['Home', 'Data Visualisation','Correlation'], 
        icons=['house','folder','file-earmark-text'], menu_icon="sliders", default_index=1)

        if selected=='Home':
            st.markdown("<h2 style='text-align: center; color: brown;'>Welcome to the Homepage</h2>", unsafe_allow_html=True)

            total_tcount=df2['Transaction Count'].sum()
            highest_tcount=df2['Transaction Count'].max()
            total_tamount=df2['Transaction Amount'].sum()
            highest_tamount=df2['Transaction Amount'].max()
            highest_ttype=df2['Transaction Type'].max()
            highest_brand=df3['Brand'].max()
            max=df4.nlargest(1,'Transaction Count')
            district=max['District'].values[0]
            state=max['State'].values[0]
            t_count=max['Transaction Count'].values[0]
            highest_ins_tc_state=f"{district} from {state} with {t_count:,} for Insurance Transactions"
            max1=df4.nlargest(1,'Transaction Amount')
            district1=max1['District'].values[0]
            state1=max1['State'].values[0]
            t_amount=max1['Transaction Amount'].values[0]
            highest_ins_ta_state=f"{district1} from {state1} with {t_amount:,} Transaction Amount for Insurance"
            max2=df5.nlargest(1,'Transaction Count')
            district2=max2['District'].values[0]
            state2=max2['State'].values[0]
            t_count1=max2['Transaction Count'].values[0]
            highest_trans_tcount=f"{district2} from {state2} with {t_count1:,} Transactions"
            max3=df5.nlargest(1,'Transaction Amount')
            district3=max3['District'].values[0]
            state3=max3['State'].values[0]
            t_amount1=max['Transaction Amount'].values[0]
            highest_trans_tamount=f"{district3} from {state3} with {t_amount1:,} Transaction Amount"
            t_reg_users=df6['Registered Users'].sum()
            t_app_opens=df6['App Opens'].sum()
            max4=df6.nlargest(1,'Registered Users')
            district4=max4['District'].values[0]
            state4=max4['State'].values[0]
            reg_user=max4['Registered Users'].values[0]
            highest_reg_users=f"{district4} from {state4} with {reg_user:,} Count of Registered Users"
            max5=df6.nlargest(1,'App Opens')
            district5=max5['District'].values[0]
            state5=max5['State'].values[0]
            app_opens=max5['App Opens'].values[0]
            highest_app_opens=f"{district5} from {state5} with {app_opens:,} Count of App Opens"

            st.markdown("<h3 style='text-align: left; color: teal;'>State Leading in Insurance Transactions by Count</h3>", unsafe_allow_html=True)
            st.markdown(f"<h5 style='text-align: left; color: olive;'>{highest_ins_tc_state}</h5>", unsafe_allow_html=True)
            st.markdown("<h3 style='text-align: left; color: teal;'>State Leading in Insurance Transactions by Amount</h3>", unsafe_allow_html=True)
            st.markdown(f"<h5 style='text-align: left; color: olive;'>{highest_ins_ta_state}</h5>", unsafe_allow_html=True)
            st.markdown("<h3 style='text-align: left; color: teal;'>Total Transactions</h3>", unsafe_allow_html=True)
            st.markdown(f"<h5 style='text-align: left; color: olive;'>{total_tcount:,}</h5>", unsafe_allow_html=True)
            st.markdown("<h3 style='text-align: left; color: teal;'>Highest Transaction Count</h3>", unsafe_allow_html=True)
            st.markdown(f"<h5 style='text-align: left; color: olive;'>{highest_tcount:,}</h5>", unsafe_allow_html=True)
            st.markdown("<h3 style='text-align: left; color: teal;'>Total Transaction Amount</h3>", unsafe_allow_html=True)
            st.markdown(f"<h5 style='text-align: left; color: olive;'>{total_tamount:,}</h5>", unsafe_allow_html=True)
            st.markdown("<h3 style='text-align: left; color: teal;'>Highest Transaction Amount</h3>", unsafe_allow_html=True)
            st.markdown(f"<h5 style='text-align: left; color: olive;'>{highest_tamount:,}</h5>", unsafe_allow_html=True)
            st.markdown("<h3 style='text-align: left; color: teal;'>Most Frequent Transaction Type</h3>", unsafe_allow_html=True)
            st.markdown(f"<h5 style='text-align: left; color: olive;'>{highest_ttype}</h5>", unsafe_allow_html=True)
            st.markdown("<h3 style='text-align: left; color: teal;'>Leading Brand in Transactions</h3>", unsafe_allow_html=True)
            st.markdown(f"<h5 style='text-align: left; color: olive;'>{highest_brand}</h5>", unsafe_allow_html=True)
            st.markdown("<h3 style='text-align: left; color: teal;'>State Leading in Transaction Count</h3>", unsafe_allow_html=True)
            st.markdown(f"<h5 style='text-align: left; color: olive;'>{highest_trans_tcount}</h5>", unsafe_allow_html=True)
            st.markdown("<h3 style='text-align: left; color: teal;'>State Leading in Transaction Amount</h3>", unsafe_allow_html=True)
            st.markdown(f"<h5 style='text-align: left; color: olive;'>{highest_trans_tamount}</h5>", unsafe_allow_html=True)
            st.markdown("<h3 style='text-align: left; color: teal;'>Total Registered Users</h3>", unsafe_allow_html=True)
            st.markdown(f"<h5 style='text-align: left; color: olive;'>{t_reg_users:,}</h5>", unsafe_allow_html=True)
            st.markdown("<h3 style='text-align: left; color: teal;'>Total App Opens</h3>", unsafe_allow_html=True)
            st.markdown(f"<h5 style='text-align: left; color: olive;'>{t_app_opens:,}</h5>", unsafe_allow_html=True)
            st.markdown("<h3 style='text-align: left; color: teal;'>State with Highest Registered Users</h3>", unsafe_allow_html=True)
            st.markdown(f"<h5 style='text-align: left; color: olive;'>{highest_reg_users}</h5>", unsafe_allow_html=True)
            st.markdown("<h3 style='text-align: left; color: teal;'>State with Highest App Opens</h3>", unsafe_allow_html=True)
            st.markdown(f"<h5 style='text-align: left; color: olive;'>{highest_app_opens}</h5>", unsafe_allow_html=True)
            
            st.subheader("Key takeaways from the data")
            st.write("- Bengaluru has topped the charts in every metric, including Insurance, Transaction Count, App opens and Registered users.")
            st.write("- Hyderabad has outpaced Bengaluru only in terms of Transaction amount.")
            st.write("- Xiaomi has consistently been the top brand used for transactions.")
            st.write("- Recharge and bill payments have consistently been the most commonly used transaction types over the years.")

        if selected=='Correlation':
            st.write('')
        if selected=='Data Visualisation':
            tab1, tab2, tab3 = st.tabs(["Aggregated", "Map","Top"])

    tab1.markdown("<h3 style='text-align: center; color: white;'>Aggregated Phonepe Data</h3>", unsafe_allow_html=True)
    tab2.markdown("<h3 style='text-align: center; color: white;'>Map Phonepe Data</h3>", unsafe_allow_html=True)
    tab3.markdown("<h3 style='text-align: center; color: white;'>Top Phonepe Data</h3>", unsafe_allow_html=True)

    with tab1:
        st.write('')
        st.markdown("<h5 style='text-align:left; color: white;'>Choose the visualisation you would like to proceed with</h5>", unsafe_allow_html=True)
        option=st.radio('',("Time Series Analysis", "State-Level Insights", "Transaction Type Analysis","Quarterly Breakdown","Brand Performance and Distribution Analysis"))

        if option =="Time Series Analysis":
            options=st.selectbox('Select the type to visualise',('Transaction Count','Transaction Amount'),
                                index=None,
                                placeholder="Select the type to visualise")

            if options=='Transaction Count':
                    st.write(' ')
                    st.markdown("<h4 style='text-align:center; color: white;'>Time Series Analysis for Transaction Count of Aggregated Insurance Data</h4>", unsafe_allow_html=True)
                    st.plotly_chart(agg_ins_tcount())
                
                    st.write(' ')
                    st.markdown("<h4 style='text-align:center; color: white;'>Time Series Analysis for Transaction Count of Aggregated Transaction Data</h4>", unsafe_allow_html=True)
                    st.plotly_chart(agg_trans_tcount())

            if options=='Transaction Amount':
                    st.markdown("<h4 style='text-align:center; color: white;'>Time Series Analysis for Transaction Amount of Aggregated Insurance Data</h4>", unsafe_allow_html=True)
                    st.plotly_chart(agg_ins_tamount())
                
                    st.markdown("<h4 style='text-align:center; color: white;'>Time Series Analysis for Transaction Amount of Aggregated Transaction Data</h4>", unsafe_allow_html=True)
                    st.plotly_chart(agg_trans_tamount())

        if option=="State-Level Insights":
            options=st.selectbox('Select the type to visualise',('Transaction Count','Transaction Amount','Transaction Type','Brand'),
                                index=None,
                                placeholder="Select the type to visualise")
            if options=='Transaction Count':
                st.markdown("<h4 style='text-align:center; color: white;'>State wise Analysis for Transaction Count Over Years</h4>", unsafe_allow_html=True)
                st.plotly_chart(agg_state_tcount())
            if options=='Transaction Type':
                st.markdown("<h4 style='text-align:center; color: white;'>State wise Analysis for Transaction Type ForEach State</h4>", unsafe_allow_html=True)
                ttype_tcount_tamount()
            if options=='Transaction Amount':
                st.markdown("<h4 style='text-align:center; color: white;'>State wise Analysis for Transaction Amount Over Years</h4>", unsafe_allow_html=True)
                st.plotly_chart(agg_state_tamount())
            if options=='Brand':
                st.markdown("<h4 style='text-align:center; color: white;'>State wise Analysis for Each Brand</h4>", unsafe_allow_html=True)
                st.plotly_chart(agg_state_brand())

        if option =="Transaction Type Analysis":
            options=st.selectbox('Select the type to visualise',('Transaction Count','Transaction Amount'),
                                index=None,
                                placeholder="Select the type to visualize")
            if options=='Transaction Count':
                st.write("Transaction Type Analysis with Transaction Count of Aggregated Transaction Data")
                st.plotly_chart(agg_ttype_tcount())
            if options=='Transaction Amount':
                st.write("Transaction Type Analysis with Transaction Amount of Aggregated Transaction Data")
                st.plotly_chart(agg_ttype_tamount())

        if option =="Quarterly Breakdown":
            options=st.selectbox('Select the type to visualise',('Transaction Count','Transaction Amount'),
                                index=None,
                                placeholder="Select the type to visualize")
            if options=='Transaction Count':
                st.write("Quarter wise Analysis for Transaction Count of Aggregated Insurance Data")
                st.plotly_chart(agg_ins_qwise_tcount())
            
                st.write("Quarter wise Analysis for Transaction Count of Aggregated Transaction Data")
                st.plotly_chart(agg_trans_qwise_tcount())
            if options=='Transaction Amount':
                    st.write("Quarter wise Analysis for Transaction Amount of Aggregated Insurance Data")
                    st.plotly_chart(agg_ins_qwise_tamount())
                
                    st.write("Quarter wise Analysis for Transaction Amount of Aggregated Transaction Data")
                    st.plotly_chart(agg_trans_qwise_tamount())

        if option =="Brand Performance and Distribution Analysis":
            options=st.selectbox('Select the type to visualise',('Leveraging Transaction Data','Incorporating Percentage Data','Over Years'),
                                index=None,
                                placeholder="Select the type to visualize")
            if options=='Leveraging Transaction Data':
                st.markdown("<h4 style='text-align: center; color: white;'>Total Transaction Count Per Year for Each Brand</h4>", unsafe_allow_html=True)
                st.plotly_chart(agg_brand_tcount())
            if options=='Incorporating Percentage Data':
                st.markdown("<h4 style='text-align: center; color: white;'>Brand Performance with Percentage of each Brand</h4>", unsafe_allow_html=True)
                st.plotly_chart(agg_bpercent_over_yrs())
            if options=='Over Years':
                st.markdown("<h4 style='text-align: center; color: white;'>Brand Performance Over Years</h4>", unsafe_allow_html=True)
                st.plotly_chart(agg_bpercent_each_yr())

    with tab2:
        options=st.radio('Select the type to visualize',('Time Series Analysis','District Level Insights','Quarterly Breakdown'),
                        index=None)
        
        if options=='Time Series Analysis':
            st.write('')
            st.markdown("<h5 style='text-align:left; color: white;'>Pick a data to proceed with for the visualization</h5>", unsafe_allow_html=True)
            option=st.selectbox('Select a type to visualise',('Registered Users','App Opens'),
                            index=None,
                            placeholder="Select a type to visualise")
            if option=='Registered Users':
                st.markdown("<h5 style='text-align:left; color: white;'>Pick a data to proceed with for the visualization</h5>", unsafe_allow_html=True)
                st.plotly_chart(map_user_yreg_user())
            if option=='App Opens':
                st.plotly_chart(map_user_yapp_opens())

        if options=='District Level Insights': 
            st.write('')
            st.markdown("<h5 style='text-align:left; color: white;'>Pick a data to proceed with for the visualization</h5>", unsafe_allow_html=True)   
            option=st.selectbox('Select a type to visualize',('Insurance Count','Transaction Count','Transaction Amount','Highest Registration Count','Highest App Opens'),
                        index=None,placeholder='Select the type to visualize')
            if option =='Insurance Count':
                fig_count, fig_amount = map_ins_state_wise()     
                st.plotly_chart(fig_count)
                st.plotly_chart(fig_amount)
            if option=='Transaction Count':
                fig_count=map_trans_stwise_tcount()
                st.plotly_chart(fig_count)
            if option=='Transaction Amount':
                fig_amount=map_trans_stwise_tamount()
                st.plotly_chart(fig_amount)
            if option=='Highest Registration Count':
                st.plotly_chart(map_high_reg_count())
            if option=='Highest App Opens':
                st.plotly_chart(map_high_app_opens())
            

        if options=='Quarterly Breakdown':
            option=st.selectbox('Select a type to visualise',('Registered Users','App Opens'),
                            index=None,
                            placeholder="Select a type to visualise")
            if option=='Registered Users':
                st.plotly_chart(map_user_qreg_user())
            if option=='App Opens':
                st.plotly_chart(map_user_qapp_opens())

    with tab3:
        st.markdown("<h5 style='text-align:left; color: white;'>Pick a data to proceed with for the visualization</h5>", unsafe_allow_html=True)
        options=st.radio('Select the type to visualize',('Top Insurance Analysis','Top Transaction Analysis','Top User Analysis'),
                            index=None)
        if options=='Top Insurance Analysis':
            option=st.selectbox('Select a type to visualize',('Insurance Count by Pincode','Insurance Transaction Amount by Pincode','Top 10 Insurance Transactions by Pincode','Top 10 Insurance Transaction Amount by Pincode'),
                                index=None,placeholder='Select the type to visualize')
            if option=='Insurance Count by Pincode':
                st.plotly_chart(top_ins_tcount())
            if option=="Insurance Transaction Amount by Pincode":
                st.plotly_chart(top_ins_tamount())
            if option=='Top 10 Insurance Transactions by Pincode':
                st.plotly_chart(top_ins_tc10())
            if option=='Top 10 Insurance Transaction Amount by Pincode':
                st.plotly_chart(top_ins_ta10())
        if options=='Top Transaction Analysis':
            option=st.selectbox('Select the type to visualize',('Transaction Count','Transaction Amount','Top Pincodes in Transaction Count','Top Pincodes in Transaction Amount'),
                                placeholder="Select the type to visualise")
            if option=='Transaction Count':
                st.plotly_chart(top_trans_tcount())
            if option=='Transaction Amount':
                st.plotly_chart(top_trans_tamount())
            if option=='Top Pincodes in Transaction Count':
                st.plotly_chart(top_trans_tc10())
            if option=='Top Pincodes in Transaction Amount':
                st.plotly_chart(top_trans_ta10())
        if options=='Top User Analysis':
            option=st.selectbox('Select the type to visualize',('Transaction Count','Transaction Amount','Top Pincodes in Transaction Count','Top Pincodes in Transaction Amount'),
                                placeholder="Select the type to visualise")