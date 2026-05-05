import streamlit as st
from streamlit_option_menu import option_menu
import pandas as pd
import numpy as np
import plotly.express as px
import seaborn as sns
import matplotlib.pyplot as plt
import duckdb
import plotly.graph_objects as go

st.set_page_config(
    page_icon='🏛️',
    page_title='US Accident Dashboard',
    layout='wide'
)


@st.cache_resource
def get_conn():
    return duckdb.connect()

conn = get_conn()


with st.sidebar:
    page = option_menu(
        menu_title="US Accidents",
        options=["Home","Geographic","Time","Weather","Road Features","Risk Report"],
        icons=["house","map","clock","cloud","sign-stop-fill","exclamation-triangle"],
        menu_icon="car-front",
        default_index=0,
    )


if page == 'Home':
    st.title("🏠 Overview")
    st.markdown('---')

    col1 , col2 , col3 , col4 , col5 , col6 = st.columns(6)

    with col1:
        #Total Accidents
        toatl_accidents = conn.execute("""
        SELECT COUNT(*) FROM 'accidents.parquet'
            """).fetchone()[0]
        st.metric("Total Accidents" , toatl_accidents)

    with col2:
        #Avg Severity Score
        avg_severity = conn.execute("""
        SELECT ROUND(AVG(Severity)) FROM 'accidents.parquet'
            """).fetchone()[0]
        st.metric("Avg Severity" , avg_severity)

    with col3:
        #Most Dangerous State
        most_dangerous_state = conn.execute("""
        SELECT State , COUNT(*) as 'Count' FROM 'accidents.parquet' 
        GROUP BY State
        ORDER BY Count DESC 
        LIMIT 1
            """).df()
        most_dangerous_state = most_dangerous_state['State'].iloc[0]
        st.metric("Most Dangerous State" , most_dangerous_state)

    with col4:
        #Most Dangerous City
        most_dangerous_city = conn.execute("""
        SELECT City , COUNT(*) as 'Count' FROM 'accidents.parquet'
        GROUP BY City
        ORDER BY Count DESC
            """).df()
        most_dangerous_city = most_dangerous_city['City'].iloc[0]
        st.metric("Most Dangerous State" , most_dangerous_city)

    with col5:
        #% Severe Accidents
        severity = conn.execute("""
        SELECT COUNT(*) FROM 'accidents.parquet' 
        WHERE Severity = 3 OR Severity = 3
            """).fetchone()[0]
        
        Sever_accidents =  round(( severity / toatl_accidents) * 100 , 2)
        st.metric("Sever %" , Sever_accidents)

    with col6:
        #Peak Accident Hour
        peak_hour = conn.execute("""
        SELECT HOUR(CAST (Start_Time as TIMESTAMP)) as 'Hour' ,COUNT(*) as 'Count' FROM 'accidents.parquet'
        GROUP BY Hour
        ORDER BY Count DESC
            """).df()
        
        peak_hour = peak_hour['Hour'].iloc[0]
        st.metric("Peak Accident Hour" , peak_hour)

    st.markdown('---')


    # Accidents Year over Year

    temp_df = conn.execute("""
    SELECT YEAR(CASt(Start_Time as TIMESTAMP)) as 'Year' , COUNT(*) AS 'Count' FROM 'accidents.parquet'
    GROUP BY YEAR(CASt(Start_Time as TIMESTAMP))
    ORDER BY Year
    """).df()
    fig = px.line(data_frame=temp_df , x='Year' , y='Count',title='Accidents Year over Year')
    st.plotly_chart(fig)

    st.markdown('---')

    # Severity Distribution
    temp_df = conn.execute("""
    SELECT Severity , COUNT(*) as 'Count' FROM 'accidents.parquet'
    GROUP BY Severity
    ORDER BY Severity
    """).df()

    temp_df['Severity_Label'] = temp_df['Severity'].replace({
        1 : 'Low',
        2: 'Mild',
        3 : 'High',
        4 : 'Very High'
    })

    fig = px.pie(data_frame=temp_df,values='Count',names='Severity_Label',labels={'Severity_Label':'Severity_Level','Count' : 'Total Accidents'},title='Severity Distribution',hole=0.5) ## for lables use names not labels
    st.plotly_chart(fig)

    st.markdown('---')

    # Top 10 States

    temp_df = conn.execute("""
    SELECT State , COUNT(*) as 'Count' FROM 'accidents.parquet'
    GROUP BY State
    ORDER BY COUNT(*) 
    """).df()

    temp_df= temp_df.sort_values('Count',ascending=False).head(10)
    fig = px.bar(data_frame=temp_df , x='Count' , y='State' , title='Top 10 States')
    st.plotly_chart(fig)
    st.markdown('---')

    # Day vs Night Split

    temp_df = conn.execute("""
    SELECT Sunrise_Sunset as 'Timming' , COUNT(*) as 'Count' FROM 'accidents.parquet'
    GROUP BY Sunrise_Sunset
    ORDER BY COUNT(*) 
    """).df()

    fig = px.pie(data_frame=temp_df , values='Count' , names='Timming' , title='Day vs Night Split')
    st.plotly_chart(fig)
    st.markdown('---')

    # Accidents by Season

    temp_df = conn.execute("""
    SELECT MONTH(CAST(Start_Time AS TIMESTAMP)) as 'Month' , COUNT(*) as 'Count' FROM 'accidents.parquet'
    GROUP BY MONTH(CAST(Start_Time AS TIMESTAMP))
    ORDER BY COUNT(*) 
    """).df()

    temp_df['Season'] = temp_df['Month'].replace({
        1 : 'Winter' , 2 : 'Winter' , 12 : 'Winter',
        3 : 'Spring', 4 : 'Spring' , 5 : 'Spring',
        6 : 'Summer' , 7 : 'Summer' , 8 : 'Summer',
        9 : 'Fall' , 10 : 'Fall' , 11 : 'Fall'
    })

    fig = px.bar(data_frame=temp_df , x='Season' , y='Count' , title='Accidents by Season')
    st.plotly_chart(fig)
    st.markdown('---')


    # Month Wise Trend

    temp_df = conn.execute("""
    SELECT 
    MONTH(CAST(Start_Time AS TIMESTAMP)) AS Month,
    MONTHNAME(CAST(Start_Time AS TIMESTAMP)) AS Month_Name,
    COUNT(*) AS Count
    FROM 'accidents.parquet'
    GROUP BY Month, Month_Name
    ORDER BY Month;
    """).df()
    fig = px.line(data_frame=temp_df , x='Month_Name' , y='Count',title='Month Wise Trend') # If you select boht then group by both also
                                                                                            # All non aggregated columns must in group by
    st.plotly_chart(fig)
    st.markdown('---')











elif page == 'Geographic':
    st.title("🗺️ Geographic")

    # Step 3: State name → code mapping
    us_state_abbrev = {
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


    # States according to Accident Count
    # Step 1: Get data
    temp_df = conn.execute("""
    SELECT State, COUNT(*) AS Count
    FROM 'accidents.parquet'
    GROUP BY State
    ORDER BY Count DESC
    """).df()

    # Step 2: Clean state names
    temp_df['State'] = temp_df['State'].str.strip().str.title()

    # Step 3 is commong so at top

    # Step 4: Convert to state codes
    temp_df['State_Code'] = temp_df['State'].map(us_state_abbrev)

    # Step 5: Plot choropleth
    fig = px.choropleth(
        temp_df,
        locations='State_Code',
        locationmode='USA-states',
        color='Count',
        hover_name='State',
        scope='usa',
        color_continuous_scale='Reds',
        title='USA Choropleth Map — Accident Count'
    )
    fig.update_layout(
    height = 700
    )

    # Step 6: Show in Streamlit
    st.plotly_chart(fig, use_container_width=True)

    temp_df = temp_df.drop(columns='State_Code')
    temp_df = temp_df.rename(columns={'Count' : 'Accident_Count'})
    with st.expander("📊 View Data"):
        st.dataframe(temp_df)
    st.markdown('---')



    # States according to Accident Severity

    # Step 1: Get data
    temp_df = conn.execute("""
    SELECT State, ROUND(AVG(Severity) , 2) AS Avg_Severity
    FROM 'accidents.parquet'
    GROUP BY State
    ORDER BY Avg_Severity DESC
    """).df()

    # Step 2: Clean state names
    temp_df['State'] = temp_df['State'].str.strip().str.title()

    # Step 4: Convert to state codes
    temp_df['State_Code'] = temp_df['State'].map(us_state_abbrev)

    # Step 5: Plot choropleth
    fig = px.choropleth(
        temp_df,
        locations='State_Code',
        locationmode='USA-states',
        color='Avg_Severity',
        hover_name='State',
        scope='usa',
        color_continuous_scale='Reds',
        title='USA Choropleth Map — Avg Severity'
    )
    fig.update_layout(
    height = 700
    )

    # Step 6: Show in Streamlit
    st.plotly_chart(fig, use_container_width=True)

    temp_df = temp_df.drop(columns='State_Code')
    temp_df = temp_df.rename(columns={'Count' : 'Accident_Count'})
    with st.expander("📊 View Data"):
        st.dataframe(temp_df)
    st.markdown('---')



    # Top 20 citites by accident count

    temp_df = conn.execute("""
    SELECT City , COUNT(*) AS 'Count' FROM accidents.parquet 
    GROUP BY City
    ORDER BY COUNT(*) DESC
    """).df()

    temp_df = temp_df.head(20)

    fig = px.bar(temp_df , x= 'Count' , y = 'City' , title= 'Top 20 citites by accident count',orientation='h')
    fig.update_layout(height=800)
    st.plotly_chart(fig)
    st.markdown('---')


    # Top 20 citites by accident Severity

    temp_df = conn.execute("""
    SELECT City , ROUND(AVG(Severity) , 2) AS 'Avg_Severity' , COUNT(*) FROM accidents.parquet
    GROUP BY City
    HAVING COUNT(*) > 35000
    ORDER BY AVG(Severity) DESC
    """).df()

    temp_df = temp_df.head(20)
    fig = px.bar(temp_df , x= 'City' , y = 'Avg_Severity' , title= 'Top 20 citites by accident Severity')
    fig.update_layout(height=800)
    st.plotly_chart(fig)
    st.markdown('---')


    # State vs Severity Heatmap

    temp_df = conn.execute("""
    SELECT State , Severity , ROUND(COUNT(*),2) AS 'Count' FROM accidents.parquet
    GROUP BY State , severity
    ORDER BY COUNT(*) DESC
    """).df()

    temp_df = temp_df.head(10)
    fig = px.density_heatmap(
        temp_df,
        x='Severity',
        y='State',
        z='Count',
        color_continuous_scale='YlOrRd',
        title='State vs Severity Heatmap'
    )

    fig.update_traces(
        xgap = 3,
        ygap = 3
    )
    st.plotly_chart(fig)

    st.markdown('---')


    # Day vs Night ratio per State
    col1 , col2 = st.columns(2)

    # For Day 

    # Step 1: Get data
    with col1 : 
        temp_df = conn.execute("""
        SELECT State  , ROUND(COUNT(*) , 2) as 'Count' FROM accidents.parquet
        WHERE Sunrise_Sunset = 'Day'
        GROUP BY State 
        ORDER BY COUNT(*)
        """).df()

        # Step 2: Clean state names
        temp_df['State'] = temp_df['State'].str.strip().str.title()

        # Step 4: Convert to state codes
        temp_df['State_Code'] = temp_df['State'].map(us_state_abbrev)

        # Step 5: Plot choropleth
        fig = px.choropleth(
            temp_df,
            locations='State_Code',
            locationmode='USA-states',
            color='Count',
            hover_name='State',
            scope='usa',
            color_continuous_scale='Reds',
            title='USA Choropleth Map — Day Accidents'
        )


        # Step 6: Show in Streamlit
        st.plotly_chart(fig, use_container_width=True)


        temp_df = temp_df.drop(columns='State_Code')
        temp_df = temp_df.rename(columns={'Count' : 'Accident_Count'})
        temp_df = temp_df.sort_values('Accident_Count' , ascending=False)
        with st.expander("📊 View Day Data"):
            st.dataframe(temp_df)

        st.markdown('---')


    # For Night

    with col2 : 

        # Step 1: Get data
        temp_df = conn.execute("""
        SELECT State  , ROUND(COUNT(*) , 2) as 'Count' FROM accidents.parquet
        WHERE Sunrise_Sunset = 'Night'
        GROUP BY State 
        ORDER BY COUNT(*)
        """).df()

        # Step 2: Clean state names
        temp_df['State'] = temp_df['State'].str.strip().str.title()

        # Step 4: Convert to state codes
        temp_df['State_Code'] = temp_df['State'].map(us_state_abbrev)

        # Step 5: Plot choropleth
        fig = px.choropleth(
            temp_df,
            locations='State_Code',
            locationmode='USA-states',
            color='Count',
            hover_name='State',
            scope='usa',
            color_continuous_scale='Reds',
            title='USA Choropleth Map — Night Accidents'
        )

        # Step 6: Show in Streamlit
        st.plotly_chart(fig, use_container_width=True)
        temp_df = temp_df.drop(columns='State_Code')
        temp_df = temp_df.rename(columns={'Count' : 'Accident_Count'})
        temp_df = temp_df.sort_values('Accident_Count' , ascending=False)
        with st.expander("📊 View Night Data"):
            st.dataframe(temp_df)

        st.markdown('---')




elif page == 'Time':
    st.title("⏰ Time")


    # Accidents by Hour of Day
    temp_df = conn.execute("""
    SELECT HOUR(CAST(Start_Time as TIMESTAMP)) as 'Hour',COUNT(*) as 'Count' FROM accidents.parquet
    GROUP BY HOUR(CAST(Start_Time as TIMESTAMP))
    ORDER BY HOUR(CAST(Start_Time as TIMESTAMP))
    """).df()
    
    fig = px.bar(temp_df , x = 'Hour' , y = 'Count' , title='Accidents by Hour of Day')
    st.plotly_chart(fig)
    with st.expander("📊 View Data"):
        st.dataframe(temp_df)
    st.markdown('---')

    # Accudents by Day of week
    temp_df = conn.execute("""
    SELECT DAYNAME(CAST(Start_Time as TIMESTAMP)) as 'Day',ROUND(COUNT(*) , 2) as 'Count' FROM accidents.parquet
    GROUP BY DAYNAME(CAST(Start_Time as TIMESTAMP))
    ORDER BY COUNT(*) DESC
    """).df()
    
    fig = px.bar(temp_df , x = 'Count' , y = 'Day' , title='Accidents by Day of week' )
    st.plotly_chart(fig)
    with st.expander("📊 View Data"):
        st.dataframe(temp_df)
    st.markdown('---')

    # Accidents by Month
    temp_df = conn.execute("""
    SELECT MONTHNAME(CAST(Start_Time as TIMESTAMP)) as 'Month',COUNT(*) as 'Count' FROM accidents.parquet
    GROUP BY MONTHNAME(CAST(Start_Time as TIMESTAMP))
    ORDER BY COUNT(*) DESC
    """).df()
    
    fig = px.bar(temp_df , x = 'Month' , y = 'Count' , title='Accidents by Month' )
    st.plotly_chart(fig)
    with st.expander("📊 View Data"):
        st.dataframe(temp_df)
    st.markdown('---')

    # Accidents Per year
    temp_df = conn.execute("""
    SELECT YEAR(CASt(Start_Time as TIMESTAMP)) as 'Year' , COUNT(*) AS 'Count' FROM 'accidents.parquet'
    GROUP BY YEAR(CASt(Start_Time as TIMESTAMP))
    ORDER BY Year
    """).df()

    fig = px.line(data_frame=temp_df , x='Year' , y='Count',title='Accidents Year over Year')
    
    st.plotly_chart(fig)
    with st.expander("📊 View Data"):
        st.dataframe(temp_df)

    st.markdown('---')

    # Hour Day Heatmap

    temp_df = conn.execute("""
    SELECT DAYNAME(CAST(Start_Time as TIMESTAMP)) AS 'Dayname' , Hour(CAST(Start_Time as TIMESTAMP)) AS 'Hour' , COUNT(*) AS 'Count' FROM accidents.parquet
    GROUP BY DAYNAME(CAST(Start_Time as TIMESTAMP)) , Hour(CAST(Start_Time as TIMESTAMP))
    ORDER BY Count DESC
    """).df()

    fig = px.density_heatmap(
        temp_df,
        x = 'Dayname',
        y = 'Hour',
        z = 'Count',
        title='Hour Vs Day Heatmap'
    )

    fig.update_traces(
        xgap = 3,
        ygap = 3
    )

    fig.update_layout(
        height = 700
    )

    st.plotly_chart(fig)
    with st.expander("📊 View Data"):
        st.dataframe(temp_df)

    st.markdown('---')


    # Accidents By Season Per Year

    temp_df = conn.execute("""
    SELECT YEAR(CAST(Start_Time AS TIMESTAMP)) as 'Year' , MONTH(CAST(Start_Time AS TIMESTAMP)) as 'Month' , COUNT(*) as 'Count' FROM 'accidents.parquet'
    GROUP BY YEAR(CAST(Start_Time AS TIMESTAMP))  , MONTH(CAST(Start_Time AS TIMESTAMP))
    ORDER BY COUNT(*) 
    """).df()

    temp_df['Season'] = temp_df['Month'].replace({
        1 : 'Winter' , 2 : 'Winter' , 12 : 'Winter',
        3 : 'Spring', 4 : 'Spring' , 5 : 'Spring',
        6 : 'Summer' , 7 : 'Summer' , 8 : 'Summer',
        9 : 'Fall' , 10 : 'Fall' , 11 : 'Fall'
    })

    fig = px.bar(data_frame=temp_df , x='Year' , y='Count' , color='Season', title='Accidents by Season' , barmode='group')
    st.plotly_chart(fig)
    with st.expander("📊 View Data"):
        st.dataframe(temp_df)

    st.markdown('---')

    # Rush Hour Vs Not rush Hour

    temp_df = conn.execute("""
    SELECT HOUR(CAST(Start_Time AS TIMESTAMP)) as 'Hour' , COUNT(*) AS 'Count' FROM accidents.parquet
    GROUP BY HOUR(CAST(Start_Time AS TIMESTAMP))
    ORDER BY Count DESC
    """).df()

    temp_df['Rush_Hour'] = temp_df['Count'] > 400000
    temp_df  = temp_df.groupby('Rush_Hour')['Count'].sum().reset_index()

    fig = px.pie(temp_df , values='Count' , names='Rush_Hour' ,title='Rush Hour Vs Non Rush Hour')
    st.plotly_chart(fig)
    with st.expander("📊 View Data"):
        st.dataframe(temp_df)

    st.markdown('---')

    # Weekend Vs Not weekend

    temp_df = conn.execute("""
    SELECT DAYNAME(CAST(Start_Time as TIMESTAMP)) AS 'Day_Name' , COUNT(*) AS 'Count' FROM accidents.parquet
    GROUP BY DAYNAME(CAST(Start_Time as TIMESTAMP))
    ORDER BY Count DESC
    """).df()

    temp_df['Weekend'] = (temp_df['Day_Name'] == 'Saturday') | (temp_df['Day_Name'] == 'Sunday')
    temp_df  = temp_df.groupby('Weekend')['Count'].sum().reset_index()

    fig = px.pie(temp_df , values='Count' , names='Weekend' ,title='Weekend Vs Not weekend')
    st.plotly_chart(fig)
    with st.expander("📊 View Data"):
        st.dataframe(temp_df)

    st.markdown('---')


elif page == 'Weather':
    st.title("☁️ Weather")

    # Top 15 Weather Conditions

    temp_df = conn.execute("""
    SELECT Weather_Condition , COUNT(*) AS 'Count' FROM accidents.parquet
    GROUP BY Weather_Condition
    ORDER BY Count  DESC 
    """).df()
    temp_df = temp_df.head(10)

    fig = px.bar(temp_df , x = 'Count' , y = 'Weather_Condition' , title='Top 15 Weather Conditions')
    st.plotly_chart(fig)
    with st.expander("📊 View Data"):
        st.dataframe(temp_df)

    st.markdown('---')


    # Weather Condition By Severity

    temp_df = conn.execute("""
    SELECT Weather_Condition  , Severity, COUNT(*) AS 'Count' FROM accidents.parquet
    GROUP BY Weather_Condition  , Severity
    HAVING Count > 100000
    ORDER BY Severity DESC
    """).df()

    temp_df = temp_df.head(10)

    fig = px.density_heatmap(
        temp_df,
        x = 'Severity',
        y = 'Weather_Condition',
        z = 'Count',
        title='Weather Condition By Severity'
    )

    fig.update_traces(
        xgap = 3,
        ygap = 3
    )

    fig.update_layout(
        height = 700
    )

    st.plotly_chart(fig)
    with st.expander("📊 View Data"):
        st.dataframe(temp_df)

    st.markdown('---')

    # Visibility Distribution

    temp_df = conn.execute("""
    SELECT "Visibility(mi)"  FROM accidents.parquet             
    """).df()

    temp_df = temp_df[temp_df['Visibility(mi)'] <= 20]

    fig = px.histogram(temp_df,x='Visibility(mi)' , nbins=30 , title="Visibility Distribution")

    fig.update_yaxes(range=[0, 1000000])

    fig.update_layout(bargap=0.1)
    st.plotly_chart(fig)

    with st.expander("📊 View Data"):
        st.dataframe(temp_df)

    st.markdown('---')

    # ## Low vsisbility accident Percentage

    # temp_df = conn.execute("""
    # SELECT "Visibility(mi)"  FROM accidents.parquet             
    # """).df()

    # total_df = temp_df

    # temp_df = temp_df[temp_df['Visibility(mi)'] <= 1]

    # low_visibility_percentage = (temp_df.shape[0] / total_df.shape[0]) * 100

    # st.title(f'Accidents happens due to Low Visisbility : {low_visibility_percentage : .2f}%')
    # st.markdown('----')

    # Avg severiy by Visisbility

    temp_df = conn.execute(""" SELECT Severity , AVG( "Visibility(mi)" ) as 'Avg_Visibility'  FROM accidents.parquet 
                                GROUP BY Severity
                                ORDER BY Avg_Visibility DESC
                                """).df()
    
    fig  = px.bar(temp_df , x = 'Severity' , y = 'Avg_Visibility' , title='Avg severiy by Visisbility')
    st.plotly_chart(fig)
    with st.expander("📊 View Data"):
        st.dataframe(temp_df)
    st.markdown('---')
    

    # Temprature Category donut chart

    temp_df = conn.execute(""" SELECT "Temperature(F)"  FROM accidents.parquet""").df()
    bins = [-89 , 0 , 50 ,100, 207]
    labels = ['Very_Low' , 'Low' , 'Medium' , 'High']
    temp_df['Temp_Category'] = pd.cut(temp_df['Temperature(F)'] , bins=bins , labels=labels)

    temp_df = temp_df.groupby('Temp_Category')['Temperature(F)'].count().reset_index(name='Accident_Count')


    fig = px.pie(temp_df , values='Accident_Count', names='Temp_Category' , title='Temprature Category By Percentage')
    st.plotly_chart(fig)
    with st.expander("📊 View Data"):
        st.dataframe(temp_df)
    st.markdown('---')


    # Severity Vs Wind_speed
    temp_df = conn.execute(""" SELECT Severity , AVG("Wind_Speed(mph)") AS 'Avg_Wind_Speed'  FROM accidents.parquet 
                                GROUP BY Severity """).df()
    

    fig = px.bar(temp_df,x='Severity' , y = 'Avg_Wind_Speed' , title='Severity Vs Wind_speed')
    st.plotly_chart(fig)
    with st.expander("📊 View Data"):
        st.dataframe(temp_df)
    st.markdown('---')


    # Speed wind Distrobution

    temp_df = conn.execute(""" SELECT "Wind_Speed(mph)" FROM accidents.parquet """).df()
    

    fig = px.histogram(temp_df,x='Wind_Speed(mph)' , title='Severity Vs Wind_speed')
    st.plotly_chart(fig)
    with st.expander("📊 View Data"):
        st.dataframe(temp_df)
    st.markdown('---')




 
elif page == 'Road Features':
    st.title("🚦Road Features")

    # Road Feature Frequency

    temp_df = conn.execute("""
    SELECT 
        Amenity, Bump, Crossing, Give_Way, Junction,
        No_Exit, Railway, Roundabout, Station, Stop,
        Traffic_Calming, Traffic_Signal
    FROM accidents.parquet
    """).df()

    features = [
    'Amenity','Bump','Crossing','Give_Way','Junction',
    'No_Exit','Railway','Roundabout','Station','Stop',
    'Traffic_Calming','Traffic_Signal'
    ]

    feature_counts = []

    for col in features:
        count = temp_df[col].sum()   # True = 1, False = 0
        feature_counts.append((col, count))

    feature_df = pd.DataFrame(feature_counts, columns=['Feature', 'Count'])
    feature_df = feature_df.sort_values('Count',ascending=False)

    fig = px.bar(
    feature_df,
    x='Count',
    y='Feature',
    orientation='h',   # 👈 horizontal
    title='Road Feature Frequency'
    )

    fig.update_layout(
        # title_x=0.5,
        bargap=0.3
    )

    st.plotly_chart(fig)
    with st.expander("📊 View Data"):
        st.dataframe(temp_df)
    st.markdown('---')


    # Road Feature vs Avg Severity

    temp_df = conn.execute("""
    SELECT 
        Severity,
        Amenity, Bump, Crossing, Give_Way, Junction,
        No_Exit, Railway, Roundabout, Station, Stop,
        Traffic_Calming, Traffic_Signal
    FROM accidents.parquet
    """).df()

    feature_counts = []

    for col in temp_df.columns:
        if col == 'Severity':
            continue

        avg_severity = temp_df[temp_df[col] == True]['Severity'].mean()
        feature_counts.append((col,avg_severity))

    feature_df = pd.DataFrame(feature_counts , columns=['Column','Avg_Severity'])

    fig = px.bar(feature_df , x = 'Column' , y = 'Avg_Severity' , title='Road Feature vs Avg Severity')
    st.plotly_chart(fig)
    with st.expander("📊 View Data"):
        st.dataframe(temp_df)
    st.markdown('---')




    # Junction Vs Non Junction Severity

    temp_df = conn.execute(""" SELECT  Junction , COUNT(*) AS 'Count' FROM accidents.parquet 
                                GROUP BY Junction 
                                ORDER BY Count DESC """).df()
    
    fig = px.bar(temp_df, x = 'Junction' , y = 'Count' , title='Junction Vs Non-Junction severity')

    fig.update_layout(
    bargap=0.5,        # space between bars (0 → no gap, 1 → max gap)
    bargroupgap=0.2    # gap between grouped bars
    )

    st.plotly_chart(fig)
    with st.expander("📊 View Data"):
        st.dataframe(temp_df)
    st.markdown('---')






    # Crossing Vs Severity

    temp_df = conn.execute("""  SELECT Crossing , AVG(Severity) AS 'Avg_Severity' FROM accidents.parquet 
                                GROUP BY Crossing """).df()
    

    fig = px.bar(temp_df , x = 'Crossing' , y = 'Avg_Severity' , title='Crossing Vs Severity')

    fig.update_layout(
    bargap=0.5,        # space between bars (0 → no gap, 1 → max gap)
    bargroupgap=0.2    # gap between grouped bars
    )

    st.plotly_chart(fig)
    with st.expander("📊 View Data"):
        st.dataframe(temp_df)
    st.markdown('---')




    # Traffic Signal Vs Severity



    temp_df = conn.execute("""  SELECT Traffic_Signal , AVG(Severity) AS 'Avg_Severity' FROM accidents.parquet 
                                GROUP BY Traffic_Signal """).df()
    

    fig = px.bar(temp_df , x = 'Traffic_Signal' , y = 'Avg_Severity' , title='Traffic Signal Vs Severity')

    fig.update_layout(
    bargap=0.5,        # space between bars (0 → no gap, 1 → max gap)
    bargroupgap=0.2    # gap between grouped bars
    )

    st.plotly_chart(fig)
    with st.expander("📊 View Data"):
        st.dataframe(temp_df)
    st.markdown('---')

    # Top Streets By accidents

    temp_df = conn.execute("""    SELECT Street , COUNT(*) AS 'Count' FROM accidents.parquet
                                    GROUP BY Street
                                    ORDER BY Count DESC """).df()
    
    temp_df = temp_df.head(10)

    fig = px.bar(temp_df , x = 'Count' , y = 'Street' , title='Top Streets By accidents')
    st.plotly_chart(fig)
    with st.expander("📊 View Data"):
        st.dataframe(temp_df)
    st.markdown('---')



    # Interstate vs Regular Road

    temp_df = conn.execute(""" SELECT Railway , COUNT(*) AS 'Count' FROM accidents.parquet
                                GROUP BY Railway
                                ORDER BY Count DESC """).df()

    fig = px.pie(temp_df , values= 'Count' , names='Railway' , title='Railway vs Non Railway Road')
    st.plotly_chart(fig)
    with st.expander("📊 View Data"):
        st.dataframe(temp_df)
    st.markdown('---')


elif page == 'Risk Report':
    st.title("⚠️ Risk Report")

    # State Risk Score Ranking

    temp_df = conn.execute(""" SELECT State , COUNT(*) AS 'Count' FROM accidents.parquet 
                                GROUP BY State
                                ORDER BY Count DESC""").df()
    
    temp_df = temp_df.head(10)
    fig = px.bar(temp_df , x = 'State' , y = 'Count' , title='State Risk Score Ranking')
    st.plotly_chart(fig)
    with st.expander("📊 View Data"):
        st.dataframe(temp_df)
    st.markdown('---')

    # Risk Score Components Table

    temp_df = conn.execute(""" SELECT State , COUNT(*) AS 'Count' , AVG(Severity) AS 'Avg_Severity' FROM accidents.parquet
                                GROUP BY State
                                ORDER BY Count DESC""").df()
    
    fig = go.Figure(data=[go.Table(
    header=dict(
        values=['State', 'Count', 'Avg Severity'],
        fill_color='darkblue',
        font=dict(color='white', size=14),
        align='center'
    ),
    cells=dict(
        values=[
            temp_df['State'],
            temp_df['Count'],
            temp_df['Avg_Severity']
        ],
        fill_color='black',
        align='center'
        )
    )])

    fig.update_layout(
    title="State-wise Accident Summary"
        )
    st.plotly_chart(fig)
    with st.expander("📊 View Data"):
        st.dataframe(temp_df)
    st.markdown('---')





    # YoY Accident Change per State

    # -------------------------------
    # STEP 1: Fetch data
    # -------------------------------
    temp_df = conn.execute("""
    SELECT 
        State,
        YEAR(CAST(Start_Time AS TIMESTAMP)) AS Year,
        COUNT(*) AS Count
    FROM accidents.parquet
    GROUP BY State, Year
    """).df()

    # -------------------------------
    # STEP 2: Sort data (IMPORTANT)
    # -------------------------------
    temp_df = temp_df.sort_values(['State', 'Year'])

    # -------------------------------
    # STEP 3: Calculate YoY %
    # -------------------------------
    temp_df['YoY_Percent'] = temp_df.groupby('State')['Count'].pct_change() * 100

    # -------------------------------
    # STEP 4: Remove NaN (first year)
    # -------------------------------
    temp_df = temp_df.dropna()

    # -------------------------------
    # STEP 5: Take latest YoY per state
    # -------------------------------
    latest_df = temp_df.sort_values('Year').groupby('State').tail(1)

    # -------------------------------
    # STEP 6: Select Top & Bottom states
    # -------------------------------
    top10 = latest_df.sort_values('YoY_Percent', ascending=False).head(10)
    bottom10 = latest_df.sort_values('YoY_Percent').head(10)

    plot_df = pd.concat([top10, bottom10])

    # -------------------------------
    # STEP 7: Optional cap (avoid extreme distortion)
    # -------------------------------
    plot_df['YoY_Percent'] = plot_df['YoY_Percent'].clip(-100, 100)

    # -------------------------------
    # STEP 8: Plot horizontal bar chart
    # -------------------------------
    fig = px.bar(
        plot_df.sort_values('YoY_Percent'),
        x='YoY_Percent',
        y='State',
        orientation='h',
        title='YoY Accident Change per State (Top ↑ & Bottom ↓)',
        color='YoY_Percent',
        color_continuous_scale='RdYlGn_r'   # red = worse, green = better
    )

    # -------------------------------
    # STEP 9: Improve layout
    # -------------------------------
    fig.update_layout(
        # title_x=0.5,
        height=600,
        xaxis_title='YoY Change (%)',
        yaxis_title='State'
    )

    # Add labels
    fig.update_traces(
        text=plot_df['YoY_Percent'].round(1),
        textposition='outside'
    )

    # -------------------------------
    # STEP 10: Show in Streamlit
    # -------------------------------
    st.plotly_chart(fig, use_container_width=True)









    # States Excedding National severity average

    national_average = conn.execute("""SELECT AVG(Severity) FROM accidents.parquet """).fetchone()[0]

    temp_df = conn.execute(""" SELECT State , COUNT(*) AS 'Count' , AVG(Severity) AS 'Avg_Severity' FROM accidents.parquet
                                GROUP BY State
                                ORDER BY Count DESC""").df()

    temp_df = temp_df[temp_df['Avg_Severity'] > national_average]

    fig = go.Figure(data=[go.Table(
    header=dict(
        values=['State', 'Count', 'Avg Severity'],
        fill_color='orange',
        font=dict(color='white', size=14),
        align='center'
    ),
    cells=dict(
        values=[
            temp_df['State'],
            temp_df['Count'],
            temp_df['Avg_Severity']
        ],
        fill_color='black',
        align='center'
        )
    )])

    fig.update_layout(
    title="States Excedding National severity average"
        )
    st.plotly_chart(fig)
    with st.expander("📊 View Data"):
        st.dataframe(temp_df)
    st.markdown('---')

    # Severity 4 % per State

    temp_df = conn.execute("""SELECT State , COUNT(*) AS 'Count' FROM accidents.parquet 
                            WHERE Severity = 4
                           GROUP BY State
                           ORDER BY Count DESC """).df()
    
    fig = px.bar(temp_df , x = 'Count',y = 'State' , title= 'Severity 4 % per State')

    fig.update_layout(
        height = 900
    )

    st.plotly_chart(fig)
    with st.expander("📊 View Data"):
        st.dataframe(temp_df)
    st.markdown('---')


    # Month Peak per State

    temp_df = conn.execute("""  SELECT State , MONTHNAME(CAST(Start_Time AS TIMESTAMP)) as 'Month_Name' , COUNT(*) as 'Count' FROM accidents.parquet
                                GROUP BY State , MONTHNAME(CAST(Start_Time AS TIMESTAMP))
                                ORDER BY  Count DESC """).df()

    peak_df = temp_df.loc[
    temp_df.groupby('State')['Count'].idxmax()
        ]
    

    fig = go.Figure(data=[go.Table(
    header=dict(
        values=['State', 'Month_Name', 'Count'],
        fill_color='lime',
        font=dict(color='black', size=14),
        align='center'
    ),
    cells=dict(
        values=[
            peak_df['State'],
            peak_df['Month_Name'],
            peak_df['Count']
        ],
        fill_color='black',
        align='center'
        )
    )])

    fig.update_layout(
    title="Month Peak per State"
        )
    st.plotly_chart(fig)
    with st.expander("📊 View Data"):
        st.dataframe(peak_df)
    st.markdown('---')


    # Peak Hour per State

    temp_df = conn.execute("""  SELECT State , HOUR(CAST(Start_Time AS TIMESTAMP)) as 'Hour' , COUNT(*) as 'Count' FROM accidents.parquet
                                GROUP BY State , HOUR(CAST(Start_Time AS TIMESTAMP))
                                ORDER BY  Count DESC """).df()

    peak_df = temp_df.loc[
    temp_df.groupby('State')['Count'].idxmax()
        ]
    

    fig = go.Figure(data=[go.Table(
    header=dict(
        values=['State', 'Hour', 'Count'],
        fill_color='teal',
        font=dict(color='white', size=14),
        align='center'
    ),
    cells=dict(
        values=[
            peak_df['State'],
            peak_df['Hour'],
            peak_df['Count']
        ],
        fill_color='black',
        align='center'
        )
    )])

    fig.update_layout(
    title="Hour Peak per State"
        )
    st.plotly_chart(fig)
    with st.expander("📊 View Data"):
        st.dataframe(peak_df)
    st.markdown('---')
    






