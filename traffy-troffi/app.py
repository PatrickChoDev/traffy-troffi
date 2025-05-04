import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# Set page title
st.title("Streamlit Demo Dashboard")

# Add a sidebar
st.sidebar.header("Dashboard Settings")

# Add sidebar options
option = st.sidebar.selectbox(
    "Choose a demo",
    ["Data Explorer", "Chart Visualization", "Interactive Map"]
)

# Based on selection, show different demos
if option == "Data Explorer":
    st.header("Data Explorer Demo")

    # Generate sample data
    data = pd.DataFrame({
        'Name': ['Alice', 'Bob', 'Charlie', 'David', 'Eva'],
        'Age': [24, 32, 18, 47, 29],
        'City': ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix'],
        'Salary': [60000, 75000, 48000, 92000, 65000]
    })

    # Display data
    st.subheader("Sample Data")
    st.dataframe(data)

    # Add some filters
    name_filter = st.multiselect("Filter by name", data['Name'].tolist())
    if name_filter:
        data = data[data['Name'].isin(name_filter)]
        st.write("Filtered data:")
        st.dataframe(data)

    # Show statistics
    st.subheader("Data Statistics")
    st.write(data.describe())

elif option == "Chart Visualization":
    st.header("Chart Visualization Demo")

    # Create sample data
    chart_data = pd.DataFrame(
        np.random.randn(20, 3),
        columns=['A', 'B', 'C']
    )

    # Chart type selector
    chart_type = st.radio(
        "Select chart type",
        ["Line Chart", "Bar Chart", "Area Chart"]
    )

    # Display different chart types
    if chart_type == "Line Chart":
        st.line_chart(chart_data)
    elif chart_type == "Bar Chart":
        st.bar_chart(chart_data)
    else:
        st.area_chart(chart_data)

    # Add a slider to show more data manipulation
    num_values = st.slider("Number of values", 5, 100, 20)
    new_data = pd.DataFrame(
        np.random.randn(num_values, 3),
        columns=['X', 'Y', 'Z']
    )
    st.line_chart(new_data)

else:
    st.header("Interactive Map Demo")

    # Generate some random map data
    map_data = pd.DataFrame(
        np.random.randn(1000, 2) / [50, 50] + [37.76, -122.4],
        columns=['lat', 'lon']
    )

    # Display a map
    st.map(map_data)

    # Add a checkbox to show/hide density map
    if st.checkbox("Show as density heatmap"):
        fig, ax = plt.subplots()
        ax.hist2d(map_data['lon'], map_data['lat'], bins=20, cmap='viridis')
        st.pyplot(fig)

# Add some general widgets to demonstrate
st.header("Interactive Widgets")
name = st.text_input("Enter your name", "Guest")
st.write(f"Hello, {name}!")

age = st.number_input("Enter your age", min_value=0, max_value=120, value=30)
st.write(f"You are {age} years old.")

happy = st.checkbox("Are you happy?")
if happy:
    st.write("Great! 😊")
else:
    st.write("I hope your day gets better! 🌈")

# Progress bar demo
st.header("Progress Bar Demo")
import time

progress_bar = st.progress(0)
status_text = st.empty()

for i in range(100):
    # Update progress bar
    progress_bar.progress(i + 1)
    status_text.text(f"Progress: {i+1}%")
    # Simulate some computation
    time.sleep(0.05)

st.success("Done!")
