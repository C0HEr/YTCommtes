import glob
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt

st.title('YouTube Comment Sentiment Analysis')

# Path to the CSV file generated by the Spark application
csv_file_path = 'data/comments.csv'

@st.cache
def load_data():
    # Read all CSV files in the directory
    df = pd.concat([pd.read_csv(f) for f in glob.glob(csv_file_path)])
    return df

data = load_data()

# Plotting the sentiment analysis results
st.subheader('Sentiment Analysis Results')

if not data.empty:
    positive = data[data['sentiment'] > 0].shape[0]
    negative = data[data['sentiment'] < 0].shape[0]
    neutral = data[data['sentiment'] == 0].shape[0]

    labels = ['Positive', 'Negative', 'Neutral']
    sizes = [positive, negative, neutral]

    fig, ax = plt.subplots()
    ax.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=90)
    ax.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.

    st.pyplot(fig)

else:
    st.write("No data to display.")
