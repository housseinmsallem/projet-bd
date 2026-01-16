import os
try:
    import matplotlib.pyplot as plt
    import seaborn as sns
    import pandas as pd
except ImportError:
    plt = None
    sns = None
    pd = None

class ChartGenerator:
    def __init__(self, output_dir="data/charts"):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
    def check_lib(self):
        if not plt or not pd:
            print("Visualization libraries (matplotlib/pandas) not found. Skipping chart generation.")
            return False
        return True

    def plot_sentiment_trend(self, sentiment_data, filename="sentiment_trend.png"):
        """
        sentiment_data: list of dicts or pandas DataFrame with 'date', 'sentiment_score'
        """
        if not self.check_lib(): return

        df = pd.DataFrame(sentiment_data)
        if 'date' not in df.columns:
            print("Date column missing for sentiment plot")
            return
            
        plt.figure(figsize=(12, 6))
        sns.lineplot(data=df, x='date', y='sentiment_score')
        plt.title('Average Sentiment Over Time')
        plt.xlabel('Date')
        plt.ylabel('Sentiment Polarity')
        plt.tight_layout()
        plt.savefig(os.path.join(self.output_dir, filename))
        plt.close()

    def plot_top_channels(self, channel_data, filename="top_channels.png"):
        """
        channel_data: list of dicts with 'channel_title', 'total_views'
        """
        if not self.check_lib(): return
        
        df = pd.DataFrame(channel_data)
        plt.figure(figsize=(10, 6))
        sns.barplot(data=df.head(10), x='total_views', y='channel_title')
        plt.title('Top 10 Influential Channels by Views')
        plt.xlabel('Total Views')
        plt.ylabel('Channel')
        plt.tight_layout()
        plt.savefig(os.path.join(self.output_dir, filename))
        plt.close()

    def plot_topic_distribution(self, topic_data, filename="topic_dist.png"):
        """
        topic_data: list of dicts with 'topic_id', 'weight'
        """
        if not self.check_lib(): return
        
        df = pd.DataFrame(topic_data)
        plt.figure(figsize=(8, 8))
        plt.pie(df['weight'], labels=df['topic_id'], autopct='%1.1f%%')
        plt.title('Topic Distribution')
        plt.savefig(os.path.join(self.output_dir, filename))
        plt.close()

    def plot_stance_trend(self, stance_data, filename="stance_trend.png"):
        """
        stance_data: pandas DataFrame with 'year', 'month', 'Pro-Israel', 'Pro-Palestine', 'Neutral'
        """
        if not self.check_lib(): return
        
        # Create a date column from year/month for plotting
        if 'year' in stance_data.columns and 'month' in stance_data.columns:
            stance_data['date'] = pd.to_datetime(stance_data[['year', 'month']].assign(DAY=1))
        
        plt.figure(figsize=(12, 6))
        
        # Plot lines for each stance
        if 'Pro-Israel' in stance_data.columns:
            sns.lineplot(data=stance_data, x='date', y='Pro-Israel', label='Pro-Israel', marker='o')
        if 'Pro-Palestine' in stance_data.columns:
            sns.lineplot(data=stance_data, x='date', y='Pro-Palestine', label='Pro-Palestine', marker='o')
            
        plt.title('Stance Trends Over Time (Comments)')
        plt.xlabel('Date')
        plt.ylabel('Comment Count')
        plt.legend()
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig(os.path.join(self.output_dir, filename))
        plt.close()
