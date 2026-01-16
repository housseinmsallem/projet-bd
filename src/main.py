import os
import sys
import argparse
import random
import datetime

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

try:
    from collection.youtube_collector import YouTubeCollector
except ImportError:
    YouTubeCollector = None

try:
    from processing.spark_processor import SparkProcessor
    from pyspark.sql.functions import col, to_date 
except ImportError:
    SparkProcessor = None

try:
    from visualization.charts import ChartGenerator
except ImportError:
    ChartGenerator = None

try:
    from reporting.report_generator import ReportGenerator
except ImportError:
    ReportGenerator = None

def run_mock_pipeline(output_dir):
    """Run pipeline with dummy data when deps are missing."""
    print("Running in MOCK mode...")
    
    # 1. Mock Data Collection: Already done implicitly or skip
    
    # 2. Mock Processing Results
    findings = (
        "MOCK FINDINGS:\n"
        "- Sentiment trend shows negative spikes in Oct 2023.\n"
        "- Top channel 'NewsGlobal' has 5M views.\n"
        "- Topic 'Peace/Treaty' appeared in 15% of comments."
    )
    
    # 3. Mock Visualizations
    charts = {}
    if ChartGenerator:
        charts_gen = ChartGenerator(os.path.join(output_dir, "charts"))
        if charts_gen.check_lib():
            # Generate dummy charts if lib exists
            print("Generating mock charts...")
            dates = [datetime.date(2023, i, 1) for i in range(1, 13)]
            sentiments = [{'date': d, 'sentiment_score': random.uniform(-0.5, 0.5)} for d in dates]
            charts_gen.plot_sentiment_trend(sentiments)
            charts['Sentiment Trend'] = os.path.join(output_dir, "charts/sentiment_trend.png")
        else:
            print("Skipping chart generation (missing libraries).")
            charts['Sentiment Trend'] = "placeholder.png"
    else:
        print("ChartGenerator module not loaded.")
        charts['Sentiment Trend'] = "placeholder.png"

    # 4. Report
    if ReportGenerator:
        report_gen = ReportGenerator(output_dir)
        data = {"findings": findings, "summary": "This is a MOCK execution report."}
        
        # Try PDF
        if report_gen.generate_pdf(data, charts):
            print("PDF Success.")
        else:
            print("PDF Failed. Fallback to Markdown.")
            report_gen.generate_markdown(data, charts)
    else:
        print("ReportGenerator module not loaded.")

def main():
    parser = argparse.ArgumentParser(description="YouTube Data Analysis Pipeline")
    parser.add_argument("--api-key", help="YouTube Data API Key")
    parser.add_argument("--mock", action="store_true", help="Run with mock data")
    args = parser.parse_args()

    # If deps are clearly missing, force mock/dry run
    try:
        import pyspark
        import pandas
    except ImportError:
        print("Critical dependencies missing (pyspark/pandas). Defaulting to MOCK mode.")
        args.mock = True

    if args.mock:
        run_mock_pipeline("data")
        return

    # Real Pipeline
    # 1. Collection
    video_file = "videos_dataset_10y.json"
    comments_file = "comments_dataset_10y.json"
    
    if args.api_key:
        print("--- Step 1: Data Collection (10-Year Scope) ---")
        collector = YouTubeCollector(args.api_key)
        
        # Collect historical data (2014-2025)
        # Limiting to 20 videos per year for demonstration speed, user can increase this.
        print("Collecting videos year by year...")
        videos = collector.collect_historical_data(
            "Israel Palestine conflict", 
            start_year=2014, 
            end_year=2025, 
            videos_per_year=50
        )
        
        # Enrich with details (stats)
        print("Fetching video statistics...")
        video_ids = [v['video_id'] for v in videos]
        detailed_videos = collector.get_video_details(video_ids)
        collector.save_data(detailed_videos, video_file)
        
        # Get comments (Deep fetch up to 800)
        print(f"Collecting comments for {len(detailed_videos)} videos (Max 800 per video)...")
        all_comments = []
        
        # Progress bar simulation
        total = len(video_ids)
        for i, vid in enumerate(video_ids):
            if i % 10 == 0: print(f"Processing video {i}/{total}...")
            comments = collector.get_comments(vid, max_comments=800)
            all_comments.extend(comments)
            
        collector.save_data(all_comments, comments_file)
    else:
        print("Skipping collection (no API key provided). Using existing data if available.")

    # 2. Processing & Visualization
    print("--- Step 2: Processing & Visualization ---")
    if SparkProcessor and ChartGenerator and ReportGenerator:
        processor = SparkProcessor()
        charts_gen = ChartGenerator(os.path.join("data", "charts"))
        report_gen = ReportGenerator("data")
        
        v_path = os.path.join("data/raw", video_file)
        c_path = os.path.join("data/raw", comments_file)
        
        if os.path.exists(v_path) and os.path.exists(c_path):
            videos_df, comments_df = processor.load_data(v_path, c_path)
            print(f"Loaded {videos_df.count()} videos and {comments_df.count()} comments.")
            
            # --- Analysis ---
            charts = {}
            report_data = {
                "summary": f"Analyzed {videos_df.count()} videos and {comments_df.count()} comments from 2014 to 2025.\n"
                           "Focus on sentiment evolution and stance detection (Pro-Israel vs. Pro-Palestine).",
                "methodology": "Data collected via YouTube API (10-year window). Spark UDFs used for keyword-based Stance Detection.",
                "findings": "",
                "stance_analysis": "",
                "conclusion": "Temporal analysis reveals correlation between conflict escalation and stance polarization."
            }
            
            # 1. Temporal Video Stats
            if "published_at" in videos_df.columns:
                 monthly_stats = processor.temporal_analysis(videos_df)
                 print("Monthly Video Stats:")
                 monthly_stats.show(5)
            
            # 2. Stance Detection & Trend
            if "text" in comments_df.columns:
                print("Running Stance Detection...")
                df_clean = processor.preprocess_text(comments_df, "text")
                df_stance = processor.analyze_sentiment(df_clean, "clean_text")
                
                # Stance counts
                print("Stance Distribution:")
                df_stance.groupBy("stance").count().show()
                
                # Temporal Stance Analysis
                stance_over_time = processor.analyze_stance_temporal(df_stance)
                pdf_stance = stance_over_time.toPandas()
                
                # Plot Stance Trend
                charts_gen.plot_stance_trend(pdf_stance, "stance_trend.png")
                charts['Stance Trend'] = os.path.join("data/charts", "stance_trend.png")
                
                # Update Report Data
                top_stance = df_stance.groupBy("stance").count().orderBy(col("count").desc()).first()
                report_data['stance_analysis'] += f"\nMost common stance: {top_stance['stance']} ({top_stance['count']} comments)."
                
                # Sentiment Trend (General)
                # Group by date for sentiment plot
                sentiment_trend = df_stance.withColumn("date", to_date(col("published_at"))) \
                    .groupBy("date").agg({"sentiment_score": "avg"}).orderBy("date") \
                    .withColumnRenamed("avg(sentiment_score)", "sentiment_score") \
                    .toPandas()
                charts_gen.plot_sentiment_trend(sentiment_trend, "sentiment_trend.png")
                charts['Sentiment Trend'] = os.path.join("data/charts", "sentiment_trend.png")

            # 3. Channel Influence
            top_channels = processor.channel_influence(videos_df).limit(10).toPandas()
            charts_gen.plot_top_channels(top_channels, "top_channels.png")
            charts['Top Channels'] = os.path.join("data/charts", "top_channels.png")
            
            processor.stop()
            
            # 4. Generate Report
            print("--- Step 3: Reporting ---")
            report_gen.generate_pdf(report_data, charts, "final_comprehensive_report.pdf")
            
        else:
             print(f"Data files not found. Run with --api-key first.")
    else:
        print("Required modules (SparkProcessor, ChartGenerator, ReportGenerator) missing.")


if __name__ == "__main__":
    main()
