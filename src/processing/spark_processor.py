from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf, lower, regexp_replace, explode, to_date, year, month
from pyspark.sql.types import StringType, FloatType, ArrayType, StructType, StructField, TimestampType, IntegerType
from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer, IDF
from pyspark.ml.clustering import LDA
import re

# Simple TextBlob sentiment implementation to be wrapped in UDF
def get_sentiment(text):
    try:
        from textblob import TextBlob
        return TextBlob(text).sentiment.polarity
    except ImportError:
        return 0.0

class SparkProcessor:
    def __init__(self, app_name="YouTubeAnalysis"):
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.warehouse.dir", "data/spark-warehouse") \
            .getOrCreate()
            
    def load_data(self, video_path, comments_path):
        """Load video and comment data from JSON."""
        # Define schemas manually or infer
        # For simplicity, we'll infer schema from JSON or use standard columns if known
        
        videos_df = self.spark.read.option("multiline", "true").json(video_path)
        comments_df = self.spark.read.option("multiline", "true").json(comments_path)
        
        return videos_df, comments_df

    def preprocess_text(self, df, text_col):
        """Tokenization and Stopword removal."""
        # Clean text: remove special chars, lower case
        df_clean = df.withColumn("clean_text", lower(col(text_col)))
        df_clean = df_clean.withColumn("clean_text", regexp_replace("clean_text", "[^a-zA-Z\\s]", ""))
        
        # Tokenize
        tokenizer = Tokenizer(inputCol="clean_text", outputCol="words")
        df_words = tokenizer.transform(df_clean)
        
        # Remove stopwords
        remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
        df_processed = remover.transform(df_words)
        
        return df_processed

    def analyze_sentiment(self, df, text_col):
        """Apply sentiment analysis UDF."""
        sentiment_udf = udf(get_sentiment, FloatType())
        
        # Stance detection UDF
        def detect_stance(text):
            if not text: return "Neutral"
            text = text.lower()
            pro_palestine = ["free palestine", "gaza", "genocide", "occupation", "apartheid", "save palestine"]
            pro_israel = ["stand with israel", "hamas is isis", "idf", "right to defend", "zionist"]
            
            p_score = sum(1 for w in pro_palestine if w in text)
            i_score = sum(1 for w in pro_israel if w in text)
            
            if p_score > i_score: return "Pro-Palestine"
            if i_score > p_score: return "Pro-Israel"
            return "Neutral"

        stance_udf = udf(detect_stance, StringType())
        
        df_res = df.withColumn("sentiment_score", sentiment_udf(col(text_col)))
        df_res = df_res.withColumn("stance", stance_udf(col(text_col)))
        return df_res

    def analyze_stance_temporal(self, comments_df):
        """Analyze stance distribution over time."""
        # Join with video published date if possible, but comments have published_at too
        # However, comment published_at is distinct from video. 
        # For this analysis, we want trends of COMMENTS over time.
        
        df = comments_df.withColumn("date", to_date(col("published_at")))
        df = df.withColumn("year", year(col("date"))).withColumn("month", month(col("date")))
        
        # Filter out Neutral for cleaner graph? Or keep. Let's keep.
        stance_stats = df.groupBy("year", "month", "stance").count().orderBy("year", "month")
        
        # Pivot to get columns: year, month, Pro-Israel, Pro-Palestine, Neutral
        stance_pivot = stance_stats.groupBy("year", "month").pivot("stance").sum("count").na.fill(0).orderBy("year", "month")
        return stance_pivot

    def train_topic_model(self, df, text_col, num_topics=5):
        """Train LDA model on text column."""
        # Ensure preprocessing is done
        if "filtered_words" not in df.columns:
            df = self.preprocess_text(df, text_col)
            
        cv = CountVectorizer(inputCol="filtered_words", outputCol="raw_features", vocabSize=5000, minDF=2.0)
        cv_model = cv.fit(df)
        df_vectorized = cv_model.transform(df)
        
        idf = IDF(inputCol="raw_features", outputCol="features")
        idf_model = idf.fit(df_vectorized)
        df_tfidf = idf_model.transform(df_vectorized)
        
        lda = LDA(k=num_topics, maxIter=10)
        model = lda.fit(df_tfidf)
        
        # Extract topics
        topics = model.describeTopics(5)
        
        # Map term indices to words
        vocab = cv_model.vocabulary
        topics_rdd = topics.rdd.map(lambda row: row['termIndices'])
        
        topic_words = []
        for term_indices in topics_rdd.collect():
            words = [vocab[i] for i in term_indices]
            topic_words.append(words)
            
        return model, topic_words
 
    def temporal_analysis(self, video_df):
        """Analyze video frequency and stats over time."""
        # Assuming published_at is string, convert to date
        df = video_df.withColumn("date", to_date(col("published_at")))
        df = df.withColumn("year", year(col("date"))).withColumn("month", month(col("date")))
        
        monthly_stats = df.groupBy("year", "month").count().orderBy("year", "month")
        return monthly_stats

    def engagement_analysis(self, video_df):
        """Analyze engagement metrics distribution."""
        # Summary stats for view_count, like_count, comment_count
        return video_df.select("view_count", "like_count", "comment_count").summary()

    def channel_influence(self, video_df):
        """Identify influential channels."""
        # Group by channel, sum views, count videos
        return video_df.groupBy("channel_title") \
            .agg({"view_count": "sum", "video_id": "count"}) \
            .withColumnRenamed("sum(view_count)", "total_views") \
            .withColumnRenamed("count(video_id)", "video_count") \
            .orderBy(col("total_views").desc())

    def stop(self):
        self.spark.stop()
