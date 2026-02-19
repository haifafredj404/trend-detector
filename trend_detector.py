"""
Détecteur de tendances avec Apache Spark (version simplifiée)
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime, timedelta
import yaml
import sys
import os
import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.mongodb_client import MongoDBClient


class TrendDetector:
    """Détecte les tendances dans les données collectées"""
    
    def __init__(self, config_path='config.yaml'):
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        
        # Créer la session Spark (sans connecteur MongoDB)
        self.spark = SparkSession.builder \
            .appName(self.config['spark']['app_name']) \
            .master(self.config['spark']['master']) \
            .config("spark.driver.memory", "1g") \
            .config("spark.sql.shuffle.partitions", "2") \
            .getOrCreate()
        
        # Réduire les logs
        self.spark.sparkContext.setLogLevel("ERROR")
        
        self.db_client = MongoDBClient(config_path)
        
        print("✅ Spark Session créée")
    
    def load_data(self):
        """Charge les données depuis MongoDB via PyMongo"""
        try:
            hours = self.config['trend_detection']['time_window_hours']
            
            # Charger avec PyMongo
            data = self.db_client.get_recent_data(hours=hours)
            
            if not data:
                return None
            
            # Convertir en Pandas puis Spark DataFrame
            df_pandas = pd.DataFrame(data)
            
            # Nettoyer les colonnes _id qui posent problème
            if '_id' in df_pandas.columns:
                df_pandas['_id'] = df_pandas['_id'].astype(str)
            
            df = self.spark.createDataFrame(df_pandas)
            
            return df
        except Exception as e:
            print(f"❌ Erreur lors du chargement: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def calculate_trend_score(self, df):
        """Calcule le score de tendance pour chaque sujet"""
        
        # Extraire l'engagement
        df = df.withColumn("score", col("engagement.score")) \
               .withColumn("comments", col("engagement.num_comments")) \
               .withColumn("upvote_ratio", col("engagement.upvote_ratio"))
        
        # Grouper par titre
        trends = df.groupBy("title") \
            .agg(
                count("*").alias("mentions"),
                sum("score").alias("total_score"),
                avg("score").alias("avg_score"),
                sum("comments").alias("total_comments"),
                avg("upvote_ratio").alias("avg_upvote_ratio"),
                collect_set("source").alias("sources"),
                collect_set("subreddit").alias("subreddits"),
                min("timestamp").alias("first_seen"),
                max("timestamp").alias("last_seen")
            )
        
        # Calculer le trend_score
        trends = trends.withColumn(
            "trend_score",
            (
                (col("total_score") * 0.4) + 
                (col("mentions") * 50 * 0.3) + 
                (col("total_comments") * 5 * 0.3)
            ) / 100
        )
        
        # Calculer la vélocité
        trends = trends.withColumn(
            "time_diff_hours",
            (unix_timestamp("last_seen") - unix_timestamp("first_seen")) / 3600
        )
        
        trends = trends.withColumn(
            "velocity",
            when(col("time_diff_hours") > 0, 
                 col("mentions") / col("time_diff_hours"))
            .otherwise(col("mentions"))
        )
        
        # Déterminer le statut
        trends = trends.withColumn(
            "status",
            when(col("trend_score") >= 10, "viral")
            .when(col("trend_score") >= 5, "rising")
            .when(col("trend_score") >= 2, "emerging")
            .otherwise("normal")
        )
        
        # Filtrer les tendances significatives
        min_mentions = self.config['trend_detection']['min_mentions']
        trends = trends.filter(col("mentions") >= min_mentions)
        
        return trends.orderBy(desc("trend_score"))
    
    def extract_keywords(self, df):
        """Extrait les mots-clés tendance"""
        words_df = df.select(
            explode(split(lower(col("title")), " ")).alias("word"),
            "timestamp",
            col("engagement.score").alias("score")
        )
        
        # Filtrer les mots courts et communs
        stopwords = ['the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 
                     'to', 'for', 'of', 'with', 'by', 'from', 'is', 'are', 
                     'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had',
                     'new', 'this', 'that']
        
        words_df = words_df.filter(
            (length(col("word")) > 3) & 
            (~col("word").isin(stopwords))
        )
        
        # Compter les occurrences
        keywords = words_df.groupBy("word") \
            .agg(
                count("*").alias("frequency"),
                sum("score").alias("total_engagement")
            ) \
            .orderBy(desc("frequency")) \
            .limit(50)
        
        return keywords
    
    def save_trends(self, trends_df):
        """Sauvegarde les tendances dans MongoDB"""
        # Convertir les timestamps en string pour éviter les problèmes de timezone
        trends_df = trends_df.withColumn("first_seen", col("first_seen").cast("string"))
        trends_df = trends_df.withColumn("last_seen", col("last_seen").cast("string"))
        
        trends_list = trends_df.toPandas().to_dict('records')
        
        for trend in trends_list:
            trend['detected_at'] = datetime.now()
            trend['topic'] = trend['title']
            
            # Convertir les timestamps string en datetime
            if 'first_seen' in trend and trend['first_seen']:
                try:
                    trend['first_seen'] = datetime.fromisoformat(trend['first_seen'].replace('Z', '+00:00'))
                except:
                    trend['first_seen'] = datetime.now()
            
            if 'last_seen' in trend and trend['last_seen']:
                try:
                    trend['last_seen'] = datetime.fromisoformat(trend['last_seen'].replace('Z', '+00:00'))
                except:
                    trend['last_seen'] = datetime.now()
            
            # Convertir numpy types en Python types
            for key, value in trend.items():
                if hasattr(value, 'item'):
                    trend[key] = value.item()
            
            self.db_client.save_trending_topic(trend)
        
        print(f"✅ {len(trends_list)} tendances sauvegardées")
    
    def save_keywords(self, keywords_df):
        """Sauvegarde les mots-clés dans MongoDB"""
        keywords_list = keywords_df.toPandas().to_dict('records')
        
        for kw in keywords_list:
            kw['timestamp'] = datetime.now()
            kw['keyword'] = kw.pop('word')
            
            for key, value in kw.items():
                if hasattr(value, 'item'):
                    kw[key] = value.item()
        
        if keywords_list:
            self.db_client.keywords.insert_many(keywords_list)
            print(f"✅ {len(keywords_list)} mots-clés sauvegardés")
    
    def analyze(self):
        """Analyse complète des tendances"""
        print("\n" + "="*60)
        print("🔍 ANALYSE DES TENDANCES AVEC SPARK")
        print("="*60 + "\n")
        
        # Charger les données
        df = self.load_data()
        
        if df is None:
            print("⚠️ Aucune donnée à analyser")
            print("💡 Génère d'abord des données avec:")
            print("   python src\\collectors\\reddit_collector.py")
            return
        
        count = df.count()
        if count == 0:
            print("⚠️ Aucune donnée à analyser")
            return
            
        print(f"📊 {count} documents analysés")
        
        # Calculer les tendances
        print("\n🔥 Calcul des tendances...")
        trends = self.calculate_trend_score(df)
        
        print("\n📈 TOP 10 TENDANCES:")
        trends.select("title", "trend_score", "mentions", "status", "velocity") \
              .show(10, truncate=False)
        
        # Extraire les mots-clés
        print("\n🔑 Extraction des mots-clés...")
        keywords = self.extract_keywords(df)
        
        print("\n🔤 TOP 20 MOTS-CLÉS:")
        keywords.show(20)
        
        # Sauvegarder
        self.save_trends(trends)
        self.save_keywords(keywords)
        
        print("\n" + "="*60)
        print("✅ ANALYSE TERMINÉE AVEC SUCCÈS !")
        print("="*60 + "\n")
        print("💡 Prochaine étape: Lancer le dashboard")
        print("   Commande: python src\\dashboard\\app.py\n")
    
    def stop(self):
        """Arrête la session Spark"""
        self.spark.stop()
        print("🛑 Spark arrêté")


if __name__ == "__main__":
    detector = TrendDetector()
    
    try:
        detector.analyze()
        detector.stop()
    except KeyboardInterrupt:
        print("\n⛔ Arrêt")
        detector.stop()
    except Exception as e:
        print(f"\n❌ Erreur: {e}")
        import traceback
        traceback.print_exc()
        detector.stop() 