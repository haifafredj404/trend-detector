"""
Client MongoDB pour le projet de détection de tendances
"""
from pymongo import MongoClient, ASCENDING, DESCENDING
from datetime import datetime
import yaml


class MongoDBClient:
    """Classe pour gérer la connexion et les opérations MongoDB"""
    
    def __init__(self, config_path='config.yaml'):
        """Initialise la connexion MongoDB"""
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        mongo_config = config['mongodb']
        self.client = MongoClient(
            f"mongodb://{mongo_config['host']}:{mongo_config['port']}/"
        )
        self.db = self.client[mongo_config['database']]
        
        # Collections
        self.raw_data = self.db['raw_data']
        self.trending_topics = self.db['trending_topics']
        self.keywords = self.db['keywords']
        self.alerts = self.db['alerts']
        
        # Créer les index
        self._create_indexes()
    
    def _create_indexes(self):
        """Crée les index pour optimiser les performances"""
        self.raw_data.create_index([('timestamp', DESCENDING)])
        self.raw_data.create_index([('source', ASCENDING)])
        self.raw_data.create_index([('title', ASCENDING)])
        
        self.trending_topics.create_index([('trend_score', DESCENDING)])
        self.trending_topics.create_index([('detected_at', DESCENDING)])
        
        self.keywords.create_index([('keyword', ASCENDING)])
        self.keywords.create_index([('timestamp', DESCENDING)])
        
        print("✅ Index MongoDB créés")
    
    def insert_raw_data(self, data):
        """Insère des données brutes"""
        if isinstance(data, list):
            result = self.raw_data.insert_many(data)
            return len(result.inserted_ids)
        else:
            result = self.raw_data.insert_one(data)
            return 1
    
    def get_recent_data(self, hours=1):
        """Récupère les données récentes"""
        from datetime import timedelta
        cutoff = datetime.now() - timedelta(hours=hours)
        return list(self.raw_data.find({'timestamp': {'$gte': cutoff}}))
    
    def save_trending_topic(self, topic_data):
        """Sauvegarde un sujet tendance"""
        topic_data['updated_at'] = datetime.now()
        self.trending_topics.update_one(
            {'topic': topic_data.get('topic', topic_data.get('title'))},
            {'$set': topic_data},
            upsert=True
        )
    
    def get_trending_topics(self, limit=20):
        """Récupère les sujets tendance"""
        return list(
            self.trending_topics.find()
            .sort('trend_score', DESCENDING)
            .limit(limit)
        )
    
    def get_stats(self):
        """Récupère les statistiques globales"""
        return {
            'total_raw_data': self.raw_data.count_documents({}),
            'total_trends': self.trending_topics.count_documents({}),
            'total_keywords': self.keywords.count_documents({}),
            'active_alerts': self.alerts.count_documents({'status': 'active'})
        }
    
    def clear_all(self):
        """Supprime toutes les données (pour tests)"""
        self.raw_data.delete_many({})
        self.trending_topics.delete_many({})
        self.keywords.delete_many({})
        self.alerts.delete_many({})
        print("🗑️ Toutes les données ont été supprimées")


if __name__ == "__main__":
    # Test de connexion
    try:
        client = MongoDBClient()
        print("✅ Connexion MongoDB réussie !")
        print(f"📊 Stats: {client.get_stats()}")
    except Exception as e:
        print(f"❌ Erreur: {e}")