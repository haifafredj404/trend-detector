"""
Collecteur de données avec générateur de données d'exemple
"""
import sys
import os
from datetime import datetime
import random

# Ajouter le chemin parent pour importer les modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.mongodb_client import MongoDBClient


def generate_sample_data():
    """Génère des données d'exemple pour la démonstration"""
    print("\n" + "="*60)
    print("🎲 GÉNÉRATION DE DONNÉES D'EXEMPLE")
    print("="*60 + "\n")
    
    client = MongoDBClient()
    
    # Données d'exemple réalistes
    sample_topics = [
        {
            'title': 'New AI breakthrough in machine learning',
            'category': 'technology',
            'base_score': random.randint(500, 5000)
        },
        {
            'title': 'Climate change summit announces major decisions',
            'category': 'worldnews',
            'base_score': random.randint(1000, 8000)
        },
        {
            'title': 'SpaceX launches new satellite constellation',
            'category': 'technology',
            'base_score': random.randint(800, 6000)
        },
        {
            'title': 'Quantum computing reaches new milestone',
            'category': 'science',
            'base_score': random.randint(600, 4000)
        },
        {
            'title': 'Major cybersecurity breach affects millions',
            'category': 'technology',
            'base_score': random.randint(2000, 10000)
        },
        {
            'title': 'New programming language gains popularity',
            'category': 'programming',
            'base_score': random.randint(400, 3000)
        },
        {
            'title': 'Renewable energy costs drop significantly',
            'category': 'technology',
            'base_score': random.randint(700, 5000)
        },
        {
            'title': 'Tech giant announces layoffs',
            'category': 'business',
            'base_score': random.randint(1500, 7000)
        },
        {
            'title': 'Breakthrough in cancer research announced',
            'category': 'science',
            'base_score': random.randint(3000, 12000)
        },
        {
            'title': 'Social media platform changes algorithm',
            'category': 'technology',
            'base_score': random.randint(500, 4000)
        },
        {
            'title': 'Electric vehicle sales hit record high',
            'category': 'technology',
            'base_score': random.randint(1000, 6000)
        },
        {
            'title': 'Data science job market continues to grow',
            'category': 'datascience',
            'base_score': random.randint(300, 2000)
        },
        {
            'title': 'Major tech conference announces keynote speakers',
            'category': 'technology',
            'base_score': random.randint(400, 3000)
        },
        {
            'title': 'New study reveals impact of remote work',
            'category': 'business',
            'base_score': random.randint(800, 5000)
        },
        {
            'title': 'Artificial intelligence ethics debate intensifies',
            'category': 'artificial',
            'base_score': random.randint(600, 4000)
        }
    ]
    
    posts = []
    for i, topic in enumerate(sample_topics):
        # Créer plusieurs mentions du même sujet pour simuler une tendance
        num_mentions = random.randint(3, 8)
        
        for j in range(num_mentions):
            post = {
                'source': 'reddit',
                'subreddit': topic['category'],
                'title': topic['title'],
                'content': f"Discussion about {topic['title'].lower()}. This is an important development.",
                'url': f"https://reddit.com/r/{topic['category']}/{i}_{j}",
                'author': f"user_{random.randint(1, 1000)}",
                'timestamp': datetime.now(),
                'engagement': {
                    'score': topic['base_score'] + random.randint(-200, 500),
                    'upvote_ratio': random.uniform(0.75, 0.98),
                    'num_comments': random.randint(10, 500)
                },
                'category': topic['category'],
                'collected_at': datetime.now()
            }
            posts.append(post)
    
    # Insérer dans MongoDB
    count = client.insert_raw_data(posts)
    
    print(f"✅ {count} posts d'exemple générés et insérés dans MongoDB")
    print(f"📊 Nombre de sujets uniques: {len(sample_topics)}")
    print(f"📈 Total de mentions: {len(posts)}")
    print("\n" + "="*60 + "\n")
    
    return len(posts)


if __name__ == "__main__":
    print("\n🚀 COLLECTEUR DE DONNÉES REDDIT")
    print("\nCe script génère des données d'exemple pour la démonstration.\n")
    
    try:
        generate_sample_data()
        print("✅ Génération terminée avec succès !")
        print("\n💡 Prochaine étape: Lancer l'analyse avec Spark")
        print("   Commande: python src\\processors\\trend_detector.py\n")
    except Exception as e:
        print(f"\n❌ Erreur: {e}")
        import traceback
        traceback.print_exc()