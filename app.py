"""
Dashboard Web pour la visualisation des tendances
"""
from flask import Flask, render_template, jsonify, request
from datetime import datetime, timedelta
import yaml
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.mongodb_client import MongoDBClient

app = Flask(__name__)

# Charger la configuration
with open('config.yaml', 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)

# Connexion MongoDB
db_client = MongoDBClient('config.yaml')


@app.route('/')
def index():
    """Page d'accueil du dashboard"""
    return render_template('dashboard.html')


@app.route('/api/stats')
def get_stats():
    """Statistiques globales"""
    try:
        # Stats de base
        stats = db_client.get_stats()
        
        # Données récentes (dernière heure)
        recent_count = db_client.raw_data.count_documents({
            'timestamp': {'$gte': datetime.now() - timedelta(hours=1)}
        })
        
        # Tendances actives
        active_trends = db_client.trending_topics.count_documents({
            'status': {'$in': ['viral', 'rising']},
            'detected_at': {'$gte': datetime.now() - timedelta(hours=24)}
        })
        
        return jsonify({
            'total_documents': stats['total_raw_data'],
            'total_trends': stats['total_trends'],
            'active_trends': active_trends,
            'recent_data': recent_count,
            'keywords_count': stats['total_keywords'],
            'last_update': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/trending')
@app.route('/api/trending')
@app.route('/api/trending')
def get_trending():
    """Liste des tendances actuelles"""
    try:
        import math
        import json
        
        limit = int(request.args.get('limit', 20))
        
        trends = list(
            db_client.trending_topics
            .find()
            .sort('trend_score', -1)
            .limit(limit)
        )
        
        # Nettoyer les données
        for trend in trends:
            trend['_id'] = str(trend['_id'])
            
            # Nettoyer TOUTES les valeurs numériques (enlever NaN, Infinity)
            for key, value in list(trend.items()):
                if isinstance(value, float):
                    if math.isnan(value) or math.isinf(value):
                        trend[key] = 0  # Remplacer NaN par 0
                elif hasattr(value, 'item'):  # numpy type
                    val = float(value.item())
                    if math.isnan(val) or math.isinf(val):
                        trend[key] = 0
                    else:
                        trend[key] = val
            
            # Convertir les dates
            for key in ['first_seen', 'last_seen', 'detected_at', 'updated_at']:
                if key in trend and trend[key]:
                    if hasattr(trend[key], 'isoformat'):
                        trend[key] = trend[key].isoformat()
                    else:
                        trend[key] = str(trend[key])
        
        return jsonify(trends)
    except Exception as e:
        print(f"Erreur dans get_trending: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500
@app.route('/api/keywords')
def get_keywords():
    """Mots-clés tendance"""
    try:
        limit = int(request.args.get('limit', 30))
        
        keywords = list(
            db_client.keywords
            .find()
            .sort('frequency', -1)
            .limit(limit)
        )
        
        for kw in keywords:
            kw['_id'] = str(kw['_id'])
            if 'timestamp' in kw and kw['timestamp']:
                kw['timestamp'] = kw['timestamp'].isoformat() if hasattr(kw['timestamp'], 'isoformat') else str(kw['timestamp'])
        
        return jsonify(keywords)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/timeline')
def get_timeline():
    """Évolution temporelle des mentions"""
    try:
        hours = int(request.args.get('hours', 24))
        cutoff = datetime.now() - timedelta(hours=hours)
        
        pipeline = [
            {
                '$match': {
                    'timestamp': {'$gte': cutoff}
                }
            },
            {
                '$group': {
                    '_id': {
                        '$dateToString': {
                            'format': '%Y-%m-%d %H:00',
                            'date': '$timestamp'
                        }
                    },
                    'count': {'$sum': 1},
                    'total_score': {'$sum': '$engagement.score'}
                }
            },
            {
                '$sort': {'_id': 1}
            }
        ]
        
        timeline = list(db_client.raw_data.aggregate(pipeline))
        
        return jsonify(timeline)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/sources')
def get_sources_distribution():
    """Distribution par source"""
    try:
        pipeline = [
            {
                '$group': {
                    '_id': '$source',
                    'count': {'$sum': 1},
                    'total_engagement': {'$sum': '$engagement.score'}
                }
            }
        ]
        
        sources = list(db_client.raw_data.aggregate(pipeline))
        
        return jsonify(sources)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/categories')
def get_categories():
    """Distribution par catégorie"""
    try:
        pipeline = [
            {
                '$group': {
                    '_id': '$category',
                    'count': {'$sum': 1}
                }
            },
            {
                '$sort': {'count': -1}
            }
        ]
        
        categories = list(db_client.raw_data.aggregate(pipeline))
        
        return jsonify(categories)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    dashboard_config = config['dashboard']
    
    print("\n" + "="*60)
    print("🚀 DÉMARRAGE DU DASHBOARD")
    print("="*60)
    print(f"📍 URL: http://localhost:{dashboard_config['port']}")
    print("="*60 + "\n")
    
    app.run(
        host='127.0.0.1',
        port=dashboard_config['port'],
        debug=dashboard_config['debug']
    )