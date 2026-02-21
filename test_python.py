from pymongo import MongoClient

try:
    client = MongoClient('mongodb://localhost:27017/', serverSelectionTimeoutMS=5000)
    # Test de connexion
    client.server_info()
    print("✅ MongoDB fonctionne parfaitement !")
    print(f"📊 Bases de données : {client.list_database_names()}")
except Exception as e:
    print(f"❌ Erreur : {e}")