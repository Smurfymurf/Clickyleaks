from kaggle.api.kaggle_api_extended import KaggleApi

print("🚀 Testing Kaggle API authentication...")

api = KaggleApi()
api.authenticate()

print("✅ Authenticated successfully!")

# List 5 public datasets
print("📦 Listing 5 datasets:")
datasets = api.dataset_list(search="", page=1, max_results=5)
for d in datasets:
    print(f"📁 {d.title} by {d.ref}")
