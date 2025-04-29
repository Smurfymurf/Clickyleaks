from kaggle.api.kaggle_api_extended import KaggleApi

def main():
    print("🚀 Testing Kaggle API authentication...")

    api = KaggleApi()
    api.authenticate()

    print("✅ Authenticated successfully!")

    print("📦 Fetching a few public datasets:")

    datasets = api.dataset_list(sort_by="hottest", page=1)
    for i, dataset in enumerate(datasets[:5]):  # Manually limit results
        print(f"📁 {i+1}. {dataset.title} by {dataset.ref}")

if __name__ == "__main__":
    main()
