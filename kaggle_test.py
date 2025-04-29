from kaggle.api.kaggle_api_extended import KaggleApi

def main():
    print("🚀 Testing Kaggle API authentication...")

    api = KaggleApi()
    api.authenticate()

    print("✅ Authenticated successfully!")

    print("📦 Fetching a few public datasets:")

    datasets = api.dataset_list(sort_by="hottest", page=1, max_results=5)
    for dataset in datasets:
        print(f"📁 {dataset.title} by {dataset.ref}")

if __name__ == "__main__":
    main()
