import argparse
from pathlib import Path
import yaml

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--config", default="configs/pilot.yaml")
    args = p.parse_args()

    cfg_path = Path(args.config)
    cfg = yaml.safe_load(cfg_path.read_text())

    print("Loaded config:", cfg_path)
    print("Crawls:", cfg["pilot"]["crawl_ids"])
    print("Seed:", cfg["project"]["seed"])
    print("Common Crawl module path: src/data_sources/commoncrawl/")

if __name__ == "__main__":
    main()
