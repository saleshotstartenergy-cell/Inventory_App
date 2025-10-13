import logging
from pipeline import ETLPipeline

def main():
    logging.info("🚀 Starting ETL process")
    # just pass target, mysql config is loaded automatically from .env
    etl = ETLPipeline(target="mysql")
    etl.extract()
    etl.transform()
    etl.load()
    logging.info("✅ ETL finished")

if __name__ == "__main__":
    main()
