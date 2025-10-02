import logging
import os

os.makedirs('artifacts/logs', exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join('artifacts', 'logs', 'logfile.txt')),
        logging.StreamHandler()
    ]
)