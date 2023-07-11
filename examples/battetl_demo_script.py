import os
import re
import json
import dotenv
from pathlib import Path
from battetl import BattETL


# Load environment variables
print(f'Load environment variables from "{Path.cwd() / ".env"}"')
dotenv.load_dotenv()
print(f'Check target database "{os.getenv("DB_TARGET")}"')

# Load config from file
cell = BattETL(
    config_path='/app/examples/battetl_in_container_config_example.json'
)
cell.extract().transform().load()
