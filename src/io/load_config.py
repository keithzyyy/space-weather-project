from dotenv import load_dotenv
import yaml
import os

def load_config(path="config/local.yaml"):

    """
    Loads the YAML config file "~/config/local.yaml" for local development,
    and the "env/.env" file to retrieve actual API keys to be attached to the 
    config
    """
    
    # 1. load .env file
    load_dotenv(dotenv_path="env/.env")

    # 2. load YAML config
    # very similar to JSON (key value pairs)
    with open(path, "r") as f:
        config = yaml.safe_load(f)

    # 3. attach the real API key to config["space_weather"]

    # 3.1 retrieve the environment variable name
    api_env_var = config["space_weather"]["api_key_env"]

    # 3.2 retrieve API key from the env variable name (create another key)
    config["space_weather"]["api_key"] = os.getenv(api_env_var)

    return config