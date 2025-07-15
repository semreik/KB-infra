"""Environment variable utilities."""

import os
from typing import Optional
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv(dotenv_path=Path(__file__).parent.parent / '.env')

def get_env_var(key: str, required: bool = True) -> str:
    """Get an environment variable value.

    Args:
        key: The environment variable name
        required: If True, raises RuntimeError if variable is not set

    Returns:
        The value of the environment variable

    Raises:
        RuntimeError: If required is True and the variable is not set
    """
    value = os.getenv(key)
    if required and not value:
        raise RuntimeError(f"Required environment variable '{key}' is not set")
    return value
