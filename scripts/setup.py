"""
Run this script once to set up the entire environment from scratch.
Works on Windows and Mac.
Usage: python scripts/setup.py
"""

import os
import subprocess
import sys
import shutil
from pathlib import Path

ROOT_DIR = Path(__file__).parent.parent
DBT_DIR = ROOT_DIR / "dbt_project" / "health_pipeline"


def get_executable(name: str) -> str:
    """Get the full path to an executable in the current Python environment."""
    executable = shutil.which(name)
    if not executable:
        raise FileNotFoundError(
            f"'{name}' not found in current environment. "
            f"Make sure you have activated the correct conda environment."
        )
    return executable


def load_env() -> dict:
    """Load environment variables from .env file."""
    env_path = ROOT_DIR / ".env"
    env_vars = os.environ.copy()

    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                key = key.strip()
                value = value.strip()
                os.environ[key] = value
                env_vars[key] = value

    print("✓ Environment variables loaded")
    return env_vars


def run_command(command, cwd=None, description="", env=None):
    """Run a shell command and log output."""
    print(f"\n>>> {description}")
    result = subprocess.run(
        command,
        cwd=cwd,
        capture_output=True,
        text=True,
        env=env or os.environ.copy()
    )
    if result.stdout:
        print(result.stdout)
    if result.returncode != 0:
        print(f"ERROR: {result.stderr}")
        sys.exit(1)
    print(f"✓ {description} completed")


def main():
    print("=" * 50)
    print("Health Data Pipeline — Setup")
    print("=" * 50)

    # Step 1 — Load env vars
    env_vars = load_env()

    # Step 2 — Install dependencies
    run_command(
        [sys.executable, "-m", "pip", "install", "-r", str(ROOT_DIR / "requirements.txt")],
        cwd=ROOT_DIR,
        description="Installing dependencies",
        env=env_vars
    )

    # Step 3 — Initialise Snowflake databases and schemas
    run_command(
        [sys.executable, str(ROOT_DIR / "scripts" / "init_db.py")],
        cwd=ROOT_DIR,
        description="Initialising Snowflake databases and schemas",
        env=env_vars
    )

    # Step 4 — Install dbt packages
    dbt_executable = get_executable('dbt')
    run_command(
        [dbt_executable, "deps"],
        cwd=DBT_DIR,
        description="Installing dbt packages",
        env=env_vars
    )

    print("\n" + "=" * 50)
    print("Setup complete! Run the pipeline with:")
    print("  python ingestion/load_patient.py dev")  
    print("  cd dbt_project/health_pipeline")
    print("  dbt run --target dev")
    print("  dbt test --target dev")
    print("=" * 50)


if __name__ == "__main__":
    main()