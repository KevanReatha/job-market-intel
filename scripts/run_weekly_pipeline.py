import os
import sys
import subprocess
import datetime


def run_step(step_name: str, command: list[str], env: dict) -> None:
    print(f"\n{'=' * 60}")
    print(f"RUNNING STEP: {step_name}")
    print(f"COMMAND: {' '.join(command)}")
    print(f"{'=' * 60}")

    result = subprocess.run(command, env=env)

    if result.returncode != 0:
        print(f"\nSTEP FAILED: {step_name}")
        sys.exit(result.returncode)

    print(f"\nSTEP SUCCEEDED: {step_name}")


def main():
    dt = os.getenv("DT", datetime.date.today().isoformat())
    python_executable = sys.executable

    env = os.environ.copy()
    env["DT"] = dt

    print(f"Starting weekly pipeline for dt={dt}")
    print(f"Using Python executable: {python_executable}")

    steps = [
        (
            "ingest_ft_offres_full_jsonl",
            [python_executable, "scripts/ingest_ft_offres_full_jsonl.py"],
        ),
        (
            "stage_offres_to_parquet",
            [python_executable, "transformations/stage_offres_to_parquet.py"],
        ),
        (
            "build_skill_demand",
            [python_executable, "transformations/build_skill_demand.py"],
        ),
        (
            "build_role_demand",
            [python_executable, "transformations/build_role_demand.py"],
        ),
                (
            "build_role_demand_by_seniority",
            [python_executable, "transformations/build_role_demand_by_seniority.py"],
        ),
        (
            "build_role_demand_by_domain_focus",
            [python_executable, "transformations/build_role_demand_by_domain_focus.py"],
        ),
        (
            "build_role_skill_demand",
            [python_executable, "transformations/build_role_skill_demand.py"],
        ),
    ]

    for step_name, command in steps:
        run_step(step_name, command, env)

    print(f"\nWeekly pipeline completed successfully for dt={dt}")


if __name__ == "__main__":
    main()