import subprocess as sp
import argparse
import os
from datetime import datetime

def main(folder:str="."):
    files = ["data/raw"]
    message_commit  = f"Update_DVC {datetime.today().strftime('%Y-%m-%d %H:%M:%S')}"
    #sp.run(["git", "config", "--global", "user.name", "Application"])
    #sp.run(["git", "config", "--global", "user.email", "meteo.bmle.24@gmail.com"])
    for file in files:
        sp.run(["dvc", "add", file], check=True, cwd=folder)
        sp.run(["git", "add", file+".dvc"], check=True, cwd=folder)
    sp.run(["dvc", "push"], check=True, cwd=folder)
    try:
        sp.run(["git", "commit", "-m", message_commit], check=True, cwd=folder)
        sp.run(["git", "push"], check=True, cwd=folder)
        print("Data pushed for versioning")
    except sp.CalledProcessError:
        print("No changes to commit.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--folder', type=str, default=".")
    args = parser.parse_args()
    main(args.folder)