import argparse
import subprocess

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--log", required=True, help="Path to trade log CSV")
    ap.add_argument("--paths", nargs="+", required=True, help="Files or dirs to commit")
    ap.add_argument("--message", default="Auto commit")
    ap.add_argument("--user-name", default="TennisBot")
    ap.add_argument("--user-email", default="bot@tennis-engine.local")
    args = ap.parse_args()

    subprocess.run(["git", "config", "--global", "user.name", args.user_name])
    subprocess.run(["git", "config", "--global", "user.email", args.user_email])
    subprocess.run(["git", "add"] + args.paths)
    subprocess.run(["git", "commit", "-m", args.message])
    subprocess.run(["git", "push"])

if __name__ == "__main__":
    main()
