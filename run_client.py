"""
Run the KV Store client from the command line.
Make sure the server is running first: python server.py
"""
import sys
import os

# Allow running from project root
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from client import KVStoreClient


def main():
    import argparse
    parser = argparse.ArgumentParser(description="KV Store Client")
    parser.add_argument("--host", default="127.0.0.1", help="Server host")
    parser.add_argument("--port", type=int, default=5000, help="Server port")
    parser.add_argument("command", nargs="?", choices=["set", "get", "delete", "bulk_set", "health"],
                        help="Command: set, get, delete, bulk_set, health")
    parser.add_argument("args", nargs="*", help="Key, or key value, etc.")
    args = parser.parse_args()

    client = KVStoreClient(host=args.host, port=args.port)

    try:
        if not client.health():
            print("Error: Server is not running. Start it with: python server.py")
            sys.exit(1)

        if args.command == "health":
            print("Server is OK")
            return

        if args.command == "set":
            if len(args.args) < 2:
                print("Usage: python run_client.py set <key> <value>")
                sys.exit(1)
            key, value = args.args[0], args.args[1]
            client.set(key, value)
            print(f"OK set {key} = {value}")

        elif args.command == "get":
            if len(args.args) < 1:
                print("Usage: python run_client.py get <key>")
                sys.exit(1)
            key = args.args[0]
            value = client.get(key)
            print(value if value is not None else "(not set)")

        elif args.command == "delete":
            if len(args.args) < 1:
                print("Usage: python run_client.py delete <key>")
                sys.exit(1)
            key = args.args[0]
            client.delete(key)
            print(f"OK deleted {key}")

        elif args.command == "bulk_set":
            if len(args.args) < 2 or len(args.args) % 2 != 0:
                print("Usage: python run_client.py bulk_set <key1> <value1> <key2> <value2> ...")
                sys.exit(1)
            items = [(args.args[i], args.args[i + 1]) for i in range(0, len(args.args), 2)]
            client.bulk_set(items)
            print(f"OK bulk_set {len(items)} pairs")

        else:
            print("Usage: python run_client.py <command> [args]")
            print("  set <key> <value>")
            print("  get <key>")
            print("  delete <key>")
            print("  bulk_set <key1> <val1> <key2> <val2> ...")
            print("  health")
    finally:
        client.close()


if __name__ == "__main__":
    main()
