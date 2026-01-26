from pathlib import Path

DEFAULT_DATA_ROOT = (Path(__file__).parent.parent / "data").resolve()

if __name__ == '__main__':
    print(DEFAULT_DATA_ROOT)
