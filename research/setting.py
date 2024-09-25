from pathlib import Path
import sys


def setting():
    sys.path.append(str(Path(__file__).parent.parent.joinpath("src")))
