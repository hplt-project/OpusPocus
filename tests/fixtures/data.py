from pathlib import Path

import pytest

from opuspocus.utils import decompress_file, open_file


@pytest.fixture(scope="session")
def train_data_parallel_tiny(tmp_path_factory, languages):
    """Creates a tiny mock parallel corpora (compressed)."""
    src_file = Path(
        tmp_path_factory.mktemp("train_data_parallel_tiny"),
        f"train.{languages[0]}.gz",
    )
    with open_file(src_file, "w") as fh:
        print(
            "\n".join(
                [
                    "the colorless ideas slept furiously",
                    "pooh slept all night",
                    "working class hero is something to be",
                    "I am the working class walrus",
                    "walrus for president",
                ]
            ),
            file=fh,
        )

    tgt_file = Path(src_file.parent, f"train.{languages[1]}.gz")
    with open_file(tgt_file, "w") as fh:
        print(
            "\n".join(
                [
                    "les idées incolores dormaient furieusement",
                    "le caniche dormait toute la nuit",
                    "le héros de la classe ouvrière est quelque chose à être",
                    "Je suis le morse de la classe ouvrière",
                    "morse pour président",
                ]
            ),
            file=fh,
        )
    return (src_file, tgt_file)


@pytest.fixture(scope="session")
def train_data_parallel_tiny_decompressed(train_data_parallel_tiny, tmp_path_factory, languages):
    """Creates a decompressed version of the tiny mock corpora."""
    src_file = Path(
        tmp_path_factory.mktemp("train_data_parallel_tiny_decompressed"),
        train_data_parallel_tiny[0].stem,
    )
    decompress_file(train_data_parallel_tiny[0], src_file)

    tgt_file = Path(src_file.parent, train_data_parallel_tiny[1].stem)
    decompress_file(train_data_parallel_tiny[1], tgt_file)

    return (src_file, tgt_file)
