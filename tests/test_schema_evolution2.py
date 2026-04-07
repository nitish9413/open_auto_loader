import polars as pl
import pytest

from open_auto_loader.configs.schema import SchemaEvolutionMode
from open_auto_loader.core.engine import PolarsEngine
from open_auto_loader.exceptions import SchemaMismatchError
from open_auto_loader.main import OpenAutoLoader
from tests.test_schema_enforcement import setup_test_env

# # ── helpers ──────────────────────────────────────────────────────────────────

# def setup_test_env(tmp_path):
#     source     = tmp_path / "source";     source.mkdir()
#     target     = tmp_path / "target"
#     checkpoint = tmp_path / "checkpoint"; checkpoint.mkdir()
#     schema     = tmp_path / "schema.json"
#     return source, target, checkpoint, schema


def make_loader(source, target, checkpoint, schema, mode):
    return OpenAutoLoader(
        source=str(source),
        target=str(target),
        check_point=str(checkpoint),
        schema_path=str(schema),
        evolution_mode=mode,
    )


# ── regression guard ─────────────────────────────────────────────────────────


def test_evolution_mode_is_instance_not_class(tmp_path, monkeypatch):
    """
    Root bug: run() was passing SchemaEvolutionMode (the class) instead of
    self.evolution_mode (the instance) to process_single_file.
    Verify the correct *instance* reaches the engine.
    """
    source, target, checkpoint, schema = setup_test_env(tmp_path)
    pl.DataFrame({"id": [1], "extra": ["x"]}).write_csv(source / "f.csv")

    received = {}

    original = PolarsEngine.process_single_file

    def spy(self, *args, **kwargs):
        received["evolution_mode"] = kwargs.get(
            "evolution_mode", args[4] if len(args) > 4 else None
        )
        return original(self, *args, **kwargs)

    monkeypatch.setattr(PolarsEngine, "process_single_file", spy)

    loader = make_loader(source, target, checkpoint, schema, SchemaEvolutionMode.RESCUE)
    loader.run(batch_id="b1")

    assert isinstance(
        received["evolution_mode"], SchemaEvolutionMode
    ), "evolution_mode must be an instance, not the class itself"
    assert received["evolution_mode"] is SchemaEvolutionMode.RESCUE


# ── per-mode output correctness ───────────────────────────────────────────────


def test_rescue_mode_bundles_extra_columns(tmp_path):
    """Extra columns are bundled into _rescued_data JSON; raw col is absent."""
    source, target, checkpoint, schema = setup_test_env(tmp_path)

    pl.DataFrame({"id": [1]}).write_csv(source / "f1.csv")
    loader = make_loader(source, target, checkpoint, schema, SchemaEvolutionMode.RESCUE)
    loader.run(batch_id="b1")

    pl.DataFrame({"id": [2], "secret": ["hush"]}).write_csv(source / "f2.csv")
    loader.run(batch_id="b2")

    df = pl.read_delta(str(target))
    assert "_rescued_data" in df.columns, "rescue column must be present"
    assert "secret" not in df.columns, "raw extra column must not leak through"

    rescued_row = df.filter(pl.col("id") == 2)["_rescued_data"][0]
    assert rescued_row is not None, "rescued_data must not be null for row with extras"
    assert '"hush"' in rescued_row, "extra value must appear in the JSON blob"


def test_rescue_mode_no_extras_skips_rescue_column(tmp_path):
    """When no extra columns exist, _rescued_data should not be added."""
    source, target, checkpoint, schema = setup_test_env(tmp_path)

    pl.DataFrame({"id": [1]}).write_csv(source / "f1.csv")
    loader = make_loader(source, target, checkpoint, schema, SchemaEvolutionMode.RESCUE)
    loader.run(batch_id="b1")

    pl.DataFrame({"id": [2]}).write_csv(source / "f2.csv")
    loader.run(batch_id="b2")

    df = pl.read_delta(str(target))
    assert "_rescued_data" not in df.columns


def test_none_mode_drops_extra_columns(tmp_path):
    """NONE mode must silently ignore extra columns — they must not appear in target."""
    source, target, checkpoint, schema = setup_test_env(tmp_path)

    pl.DataFrame({"id": [1]}).write_csv(source / "f1.csv")
    loader = make_loader(source, target, checkpoint, schema, SchemaEvolutionMode.NONE)
    loader.run(batch_id="b1")

    pl.DataFrame({"id": [2], "extra": ["ignored"]}).write_csv(source / "f2.csv")
    loader.run(batch_id="b2")

    df = pl.read_delta(str(target))
    assert "extra" not in df.columns
    assert list(df.filter(pl.col("id") == 2)["id"]) == [2]


def test_add_new_columns_mode_evolves_schema(tmp_path):
    """ADD_NEW_COLUMNS must add the new column to the target and evolve the schema."""
    source, target, checkpoint, schema = setup_test_env(tmp_path)

    pl.DataFrame({"id": [1]}).write_csv(source / "f1.csv")
    loader = make_loader(
        source, target, checkpoint, schema, SchemaEvolutionMode.ADD_NEW_COLUMNS
    )
    loader.run(batch_id="b1")

    pl.DataFrame({"id": [2], "new_col": ["hello"]}).write_csv(source / "f2.csv")
    loader.run(batch_id="b2")

    df = pl.read_delta(str(target))
    assert "new_col" in df.columns
    assert df.filter(pl.col("id") == 2)["new_col"][0] == "hello"
    assert df.filter(pl.col("id") == 1)["new_col"][0] is None  # backfilled null


def test_fail_on_new_columns_mode_raises(tmp_path):
    """FAIL_ON_NEW_COLUMNS must raise SchemaMismatchError when extras are detected."""
    source, target, checkpoint, schema = setup_test_env(tmp_path)

    pl.DataFrame({"id": [1]}).write_csv(source / "f1.csv")
    loader = make_loader(
        source, target, checkpoint, schema, SchemaEvolutionMode.FAIL_ON_NEW_COLUMNS
    )
    loader.run(batch_id="b1")

    pl.DataFrame({"id": [2], "intruder": [99]}).write_csv(source / "f2.csv")
    with pytest.raises(SchemaMismatchError) as exc_info:
        loader.run(batch_id="b2")

    assert "intruder" in str(exc_info.value)


# ── type-safety sweep ─────────────────────────────────────────────────────────


@pytest.mark.parametrize("mode", list(SchemaEvolutionMode))
def test_all_modes_receive_enum_instance(tmp_path, monkeypatch, mode):
    """
    Parameterized sweep: regardless of which mode is configured,
    process_single_file must receive a SchemaEvolutionMode instance,
    never the bare class.
    """
    source, target, checkpoint, schema = setup_test_env(tmp_path)
    pl.DataFrame({"id": [1]}).write_csv(source / "f.csv")

    received = {}
    original = PolarsEngine.process_single_file

    def spy(self, *args, **kwargs):
        received["mode"] = kwargs.get(
            "evolution_mode", args[4] if len(args) > 4 else None
        )
        # Only let non-failing modes actually write
        if mode != SchemaEvolutionMode.FAIL_ON_NEW_COLUMNS:
            return original(self, *args, **kwargs)

    monkeypatch.setattr(PolarsEngine, "process_single_file", spy)

    loader = make_loader(source, target, checkpoint, schema, mode)
    try:  # noqa: SIM105
        loader.run(batch_id="b1")
    except Exception:
        pass  # FAIL mode may raise — we only care about what was passed

    assert isinstance(received.get("mode"), SchemaEvolutionMode), (
        f"mode={mode.name}: expected SchemaEvolutionMode instance, "
        f"got {type(received.get('mode'))}"
    )
