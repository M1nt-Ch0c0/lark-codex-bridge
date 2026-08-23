import importlib.util
import os
import sys
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / "codex_schema.py"
SPEC = importlib.util.spec_from_file_location("codex_schema", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
codex_schema = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = codex_schema
SPEC.loader.exec_module(codex_schema)


class CodexSchemaTests(unittest.TestCase):
    def test_normalization_and_rendering_are_deterministic(self):
        first = {"required": ["z", "a"], "properties": {"z": {"type": "string"}, "a": {"type": "integer"}}}
        second = {"properties": {"a": {"type": "integer"}, "z": {"type": "string"}}, "required": ["a", "z"]}
        self.assertEqual(
            codex_schema.canonical_bytes(codex_schema.normalize_schema(first)),
            codex_schema.canonical_bytes(codex_schema.normalize_schema(second)),
        )

    def test_diff_separates_optional_additions_from_breaking_changes(self):
        before = {
            "type": "object",
            "required": ["mode"],
            "properties": {
                "mode": {"type": ["string", "null"], "enum": ["known"]},
                "removed": {"type": "string"},
            },
        }
        after = {
            "type": "object",
            "required": ["mode", "requiredNew"],
            "properties": {
                "mode": {"type": "string", "enum": ["known", "future"]},
                "optionalNew": {"type": "boolean"},
                "requiredNew": {"type": "integer"},
            },
        }
        changes = []
        codex_schema.compare_named_schemas(before, after, "root", changes)
        kinds = {(item["classification"], item["kind"]) for item in changes}
        self.assertIn(("additive", "optional_property_added"), kinds)
        self.assertIn(("additive", "enum_values_added"), kinds)
        self.assertIn(("breaking", "required_property_added"), kinds)
        self.assertIn(("breaking", "property_removed"), kinds)
        self.assertIn(("breaking", "type_narrowed"), kinds)

    @unittest.skipIf(os.name == "nt", "fixture executable uses a POSIX shell")
    def test_version_probe_accepts_only_the_exact_shape(self):
        with tempfile.TemporaryDirectory() as directory:
            binary = Path(directory) / "codex"
            binary.write_text("#!/bin/sh\nprintf 'codex-cli 0.146.0\\n'\n", encoding="utf-8")
            binary.chmod(0o755)
            self.assertEqual(codex_schema.probe_version(binary), "0.146.0")
            binary.write_text("#!/bin/sh\nprintf 'codex 0.146.0\\n'\n", encoding="utf-8")
            with self.assertRaises(codex_schema.SchemaToolError):
                codex_schema.probe_version(binary)


if __name__ == "__main__":
    unittest.main()
