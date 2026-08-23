import importlib.util
import os
import sys
import tempfile
import time
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
        self.assertIn(("additive", "finite_values_added"), kinds)
        self.assertIn(("breaking", "required_property_added"), kinds)
        self.assertIn(("breaking", "property_removed"), kinds)
        self.assertIn(("breaking", "type_narrowed_or_changed"), kinds)

    def classified(self, before, after, *, incoming=False):
        changes = []
        codex_schema.compare_named_schemas(before, after, "root", changes, incoming=incoming)
        return {(item["classification"], item["kind"]) for item in changes}, changes

    def test_type_lattice_and_finite_constraints_are_directional(self):
        kinds, _ = self.classified({"type": "integer"}, {"type": "number"})
        self.assertEqual(kinds, {("additive", "type_widened")})
        kinds, _ = self.classified({"type": "number"}, {"type": "integer"})
        self.assertIn(("breaking", "type_narrowed_or_changed"), kinds)
        kinds, _ = self.classified({}, {"enum": ["a"]})
        self.assertIn(("breaking", "finite_constraint_added"), kinds)
        kinds, _ = self.classified({"const": "a"}, {})
        self.assertIn(("additive", "finite_constraint_removed"), kinds)
        kinds, _ = self.classified({"enum": [1, True]}, {"const": 1.0})
        self.assertIn(("breaking", "finite_values_removed"), kinds)

    def test_constraint_narrowing_and_widening_cannot_be_silent(self):
        kinds, _ = self.classified(
            {"minimum": 1, "maxLength": 9, "minItems": 1},
            {"minimum": 2, "maxLength": 10, "minItems": 0},
        )
        self.assertIn(("breaking", "minimum_narrowed"), kinds)
        self.assertIn(("additive", "max_length_widened"), kinds)
        self.assertIn(("additive", "min_items_widened"), kinds)
        kinds, _ = self.classified({}, {"items": {"type": "string"}, "uniqueItems": True})
        self.assertIn(("breaking", "items_constraint_added"), kinds)
        self.assertIn(("breaking", "unique_items_enabled"), kinds)
        kinds, _ = self.classified(
            {"additionalProperties": {"type": "number"}},
            {"additionalProperties": {"type": "integer"}},
        )
        self.assertIn(("breaking", "type_narrowed_or_changed"), kinds)
        kinds, _ = self.classified({"multipleOf": 2}, {"multipleOf": 4})
        self.assertIn(("breaking", "multiple_of_changed"), kinds)

    def test_combinators_are_conservative_without_double_counting(self):
        before = {
            "oneOf": [
                {
                    "type": "object",
                    "properties": {"kind": {"const": "a"}},
                    "required": ["kind"],
                }
            ]
        }
        after = {
            "oneOf": [
                {
                    "type": "object",
                    "properties": {
                        "kind": {"const": "a"},
                        "note": {"type": "string"},
                    },
                    "required": ["kind"],
                }
            ]
        }
        _, changes = self.classified(before, after)
        self.assertEqual([item["kind"] for item in changes], ["optional_property_added"])
        kinds, _ = self.classified(
            {"oneOf": [{"type": "number"}]},
            {"oneOf": [{"type": "number"}, {"type": "integer"}]},
        )
        self.assertIn(("breaking", "one_of_variant_added_unproven_or_closed"), kinds)
        kinds, _ = self.classified(
            {"allOf": [{"type": "object"}]},
            {"allOf": [{"type": "object"}, {"required": ["x"]}]},
        )
        self.assertIn(("breaking", "all_of_variants_added"), kinds)
        tagged_b = {
            "type": "object",
            "properties": {"kind": {"const": "b"}},
            "required": ["kind"],
        }
        kinds, _ = self.classified(before, {"oneOf": [before["oneOf"][0], tagged_b]})
        self.assertIn(("additive", "one_of_variants_added"), kinds)

    def test_unknown_validation_keyword_changes_block(self):
        kinds, _ = self.classified(
            {"dependentSchemas": {"a": {"required": ["b"]}}},
            {"dependentSchemas": {"a": {"required": ["c"]}}},
        )
        self.assertIn(("breaking", "unknown_constraint_changed"), kinds)

    def test_incoming_closed_values_and_unions_block_promotion(self):
        kinds, _ = self.classified({"enum": ["a"]}, {"enum": ["a", "b"]}, incoming=True)
        self.assertIn(("breaking", "incoming_closed_values_added"), kinds)
        kinds, _ = self.classified(
            {"anyOf": [{"type": "string"}]},
            {"anyOf": [{"type": "string"}, {"type": "number"}]},
            incoming=True,
        )
        self.assertIn(("breaking", "incoming_closed_union_variants_added"), kinds)
        kinds, _ = self.classified(
            {"type": "integer"}, {"type": "number"}, incoming=True
        )
        self.assertIn(("breaking", "type_widened"), kinds)

    def test_support_history_pins_the_established_baseline(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "history.json"
            path.write_text(
                '{"formatVersion":1,"establishedBaselineVersion":"0.149.0","releases":[]}',
                encoding="utf-8",
            )
            with self.assertRaises(codex_schema.SchemaToolError):
                codex_schema.read_history(path)

    def test_incoming_inventory_covers_open_and_closed_constructs(self):
        bundle = codex_schema.load_bundle("0.146.0")
        audit = codex_schema.incoming_audit("0.146.0", bundle)
        entries = {
            (item["schemaPath"], item["construct"]): item for item in audit["constructs"]
        }
        self.assertTrue(entries)
        self.assertTrue(all(item["handling"] for item in entries.values()))
        self.assertEqual(
            entries[("definitions/TurnStatus", "enum")]["handling"],
            "open-string-fallback",
        )
        self.assertEqual(
            entries[("definitions/ThreadItem", "oneOf")]["handling"],
            "open-tagged-fallback",
        )
        for name, construct in (
            ("definitions/SandboxPolicy", "oneOf"),
            ("definitions/NetworkAccess", "enum"),
            ("definitions/UserInput", "oneOf"),
            ("definitions/ImageDetail", "enum"),
        ):
            self.assertEqual(entries[(name, construct)]["handling"], "promotion-blocking")
        self.assertEqual(
            entries[("definitions/ThreadItem/oneOf/2/properties/phase", "anyOf")]["handling"],
            "promotion-blocking",
        )

    def test_manifest_verification_rerenders_rust_instead_of_trusting_its_hash(self):
        selection = codex_schema.read_selection()
        policy = codex_schema.read_policy()
        version = "0.146.0"
        bundle = codex_schema.load_bundle(version)
        schema_bytes = codex_schema.canonical_bytes(bundle)
        expected_wire = codex_schema.render_wire(
            version, selection.protocol_family, codex_schema.sha256_bytes(schema_bytes), bundle
        )
        audit_bytes = codex_schema.canonical_bytes(codex_schema.incoming_audit(version, bundle))
        tampered_wire = expected_wire + b"// manual edit\n"
        manifest = codex_schema.manifest_for(
            version, selection, schema_bytes, tampered_wire, audit_bytes, policy
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            schemas_root = root / "schemas"
            wire_root = root / "wire"
            version_root = schemas_root / version
            version_root.mkdir(parents=True)
            wire_root.mkdir(parents=True)
            (version_root / "selected.schema.json").write_bytes(schema_bytes)
            (version_root / "incoming-audit.json").write_bytes(audit_bytes)
            (version_root / "manifest.json").write_bytes(codex_schema.canonical_bytes(manifest))
            (wire_root / "v0_146_0.rs").write_bytes(tampered_wire)
            original_schemas = codex_schema.SCHEMAS_ROOT
            original_wire = codex_schema.WIRE_ROOT
            codex_schema.SCHEMAS_ROOT = schemas_root
            codex_schema.WIRE_ROOT = wire_root
            try:
                with self.assertRaises(codex_schema.SchemaToolError):
                    codex_schema.verify_manifest(version, selection, policy)
            finally:
                codex_schema.SCHEMAS_ROOT = original_schemas
                codex_schema.WIRE_ROOT = original_wire

    @unittest.skipIf(os.name == "nt", "process-group regression uses POSIX semantics")
    def test_bounded_process_streams_and_terminates_process_groups(self):
        exact = codex_schema.run_bounded(
            [
                sys.executable,
                "-c",
                f"import sys; sys.stdout.buffer.write(b'x'*{codex_schema.MAX_CAPTURE_BYTES})",
            ],
            timeout=3,
        )
        self.assertFalse(exact.overflowed)
        result = codex_schema.run_bounded(
            [
                sys.executable,
                "-c",
                "import sys; sys.stdout.buffer.write(b'x'*200000); sys.stdout.flush(); "
                "sys.stderr.buffer.write(b'y'*200000); sys.stderr.flush()",
            ],
            timeout=3,
        )
        self.assertTrue(result.overflowed)
        self.assertLessEqual(len(result.stdout), codex_schema.MAX_CAPTURE_BYTES)
        self.assertLessEqual(len(result.stderr), codex_schema.MAX_CAPTURE_BYTES)
        started = time.monotonic()
        result = codex_schema.run_bounded(
            ["/bin/sh", "-c", "sleep 30 & wait"], timeout=0.1
        )
        self.assertTrue(result.timed_out)
        self.assertLess(time.monotonic() - started, 2.0)
        started = time.monotonic()
        with self.assertRaises(codex_schema.SchemaToolError):
            codex_schema.run_bounded(["/bin/sh", "-c", "sleep 30 &"], timeout=3)
        self.assertLess(time.monotonic() - started, 2.0)

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
