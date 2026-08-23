import importlib.util
import json
import os
import stat
import sys
import tempfile
import time
import unittest
from unittest import mock
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

    def test_references_and_schema_drafts_cannot_change_silently(self):
        cases = (
            ({}, {"$ref": "#/definitions/Next"}, "reference_added"),
            ({"$ref": "#/definitions/Old"}, {}, "reference_removed"),
            (
                {"$ref": "#/definitions/Old"},
                {"$ref": "#/definitions/Next"},
                "reference_changed",
            ),
            ({}, {"$schema": "https://json-schema.org/draft/2020-12/schema"}, "schema_draft_added"),
            (
                {"$schema": "http://json-schema.org/draft-07/schema#"},
                {},
                "schema_draft_removed",
            ),
            (
                {"$schema": "http://json-schema.org/draft-07/schema#"},
                {"$schema": "https://json-schema.org/draft/2020-12/schema"},
                "schema_draft_changed",
            ),
        )
        for before, after, expected in cases:
            with self.subTest(expected=expected):
                kinds, _ = self.classified(before, after)
                self.assertTrue(
                    any(kind == expected for _classification, kind in kinds),
                    kinds,
                )

    def test_boolean_schemas_are_compared_at_every_selected_position(self):
        cases = (
            (True, False, "root"),
            (
                {"properties": {"value": True}},
                {"properties": {"value": False}},
                "property",
            ),
            ({"items": True}, {"items": False}, "items"),
            ({"items": [True]}, {"items": [False]}, "tuple-items"),
            (
                {"definitions": {"Value": True}},
                {"definitions": {"Value": False}},
                "definition",
            ),
            ({"oneOf": [True]}, {"oneOf": [False]}, "combinator"),
        )
        for before, after, location in cases:
            with self.subTest(location=location):
                kinds, _ = self.classified(before, after)
                self.assertIn(("breaking", "boolean_schema_narrowed"), kinds)

        kinds, _ = self.classified(False, True)
        self.assertIn(("additive", "boolean_schema_widened"), kinds)
        incoming_kinds, _ = self.classified(False, True, incoming=True)
        self.assertIn(("breaking", "boolean_schema_widened"), incoming_kinds)

    def test_semantic_json_numbers_preserve_exact_large_integers(self):
        exact_integers = [
            -(10**10_000),
            -(10**400),
            -(2**63) - 1,
            -(2**53) - 1,
            -(2**53),
            -(2**53) + 1,
            2**53 - 1,
            2**53,
            2**53 + 1,
            2**63,
            10**400,
            10**10_000,
        ]
        keys = [codex_schema.semantic_json_key(value) for value in exact_integers]
        self.assertEqual(len(keys), len(set(keys)))
        self.assertEqual(
            codex_schema.semantic_json_key(1),
            codex_schema.semantic_json_key(1.0),
        )
        self.assertNotEqual(
            codex_schema.semantic_json_key(2**53 + 1),
            codex_schema.semantic_json_key(float(2**53 + 1)),
        )

        kinds, changes = self.classified(
            {"enum": [2**53, -(10**400)]},
            {"enum": [2**53 + 1, 10**400]},
        )
        self.assertIn(("breaking", "finite_values_removed"), kinds)
        self.assertIn(("additive", "finite_values_added"), kinds)
        encoded = json.dumps(changes, sort_keys=True)
        self.assertIn(str(2**53), encoded)
        self.assertIn(str(2**53 + 1), encoded)
        self.assertIn(str(10**400), encoded)

        const_kinds, const_changes = self.classified(
            {"const": -(10**400)},
            {"const": 10**400},
        )
        self.assertIn(("breaking", "finite_values_removed"), const_kinds)
        self.assertIn(("additive", "finite_values_added"), const_kinds)
        const_encoded = json.dumps(const_changes, sort_keys=True)
        self.assertIn(str(-(10**400)), const_encoded)
        self.assertIn(str(10**400), const_encoded)

        multiple_kinds, _ = self.classified(
            {"multipleOf": 10**400},
            {"multipleOf": 2 * 10**400},
        )
        self.assertIn(("breaking", "multiple_of_changed"), multiple_kinds)
        widened_kinds, _ = self.classified(
            {"multipleOf": 2 * 10**400},
            {"multipleOf": 10**400},
        )
        self.assertIn(("additive", "multiple_of_changed"), widened_kinds)

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

    def test_artifact_and_comparison_resource_budgets_fail_closed(self):
        payload_sentinel = "payload-secret-must-not-leak"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            oversized = root / "oversized.json"
            oversized.write_text(json.dumps(payload_sentinel), encoding="utf-8")
            with self.assertRaises(codex_schema.SchemaToolError) as raised:
                codex_schema.read_bounded_bytes(oversized, maximum=8)
            self.assertNotIn(payload_sentinel, str(raised.exception))

            symlink = root / "artifact-symlink"
            try:
                symlink.symlink_to(oversized)
            except OSError:
                pass
            else:
                with self.assertRaises(codex_schema.SchemaToolError):
                    codex_schema.read_bounded_bytes(symlink)

            if hasattr(os, "mkfifo"):
                fifo = root / "artifact-fifo"
                os.mkfifo(fifo)
                started = time.monotonic()
                with self.assertRaises(codex_schema.SchemaToolError):
                    codex_schema.read_bounded_bytes(fifo)
                self.assertLess(time.monotonic() - started, 1.0)

            first = root / "first.json"
            second = root / "second.json"
            first.write_text("[0,1]", encoding="utf-8")
            second.write_text("[2,3]", encoding="utf-8")
            with self.assertRaises(codex_schema.SchemaToolError) as raised:
                with codex_schema.operation_budget(maximum_aggregate_bytes=9):
                    codex_schema.load_json(first)
                    codex_schema.load_json(second)
            self.assertIn("aggregate artifact-byte", str(raised.exception))

            nested = root / "nested.json"
            nested.write_text("[[[0]]]", encoding="utf-8")
            with self.assertRaises(codex_schema.SchemaToolError) as raised:
                with codex_schema.operation_budget(maximum_json_depth=2):
                    codex_schema.load_json(nested)
            self.assertIn("nesting-depth", str(raised.exception))

            nodes = root / "nodes.json"
            nodes.write_text("[0,1,2]", encoding="utf-8")
            with self.assertRaises(codex_schema.SchemaToolError) as raised:
                with codex_schema.operation_budget(maximum_file_json_nodes=3):
                    codex_schema.load_json(nodes)
            self.assertIn("per-file node", str(raised.exception))
            with self.assertRaises(codex_schema.SchemaToolError) as raised:
                with codex_schema.operation_budget(maximum_json_nodes=7):
                    codex_schema.load_json(nodes)
                    codex_schema.load_json(nodes)
            self.assertIn("aggregate JSON-node", str(raised.exception))

            huge_number = root / "huge-number.json"
            huge_number.write_text(
                "9" * (codex_schema.MAX_JSON_NUMBER_CHARACTERS + 1),
                encoding="utf-8",
            )
            with self.assertRaises(codex_schema.SchemaToolError) as raised:
                codex_schema.load_json(huge_number)
            self.assertIn("per-number character", str(raised.exception))

        with self.assertRaises(codex_schema.SchemaToolError) as raised:
            with codex_schema.operation_budget(maximum_changes=1):
                codex_schema.compare_named_schemas(
                    {"properties": {}},
                    {"properties": {"one": True, "two": True}},
                    "root",
                    [],
                )
        self.assertIn("classified-change", str(raised.exception))

        with self.assertRaises(codex_schema.SchemaToolError) as raised:
            with codex_schema.operation_budget(maximum_work=20):
                variants = [{"const": value} for value in range(16)]
                codex_schema.compare_named_schemas(
                    {"oneOf": variants},
                    {"oneOf": list(reversed(variants))},
                    "root",
                    [],
                )
        self.assertIn("work limit", str(raised.exception))

        with self.assertRaises(codex_schema.SchemaToolError) as raised:
            with codex_schema.operation_budget(timeout=0.001):
                time.sleep(0.01)
                codex_schema.active_budget().checkpoint()
        self.assertIn("deadline", str(raised.exception))
        self.assertNotIn(payload_sentinel, str(raised.exception))

        cyclic_schema = {
            "$ref": "#/definitions/Loop",
            "definitions": {"Loop": {"$ref": "#/definitions/Loop"}},
        }
        with self.assertRaises(codex_schema.ValidationFailure) as raised:
            codex_schema.validate_instance(None, cyclic_schema, cyclic_schema)
        self.assertIn("nesting limit", str(raised.exception))

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

        with tempfile.TemporaryDirectory() as directory:
            escaped = Path(directory) / "closed-pipe-descendant-escaped"
            descendant = (
                "import pathlib,time;time.sleep(1.0);"
                f"pathlib.Path({os.fspath(escaped)!r}).write_text('escaped')"
            )
            parent = (
                "import subprocess,sys;"
                f"subprocess.Popen([sys.executable,'-c',{descendant!r}],"
                "stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)"
            )
            result = codex_schema.run_bounded(
                [sys.executable, "-c", parent], timeout=3
            )
            self.assertEqual(result.returncode, 0)
            time.sleep(1.2)
            self.assertFalse(escaped.exists())

    @unittest.skipUnless(os.name == "nt", "Windows Job Object regression")
    def test_windows_job_owns_descendants_after_the_direct_child_exits(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            started = root / "descendant-started"
            escaped = root / "descendant-escaped"
            descendant = (
                "import pathlib,time;"
                f"pathlib.Path({os.fspath(started)!r}).write_text('started');"
                "time.sleep(1.0);"
                f"pathlib.Path({os.fspath(escaped)!r}).write_text('escaped')"
            )
            parent = (
                "import pathlib,subprocess,sys,time;"
                f"started=pathlib.Path({os.fspath(started)!r});"
                f"subprocess.Popen([sys.executable,'-c',{descendant!r}]);"
                "deadline=time.monotonic()+3;"
                "\nwhile not started.exists() and time.monotonic()<deadline: time.sleep(0.01)\n"
                "raise SystemExit(0 if started.exists() else 2)"
            )
            result = codex_schema.run_bounded(
                [sys.executable, "-c", parent], timeout=5
            )
            self.assertEqual(result.returncode, 0)
            self.assertTrue(started.exists())
            time.sleep(1.2)
            self.assertFalse(escaped.exists())

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

    @unittest.skipIf(os.name == "nt", "fixture executable uses a POSIX shell")
    def test_schema_export_errors_never_echo_child_payloads(self):
        diagnostic_secret = "child-diagnostic-secret-must-not-leak"
        with tempfile.TemporaryDirectory() as directory:
            binary = Path(directory) / "codex"
            binary.write_text(
                "#!/bin/sh\n"
                "if [ \"$1\" = \"--version\" ]; then\n"
                "  printf 'codex-cli 0.146.0\\n'\n"
                "  exit 0\n"
                "fi\n"
                f"printf '{diagnostic_secret}\\n' >&2\n"
                "exit 7\n",
                encoding="utf-8",
            )
            binary.chmod(0o755)
            selection = codex_schema.Selection(
                "test-protocol",
                ("app-server", "generate-json-schema", "--out", "<temporary-directory>"),
                (),
                "ServerNotification.json",
            )
            with self.assertRaises(codex_schema.SchemaToolError) as raised:
                codex_schema.generate_schema_directory(binary, selection)
            self.assertNotIn(diagnostic_secret, str(raised.exception))
            self.assertIn("code 7", str(raised.exception))

    @unittest.skipIf(os.name == "nt", "fixture executable uses a POSIX shebang")
    def test_schema_export_uses_one_private_disposable_profile_and_cwd(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            real_profile = root / "real-profile"
            real_cwd = root / "real-cwd"
            real_profile.mkdir()
            real_cwd.mkdir()
            (real_profile / "profile-sentinel").write_text("secret", encoding="utf-8")
            (real_cwd / "cwd-sentinel").write_text("secret", encoding="utf-8")
            observations = root / "observations.jsonl"
            binary = root / "codex"
            binary.write_text(
                "#!/usr/bin/env python3\n"
                "import json, os, pathlib, stat, sys\n"
                f"observations = pathlib.Path({os.fspath(observations)!r})\n"
                "record = {\n"
                "    'cwd': os.getcwd(),\n"
                "    'codexHome': os.environ.get('CODEX_HOME'),\n"
                "    'home': os.environ.get('HOME'),\n"
                "    'secretInherited': 'SCHEMA_PARENT_SECRET' in os.environ,\n"
                "}\n"
                "with observations.open('a', encoding='utf-8') as handle:\n"
                "    handle.write(json.dumps(record, sort_keys=True) + '\\n')\n"
                "if sys.argv[1:] == ['--version']:\n"
                "    print('codex-cli 0.146.0')\n"
                "    raise SystemExit(0)\n"
                "arguments = sys.argv[1:]\n"
                "output = pathlib.Path(arguments[arguments.index('--out') + 1])\n"
                "output.mkdir(parents=True)\n",
                encoding="utf-8",
            )
            binary.chmod(0o755)
            selection = codex_schema.Selection(
                "test-protocol",
                (
                    "app-server",
                    "generate-json-schema",
                    "--out",
                    "<temporary-directory>",
                ),
                (),
                "ServerNotification.json",
            )
            previous_cwd = Path.cwd()
            temporary = None
            try:
                os.chdir(real_cwd)
                with mock.patch.dict(
                    os.environ,
                    {
                        "CODEX_HOME": os.fspath(real_profile),
                        "HOME": os.fspath(real_profile),
                        "SCHEMA_PARENT_SECRET": "must-not-cross",
                    },
                    clear=False,
                ):
                    version, export, temporary = codex_schema.generate_schema_directory(
                        binary, selection
                    )
                self.assertEqual(version, "0.146.0")
                self.assertTrue(export.is_dir())
                records = [json.loads(line) for line in observations.read_text().splitlines()]
                self.assertEqual(len(records), 2)
                self.assertFalse(any(record["secretInherited"] for record in records))
                self.assertEqual({record["cwd"] for record in records}, {records[0]["cwd"]})
                self.assertEqual(
                    {record["codexHome"] for record in records},
                    {records[0]["codexHome"]},
                )
                isolated_cwd = Path(records[0]["cwd"])
                isolated_profile = Path(records[0]["codexHome"])
                isolated_home = Path(records[0]["home"])
                self.assertNotEqual(isolated_cwd, real_cwd)
                self.assertNotEqual(isolated_profile, real_profile)
                self.assertNotEqual(isolated_home, real_profile)
                self.assertNotEqual(isolated_cwd, isolated_profile)
                self.assertFalse((isolated_cwd / "cwd-sentinel").exists())
                self.assertFalse((isolated_profile / "profile-sentinel").exists())
                self.assertEqual(stat.S_IMODE(isolated_cwd.stat().st_mode), 0o700)
                self.assertEqual(stat.S_IMODE(isolated_profile.stat().st_mode), 0o700)
            finally:
                os.chdir(previous_cwd)
                if temporary is not None:
                    isolated_root = Path(temporary.name)
                    temporary.cleanup()
                    self.assertFalse(isolated_root.exists())


if __name__ == "__main__":
    unittest.main()
