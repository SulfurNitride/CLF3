import importlib.util
import sys
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "wabbajack_overlay.py"
SPEC = importlib.util.spec_from_file_location("wabbajack_overlay", SCRIPT)
overlay = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = overlay
SPEC.loader.exec_module(overlay)


class OverlayPlanTests(unittest.TestCase):
    def test_group_sort_uses_source_conflict_order(self):
        groups = {
            "Physics_separator": ["Faster HDT-SMP", "CBPC", "Faster HDT-SMP - AVX512 DLL"]
        }
        overlay.sort_groups_by_source(
            groups,
            ["Faster HDT-SMP - AVX512 DLL", "Faster HDT-SMP", "CBPC"]
        )
        self.assertEqual(
            groups["Physics_separator"],
            ["Faster HDT-SMP - AVX512 DLL", "Faster HDT-SMP", "CBPC"]
        )

    def test_selects_whole_core_folder_and_only_preset_xml(self):
        manifest = {
            "Name": "Fixture",
            "Version": "1",
            "Archives": [
                {"Hash": "core", "Size": 10},
                {"Hash": "preset", "Size": 20},
                {"Hash": "other", "Size": 30}
            ],
            "Directives": [
                {"$type": "FromArchive", "To": "mods\\CBBE 3BA\\meshes\\body.nif", "ArchiveHashPath": ["core", "body.nif"]},
                {"$type": "FromArchive", "To": "mods\\Nice Preset\\CalienteTools\\BodySlide\\SliderPresets\\Nice.xml", "ArchiveHashPath": ["preset", "Nice.xml"]},
                {"$type": "FromArchive", "To": "mods\\Nice Preset\\readme.txt", "ArchiveHashPath": ["preset", "readme.txt"]},
                {"$type": "FromArchive", "To": "mods\\Unrelated\\file.txt", "ArchiveHashPath": ["other", "file.txt"]}
            ]
        }
        recipe = {
            "groups": [{"separator": "Body_separator", "folders": ["CBBE 3BA"]}],
            "presets": {"separator": "Presets_separator", "exclude_folders": []},
            "outfits": {"separator": "Outfits_separator", "patterns": [], "extra_folders": []}
        }
        plan = overlay.build_plan(overlay.SourceManifest(manifest, None), recipe)
        self.assertEqual(
            [item["To"] for item in plan.manifest["Directives"]],
            [
                "mods\\CBBE 3BA\\meshes\\body.nif",
                "mods\\Nice Preset\\CalienteTools\\BodySlide\\SliderPresets\\Nice.xml"
            ]
        )
        self.assertEqual({item["Hash"] for item in plan.manifest["Archives"]}, {"core", "preset"})
        self.assertEqual(plan.archive_bytes, 30)

    def test_archive_source_override_preserves_hash_and_size(self):
        manifest = {
            "Name": "Fixture", "Version": "1",
            "Archives": [{
                "Hash": "core", "Name": "Core.7z", "Size": 10,
                "State": {"$type": "GoogleDriveDownloader, Wabbajack.Lib", "Id": "old"}
            }],
            "Directives": [{
                "$type": "FromArchive", "To": "mods\\CBBE 3BA\\body.nif",
                "ArchiveHashPath": ["core", "body.nif"]
            }]
        }
        replacement = {
            "$type": "MediaFireDownloader+State, Wabbajack.Lib",
            "Url": "https://example.invalid/mirror"
        }
        recipe = {
            "archive_state_overrides": {"Core.7z": replacement},
            "groups": [{"separator": "Body_separator", "folders": ["CBBE 3BA"]}],
            "presets": {"separator": "Presets_separator"},
            "outfits": {"separator": "Outfits_separator", "patterns": []}
        }
        plan = overlay.build_plan(overlay.SourceManifest(manifest, None), recipe)
        self.assertEqual(plan.manifest["Archives"][0]["State"], replacement)
        self.assertEqual(plan.manifest["Archives"][0]["Hash"], "core")
        self.assertEqual(plan.manifest["Archives"][0]["Size"], 10)
        self.assertEqual(manifest["Archives"][0]["State"]["Id"], "old")

    def test_plain_manifest_reports_essential_inline_data_as_omitted(self):
        manifest = {
            "Name": "Fixture", "Version": "1", "Archives": [],
            "Directives": [{"$type": "InlineFile", "To": "mods\\CBBE 3BA\\config.ini", "SourceDataID": "deadbeef"}]
        }
        recipe = {
            "groups": [{"separator": "Body_separator", "folders": ["CBBE 3BA"]}],
            "presets": {"separator": "Presets_separator"},
            "outfits": {"separator": "Outfits_separator", "patterns": []}
        }
        plan = overlay.build_plan(overlay.SourceManifest(manifest, None), recipe)
        self.assertEqual(plan.omitted_directives, ["mods\\CBBE 3BA\\config.ini"])
        self.assertEqual(plan.manifest["Directives"], [])


if __name__ == "__main__":
    unittest.main()
