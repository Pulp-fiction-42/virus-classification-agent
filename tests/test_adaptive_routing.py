from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from adaptive_routing import (
    aggregate_screening,
    collect_resource_snapshot,
    decide_route,
    normalize_routing_request,
    parse_kraken_report,
    screening_metrics,
)
from adaptive_screening import validate_paired_fastq


class AdaptiveRoutingTests(unittest.TestCase):
    @staticmethod
    def _sample(*, reads: int, sampled_pairs: int, viral_reads: int):
        return screening_metrics(
            {
                "viral_reads": viral_reads,
                "species": [{"taxid": "2697049", "name": "SARS-CoV-2", "reads": reads}],
            },
            sample_id="sample",
            sampled_pairs=sampled_pairs,
        )

    def test_threshold_boundaries_are_inclusive(self):
        metrics = self._sample(reads=1_000, sampled_pairs=100_000, viral_reads=1_250)
        self.assertTrue(metrics["known_virus_high_confidence"])
        self.assertEqual(metrics["top_species_sample_fraction"], 0.01)
        self.assertEqual(metrics["top_species_viral_fraction"], 0.8)
        self.assertTrue(all(check["passed"] for check in metrics["confidence_checks"]))

        for metrics in (
            self._sample(reads=999, sampled_pairs=99_900, viral_reads=1_000),
            self._sample(reads=1_000, sampled_pairs=100_001, viral_reads=1_000),
            self._sample(reads=1_000, sampled_pairs=100_000, viral_reads=1_251),
        ):
            self.assertFalse(metrics["known_virus_high_confidence"])

    def test_low_confidence_records_failed_checks_and_decision_reasons(self):
        sample = self._sample(reads=811, sampled_pairs=1_087_028, viral_reads=840)
        screening = aggregate_screening([sample])
        decision = decide_route({"mode": "auto"}, screening)

        checks = {check["metric"]: check for check in sample["confidence_checks"]}
        self.assertFalse(checks["top_species_reads"]["passed"])
        self.assertFalse(checks["top_species_sample_fraction"]["passed"])
        self.assertTrue(checks["top_species_viral_fraction"]["passed"])
        self.assertEqual(screening["confidence_thresholds"]["top_species_reads"], 1_000)
        self.assertIn("KNOWN_VIRUS_CONFIDENCE_INSUFFICIENT", decision["reason_codes"])
        self.assertIn("TOP_SPECIES_READS_BELOW_MINIMUM", decision["reason_codes"])
        self.assertIn(
            "TOP_SPECIES_SAMPLE_FRACTION_BELOW_MINIMUM",
            decision["reason_codes"],
        )

    def test_all_samples_must_be_high_confidence(self):
        high = self._sample(reads=1_000, sampled_pairs=100_000, viral_reads=1_250)
        low = self._sample(reads=999, sampled_pairs=100_000, viral_reads=1_249)
        self.assertTrue(
            aggregate_screening([high, high])["known_virus_high_confidence"]
        )
        self.assertFalse(
            aggregate_screening([high, low])["known_virus_high_confidence"]
        )
        self.assertFalse(aggregate_screening([])["known_virus_high_confidence"])

    def test_mode_matrix_and_emergency_low_confidence(self):
        high = {"known_virus_high_confidence": True}
        low = {"known_virus_high_confidence": False}
        self.assertEqual(decide_route({"mode": "auto"}, high)["selected_route"], "A")
        self.assertEqual(decide_route({"mode": "auto"}, low)["selected_route"], "B")
        self.assertEqual(
            decide_route({"mode": "standard"}, high)["selected_route"], "B"
        )

        emergency = decide_route({"mode": "emergency", "deadline_seconds": 900}, low)
        self.assertEqual(emergency["selected_route"], "A")
        self.assertEqual(emergency["report_kind"], "preliminary_screen")
        self.assertIn("EMERGENCY_LOW_CONFIDENCE_REPORT", emergency["reason_codes"])

    def test_full_path_is_blocked_without_fallback(self):
        decision = decide_route(
            {"mode": "auto"},
            {"known_virus_high_confidence": False},
            full_resources={
                "admission_ok": False,
                "reason_codes": ["MEMORY_INSUFFICIENT"],
            },
        )
        self.assertEqual(decision["selected_route"], "B")
        self.assertEqual(decision["phase"], "blocked")
        self.assertIn("MEMORY_INSUFFICIENT", decision["reason_codes"])
        self.assertEqual(decision["report_kind"], "preliminary_screen")

    def test_minimum_admission_accepts_108_gib_available_memory(self):
        with (
            patch("adaptive_routing.os.cpu_count", return_value=64),
            patch("adaptive_routing._cpu_percent", return_value=20.0),
            patch("adaptive_routing._memory_available_mb", return_value=108 * 1024),
            patch("adaptive_routing.count_allocated_threads", return_value=0),
        ):
            snapshot = collect_resource_snapshot(
                Path("/tmp/runs"),
                required_threads=8,
                required_memory_mb=24 * 1024,
            )

        self.assertTrue(snapshot["admission_ok"])
        self.assertEqual(snapshot["reason_codes"], [])

    def test_emergency_default_deadline_and_deep_mode(self):
        self.assertEqual(
            normalize_routing_request({"mode": "emergency"})["deadline_seconds"], 900
        )
        self.assertEqual(normalize_routing_request({"mode": "deep"}), {"mode": "deep"})

    def test_parses_species_inside_virus_clade(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            report = Path(temp_dir) / "kraken.report"
            report.write_text(
                "100.00\t2000\t0\tR\t1\troot\n"
                "90.00\t1800\t0\tD\t10239\t  Viruses\n"
                "80.00\t1600\t1500\tS\t2697049\t    SARS-CoV-2\n"
                "10.00\t200\t200\tS\t9606\t  Homo sapiens\n",
                encoding="utf-8",
            )
            parsed = parse_kraken_report(report)
        self.assertEqual(parsed["total_reads"], 2000)
        self.assertEqual(parsed["viral_reads"], 1800)
        self.assertEqual(
            parsed["species"],
            [{"taxid": "2697049", "name": "SARS-CoV-2", "reads": 1600}],
        )


class PairedFastqValidationTests(unittest.TestCase):
    def test_rejects_mismatched_read_ids(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            read1 = Path(temp_dir) / "r1.fq"
            read2 = Path(temp_dir) / "r2.fq"
            read1.write_text("@read-1/1\nAC\n+\nII\n", encoding="utf-8")
            read2.write_text("@read-2/2\nTG\n+\nII\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "mate IDs differ"):
                validate_paired_fastq(read1, read2)


if __name__ == "__main__":
    unittest.main()
