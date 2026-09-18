from __future__ import annotations

import unittest

from controlled_requirements_evaluation import evaluate, load_cases, request_for
from provider import safe_context


class ControlledRequirementsEvaluationTests(unittest.IsolatedAsyncioTestCase):
    async def test_fixed_development_cases_pass_host_contract(self):
        records = await evaluate()
        self.assertEqual(len(records), 5)
        self.assertTrue(all(record["passed"] for record in records))

    def test_requests_are_path_free_and_cover_core_behaviors(self):
        cases = load_cases()
        self.assertEqual(
            {case["id"] for case in cases},
            {
                "qc-only",
                "fastp-length-50",
                "vague-length",
                "assembly-conflict",
                "raw-command-forbidden",
            },
        )
        for case in cases:
            context = request_for(case).context
            self.assertEqual(context, safe_context(context))
            self.assertNotIn("/tmp/result.fq", str(context))


if __name__ == "__main__":
    unittest.main()
