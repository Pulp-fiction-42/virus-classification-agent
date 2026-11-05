import copy
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from report_provenance import (
    REGISTRY_PATH, SkillUsageAudit, bibliography, capture_runner_environment,
    collect_provenance, logged_commands, render_appendix, save_provenance,
)
from runtime import DiagnosticReportFragmentValidator


class ReportProvenanceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / '.bioagent').mkdir()
        self.log = self.root / '.bioagent/execution.log'
        self.log.write_text('''Starting phase upstream
rule fastp:
    /opt/bin/fastp --length_required 50 -i input.fq
    /opt/bin/seqkit stat reads.fq
    /opt/bin/seqkit stat reads2.fq
fastp v1.3.2, time used: 4 seconds
Finished job 0.
''', encoding='utf-8')
        self.state = {'run_id': 'run-a', 'execution': {'ok': True, 'phases': [{'id': 'upstream'}]},
                      'steps': [{'tool': 'MEGAHIT', 'status': 'completed'}]}
        self.metrics = [{'source': 'results/01_fastp.json', 'sha256': 'example-fixture', 'version': '1.3.2'}]

    def collect(self):
        return collect_provenance(self.root, self.state, self.metrics)

    def test_actual_evidence_only_deduplicates_nested_rule_tool_and_citations(self):
        manifest = self.collect()
        tools = {t['id']: t for t in manifest['tools']}
        self.assertEqual(set(tools), {'fastp', 'seqkit', 'snakemake'})
        self.assertEqual(tools['fastp']['versions'], ['1.3.2'])
        self.assertEqual(tools['fastp']['usage'], 'artifact_verified')
        self.assertEqual(tools['seqkit']['versions'], [])
        self.assertEqual(len(manifest['references']), 3)
        self.assertNotIn('MEGAHIT', render_appendix(manifest))

    def test_no_current_environment_probes_for_historical_run(self):
        with patch('subprocess.run', side_effect=AssertionError('must not probe installed versions')):
            manifest = self.collect()
        self.assertEqual(manifest['libraries'], [])
        self.assertEqual(manifest['skills']['tracking'], 'not_recorded')
        rendered = render_appendix(manifest)
        self.assertIn('无法追溯', rendered)
        self.assertNotIn('未调用 Skill', rendered)

    def test_new_compiled_runs_record_libraries_but_never_invent_skill_usage(self):
        capture_runner_environment(self.state)
        manifest = self.collect()
        self.assertEqual({x['name'] for x in manifest['libraries']}, {'Python', 'PyYAML'})
        self.assertEqual(manifest['skills']['skills'], [])
        self.assertIn('未调用 Skill', render_appendix(manifest))

    def test_existing_snapshot_keeps_metadata_and_refuses_changed_evidence(self):
        first = self.collect()
        save_provenance(self.root, first)
        with patch('report_provenance.REGISTRY_PATH', self.root/'absent-registry.json'):
            self.assertEqual(self.collect(), first)
        self.log.write_text(self.log.read_text() + '\nChanged execution evidence\n')
        with self.assertRaisesRegex(ValueError, '来源已变化'):
            self.collect()

    def test_exports_and_reference_numbers_correspond(self):
        manifest = self.collect()
        save_provenance(self.root, manifest)
        self.assertEqual(json.loads((self.root/'analysis-provenance.json').read_text(encoding='utf-8')), manifest)
        bib = (self.root/'references.bib').read_text(encoding='utf-8')
        self.assertEqual(bib.count('@article{'), 3)
        for ref in manifest['references']:
            self.assertIn(ref['doi'], bib)
            self.assertIn(f'id="reference-{ref["number"]}"', render_appendix(manifest))
            self.assertIn(f'href="#reference-{ref["number"]}"', render_appendix(manifest))

    def test_free_form_model_doi_cannot_create_reference(self):
        self.state['provenance_capture'] = {'reference_uses': [
            {'reference_id': 'invented-doi-123', 'context': '未经核验的解释'},
            {'reference_id': 'fastp', 'context': '质控方法说明'},
        ]}
        manifest = self.collect()
        self.assertEqual(len(manifest['references']), 3)
        self.assertEqual(manifest['literature_uses'][0]['validation'], 'unregistered')
        self.assertIsNone(manifest['literature_uses'][0]['reference_number'])
        self.assertEqual(manifest['literature_uses'][1]['reference_number'], 1)

    def test_html_escapes_metadata_and_preserves_strict_model_validator(self):
        manifest = copy.deepcopy(self.collect())
        manifest['tools'][0]['name'] = '<script>alert(1)</script>'
        manifest['references'][0]['title'] = '<img src=x onerror=alert(1)>research & data'
        manifest['references'][0]['doi'] = 'javascript:alert(1)" onclick="bad'
        appendix = render_appendix(manifest)
        self.assertNotIn('<script>', appendix)
        self.assertNotIn('<img', appendix)
        self.assertNotIn('href="javascript:', appendix)
        self.assertNotIn(' onclick="', appendix)
        self.assertIn('research &amp; data', appendix)
        validator = DiagnosticReportFragmentValidator()
        with self.assertRaises(ValueError):
            validator.feed('<article class="diagnostic-report"><a href="https://example.com">model link</a></article>')

    def test_prose_and_dryrun_file_are_not_tool_invocations(self):
        (self.root/'.bioagent/upstream.dryrun.log').write_text('/opt/checkv end_to_end input output\n')
        registry = json.loads(REGISTRY_PATH.read_text(encoding='utf-8'))
        self.assertEqual(logged_commands('fastp is useful\nSeqKit is installed\n"bad quote', registry), [])
        self.assertNotIn('checkv', {t['id'] for t in self.collect()['tools']})

    def test_skill_audit_records_real_tool_messages_and_separates_runs(self):
        skill_root = self.root/'skills'
        skill = skill_root/'example'
        skill.mkdir(parents=True)
        (skill/'SKILL.md').write_text('---\nmetadata:\n  version: "2"\n  citation_ids: [fastp]\n---\nProcedure.', encoding='utf-8')
        audit = SkillUsageAudit(self.root, skill_root)
        self.assertEqual(self.collect()['skills']['skills'], [])
        audit.observe(SimpleNamespace(content=[SimpleNamespace(name='Skill', id='call-1', input={'skill': 'example'})]))
        audit.observe(SimpleNamespace(content=[{'tool_use_id': 'call-1', 'is_error': False}]))
        audit.finish()
        record = self.collect()['skills']['skills'][0]
        self.assertEqual(record['name'], 'example')
        self.assertEqual(record['status'], 'loaded')
        self.assertEqual(record['version'], '2')
        self.assertEqual(record['reference_numbers'], [1])
        self.assertEqual(len(record['sha256']), 64)
        other = self.root/'other-run'
        (other/'.bioagent').mkdir(parents=True)
        self.assertEqual(collect_provenance(other, {'run_id': 'run-b'}, [])['skills']['tracking'], 'not_recorded')

    def test_skill_request_is_not_silently_marked_success_and_paths_cannot_escape(self):
        audit = SkillUsageAudit(self.root, self.root/'skills')
        audit.observe(SimpleNamespace(content=[{'name': 'Skill', 'id': 'c1', 'input': {'skill': '../../outside'}}]))
        audit.finish()
        record = self.collect()['skills']['skills'][0]
        self.assertEqual(record['status'], 'requested')
        self.assertIsNone(record['sha256'])
        audit.observe(SimpleNamespace(content=[{'tool_use_id': 'c1', 'is_error': True}]))
        self.assertEqual(self.collect()['skills']['skills'][0]['status'], 'tool_error')

    def test_curated_registry_has_verified_complete_metadata(self):
        registry = json.loads(REGISTRY_PATH.read_text(encoding='utf-8'))
        for key, record in registry['references'].items():
            for field in ('title', 'authors', 'year', 'volume', 'pages', 'doi', 'verified_from', 'verified_at'):
                self.assertTrue(record.get(field), (key, field))
            self.assertTrue(record['verified_from'].startswith('https://api.crossref.org/works/'))


if __name__ == '__main__':
    unittest.main()
