import unittest
from intent_router import extract_inputs


class UploadedInputTests(unittest.TestCase):
    def paths(self, folder='folder'):
        base = '/runs/.conversation-uploads/userhash/chathash/'
        return [base+'12345678-1234-1234-1234-123456789abc/'+folder+'/demo_R1.fastq.gz',
                base+'12345678-1234-1234-1234-123456789def/'+folder+'/demo_R2.fastq.gz']

    def test_separate_upload_ids_still_form_a_read_pair(self):
        inputs, problems = extract_inputs('原始双端reads', self.paths())
        self.assertFalse(problems)
        self.assertEqual(len(inputs[0]['records']), 1)
        self.assertEqual(set(inputs[0]['records'][0]['files']), {'read1','read2'})

    def test_different_folders_or_conversations_do_not_form_a_pair(self):
        for old, new in [('folder/','other/'), ('chathash/','otherchat/')]:
            paths = self.paths(); paths[1] = paths[1].replace(old, new)
            _inputs, problems = extract_inputs('', paths)
            self.assertEqual(len(problems), 2)

    def test_duplicate_mates_require_clarification(self):
        paths = self.paths()
        _inputs, problems = extract_inputs('', paths+[paths[0].replace('789abc','789aaa')])
        self.assertTrue(any('多个文件' in problem for problem in problems))


if __name__ == '__main__': unittest.main()
