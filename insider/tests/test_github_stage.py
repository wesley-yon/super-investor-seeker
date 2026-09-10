import unittest

from insider_pipeline.github_stage import REPOSITORY, check_assets, check_release, check_repository


class GitHubStageTests(unittest.TestCase):
    def test_private_repository_and_matching_draft_required(self):
        good = {'full_name': REPOSITORY, 'private': True, 'permissions': {'push': True}}
        check_repository(good)
        for change in [{'private': False}, {'full_name': 'another/repository'}, {'permissions': {'push': False}}]:
            with self.assertRaises(ValueError):
                check_repository({**good, **change})
        digest, tag = 'a' * 64, 'insider-preflight-20260909-' + 'a' * 12
        release = {'tag_name': tag, 'draft': True, 'body': 'Baseline SHA-256: ' + digest}
        check_release(release, tag, digest)
        with self.assertRaises(ValueError):
            check_release(None, tag, digest)
        for change in [{'draft': False}, {'tag_name': 'dataset-existing'}, {'body': 'unrelated'}]:
            with self.assertRaises(ValueError):
                check_release({**release, **change}, tag, digest)

    def test_asset_resume_refuses_clobber_and_incomplete_final_set(self):
        expected = {'one.zip': {'bytes': 123, 'sha256': 'a' * 64}, 'two.json': {'bytes': 456, 'sha256': 'b' * 64}}
        asset = {'name': 'one.zip', 'size': 123, 'digest': 'sha256:' + 'a' * 64, 'state': 'uploaded'}
        check_assets([asset], expected)
        with self.assertRaises(ValueError):
            check_assets([asset], expected, complete=True)
        for change in [{'name': 'foreign.zip'}, {'size': 1}, {'digest': 'sha256:' + 'c' * 64}, {'state': 'starter'}]:
            with self.assertRaises(ValueError):
                check_assets([{**asset, **change}], expected)
        with self.assertRaises(ValueError):
            check_assets([asset, asset], expected)
        check_assets([asset, {'name': 'two.json', 'size': 456, 'digest': None, 'state': 'uploaded'}], expected, complete=True)


if __name__ == '__main__':
    unittest.main()
