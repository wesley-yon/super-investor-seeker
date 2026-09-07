import unittest
from unittest.mock import patch

from parallel_validation import worker_limit


class WorkerLimitTests(unittest.TestCase):
    def test_limits_processes_to_cpu_and_memory_capacity(self):
        def sysconf(name):
            return {'SC_AVPHYS_PAGES': 6 * 1024 * 1024 // 4, 'SC_PAGE_SIZE': 4096}[name]
        with patch('parallel_validation.os.cpu_count', return_value=12), \
             patch('parallel_validation.os.sched_getaffinity', return_value=set(range(12)), create=True), \
             patch('parallel_validation.os.sysconf', side_effect=sysconf), \
             patch('parallel_validation.Path.read_text', side_effect=OSError):
            self.assertEqual(3, worker_limit(12))
            self.assertEqual(1, worker_limit(1))

    def test_container_cpu_quota_caps_available_host_cores(self):
        def read(path):
            if str(path).endswith('/cpu.max'):
                return '200000 100000'
            raise OSError
        with patch('parallel_validation.os.cpu_count', return_value=12), \
             patch('parallel_validation.os.sched_getaffinity', return_value=set(range(12)), create=True), \
             patch('parallel_validation.os.sysconf', side_effect=lambda name: 4096 if name == 'SC_PAGE_SIZE' else 8 * 1024 * 1024), \
             patch('parallel_validation.Path.read_text', autospec=True, side_effect=read):
            self.assertEqual(2, worker_limit(4))

    def test_rejects_invalid_worker_requests(self):
        for value in (0, -1, True, 1.5):
            with self.subTest(value=value), self.assertRaises(ValueError):
                worker_limit(value)
