import unittest
from task26_maintenance_cases import release_allowed, replay, Proxy


class RotateContract(unittest.TestCase):
    def test_no_secret_before_confirmed_finalize(self):
        for status in (None, '', 'pending', 'verified', 'recovery_needed'):
            self.assertFalse(release_allowed(status, True))
        self.assertFalse(release_allowed('active', False))
        self.assertTrue(release_allowed('active', True))

    def test_replay_never_contains_secret(self):
        self.assertEqual(replay('active'), {'broker': 'active', 'delivery': 'unknown',
                                          'action': 'manual_rerotate_if_secret_lost'})

    def test_closed_proxy_has_no_socket(self):
        proxy = Proxy(1)
        proxy.close()
        self.assertIsNone(proxy.listener)


if __name__ == '__main__':
    unittest.main()
