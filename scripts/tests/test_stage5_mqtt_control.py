"""Task 5.4 exact operator guards; mocked calls are not native acceptance."""
import copy
import importlib.util
import unittest
from pathlib import Path

spec = importlib.util.spec_from_file_location('mqtt_backend_acl', Path(__file__).resolve().parents[1] / 'mqtt_backend_acl.py')
acl = importlib.util.module_from_spec(spec)
spec.loader.exec_module(acl)


class OperatorACLTests(unittest.TestCase):
    def setUp(self):
        self.acls = set(acl.expected_acls(False))
        self.calls = []
        self.groups = []
        self.clients = [{'username': 'backend', 'roles': [{'rolename': 'bootstrap_backend'}]}]
        self.defaults = [{'acltype': k, 'allow': False} for k in ('publishClientSend', 'publishClientReceive', 'subscribe', 'unsubscribe')]

    def request(self, command, **fields):
        self.calls.append(command)
        if command == 'getClient':
            return {'client': copy.deepcopy(self.clients[0])}
        if command == 'getDefaultACLAccess':
            return {'acls': copy.deepcopy(self.defaults)}
        if command == 'listClients':
            return {'clients': copy.deepcopy(self.clients)}
        if command == 'listGroups':
            return {'groups': copy.deepcopy(self.groups)}
        if command == 'getRole':
            return {'role': {'rolename': 'bootstrap_backend', 'acls': [
                dict(acltype=k, topic=t, allow=a, priority=p) for k, t, a, p in sorted(self.acls)]}}
        if command == 'addRoleACL':
            self.assertEqual(fields['rolename'], 'bootstrap_backend')
            self.acls.add((fields['acltype'], fields['topic'], fields['allow'], fields['priority']))
            return {}
        self.fail('unexpected mutation/command: ' + command)

    def update(self):
        return acl.update_existing_backend_role(self.request, 'backend', exclusive_admin_confirmed=True)

    def test_four_additions_exact_readback_then_read_only_idempotence(self):
        self.update()
        self.assertEqual(self.acls, acl.expected_acls(True))
        self.assertEqual(self.calls.count('addRoleACL'), 4)
        self.calls.clear()
        self.update()
        self.assertNotIn('addRoleACL', self.calls)

    def test_ownership_exclusivity_and_exact_policy_fail_closed(self):
        for mode in ('no_exclusivity', 'shared_client', 'shared_group', 'client_group', 'extra', 'partial', 'default'):
            with self.subTest(mode=mode):
                self.setUp()
                if mode == 'shared_client':
                    self.clients.append({'username': 'foreign', 'roles': [{'rolename': 'bootstrap_backend'}]})
                elif mode == 'shared_group':
                    self.groups.append({'groupname': 'foreign', 'roles': [{'rolename': 'bootstrap_backend'}]})
                elif mode == 'client_group':
                    self.clients[0]['groups'] = [{'groupname': 'foreign'}]
                elif mode == 'extra':
                    self.acls.add(('publishClientSend', '#', True, 0))
                elif mode == 'partial':
                    self.acls.pop()
                elif mode == 'default':
                    self.defaults[0]['allow'] = True
                with self.assertRaises(acl.ACLUpdateError):
                    acl.update_existing_backend_role(self.request, 'backend', exclusive_admin_confirmed=mode != 'no_exclusivity')
                self.assertNotIn('addRoleACL', self.calls)


if __name__ == '__main__':
    unittest.main()
