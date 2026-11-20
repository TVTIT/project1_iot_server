import importlib.util
import json
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('mqtt_bootstrap_media', ROOT / 'scripts/mqtt-bootstrap.py')
bootstrap = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bootstrap)
spec = importlib.util.spec_from_file_location('mqtt_backend_acl_media', ROOT / 'scripts/mqtt_backend_acl.py')
policy = importlib.util.module_from_spec(spec)
spec.loader.exec_module(policy)


def native(username, role):
    return {'clients': [{'username': username, 'password': 'hash', 'salt': 'salt',
                         'iterations': 1, 'roles': []}],
            'roles': [{'rolename': role, 'acls': []}]}


class DynsecModel:
    def __init__(self, gateway='media_a'):
        self.gateway = gateway
        self.backend = set(policy.expected_acls(True))
        self.gateway_acl = policy.expected_gateway_acls(gateway)
        self.clients = [
            {'username': 'bootstrap_backend', 'password': 'backend-hash',
             'roles': [{'rolename': 'bootstrap_backend'}]},
            {'username': gateway, 'password': 'gateway-hash', 'disabled': False,
             'roles': [{'rolename': f'gateway_{gateway}'}]},
        ]
        self.groups = []
        self.calls = []
        self.defaults = [{'acltype': k, 'allow': False} for k in
                         ('publishClientSend', 'publishClientReceive', 'subscribe', 'unsubscribe')]

    def request(self, command, **args):
        self.calls.append((command, args))
        if command == 'getClient':
            return {'client': next(c for c in self.clients if c['username'] == args['username'])}
        if command == 'getDefaultACLAccess':
            return {'acls': self.defaults}
        if command == 'listClients':
            return {'clients': self.clients}
        if command == 'listGroups':
            return {'groups': self.groups}
        if command == 'getRole':
            name = args['rolename']
            acl_set = self.backend if name == 'bootstrap_backend' else self.gateway_acl
            return {'role': {'rolename': name, 'acls': [
                {'acltype': k, 'topic': t, 'allow': a, 'priority': p}
                for k, t, a, p in acl_set]}}
        if command == 'addRoleACL':
            row = (args['acltype'], args['topic'], args['allow'], args['priority'])
            (self.backend if args['rolename'] == 'bootstrap_backend' else self.gateway_acl).add(row)
            return {}
        raise AssertionError(command)


class MediaACLTests(unittest.TestCase):
    def test_bootstrap_legacy_exact_and_opt_in_media_profiles(self):
        value = compose = bootstrap.compose_store(native('manager', 'm'), native('backend', 'b'))
        role = next(r for r in value['roles'] if r['rolename'] == 'bootstrap_backend')
        self.assertEqual({(a['acltype'], a['topic']) for a in role['acls']},
                         {(k, t)[:2] for k, t, _, _ in policy.expected_acls(True)})
        enabled = bootstrap.compose_store(native('manager', 'm'), native('backend', 'b'),
                                          media_upload_enabled=True)
        media = next(r for r in enabled['roles'] if r['rolename'] == 'bootstrap_backend')
        self.assertEqual({(a['acltype'], a['topic']) for a in media['acls']},
                         {(k, t)[:2] for k, t, _, _ in policy.expected_backend_media_acls()})
        self.assertEqual(compose['defaultACLAccess'], value['defaultACLAccess'])

    def test_config_media_switch_requires_explicit_boolean(self):
        base = {'store': '/owned/store', 'manager': 'manager', 'backend': 'backend',
                'manager_password': 'A' * 32, 'backend_password': 'B' * 32,
                'mapping': 'rootful', 'broker_uid': 1883, 'broker_gid': 1883}
        parsed = bootstrap.parse_config(__import__('io').StringIO(json.dumps(base)))
        self.assertFalse(parsed.get('media_upload_enabled', False))
        for invalid in ('true', 1, None):
            with self.subTest(invalid=invalid), self.assertRaises(bootstrap.BootstrapError):
                bootstrap.parse_config(__import__('io').StringIO(json.dumps(
                    {**base, 'media_upload_enabled': invalid})))

    def test_operator_transitions_exact_backend_and_gateway_without_credential_changes(self):
        model = DynsecModel()
        self.assertNotIn('media/up', repr(model.backend) + repr(model.gateway_acl))
        self.assertIn('exact backend media', policy.update_existing_backend_media_role(
            model.request, 'bootstrap_backend', exclusive_admin_confirmed=True))
        self.assertEqual(model.backend, policy.expected_backend_media_acls())
        self.assertIn('exact Gateway media', policy.update_existing_gateway_media_role(
            model.request, 'media_a', exclusive_admin_confirmed=True))
        self.assertEqual(model.gateway_acl, policy.expected_gateway_acls('media_a', True))
        policy.update_existing_backend_media_role(
            model.request, 'bootstrap_backend', exclusive_admin_confirmed=True)
        policy.update_existing_gateway_media_role(
            model.request, 'media_a', exclusive_admin_confirmed=True)

    def test_operator_refuses_partial_or_shared_roles_without_acl_mutation(self):
        for role in ('backend', 'gateway'):
            model = DynsecModel()
            if role == 'backend':
                model.backend.pop()
            else:
                model.gateway_acl.pop()
            before = (set(model.backend), set(model.gateway_acl))
            with self.assertRaises(policy.ACLUpdateError):
                (policy.update_existing_backend_media_role(model.request, 'bootstrap_backend',
                    exclusive_admin_confirmed=True) if role == 'backend' else
                 policy.update_existing_gateway_media_role(model.request, 'media_a',
                    exclusive_admin_confirmed=True))
            self.assertEqual((model.backend, model.gateway_acl), before)
        model = DynsecModel()
        model.clients.append({'username': 'other', 'roles': [{'rolename': 'gateway_media_a'}]})
        before = set(model.gateway_acl)
        with self.assertRaises(policy.ACLUpdateError):
            policy.update_existing_gateway_media_role(model.request, 'media_a',
                                                       exclusive_admin_confirmed=True)
        self.assertEqual(model.gateway_acl, before)

    def test_gateway_id_is_validated_before_dynsec_requests(self):
        for gateway_id in ('backend_service', 'bad/name', 'bad+', 'ümlaut', '-bad', 'x' * 65):
            model = DynsecModel()
            with self.subTest(gateway_id=gateway_id), self.assertRaises(policy.ACLUpdateError):
                policy.update_existing_gateway_media_role(
                    model.request, gateway_id, exclusive_admin_confirmed=True)
            self.assertEqual(model.calls, [])

    def test_disabled_owned_gateway_can_transition_without_mutating_identity_or_defaults(self):
        model = DynsecModel()
        gateway = model.clients[1]
        gateway['disabled'] = True
        before_password = gateway['password']
        before_defaults = json.loads(json.dumps(model.defaults))
        policy.update_existing_gateway_media_role(
            model.request, 'media_a', exclusive_admin_confirmed=True)
        self.assertTrue(gateway['disabled'])
        self.assertEqual(gateway['password'], before_password)
        self.assertEqual(model.defaults, before_defaults)
        commands = [command for command, _ in model.calls]
        self.assertGreaterEqual(commands.count('getClient'), 2)
        self.assertGreaterEqual(commands.count('getDefaultACLAccess'), 2)
        self.assertFalse(set(commands) & {'disableClient', 'enableClient', 'setClientPassword',
                                         'removeClient', 'createClient'})

    def test_gateway_uses_active_go_nine_rule_baseline_and_four_media_additions(self):
        gw = policy.expected_gateway_acls('media_a')
        self.assertEqual(len(gw), 9)
        self.assertIn(('publishClientSend', 'gateways/media_a/status', True, 0), gw)
        self.assertIn(('publishClientSend', 'gateways/media_a/responses/#', True, 0), gw)
        model = DynsecModel()
        before = set(model.gateway_acl)
        policy.update_existing_gateway_media_role(
            model.request, 'media_a', exclusive_admin_confirmed=True)
        self.assertEqual(model.gateway_acl - before, policy.expected_gateway_acls('media_a', True) - before)
        self.assertEqual(len(model.gateway_acl - before), 4)

    def test_wrong_four_rule_uplink_role_is_rejected_without_dynsec_request(self):
        model = DynsecModel()
        model.clients[1]['roles'] = [{'rolename': 'gateway_media_a_uplink'}]
        before = set(model.gateway_acl)
        with self.assertRaises(policy.ACLUpdateError):
            policy.update_existing_gateway_media_role(
                model.request, 'media_a', exclusive_admin_confirmed=True)
        self.assertNotIn('addRoleACL', [command for command, _ in model.calls])
        self.assertEqual(model.gateway_acl, before)

    def test_four_acl_policy_on_active_role_name_is_rejected_without_mutation(self):
        model = DynsecModel()
        model.gateway_acl = {
            ('publishClientSend', 'gateways/media_a/telemetry/#', True, 0),
            ('publishClientReceive', 'gateways/media_a/acks/#', True, 0),
            ('subscribeLiteral', 'gateways/media_a/acks/#', True, 0),
            ('unsubscribeLiteral', 'gateways/media_a/acks/#', True, 0),
        }
        with self.assertRaises(policy.ACLUpdateError):
            policy.update_existing_gateway_media_role(
                model.request, 'media_a', exclusive_admin_confirmed=True)
        self.assertEqual(len(model.gateway_acl), 4)
        self.assertNotIn('addRoleACL', [command for command, _ in model.calls])


if __name__ == '__main__':
    unittest.main()
