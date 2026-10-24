"""Explicit operator-only existing-store upgrade; NOT called by the backend.

The caller supplies an authenticated bounded DynSec request capability after
verifying deployment/store ownership and obtaining exclusive administration.
No credentials, clients, groups, defaults or stores are created/reset here.
"""


class ACLUpdateError(Exception):
    """Safe policy/precondition failure; never include native data."""


def expected_acls(control=True):
    rows = [(k, f'gateways/+/{suffix}/#')
            for suffix in ('telemetry', 'responses') if control or suffix == 'telemetry'
            for k in ('publishClientReceive', 'subscribeLiteral', 'unsubscribeLiteral')]
    rows += [('publishClientSend', f'gateways/+/{suffix}/#')
             for suffix in ('acks', 'commands') if control or suffix == 'acks']
    return {(kind, topic, True, 0) for kind, topic in rows}


def role_acls(data):
    try:
        role = data['role']
        if role['rolename'] != 'bootstrap_backend':
            raise ACLUpdateError('unexpected role')
        acls = role['acls']
        result = {(a['acltype'], a['topic'], a['allow'], a['priority']) for a in acls}
        if len(result) != len(acls) or any(type(a['allow']) is not bool or type(a['priority']) is not int for a in acls):
            raise ACLUpdateError('invalid ACL projection')
        return result
    except (KeyError, TypeError):
        raise ACLUpdateError('invalid role readback') from None


def check_ownership(request, username):
    """Bound the inventory; require the role to belong only to this data client."""
    client = request('getClient', username=username)['client']
    if (client.get('username') != username or client.get('disabled', False)
            or client.get('groups') or
            [r['rolename'] for r in client.get('roles', [])] != ['bootstrap_backend']):
        raise ACLUpdateError('data client ownership mismatch')
    defaults = request('getDefaultACLAccess')['acls']
    if ({a['acltype']: a['allow'] for a in defaults} !=
            {k: False for k in ('publishClientSend', 'publishClientReceive', 'subscribe', 'unsubscribe')}
            or len(defaults) != 4 or any(a['allow'] is not False for a in defaults)):
        raise ACLUpdateError('default deny required')
    owners = []
    for command, key, field in (('listClients', 'clients', 'username'), ('listGroups', 'groups', 'groupname')):
        for page in range(16):
            rows = request(command, verbose=True, count=128, offset=page * 128)[key]
            if not isinstance(rows, list) or len(rows) > 128:
                raise ACLUpdateError('invalid inventory')
            for row in rows:
                if any(r['rolename'] == 'bootstrap_backend' for r in row.get('roles', [])):
                    owners.append((key, row[field]))
            if len(rows) < 128:
                break
        else:
            raise ACLUpdateError('inventory bound exceeded; supervised review required')
    if owners != [('clients', username)]:
        raise ACLUpdateError('backend role shared or inventory mismatch')


def update_existing_backend_role(request, username, *, exclusive_admin_confirmed=False):
    """Add exactly four permissions after ownership/readback, then verify again.

    DynSec offers no compare-and-swap for this sequence. Operator exclusivity is
    mandatory. On interruption/error stop: inspect the existing role, do not reset
    anything. Already-exact final policy is an idempotent read-only success.
    """
    if not exclusive_admin_confirmed:
        raise ACLUpdateError('exclusive operator administration required')
    check_ownership(request, username)
    before = role_acls(request('getRole', rolename='bootstrap_backend'))
    if before not in (expected_acls(False), expected_acls(True)):
        raise ACLUpdateError('unexpected or partial ACL baseline; supervised review required')
    for kind, topic, allow, priority in sorted(expected_acls(True) - before):
        request('addRoleACL', rolename='bootstrap_backend', acltype=kind,
                topic=topic, allow=allow, priority=priority)
    if role_acls(request('getRole', rolename='bootstrap_backend')) != expected_acls(True):
        raise ACLUpdateError('post-update ACL readback mismatch')
    check_ownership(request, username)
    return 'exact backend policy verified; no store or credential reset'
