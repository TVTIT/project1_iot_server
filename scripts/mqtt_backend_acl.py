"""Explicit operator-only existing-store upgrade; NOT called by the backend.

The caller supplies an authenticated bounded DynSec request capability after
verifying deployment/store ownership and obtaining exclusive administration.
No credentials, clients, groups, defaults or stores are created/reset here.
"""

import re

class ACLUpdateError(Exception):
    """Safe policy/precondition failure; never include native data."""


def expected_acls(control=True):
    rows = [(k, f'gateways/+/{suffix}/#')
            for suffix in ('telemetry', 'responses') if control or suffix == 'telemetry'
            for k in ('publishClientReceive', 'subscribeLiteral', 'unsubscribeLiteral')]
    rows += [('publishClientSend', f'gateways/+/{suffix}/#')
             for suffix in ('acks', 'commands') if control or suffix == 'acks']
    return {(kind, topic, True, 0) for kind, topic in rows}


def expected_backend_media_acls():
    return expected_acls(True) | {
        (kind, 'gateways/+/media/up/#', True, 0)
        for kind in ('publishClientReceive', 'subscribeLiteral', 'unsubscribeLiteral')
    } | {('publishClientSend', 'gateways/+/media/down/#', True, 0)}


def expected_gateway_acls(gateway_id, media_enabled=False):
    validate_gateway_id(gateway_id)
    prefix = f'gateways/{gateway_id}'
    rows = {
        ('publishClientSend', f'{prefix}/telemetry/#', True, 0),
        ('publishClientSend', f'{prefix}/status', True, 0),
        ('publishClientSend', f'{prefix}/responses/#', True, 0),
        ('publishClientReceive', f'{prefix}/acks/#', True, 0),
        ('publishClientReceive', f'{prefix}/commands/#', True, 0),
        ('subscribeLiteral', f'{prefix}/acks/#', True, 0),
        ('subscribeLiteral', f'{prefix}/commands/#', True, 0),
        ('unsubscribeLiteral', f'{prefix}/acks/#', True, 0),
        ('unsubscribeLiteral', f'{prefix}/commands/#', True, 0),
    }
    if media_enabled:
        rows.update({
            ('publishClientSend', f'{prefix}/media/up/#', True, 0),
            ('publishClientReceive', f'{prefix}/media/down/#', True, 0),
            ('subscribeLiteral', f'{prefix}/media/down/#', True, 0),
            ('unsubscribeLiteral', f'{prefix}/media/down/#', True, 0),
        })
    return rows


def validate_gateway_id(gateway_id):
    # Mirror internal/gateway.ValidateGatewayID: ASCII identifier and reserved backend name.
    if (not isinstance(gateway_id, str) or gateway_id == 'backend_service'
            or re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,63}', gateway_id, flags=re.ASCII) is None):
        raise ACLUpdateError('invalid Gateway identity')


def role_acls(data, role_name='bootstrap_backend'):
    try:
        role = data['role']
        if role['rolename'] != role_name:
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


def update_existing_backend_media_role(request, username, *, exclusive_admin_confirmed=False):
    """Explicitly add only the media direction rules to an owned Stage 5 role."""
    if not exclusive_admin_confirmed:
        raise ACLUpdateError('exclusive operator administration required')
    check_ownership(request, username)
    before = role_acls(request('getRole', rolename='bootstrap_backend'))
    target = expected_backend_media_acls()
    if before not in (expected_acls(True), target):
        raise ACLUpdateError('unexpected or partial ACL baseline; supervised review required')
    for kind, topic, allow, priority in sorted(target - before):
        request('addRoleACL', rolename='bootstrap_backend', acltype=kind,
                topic=topic, allow=allow, priority=priority)
    if role_acls(request('getRole', rolename='bootstrap_backend')) != target:
        raise ACLUpdateError('post-update ACL readback mismatch')
    check_ownership(request, username)
    return 'exact backend media policy verified; no store or credential reset'


def check_gateway_ownership(request, gateway_id):
    """Validate one Gateway's identity, role ownership, and global deny defaults."""
    validate_gateway_id(gateway_id)
    username = gateway_id
    role_name = f'gateway_{gateway_id}'
    client = request('getClient', username=username)['client']
    # Disabled is intentionally allowed: transition must preserve its current
    # state so a revoked identity remains revoked until normal reprovisioning.
    if (client.get('username') != username or type(client.get('disabled', False)) is not bool
            or client.get('groups') or
            [r['rolename'] for r in client.get('roles', [])] != [role_name]):
        raise ACLUpdateError('Gateway client ownership mismatch')
    defaults = request('getDefaultACLAccess')['acls']
    if (len(defaults) != 4 or
            {a['acltype']: a['allow'] for a in defaults} !=
            {k: False for k in ('publishClientSend', 'publishClientReceive', 'subscribe', 'unsubscribe')}):
        raise ACLUpdateError('default deny required')
    owners = []
    for command, key, field in (('listClients', 'clients', 'username'), ('listGroups', 'groups', 'groupname')):
        for page in range(16):
            rows = request(command, verbose=True, count=128, offset=page * 128)[key]
            if not isinstance(rows, list) or len(rows) > 128:
                raise ACLUpdateError('invalid inventory')
            for row in rows:
                if any(r['rolename'] == role_name for r in row.get('roles', [])):
                    owners.append((key, row[field]))
            if len(rows) < 128:
                break
        else:
            raise ACLUpdateError('inventory bound exceeded; supervised review required')
    if owners != [('clients', username)]:
        raise ACLUpdateError('Gateway role shared or inventory mismatch')
    return client, role_name


def update_existing_gateway_media_role(request, gateway_id, *, exclusive_admin_confirmed=False):
    """Transition one exact owned Gateway role; never mutate credentials/defaults."""
    validate_gateway_id(gateway_id)
    if not exclusive_admin_confirmed:
        raise ACLUpdateError('exclusive operator administration required')
    client_before, role_name = check_gateway_ownership(request, gateway_id)
    username = gateway_id
    before = role_acls(request('getRole', rolename=role_name), role_name)
    target = expected_gateway_acls(gateway_id, True)
    if before not in (expected_gateway_acls(gateway_id), target):
        raise ACLUpdateError('unexpected or partial ACL baseline; supervised review required')
    for kind, topic, allow, priority in sorted(target - before):
        request('addRoleACL', rolename=role_name, acltype=kind, topic=topic,
                allow=allow, priority=priority)
    if role_acls(request('getRole', rolename=role_name), role_name) != target:
        raise ACLUpdateError('post-update ACL readback mismatch')
    client_after, _ = check_gateway_ownership(request, gateway_id)
    if client_after.get('disabled', False) != client_before.get('disabled', False):
        raise ACLUpdateError('Gateway disabled state changed during ACL update')
    return 'exact Gateway media policy verified; no store or credential reset'


def verify_existing_gateway_media_role(request, gateway_id, media_enabled=False):
    """Exact read-only policy check for the owned Gateway principal."""
    _, role_name = check_gateway_ownership(request, gateway_id)
    username = gateway_id
    if role_acls(request('getRole', rolename=role_name), role_name) != expected_gateway_acls(gateway_id, media_enabled):
        raise ACLUpdateError('Gateway ACL readback mismatch')
