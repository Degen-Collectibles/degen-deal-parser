"""Permission matrix integration, independent of employee portal activation."""
import logging
from .service import insert_missing
from ..auth import has_permission
from ..models import RolePermission

KEYS=('ops.loyalty.view','admin.loyalty.reconcile','admin.loyalty.correct')


def seed_loyalty_permissions(session):
    for role in ('employee','viewer','manager','reviewer','admin'):
        for resource in KEYS:
            insert_missing(session,RolePermission,dict(role=role,resource_key=resource,is_allowed=role=='admin'),['role','resource_key'])
    session.commit()


def loyalty_template_context(request):
    user=getattr(request.state,'current_user',None)
    user_id=getattr(user,'id',None)
    if type(user_id) is not int or (getattr(request,'scope',{}).get('session') or {}).get('user_id')!=user_id:
        return {'loyalty_nav_allowed':False}
    from ..shared import managed_session
    try:
        with managed_session() as session:
            return {'loyalty_nav_allowed':has_permission(session,user,'ops.loyalty.view')}
    except Exception:
        logging.getLogger(__name__).warning('loyalty navigation permission unavailable')
        return {'loyalty_nav_allowed':False}
