from flask import Blueprint
from flask import request
from flask_jsonpify import jsonpify

from .models import FileManager, Base

from .config import db_connection_string


def make_blueprint():
    """Create blueprint.
    """

    fm = FileManager(db_connection_string)
    Base.metadata.create_all(fm.engine)

    # Create instance
    blueprint = Blueprint('filemanager', 'filemanager')

    def call(method, arg):
        ret = method(arg, findability=request.values.get('findability'))
        ret = {
            'totalBytes': ret
        }
        return jsonpify(ret)

    def file_info(bucket, object_name):
        ret = fm.get_file_info(bucket, object_name)
        if ret.get('created_at'):
            ret['created_at'] = ret['created_at'].isoformat()
        return jsonpify(ret)

    def total_storage_for_owner(owner):
        return call(fm.get_total_size_for_owner, owner)

    def total_storage_for_dataset_id(dataset_id):
        return call(fm.get_total_size_for_dataset_id, dataset_id)

    def total_storage_for_flow_id(flow_id):
        return call(fm.get_total_size_for_flow_id, flow_id)

    # Register routes
    blueprint.add_url_rule(
        'storage/info/<bucket>/<path:object_name>',
        'get_file_info',
        file_info, methods=['GET'])
    blueprint.add_url_rule(
        'storage/owner/<owner>',
        'total_storage_for_owner',
        total_storage_for_owner, methods=['GET'])
    blueprint.add_url_rule(
        'storage/dataset_id/<path:dataset_id>',
        'total_storage_for_dataset_id',
        total_storage_for_dataset_id, methods=['GET'])
    blueprint.add_url_rule(
        'storage/flow_id/<path:flow_id>',
        'total_storage_for_flow_id',
        total_storage_for_flow_id, methods=['GET'])

    # Return blueprint
    return blueprint
