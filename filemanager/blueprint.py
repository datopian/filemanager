from flask import Blueprint
from flask_jsonpify import jsonpify

from .models import FileManager

from .config import db_connection_string


def make_blueprint():
    """Create blueprint.
    """

    fm = FileManager(db_connection_string)

    # Create instance
    blueprint = Blueprint('filemanager', 'filemanager')

    def total_storage_for_owner(owner):
        return jsonpify(fm.get_total_size_for_owner(owner))

    def total_storage_for_dataset_id(dataset_id):
        return jsonpify(fm.get_total_size_for_dataset_id(dataset_id))

    def total_storage_for_flow_id(flow_id):
        return jsonpify(fm.get_total_storage_for_flow_id(flow_id))

    # Register routes
    blueprint.add_url_rule(
        'storage/owner/<owner>',
        'total_storage_for_owner',
        total_storage_for_owner, methods=['GET'])
    blueprint.add_url_rule(
        'storage/dataset_id/<dataset_id>',
        'total_storage_for_dataset_id',
        total_storage_for_dataset_id, methods=['GET'])
    blueprint.add_url_rule(
        'storage/flow_id/<flow_id>',
        'total_storage_for_flow_id',
        total_storage_for_flow_id, methods=['GET'])

    # Return blueprint
    return blueprint
