# -*- coding: utf-8 -*-
import datetime

import pytest
import time

from filemanager import FileManager

FILE_TO_FLOW = {}
FILES = [
    ('ow1/ds1/1', 'f1', 1000),
    ('ow1/ds1/1', 'f2', 100),
    ('ow1/ds1/1', 'f3', 10),

    ('ow1/ds1/2', 'f1', 1000),
    ('ow1/ds1/2', 'f2', 100),
    ('ow1/ds1/2', 'f3', 10),

    ('ow2/ds1/1', 'f1', 1000),
    ('ow2/ds1/1', 'f2', 100),

    ('ow3/ds2/1', 'f1', 1000),
    ('ow3/ds2/1', 'f2', 100),

    ('ow3/ds2/2', 'f1', 1000),
    ('ow3/ds2/2', 'f2', 100),

    ('ow1/ds1/3', 'f1', 1000),
    ('ow1/ds1/3', 'f2', 100),
    ('ow1/ds1/3', 'f4', 11),
]

now = datetime.datetime.now()
@pytest.fixture()
def full_db():

    fm = FileManager('sqlite://')

    for flow_id, object_name, size in FILES:
        parts = flow_id.split('/')
        dataset_id = '/'.join(parts[:2])
        object_name = '/'.join([dataset_id, object_name])
        FILE_TO_FLOW.setdefault(object_name, set()).add(flow_id)
        fm.add_file('bucket',
                    object_name,
                    parts[0],
                    parts[0]+'_id',
                    dataset_id,
                    flow_id,
                    size,
                    now)

    return fm

@pytest.mark.parametrize('sf', FILES)
def test_get_file_info(full_db: FileManager, sf):
    flow_id, object_name, size = sf
    parts = flow_id.split('/')
    dataset_id = '/'.join(parts[:2])
    object_name = '/'.join([dataset_id, object_name])
    flow_ids = FILE_TO_FLOW[object_name]
    info = full_db.get_file_info('bucket', object_name)
    assert info == {
        'bucket': 'bucket',
        'object_name': object_name,
        'owner': parts[0],
        'owner_id': parts[0]+'_id',
        'dataset_id': dataset_id,
        'last_flow_id': max(flow_ids),
        'flow_ids': list(sorted(flow_ids)),
        'size': size,
        'created_at': now
    }