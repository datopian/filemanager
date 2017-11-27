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

files_on_disk = {}
for fs in FILES:
    files_on_disk['/'.join(fs[0].split('/')[:2] + [fs[1]])] = (fs[0], fs[2])

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

get_total_size_for_owner_expected = [
    (owner,
     sum(ff[1]
         for filename, ff in files_on_disk.items()
         if filename.startswith(owner)))
    for owner
    in set([fs_[0].split('/')[0]
            for fs_ in FILES])]


@pytest.mark.parametrize('owner,size', get_total_size_for_owner_expected)
def test_get_total_size_for_owner(full_db: FileManager, owner, size):
    assert full_db.get_total_size_for_owner(owner) == size

get_total_size_for_dataset_id_expected = [
    (dataset_id,
     sum(ff[1]
         for filename, ff in files_on_disk.items()
         if filename.startswith(dataset_id)))
    for dataset_id
    in set(['/'.join(fs_[0].split('/')[:2])
            for fs_ in FILES])]


@pytest.mark.parametrize('dataset_id,size', get_total_size_for_dataset_id_expected)
def test_get_total_size_for_dataset_id(full_db: FileManager, dataset_id, size):
    assert full_db.get_total_size_for_dataset_id(dataset_id) == size

get_total_size_for_flow_id_expected = [
    (flow_id,
     sum(ff[1]
         for filename, ff in files_on_disk.items()
         if ff[0] == flow_id))
    for flow_id
    in set(fs_[0] for fs_ in FILES)
]


@pytest.mark.parametrize('flow_id,size', get_total_size_for_flow_id_expected)
def test_get_total_size_for_flow_id(full_db: FileManager, flow_id, size):
    assert full_db.get_total_size_for_flow_id(flow_id) == size


def test_get_total_size_for_unknown_owner(full_db: FileManager):
    assert full_db.get_total_size_for_owner('unknown') == 0


def test_get_total_size_for_unknown_dataset_id(full_db: FileManager):
    assert full_db.get_total_size_for_dataset_id('unknown') == 0


def test_get_total_size_for_unknown_flow_id(full_db: FileManager):
    assert full_db.get_total_size_for_flow_id('unknown') == 0
