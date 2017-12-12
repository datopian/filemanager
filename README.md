# Tracking

[![Travis](https://img.shields.io/travis/datahq/filemanager/master.svg)](https://travis-ci.org/datahq/filemanager)
[![Coveralls](http://img.shields.io/coveralls/datahq/filemanager.svg?branch=master)](https://coveralls.io/r/datahq/filemanager?branch=master)

### Manage files in storage

```python
def add_file(self, 
             bucket, 
             object_name, 
             findability, 
             owner, 
             owner_id, 
             dataset_id, 
             flow_id, 
             size, 
             created_at):
    # Record a file was added to the storage
    pass
```

### API

#### Get info about file
`storage/info/<bucket>/<path:object_name>`

return
```json
{
  "bucket": "the-bucket",
  "object_name": "path/to/file",
  "findability": "published/unlisted/private/etc.",
  "owner": "username",
  "owner_id": "userid",
  "dataset_id": "owner/dataset-name",
  "last_flow_id": "owner/dataset-name/100",
  "flow_ids": ["owner/dataset-name/99", "owner/dataset-name/100"],  
  "size": 1234,
  "created_at": "2017-09-23T12:00:00"
}
```

#### Get total size for owner/dataset/flow

`storage/owner/<owner>`
`storage/dataset_id/<path:dataset_id>`
`storage/flow_id/<path:flow_id>`

return
```json
{
  "totalBytes": 12345
}
```

## Contributing

Please read the contribution guideline:

[How to Contribute](CONTRIBUTING.md)

Thanks!
