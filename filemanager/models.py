import json
from contextlib import contextmanager

from sqlalchemy import Column, types
from sqlalchemy import DateTime
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import create_engine
from sqlalchemy import func
from sqlalchemy import inspect
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()


# ## Json as string Type
class JsonType(types.TypeDecorator):
    impl = types.Unicode

    def process_bind_param(self, value, dialect):
        return json.dumps(value)

    def process_result_value(self, value, dialect):
        if value:
            return json.loads(value)
        else:
            return None

    def copy(self, **kw):
        return JsonType(self.impl.length)


class StoredFile(Base):
    __tablename__ = 'storedfiles'
    bucket = Column(String, primary_key=True)
    object_name = Column(String, primary_key=True)
    findability = Column(String)
    owner = Column(String)
    owner_id = Column(String)
    dataset_id = Column(String, index=True)
    last_flow_id = Column(String, index=True)
    flow_ids = Column(JsonType)
    size = Column(Integer)
    created_at = Column(DateTime)


class FileManager:

    def __init__(self, db_connection_string, engine=None):
        self._db_connection_string = db_connection_string
        self._engine = engine
        self._session = None

    @property
    def engine(self):
        if self._engine is None:
            self._engine = create_engine(self._db_connection_string)
        return self._engine

    @contextmanager
    def session_scope(self):
        """Provide a transactional scope around a series of operations."""
        if self._session is None:
            self._session = sessionmaker(bind=self.engine)
        session = self._session()
        try:
            yield session
            session.commit()
        except: #noqa
            session.rollback()
            raise
        finally:
            session.expunge_all()
            session.close()

    @staticmethod
    def object_as_dict(obj):
        return {c.key: getattr(obj, c.key)
                for c in inspect(obj).mapper.column_attrs}

    def init_db(self):
        Base.metadata.create_all(self.engine)

    # ### WRITE API
    def add_file(self, bucket, object_name, findability, owner, owner_id, dataset_id, flow_id, size, created_at):
        with self.session_scope() as s:
            sf = s.query(StoredFile).filter_by(bucket=bucket, object_name=object_name).first()
            if sf is None:
                sf = StoredFile(
                    bucket=bucket,
                    object_name=object_name,
                    findability=findability,
                    owner=owner,
                    owner_id=owner_id,
                    dataset_id=dataset_id,
                    last_flow_id=flow_id,
                    flow_ids=[flow_id],
                    size=size,
                    created_at=created_at
                )
            else:
                assert sf.owner == owner
                assert sf.owner_id == owner_id
                assert sf.dataset_id == dataset_id
                sf.findability = findability
                sf.last_flow_id = flow_id
                sf.flow_ids = list(sorted(set(sf.flow_ids + [flow_id])))
                sf.size = size
                sf.created_at = created_at
            s.add(sf)

    # ### READ API
    def get_file_info(self, bucket, object_name):
        with self.session_scope() as s:
            sf = s.query(StoredFile).filter_by(bucket=bucket, object_name=object_name).first()
            if sf is None:
                return None
            return self.object_as_dict(sf)

    def get_total_size_for_owner(self, owner, findability=None):
        with self.session_scope() as s:
            sf = s.query(func.sum(StoredFile.size).label('total'))\
                         .filter_by(owner=owner)
            if findability is not None:
                sf = sf.filter_by(findability=findability)
            sf = sf.first()
            if sf is None or sf.total is None:
                return 0
            return sf.total

    def get_total_size_for_dataset_id(self, dataset_id, findability=None):
        with self.session_scope() as s:
            sf = s.query(func.sum(StoredFile.size).label('total')) \
                .filter_by(dataset_id=dataset_id)
            if findability is not None:
                sf = sf.filter_by(findability=findability)
            sf = sf.first()
            if sf is None or sf.total is None:
                return 0
            return sf.total

    def get_total_size_for_flow_id(self, flow_id, findability=None):
        with self.session_scope() as s:
            sf = s.query(func.sum(StoredFile.size).label('total')) \
                .filter_by(last_flow_id=flow_id)
            if findability is not None:
                sf = sf.filter_by(findability=findability)
            sf = sf.first()
            if sf is None or sf.total is None:
                return 0
            return sf.total
