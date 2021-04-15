import inspect

from aintq.db import db
from aintq.utils import unpickle_data


class Task(db.Model):
    __tablename__ = 'tasks'
    __table_args__ = dict(schema='aintq')

    ctid = db.literal_column('ctid')
    schedule = db.Column(db.DateTime())
    name = db.Column(db.Unicode())
    params = db.Column(db.Binary())
    seq = db.Sequence('tasks_deletes', metadata=db, schema='aintq', cycle=True)

    def append_where_primary_key(self, q):
        return q.where(Task.ctid == self.ctid)

    async def run(self, aintq):
        cur = aintq.registry.get(self.name, None)
        if not cur:
            print(f'Task {self.name} is not exist.')
            return None
        print('Running:', self.ctid, self.name)
        res = unpickle_data(self.params)
        args = res['args']
        kwargs = res['kwargs']
        if inspect.iscoroutinefunction(cur):
            return await cur(*args, **kwargs)
        return cur(*args, **kwargs)


Task.ctid.table = Task.__table__
db.Index('tasks_schedule_index', Task.schedule.nullsfirst())
Task.trigger = (
    '''\
CREATE OR REPLACE FUNCTION aintq.notify_new_task() RETURNS TRIGGER AS $$ BEGIN
  PERFORM pg_notify('aintq_enqueue', new.CTID::VARCHAR);
  RETURN NULL;
END $$ LANGUAGE plpgsql;''',
    '''\
CREATE TRIGGER tasks_insert_notify AFTER INSERT ON aintq.tasks
FOR EACH ROW EXECUTE PROCEDURE aintq.notify_new_task();''')
