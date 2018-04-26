import asyncio
import random
from gino import Gino

db = Gino()


class Task(db.Model):
    __tablename__ = 'tasks'
    __table_args__ = dict(schema='aintq')

    ctid = db.literal_column('ctid')
    schedule = db.Column(db.DateTime())
    name = db.Column(db.Unicode())
    seq = db.Sequence('tasks_deletes', metadata=db, schema='aintq', cycle=True)

    def append_where_primary_key(self, q):
        return q.where(Task.ctid == self.ctid)

    async def run(self):
        print('Running:', self.ctid, self.name)
        await asyncio.sleep(1)
        if random.random() < 0.5:
            1/0
        return 'Done:', self.ctid, self.name


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
