import sqlite3
from itertools import count
conn = sqlite3.connect("db")
conn.row_factory = sqlite3.Row

counter = count()

def sql_run(stmt, values=None):
    print("Running", stmt, "with", values)
    cur = conn.cursor()
    cur.execute(stmt, values or {})
    conn.commit()
    return cur.lastrowid

def sql_select(stmt, values=None):
    print("Selecting", stmt, "with", values)
    cur = conn.cursor()
    cur.execute(stmt, values or {})
    yield from cur.fetchall()


class Field:
    def __init__(self, name, py_type):
        self.name = name
        self.py_type = py_type

    def __set__(self, instance, value):
        if issubclass(self.py_type, Model) and isinstance(value, int):
            value = next(self.py_type.select(self.py_type.id == value))
        instance._values[self.name] = value

    def __get__(self, instance, cls):
        if instance:  # our_talk.duration
            return instance._values[self.name]
        else:  # Talk.duration
            return self

    def sql_type(self):
        if issubclass(self.py_type, Model):
            return "INTEGER"
        return {
            int: "INTEGER",
            str: "TEXT",
        }[self.py_type]

    def to_sql(self, value):
        if issubclass(self.py_type, Model):
            return value.id
        return value

    def __eq__(self, value):
        return Condition("=", self, value)


class Condition:
    def __init__(self, op, field, value):
        self.op = op
        self.field = field
        self.value = value

    def to_sql(self):
        placeholder = f"var{next(counter)}"
        return (
            f"{self.field.name} {self.op} :{placeholder}",
            {placeholder: self.value},
        )

    def __or__(self, other):
        return BoolCondition("OR", self, other)

class BoolCondition(Condition):
    def __init__(self, op, cond1, cond2):
        self.op = op
        self.cond1 = cond1
        self.cond2 = cond2

    def to_sql(self):
        sql1, vals1 = self.cond1.to_sql()
        sql2, vals2 = self.cond2.to_sql()
        return (
            f"{sql1} {self.op} {sql2}",
            {**vals1, **vals2},
        )


class Model:
    def __init_subclass__(cls):
        cls._name = f"{cls.__name__.lower()}s"
        cls._cols = {
            name: Field(name, py_type)
            for name, py_type
            in cls.__annotations__.items()
        }
        for name, field in cls._cols.items():
            setattr(cls, name, field)
        setattr(cls, "id", Field("id", int))

    @classmethod
    def delete(cls):
        sql_run(f"DROP TABLE IF EXISTS {cls._name}")

    @classmethod
    def create(cls):
        stmt = f"CREATE TABLE {cls._name} (id INTEGER PRIMARY KEY, %s)" % ", ".join(
            f"{name} {field.sql_type()}" for name, field in cls._cols.items()
        )
        sql_run(stmt)

    def __init__(self, **kwargs):
        self._values = {"id": None}
        for key, val in kwargs.items():
            setattr(self, key, val)

    def __repr__(self):
        return f"<{self.__class__.__name__} id={self.id} %s>" % " ".join(
            f"{name}={getattr(self, name)!r}"
            for name in self._cols
        )

    def save(self):
        values = {
            name: field.to_sql(getattr(self, name)) for name, field in self._cols.items()
        }
        if self.id:
            stmt = f"UPDATE {self._name} SET %s WHERE id=:id" % ", ".join(
                f"{name} = :{name}" for name in self._cols
            )
            sql_run(stmt, {"id": self.id, **values})
        else:
            stmt = f"INSERT INTO {self._name} (%s) VALUES (%s)" % (
                ", ".join(f"{name}" for name in self._cols),
                ", ".join(f":{name}" for name in self._cols),
            )
            self.id = sql_run(stmt, values)

    @classmethod
    def select(cls, where=None):
        if where:
            where_sql, values = where.to_sql()
        else:
            where_sql, values = "1=1", {}
        stmt = f"SELECT * FROM {cls._name} WHERE {where_sql}"
        for row in sql_select(stmt, values):
            yield cls(**dict(row))


###############################

class Speaker(Model):
    name: str
    company: str

Speaker.delete()
Speaker.create()

me = Speaker(name="Jonathan", company="solute")
me.save()

class Talk(Model):
    title: str
    duration: int
    speaker: Speaker

Talk.delete()
Talk.create()

our_talk = Talk(title="ORM in 45min", duration=45, speaker=me)
print(our_talk)
our_talk.save()
our_talk.duration = 60
our_talk.save()


for talk in Talk.select(where=(Talk.duration == 60) | (Talk.duration == 45)):
    print(talk)
