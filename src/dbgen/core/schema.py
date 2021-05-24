#   Copyright 2021 Modelyst LLC
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

from abc import ABCMeta, abstractmethod

# External
from typing import TYPE_CHECKING, Any
from typing import Dict as D
from typing import Iterator as Iter
from typing import List as L
from typing import Sequence
from typing import Set as S
from typing import Tuple as T

from hypothesis.strategies import SearchStrategy, builds, lists

from dbgen.core.expr.expr import PK
from dbgen.core.expr.sqltypes import Int, SQLType, Text, Varchar
from dbgen.core.funclike import Arg, ArgLike, Const
from dbgen.core.load import Load
from dbgen.core.misc import Dep
from dbgen.utils.exceptions import DBgenInvalidArgument, DBgenMissingInfo
from dbgen.utils.misc import Base
from dbgen.utils.sql import Connection as Conn
from dbgen.utils.sql import mkInsCmd, mkSelectCmd, sqlexecute, sqlselect

# Internal
if TYPE_CHECKING:
    from dbgen.core.expr.pathattr import PathAttr
    from dbgen.core.model.model import Model
    from dbgen.core.pathconstraint import Path as AP
    from dbgen.core.query import Query
    from dbgen.core.schemaclass import Schema

    Model, Query, Schema, PathAttr, AP
"""
Components of a schema: Objects, Attributes, and Relations

Also the RelTup container class is defined
"""
########################################################################################
######################
# Simple Tuple types #
######################


class AttrTup(Base):
    def __init__(self, name: str, obj: str) -> None:
        self.name = name
        self.obj = obj
        super().__init__()

    def __str__(self) -> str:
        return self.obj + "." + self.name

    def __call__(self, x: "AP" = None) -> "PathAttr":
        from dbgen.core.expr.pathattr import PathAttr

        return PathAttr(x, self)

    @classmethod
    def _strat(cls) -> "SearchStrategy":
        from hypothesis.strategies import builds

        return builds(cls)


class RelTup(Base):
    """
    A tuple (objectname, relationname) that an object can produce independent
    of a Model instance .... it will later have to be validated by the model to
    be a relation that actually exists, but lower level classes can work with it
    in the meantime
    """

    def __init__(self, obj: str, rel: str) -> None:
        self.obj = obj.lower()
        self.rel = rel.lower()
        super().__init__()

    def __str__(self) -> str:
        return f"Rel({self.obj},{self.rel})"

    @classmethod
    def _strat(cls) -> "SearchStrategy":
        from hypothesis.strategies import builds

        return builds(cls)


################################################################################
class Attr(Base):
    """ Attribute, considered from a schema-making perspective (NOT as an Expr) """

    def __init__(
        self,
        name: str,
        dtype: SQLType = None,
        identifying: bool = False,
        default: Any = None,
        desc: str = "<No description>",
        index: bool = False,
        partition_on: bool = False,
        partition_values: Sequence = None,
        partition_default: bool = True,
    ) -> None:
        assert name
        self.name = name.lower()
        self.desc = desc
        self.dtype = dtype or Int()
        self.identifying = identifying
        self.index = index
        self.default = default
        # Partition Columns
        # Use this Attribute for partitioning the Entity
        self.partition_on = partition_on
        if partition_on:
            if not isinstance(self.dtype, (Text, Int, Varchar)):
                raise NotImplementedError(
                    f"Partitioning only implemented for Int, Text, and Varchar:\n{self.name}\n{dtype}"
                )
        # Set the partitioned values
        if partition_on and not (partition_values or partition_default):
            raise DBgenMissingInfo("Cannot partition column without default partition of default values")
        # Check values can be cast to dtype
        if partition_values:
            for val in partition_values:
                try:
                    self.dtype.cast(val)
                except ValueError:
                    raise DBgenInvalidArgument(f"Invalid partition value for dtype {dtype}: \n{val}")
        self.partition_values = partition_values or []
        self.partition_default = partition_default
        super().__init__()

    def __str__(self) -> str:
        return f"Attr<{self.name},{self.dtype}>"

    ####################
    # Public methods #
    ####################

    def create_col(self, tabname: str) -> T[str, str, str]:
        """
        Create statement for column when creating a table.
        """
        dt = str(self.dtype)
        dflt = "" if self.default is None else f"DEFAULT {self.default}"
        desc = 'comment on column "{}"."{}" is \'{}\''.format(
            tabname, self.name, self.desc.replace("'", "''")
        )

        fmt_args = [self.name, dt, dflt]
        create = '"{}" \t{} {}'.format(*fmt_args)
        index = (
            f'\nCREATE INDEX IF NOT EXISTS {tabname}_{self.name}_idx ON {tabname} ("{self.name}")'
            if self.index
            else ""
        )
        return create, desc, index


class View(Base, metaclass=ABCMeta):
    def __str__(self) -> str:
        return f"View({self.name})"

    name = ""

    @abstractmethod
    def dep(self) -> Dep:
        raise NotImplementedError

    @abstractmethod
    def qstr(self) -> str:
        """String representing the query (either JSON or raw SQL)"""
        raise NotImplementedError

    def add(self, cxn: Conn) -> int:
        """add view to metadb, return PK"""
        # Try to find an Object with an equivalent hash in the existing table
        get_v = mkSelectCmd("view", ["view_id"], ["view_id"])
        v_id = sqlselect(cxn, get_v, [self.hash])

        if v_id:
            return v_id[0][0]  # already there
        else:
            # Create a new record in the View table and get its ID
            cmd = mkInsCmd("view", ["view_id", "name", "query"])
            sqlexecute(cxn, cmd, [self.hash, self.name, self.qstr()])
            return sqlselect(cxn, get_v, [self.hash])[0][0]

    @classmethod
    @abstractmethod
    def _strat(cls) -> "SearchStrategy":
        raise NotImplementedError

    def create(self) -> str:
        """
        Generate SQL necessary to create an object's corresponding table
        """
        return f"CREATE VIEW {self.name} AS {self.qstr()}"


class QView(View):
    def __init__(self, name: str, q: "Query") -> None:
        self.name = name
        self.q = q

    def qstr(self) -> str:
        return self.q.toJSON()

    def dep(self) -> Dep:
        cd = [f"{a.obj}.{a.name}" for a in self.q.allattr()]
        nc = [f"{self.name}.{x}" for x in self.q.exprs]
        return Dep(self.q.allobj(), cd, [self.name], nc)

    @classmethod
    def _strat(cls) -> "SearchStrategy":
        return builds(cls)


class RawView(View):
    def __init__(self, name: str, q: str, deps: L[str] = None, new: L[str] = None) -> None:
        self.name = name
        self.raw = q
        self.deps = deps or []
        self.new = new or []

    def qstr(self) -> str:
        return self.raw

    def dep(self) -> Dep:
        td = [d for d in self.deps if "." not in d]
        cd = [d for d in self.deps if "." in d]
        nc = [f"{self.name}.{x}" for x in self.new]
        return Dep(td, cd, [self.name], nc)

    @classmethod
    def _strat(cls) -> "SearchStrategy":
        return builds(cls)


class UserRel(Base):
    """
    USER EXPOSED Relation between objects. no need to specify source, as it is
    declared from within the UserObj constructor.
    Can be identifying or non-identifying
    """

    def __init__(
        self,
        name: str,
        tar: str = None,
        identifying: bool = False,
        desc: str = "<No description>",
        delete_trigger: str = "NO ACTION",
    ) -> None:
        self.name = name.lower()
        self.desc = desc
        self.tar = tar.lower() if tar else self.name
        self.identifying = identifying
        self.delete_trigger = delete_trigger
        super().__init__()

    def __str__(self) -> str:
        idstr = "(id)" if self.identifying else ""
        return f"{self.name}{idstr} -> {self.tar}"

    def to_rel(self, obj: str) -> "Rel":
        return Rel(
            name=self.name,
            o1=obj,
            o2=self.tar,
            identifying=self.identifying,
            desc=self.desc,
            delete_trigger=self.delete_trigger,
        )


class Entity(Base):
    """Object with attributes. Basic entity of a model"""

    def __init__(
        self,
        name: str,
        desc: str = None,
        attrs: L[Attr] = None,
        fks: L[UserRel] = None,
        id_str: str = None,
    ) -> None:

        self.name = name.lower()
        self.desc = desc or "<No description>"
        self.attrs = attrs or []
        self.fks = fks or []
        self.id_str = id_str if id_str else self.name + "_id"
        # Validate
        self.forbidden = [
            self.id_str,
            "deleted",
            "insert",
            self.name,
        ]  # RESERVED
        assert not any([a.name in self.forbidden for a in self.attrs]), (
            self.attrs,
            self.forbidden,
        )
        attr_names = [a.name for a in self.attrs]
        assert len(attr_names) == len(
            set(attr_names)
        ), f"No duplicate column names allowed, \nObject: {self.name}"
        assert all([isinstance(x, str) for x in [self.name, self.desc, self.id_str]])
        # Check at most one partitioned attr
        partitioned_attrs = list(filter(lambda x: x.partition_on, self.attrs))
        if len(partitioned_attrs) > 1:
            raise DBgenInvalidArgument("Can only have at most 1 partitioned attributes!")
        self.partition_attr = partitioned_attrs[0] if partitioned_attrs else None

        super().__init__()

    @property
    def attrdict(self) -> D[str, Attr]:
        return {a.name: a for a in self.attrs}

    @property
    def fkdict(self) -> D[str, "Rel"]:
        return {r.name: r.to_rel(self.name) for r in self.fks}

    @property
    def is_partitioned(self) -> bool:
        return self.partition_attr is not None

    def __str__(self) -> str:
        return "Entity<%s, %d attrs>" % (self.name, len(self.attrs))

    def __call__(self, **kwargs: Any) -> Load:
        """
        Construct an Load which specifies AT LEAST how to identify this
        object (via PK or data) AND POSSIBLY more non-identifying info to update

        - Attributes and relations are referred to by name with kwargs
        - A keyword equal to the object's own name signifies a PK argument
        """
        invalid_args = list(
            filter(
                lambda keyval: keyval[0] != "insert" and not isinstance(keyval[1], (ArgLike, Load)),
                kwargs.items(),
            )
        )
        if invalid_args:
            raise DBgenInvalidArgument(f"Non ArgLike kwargs provided: {invalid_args}")
        kwargs = {k.lower(): v for k, v in kwargs.items()}  # convert keys to L.C.
        pk = kwargs.pop(self.name, None)
        insert = kwargs.pop("insert", False)

        if not pk:  # if we don't have a PK reference
            err = "Cannot refer to a row in {} without a PK or essential data." " Missing essential data: {}"
            missing = set(self.ids() + self.id_fks()) - set(kwargs)
            if missing:
                raise DBgenMissingInfo(err.format(self.name, missing))

        attrs = {k: v for k, v in kwargs.items() if k in self.attrnames()}

        fks = {k: v for k, v in kwargs.items() if k not in attrs}
        for fk in fks:
            assert fk in self.fkdict, f'unknown "{fk}" kwarg in Load of {self}'
        for k, v in fks.items():
            if not isinstance(v, Load):
                # We OUGHT have a reference to a FK from a query
                assert isinstance(v, (Arg, Const))
                rel = self.fkdict[k]
                # Check for relations with names that are different from table_names
                if rel.o2 != k:
                    fks[k] = Load(obj=rel.o2, attrs={}, fks={}, pk=v)
                else:
                    fks[k] = Load(obj=k, attrs={}, fks={}, pk=v)
        # Need partition attribute name for resolving conflicts during insertion
        partition_attr = self.partition_attr.name if self.partition_attr else None
        return Load(self.name, attrs=attrs, fks=fks, pk=pk, insert=insert, partition_attr=partition_attr)

    def __getitem__(self, key: str) -> AttrTup:
        if key in self.attrdict:
            return AttrTup(key, self.name)
        else:
            raise KeyError(key + f" not found in {self}")

    # Public methods #

    def get(self, key: str) -> AttrTup:
        """
        A version of __getitem__ that doesn't check whether attribute is defined
        Use when we need to refer to an attribute which may not (yet) exist
        """
        return AttrTup(key, self.name)

    def act(self, **kwargs: Any) -> Load:
        """Do we need to add a "insert" flag in order to say: "it's ok for this
        load to insert any required parent objects recursively?"""
        return self(**kwargs)

    def r(self, relname: str) -> RelTup:
        """
        Refer to a relation of an object. Without a Model, we have to do with
        reference by name
        """
        return RelTup(self.name, relname)

    def attrnames(self, init: bool = False) -> L[str]:
        """Names of all (top-level) attributes"""
        return [a.name for a in self.attrs if a.identifying or not init]

    def create(self) -> L[str]:
        """
        Generate SQL necessary to create an object's corresponding table
        """
        create_str = f'CREATE TABLE IF NOT EXISTS "{self.name}" '
        if len(self.attrs) != 0:
            cols, coldescs, colindexes = map(
                lambda x: list(x),
                zip(*[a.create_col(self.name) for a in self.attrs]),
            )
        else:
            cols, coldescs, colindexes = [], [], []
        pk = self.id_str + " BIGINT "

        deld = "deleted BOOLEAN NOT NULL DEFAULT FALSE"
        full_cols = [pk, deld] + list(cols)
        partition_name = self.partition_attr.name if self.partition_attr else ""
        primary_key = f",PRIMARY KEY ({self.id_str}"
        primary_key += f",\"{partition_name}\")" if partition_name else ")"

        columns = "\n\t,".join(full_cols)
        partition_str = f" PARTITION BY LIST (\"{self.partition_attr.name}\")" if self.partition_attr else ""
        create_cmd = f"{create_str}\n\t({columns}\n\t{primary_key}\n){partition_str}"
        table_description = "comment on table \"{}\" is '{}'".format(self.name, self.desc.replace("'", "''"))
        sql_statements = (
            [create_cmd, table_description]
            + self.create_partitions()
            + list(coldescs)
            + list(filter(lambda x: x != "", colindexes))
        )
        return sql_statements

    def create_partitions(self) -> L[str]:
        if not self.partition_attr:
            return []
        partition_statements = []
        part_name = self.partition_attr.name
        # Add default partition if flag set
        if self.partition_attr.partition_default:
            partition_statements.append(
                f"CREATE TABLE IF NOT EXISTS \"{self.name}__{part_name}__default\" partition of \"{self.name}\" default"
            )
        # For each val, attach a  partition
        if self.partition_attr.partition_values:
            for val in self.partition_attr.partition_values:
                partition_statements.append(
                    f"CREATE TABLE IF NOT EXISTS \"{self.name}__{part_name}__{val}\" partition of \"{self.name}\" for values in ({val})"
                )
        return partition_statements

    def get_partition(self, partition_value: Any = None, default: bool = False) -> 'Partition':
        if self.partition_attr is None:
            raise ValueError(f"Entity {self.name} is not partitioned!")
        elif default:
            Partition(
                None,
                default=True,
                name=self.name,
                desc=self.desc,
                attrs=self.attrs,
                fks=self.fks,
                id_str=self.id_str,
            )
        elif partition_value not in self.partition_attr.partition_values and not default:
            raise ValueError(
                f"Requested partition value is not declared on the Partition Attribute:"
                f"\nAttr: {self.partition_attr.name}"
                f"\nPartition Values: {self.partition_attr.partition_values}"
                f"\nRequested Value: {partition_value}"
            )

        return Partition(
            partition_value,
            default=False,
            name=self.name,
            desc=self.desc,
            attrs=self.attrs,
            fks=self.fks,
            id_str=self.id_str,
        )

    def get_all_partitions(self) -> L['Partition']:
        if not self.partition_attr:
            return []
        partitions = []
        kwargs = dict(
            name=self.name,
            desc=self.desc,
            attrs=self.attrs,
            fks=self.fks,
            id_str=self.id_str,
        )
        if self.partition_attr.partition_default:
            partitions.append(Partition(None, default=True, **kwargs))  # type: ignore
        partitions.extend(map(lambda val: self.get_partition(val), self.partition_attr.partition_values))

        return partitions

    def id(self, path: "AP" = None) -> PK:
        """Main use case: GROUP BY an object, rather than a particular column"""
        from dbgen.core.expr.pathattr import PathAttr

        return PK(PathAttr(path, AttrTup(self.id_str, self.name)))

    def ids(self) -> L[str]:
        """Names of all the identifying (top-level) attributes."""
        return [a.name for a in self.attrs if a.identifying]

    def id_fks(self) -> L[str]:
        """Names of all the identifying (top-level) FKs """
        return [f.name for f in self.fks if f.identifying]

    def add(self, cxn: Conn) -> int:
        """
        Add this Object to a metaDB that stores information about a model (if
        it's not already there), and return the ID.
        """
        # Try to find an Object with an equivalent hash in the existing table
        get_t = mkSelectCmd("object", ["object_id"], ["object_id"])
        t_id = sqlselect(cxn, get_t, [self.hash])

        if t_id:
            return t_id[0][0]  # already there
        else:
            # Create a new record in the Object table and get its ID
            name = self.name
            cmd = mkInsCmd("object", ["object_id", "name", "description"])
            sqlexecute(cxn, cmd, [self.hash, name, self.desc])
            tab_id = sqlselect(cxn, get_t, [self.hash])[0][0]

            # Before returning ID, we have to populate Attr table
            ins_cols = [
                "object",
                "name",
                "dtype",
                "description",
                "defaultval",
                "attr_id",
            ]

            for c in self.attrs:
                # Insert info about an attribute
                binds = [
                    tab_id,
                    c.name,
                    str(c.dtype),
                    c.desc,
                    str(c.default),
                    c.hash,
                ]

                cmd = mkInsCmd("attr", ins_cols)
                sqlexecute(cxn, cmd, binds)

            return tab_id

    def rename_attr(self, aname: str, newname: str) -> "Entity":
        """ Copy of object with a renamed attribute """
        o = self.copy()
        newattr = o.attrdict[aname]
        o.attrs.remove(newattr)
        newattr.name = newname
        o.attrs.append(newattr)
        return o


class Partition(Entity):
    """Sub-Entity for a specific List Partition"""

    def __init__(
        self,
        partition_value: Any,
        default: bool,
        name: str,
        desc: str,
        attrs: L[Attr],
        fks: L[UserRel],
        id_str: str,
    ) -> None:
        self.partition_value = partition_value
        self.default = default
        super().__init__(name, desc=desc, attrs=attrs, fks=fks, id_str=id_str)
        assert self.partition_attr, "Trying to make partition out of unpartitioned Entity!"
        # Set name of Partition Entity
        if default:
            self.name = f"{self.name}__{self.partition_attr.name}__default"
            self._parent_name = name
        else:
            assert partition_value in self.partition_attr.partition_values
            self.name = f"{self.name}__{self.partition_attr.name}__{self.partition_value}"
            self._parent_name = name


class Rel(Base):
    """
    Asymmetric Relation between objects

    Can be identifying or non-identifying
    """

    _valid_triggers = ("NO ACTION", "CASCADE", "SET NULL")

    def __init__(
        self,
        name: str,
        o1: str,
        o2: str = None,
        identifying: bool = False,
        desc: str = "<No description>",
        delete_trigger: str = "NO ACTION",
    ) -> None:
        self.name = name.lower()
        self.desc = desc
        self.o1 = o1.lower()
        self.o2 = o2.lower() if o2 else self.name
        self.identifying = identifying
        if delete_trigger:
            assert (
                delete_trigger in self._valid_triggers
            ), f"Invalid delete trigger value {delete_trigger} please use one of: {self._valid_triggers}"
            self.delete_trigger = delete_trigger
        super().__init__()

    def __str__(self) -> str:
        return "Rel<{}{},{} -> {}>".format(
            self.name,
            "(id)" if self.identifying else "",
            self.o1,
            self.o2,
        )

    def __repr__(self) -> str:
        return self.__str__()

    # Public methods #

    def print(self) -> str:
        return f"{self.o1}.{self.name}"

    def tup(self) -> RelTup:
        """Throw away info to make a RelTup"""
        return RelTup(self.o1, self.name)

    @property
    def default(self) -> bool:
        """Whether or not this Relation is the 'default' one"""
        return self.name == self.o2

    @property
    def objs(self) -> S[str]:
        return {self.o1, self.o2}

    def other(self, obj: str) -> str:
        """
        Often we don't know which direction we are traversing on a FK
        We know which end we're coming from and simply want the other end
        """
        assert obj in self.objs, f"{obj} not found in {self}"

        out = [o for o in self.objs if o != obj]

        if out:
            return out[0]  # normal case
        else:
            return obj  # both to and from are the same table!

    # Private Methods #
    # ...


class SuperRel(Base):
    """
    Contains information about source table name, relation name,
    and target table ID column name.

    Example:
    SuperRel: Person.favorite_book <-> book.title
        name: favorite_book
        source: Person
        book: target
        target_id_str: Title
    """

    def __init__(
        self,
        name: str,
        source: str,
        target: str,
        target_id_str: str,
        identifying: bool = False,
        desc: str = "<No description>",
    ) -> None:
        self.name = name.lower()
        self.source = source.lower()
        self.target = target.lower()
        self.target_id_str = target_id_str
        self.identifying = identifying
        self.desc = desc

        super().__init__()

    def __str__(self) -> str:
        return "SuperRel<{},{} -> {}.{}>".format(
            self.name,
            self.source,
            self.target,
            self.target_id_str,
        )

    def __repr__(self) -> str:
        return self.__str__()

    # Public methods #

    def print(self) -> str:
        return f"{self.source}.{self.name}"

    def tup(self) -> RelTup:
        """Throw away info to make a RelTup"""
        return RelTup(self.source, self.name)

    @property
    def default(self) -> bool:
        """Whether or not this Relation is the 'default' one"""
        return self.name == self.target

    @property
    def objs(self) -> S[str]:
        return {self.source, self.target}

    def other(self, obj: str) -> str:
        """
        Often we don't know which direction we are traversing on a FK
        We know which end we're coming from and simply want the other end
        """
        assert obj in self.objs, f"{obj} not found in {self}"

        out = [o for o in self.objs if o != obj]

        if out:
            return out[0]  # normal case
        else:
            return obj  # both to and from are the same table!

    def to_rel(self) -> Rel:
        return Rel(self.name, self.source, self.target, self.identifying, self.desc)

    # Private Methods #
    # ...
    @classmethod
    def _strat(cls) -> "SearchStrategy":
        return builds(cls)

    def __lt__(self, other: Any) -> bool:
        if not isinstance(other, self.__class__):
            raise ValueError(f"Comparing {type(other)} to type {self.__class__}")
        return repr(self) < repr(other)


class Path(Base):
    """
    Some list of foreign keys (possibly empty), possibly followed by attribute
    """

    def __init__(self, rels: L[RelTup] = None, attr: AttrTup = None) -> None:
        self.rels = rels or []
        self.attr = attr
        assert rels or attr
        super().__init__()

    def __str__(self) -> str:
        p = f"[{','.join(map(str, self.rels))}]" if self.rels else ""
        comma = "," if self.rels else ""
        a = comma + str(self.attr) if self.attr else ""
        return f"Path({p}{a})"

    @classmethod
    def _strat(cls) -> "SearchStrategy":
        return builds(cls, rels=lists(RelTup._strat(), max_size=2), attr=AttrTup._strat())

    def start(self) -> str:
        """Starting point of a path, always an object (name is returned)"""
        if self.rels:
            return self.rels[0].obj
        else:
            assert self.attr
            return self.attr.obj

    def select(self, m: "Model") -> str:
        """ Thing to select for when comparing path equality """
        alias = self.start()
        for r in self.rels:
            alias += "$" + r.rel
        col = self._end_attr(m).name
        return f'"{alias}"."{col}"'

    def joins(self, ids: D[str, str], m: "Model") -> L[str]:
        """From clause that makes self.select() defined in query"""

        j = []  # type: L[str]
        oldpath = self.start()
        jstr = 'JOIN "{0}" AS "{1}" ON "{1}"."{4}" = "{2}"."{3}"'
        next = self.start()
        for i, r in enumerate(self.rels[:-1]):
            newpath = oldpath + "$" + r.rel
            next = self.rels[i + 1].obj
            nextid = ids[next]
            args = [next, newpath, oldpath, r.rel, nextid]
            j.append(jstr.format(*args))
            oldpath = newpath
        if self.rels:
            last = self.rels[-1].rel
            lastid = ids[m.get_rel(self.rels[-1]).o2]
            newpath = oldpath + "$" + last
            lastargs = [self._end_attr(m).obj, newpath, oldpath, last, lastid]
            j.append(jstr.format(*lastargs))
        return j

    def _end_attr(self, m: "Model") -> AttrTup:
        """the ID colname if we don't have a normal attribute as terminus"""
        if not self.attr:
            rel = m.get_rel(self.rels[-1])
            return AttrTup(m[rel.o2].id_str, rel.o2)
        else:
            return self.attr

    def _path_end(self, m: "Schema") -> str:
        """Determine the datatype of the end of a path."""
        if (not self.attr) or (AttrTup(m[self.attr.obj].id_str, self.attr.obj) == self.attr):
            return "id"
        else:
            o = m[self.attr.obj]
            a = [a for n, a in o.attrdict.items() if n == self.attr.name]
            assert len(a) < 2
            if not a:
                err = "Could not find %s in %s: Path %s"
                raise ValueError(err % (a, o, self))
            return str(a[0].dtype)


class PathEQ(Base):
    """Specification that two paths should result in the same value."""

    def __init__(self, p1: Path, p2: Path) -> None:
        assert p1 != p2, "Cannot do pathEQ between things that are literally equivalent"
        assert p1.start() == p2.start(), "Paths must have same start point"
        self.paths = {p1, p2}
        super().__init__()

    def __str__(self) -> str:
        return "PathEQ({},{})".format(*self.paths)

    def __iter__(self) -> Iter[Path]:
        return iter(self.paths)

    def __contains__(self, a: AttrTup) -> bool:
        if isinstance(a, AttrTup):
            return any([a == p.attr for p in self.paths])
        else:
            raise TypeError("add to this to support more types of searching")

    def any(self) -> Path:
        """Gives one of the paths, doesn't matter which"""
        return next(iter(self))

    def start(self) -> str:
        return self.any().start()
