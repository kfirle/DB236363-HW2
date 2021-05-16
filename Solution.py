from typing import List
import Utility.DBConnector as Connector
from Utility.ReturnValue import ReturnValue
from Utility.Exceptions import DatabaseException
from Business.Query import Query
from Business.RAM import RAM
from Business.Disk import Disk
from psycopg2 import sql

def queryFromResultSet(result : Connector.ResultSet) -> Query:
    values = list(result.__getitem__(0).values())
    answer = Query(*values[0:3])
    return answer

def diskFromResultSet(result : Connector.ResultSet) -> Disk:
    values = list(result.__getitem__(0).values())
    answer = Disk(*values)
    return answer

def ramFromResultSet(result : Connector.ResultSet) -> RAM:
    values = list(result.__getitem__(0).values())
    answer = RAM(*values[0:3])
    return answer

def createTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("CREATE TABLE Disk(id INTEGER NOT NULL CHECK (id > 0), company TEXT NOT NULL,"
                     " speed INTEGER NOT NULL CHECK (speed > 0), space INTEGER NOT NULL CHECK (space >= 0),"
                     " cost INTEGER NOT NULL CHECK (cost > 0), PRIMARY KEY (id))")
        conn.commit()
        conn.execute("CREATE TABLE Query(id INTEGER NOT NULL CHECK (id > 0), purpose TEXT NOT NULL,"
                     " size INTEGER NOT NULL CHECK (size >= 0), disk_id INTEGER CHECK (disk_id > 0),"
                     " FOREIGN KEY (disk_id) REFERENCES DISK(id) ON DELETE CASCADE, PRIMARY KEY (id))")
        conn.commit()
        conn.execute("CREATE TABLE RAM(id INTEGER NOT NULL CHECK (id > 0), size INTEGER NOT NULL CHECK (size > 0),"
                     " company TEXT NOT NULL, disk_id INTEGER CHECK (disk_id > 0),"
                     " FOREIGN KEY (disk_id) REFERENCES DISK(id) ON DELETE CASCADE, PRIMARY KEY (id))")
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        conn.close()


def clearTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("DELETE FROM Query")
        conn.commit()
        conn.execute("DELETE FROM Disk")
        conn.commit()
        conn.execute("DELETE FROM RAM")
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        conn.close()


def dropTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("DROP TABLE IF EXISTS Query CASCADE")
        conn.commit()
        conn.execute("DROP TABLE IF EXISTS Disk CASCADE")
        conn.commit()
        conn.execute("DROP TABLE IF EXISTS RAM CASCADE")
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        # do stuff
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        # do stuff
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        # do stuff
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        # do stuff
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        # do stuff
        print(e)
    except Exception as e:
        print(e)
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()


def addQuery(query: Query) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Query(id, purpose, size) VALUES({id}, {purpose}, {size})")\
            .format(id=sql.Literal(query.getQueryID()), purpose=sql.Literal(query.getPurpose()),
                    size=sql.Literal(query.getSize()))
        rows_effected, _ = conn.execute(query, printSchema=True)
        conn.commit()
        return ReturnValue.OK
    except DatabaseException.ConnectionInvalid as e:
        print(e)
        return ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        return ReturnValue.BAD_PARAMS
    except Exception as e:
        print(e)
        return ReturnValue.ERROR
    finally:
        conn.close()
        # return ReturnValue.OK


def getQueryProfile(queryID: int) -> Query:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * FROM Query WHERE id={queryID}".format(queryID=queryID))
        rows_effected, result = conn.execute(query, printSchema=True)
        answer = queryFromResultSet(result)
        conn.commit()
        return answer
    except Exception as e:
        print(e)
        return Query.badQuery()
    finally:
        conn.close()

# TODO: do not forget to adjust the free space on disk if the query runs on one. Hint - think about
#  transactions in such cases (there are more in this assignment.
def deleteQuery(query: Query) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Query WHERE id={queryID}".format(queryID=query.getQueryID()))
        rows_effected, _ = conn.execute(query, printSchema=True)
        conn.commit()
        return ReturnValue.OK
    except Exception as e:
        print(e)
        return ReturnValue.ERROR
    finally:
        conn.close()
        # return ReturnValue.OK


def addDisk(disk: Disk) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Disk(id, company, speed, space, cost) VALUES({id}, {company}, {speed}, "
                        "{space}, {cost})") \
            .format(id=sql.Literal(disk.getDiskID()), company=sql.Literal(disk.getCompany()),
                    speed=sql.Literal(disk.getSpeed()), space=sql.Literal(disk.getFreeSpace()),
                    cost=sql.Literal(disk.getCost()))
        rows_effected, _ = conn.execute(query, printSchema=True)
        conn.commit()
        return ReturnValue.OK
    except DatabaseException.ConnectionInvalid as e:
        print(e)
        return ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        return ReturnValue.BAD_PARAMS
    except Exception as e:
        print(e)
        return ReturnValue.ERROR
    finally:
        conn.close()
        # return ReturnValue.OK


def getDiskProfile(diskID: int) -> Disk:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * FROM Disk WHERE id={diskID}".format(diskID=diskID))
        rows_effected, result = conn.execute(query, printSchema=True)
        answer = diskFromResultSet(result)
        conn.commit()
        return answer
    except Exception as e:
        print(e)
        return Disk.badDisk()
    finally:
        conn.close()


def deleteDisk(diskID: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Disk WHERE id={diskID}".format(diskID=diskID))
        rows_effected, _ = conn.execute(query, printSchema=True)
        conn.commit()
        return ReturnValue.OK
    except Exception as e:
        print(e)
        return ReturnValue.ERROR
    finally:
        conn.close()
        # return ReturnValue.OK


def addRAM(ram: RAM) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO RAM(id, size, company) VALUES({id}, {size}, {company})") \
            .format(id=sql.Literal(ram.getRamID()), company=sql.Literal(ram.getCompany()),
                    size=sql.Literal(ram.getSize()))
        rows_effected, _ = conn.execute(query, printSchema=True)
        conn.commit()
        return ReturnValue.OK
    except DatabaseException.ConnectionInvalid as e:
        print(e)
        return ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        return ReturnValue.BAD_PARAMS
    except Exception as e:
        print(e)
        return ReturnValue.ERROR
    finally:
        conn.close()
        # return ReturnValue.OK


def getRAMProfile(ramID: int) -> RAM:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * FROM RAM WHERE id={ramID}".format(ramID=ramID))
        rows_effected, result = conn.execute(query, printSchema=True)
        answer = ramFromResultSet(result)
        conn.commit()
        return answer
    except Exception as e:
        print(e)
        return RAM.badRAM()
    finally:
        conn.close()


def deleteRAM(ramID: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM RAM WHERE id={ramID}".format(ramID=ramID))
        rows_effected, _ = conn.execute(query, printSchema=True)
        conn.commit()
        return ReturnValue.OK
    except Exception as e:
        print(e)
        return ReturnValue.ERROR
    finally:
        conn.close()
        # return ReturnValue.OK


def addDiskAndQuery(disk: Disk, query: Query) -> ReturnValue:
    conn = None
    # disk_added = False
    try:
        conn = Connector.DBConnector()
        query_1 = sql.SQL("INSERT INTO Disk(id, company, speed, space, cost) VALUES({id}, {company}, {speed}, {space}, {cost})") \
            .format(id=sql.Literal(disk.getDiskID()), company=sql.Literal(disk.getCompany()),
                    speed=sql.Literal(disk.getSpeed()), space=sql.Literal(disk.getFreeSpace()),
                    cost=sql.Literal(disk.getCost()))
        rows_effected, _ = conn.execute(query_1, printSchema=True)
        # disk_added = True
        # conn.commit()
        query_2 = sql.SQL("INSERT INTO Query(id, purpose, size) VALUES({id}, {purpose}, {size})") \
            .format(id=sql.Literal(query.getQueryID()), purpose=sql.Literal(query.getPurpose()),
                    size=sql.Literal(query.getSize()))
        rows_effected, _ = conn.execute(query_2, printSchema=True)
        conn.commit()
        return ReturnValue.OK
    except DatabaseException.ConnectionInvalid as e:
        print(e)
        conn.rollback()
        return ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        conn.rollback()
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        conn.rollback()
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        conn.rollback()
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        conn.rollback()
        return ReturnValue.BAD_PARAMS
    except Exception as e:
        print(e)
        conn.rollback()
        return ReturnValue.ERROR
    finally:
        conn.close()
        # return ReturnValue.OK


def addQueryToDisk(query: Query, diskID: int) -> ReturnValue:
    return ReturnValue.OK


def removeQueryFromDisk(query: Query, diskID: int) -> ReturnValue:
    return ReturnValue.OK


def addRAMToDisk(ramID: int, diskID: int) -> ReturnValue:
    return ReturnValue.OK


def removeRAMFromDisk(ramID: int, diskID: int) -> ReturnValue:
    return ReturnValue.OK


def averageSizeQueriesOnDisk(diskID: int) -> float:
    return 0


def diskTotalRAM(diskID: int) -> int:
    return 0


def getCostForPurpose(purpose: str) -> int:
    return 0


def getQueriesCanBeAddedToDisk(diskID: int) -> List[int]:
    return []


def getQueriesCanBeAddedToDiskAndRAM(diskID: int) -> List[int]:
    return []


def isCompanyExclusive(diskID: int) -> bool:
    return True


def getConflictingDisks() -> List[int]:
    return []


def mostAvailableDisks() -> List[int]:
    return []


def getCloseQueries(queryID: int) -> List[int]:
    return []
