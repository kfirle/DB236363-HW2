import Solution as solution
from Business.Query import Query
from Business.RAM import RAM
from Business.Disk import Disk

if __name__ == '__main__':
    # solution.dropTables()
    solution.createTables()
    # i = solution.addQuery(Query(3, "check", 1))
    # q = solution.getQueryProfile(3)
    # print(q.getQueryID(), q.getPurpose(), q.getSize())
    # print(i)
    # i = solution.deleteQuery(Query(5, "H", 3))
    # print(i)
    # i = solution.deleteQuery(Query(3, "check", 1))
    # q = solution.getQueryProfile(3)
    # print(q.getQueryID(), q.getPurpose(), q.getSize())
    # solution.addQuery(Query(3, "check", 1))
    # i = solution.addDiskAndQuery(Disk(1, "h", 1, 1, 1), Query(2, "check", 1))
    # print(i)
    # solution.getDiskProfile(1)
    # solution.getQueryProfile(2)
    # solution.clearTables()
    # solution.addDisk(Disk(1, "h", 1, 1, 1))
    # solution.getDiskProfile(1)
    # solution.addDisk(Disk(2, "h", 1, 1, 1))
    # solution.getDiskProfile(2)
    solution.addQuery(Query(3, "check", 1))
    i = solution.addDiskAndQuery(Disk(1, "h", 1, 1, 1), Query(2, "check", 1))
    solution.getDiskProfile(1)
    solution.getQueryProfile(2)
