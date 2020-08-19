<!--
 ! Licensed to the Apache Software Foundation (ASF) under one
 ! or more contributor license agreements.  See the NOTICE file
 ! distributed with this work for additional information
 ! regarding copyright ownership.  The ASF licenses this file
 ! to you under the Apache License, Version 2.0 (the
 ! "License"); you may not use this file except in compliance
 ! with the License.  You may obtain a copy of the License at
 !
 !   http://www.apache.org/licenses/LICENSE-2.0
 !
 ! Unless required by applicable law or agreed to in writing,
 ! software distributed under the License is distributed on an
 ! "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 ! KIND, either express or implied.  See the License for the
 ! specific language governing permissions and limitations
 ! under the License.
 !-->

This system allows for the 13 types of Allen's interval-join relations.
The default, when using these joins, is either Nested Loop, or Hybrid Hash Join.
The optimal algorithm will be automatically selected based on the query; Hybrid
Hash Joins will be selected in the cases when the join uses an equality; these cases are:
starts(), started_by(), ends(), ended_by(), meets(), and met_by(). Otherwise, the system will
default to nested loop join. To use interval merge join you must include a range hint. Adding a
range hint allows for the system to pick interval merge join.

## <a id="Interval_joins">Interval Joins</a>
An interval join can be executed using one of the 13 functions. The 13 functions are before(), 
after(), covers(), covered_by(), ends(), ended_by(), meets(), met_by(), overlaps(), overlapping(), 
overlapped_by(), starts(), and started_by().

##### How to use an interval join

    select element { "staff" : f.name, "student" : d.name }
    from Staff as f, Students as d
    where interval_after(f.employment, d.attendance)
    
In this scenario, "interval_after" can be replaced with any of the 13 join functions eg: interval_before, 
interval_covers, etc... Here is what each of the functions represent if A represents the First set of Data,
and B represents the second set of data:

##### Before(A, B) and After(B, A)

    A.end < B.start

##### Covers(A, B), and Covered_by(B, A)

    A.start <= B.start, and
    A.end >= B.end

##### Ends(A, B), and Ended_by(B, A)

    A.end = B.end, and
    A.start >= B.start

##### Meets(A, B), and Met_by(B, A)

    A.end = B.start
    
##### Overlaps(A, B), and Overlapped_by(B, A)

    A.start < B.start, and
    B.start > A.end, and
    A.end > B.start

##### Overlapping(A, B)

    (A.start >= B.start, and
    B.start < A.end), or
    (B.end <= A.end, and
    B.end < A.start)

##### Starts(A, B), and Started_by(B, A)

    A.start = B.start, and
    A.end <= B.end

## <a id="Range_hint">Using a Range Hint</a>

Interval joins with a range hint currently work for intervals of date, datetime, or time. Adding a
range hint directly before the join will cause the system to pick interval merge join for 7 out of
the 13 Allen's relations: After, Before, Covers, Covered_By, Overlaps, Overlapping, Overlapped_By.
The other six relations have not been implemented; in those cases the range hint will not be picked,
and the system will default to the normal join.

Here are examples of how interval joins work with a range hint for all the supported data types.
Suppose that we have two sets of data, a data set of staff members with an interval for length of
employment, for breaks from employment, for office hours, and an id. The other data set represents students,
which may include an interval for years attended, vacation periods, and work hours. Each partition receives
data based on the split points; The split points in the range hint must be strategically set by the
user so that the data divides evenly among partitions. For example, if your query contains 1 split point,
and the system is using two partitions, the data before the split point will be sent to the first partition,
and the data after the split point will be sent to the second partition. This continues to work respectively
based on the number split points and number of partitions. Ideally, the number of split points should equal
the number of partitions - 1, but the query will still work if it doesn't.

##### Range Hint Example

    /*+ range [<Expression>, ..., ] */


##### Range Hint Example with Date

    select f.name as staff, d.name as student
    from Staff as f, Students as d
    where
    /*+ range [date("2003-06-30"), date("2005-12-31"), date("2008-06-30")] */
    interval_after(f.employment, d.attendance)
    order by f.name, d.name;

##### Range Hint Example with DateTime

    select f.name as staff, d.name as student
    from Students as d, Staff as f
    where
    /*+ range [datetime("2003-06-30T00:00:00.0"), datetime("2005-12-31T00:00:00.0"), datetime("2008-06-30T00:00:00.0")] */
    interval_after(d.break, f.vacation)
    order by f.name, d.name;

##### Range Hint Example with Time

    select f.name as staff, d.name as student
    from Staff as f, Students as d
    where
    /*+ range [time("03:30:00.0+00:00"), time("05:59:00.0+00:00"), time("08:30:00.0+00:00")] */
    interval_after(f.office_hours, d.work_hours)
    order by f.name, d.name;

