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

Adding a range hint to an interval join will allow an optimized interval join algorithm to be picked:

    
This system allows for the 13 types of interval-join relations. The default, when using these joins, is either Nested 
Loop, or Hybrid Hash Join. If you want to use interval merge join you must include a range hint. The 
range hint allows for the system to pick interval merge join, and partition the data according to the given split 
points.

## <a id="Using_interval_joins">Using Interval Joins</a>
An interval join can be executed using one of the 13 conditions. The 13 conditions are before, after, covers, 
covered_by, ends, ended_by, meets, met_by, overlaps, overlapping, overlapped_by, starts, and started_by.

##### How to use an interval join

    select element { "staff" : f.name, "student" : d.name }
    from Staff as f, Students as d
    where interval_after(f.employment, d.attendance)
    order by f.name, d.name;
    
In this scenario, "interval_after" can be replaced with any of the 13 join conditions eg: interval_before, 
interval_covers, etc...

## <a id="Range_hint">Using a Range Hint</a>

Interval joins currently work for intervals of date, datetime, or time. Adding  a range hint 
will allow the system to pick interval merge join for 7 out of the 13 Allen's relations: After, 
Before, Covers, Covered_By, Overlaps, Overlapping, Overlapped_By. The other six relations have
not been implemented; in those cases the range hint will not be picked, and the system will default
to the normal join.

Here are examples of how interval joins work with a range hint for all the supported points.
The data will be partitioned using the split points provided in the hint. 

##### Range Hint Example

    /*+ range [<Expression>, ..., ] */


##### Range Hint Example with Date

    select element { "staff" : f.name, "student" : d.name }
    from Staff as f, Students as d
    where
    /*+ range [date("2003-06-30"), date("2005-12-31"), date("2008-06-30")] */
    interval_after(f.employment, d.attendance)
    order by f.name, d.name;

##### Range Hint Example with DateTime

    select element { "staff" : f.name, "student" : d.name }
    from Students as d, Staff as f
    where
    /*+ range [datetime("2003-06-30T00:00:00.0"), datetime("2005-12-31T00:00:00.0"), datetime("2008-06-30T00:00:00.0")] */
    interval_after(d.break, f.vacation)
    order by f.name, d.name;

##### Range Hint Example with Time

    use TinyCollege;
    
    select element { "staff" : f.name, "student" : d.name }
    from Staff as f, Students as d
    where
    /*+ range [time("03:30:00.0+00:00"), time("05:59:00.0+00:00"), time("08:30:00.0+00:00")] */
    interval_after(f.office_hours, d.work_hours)
    order by f.name, d.name;

