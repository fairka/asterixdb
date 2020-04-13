/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.runtime.evaluators.functions.temporal;

public class IntervalLogicWithLong {

    public static boolean validateInterval(long s, long e) {
        return s <= e;
    }

    /**
     * Anything from interval 1 is less than anything from interval 2.
     *
     * @param s1
     * @param e1
     * @param s2
     * @param e2
     * @return boolean
     * @see #after(Comparable, Comparable, Comparable, Comparable)
     */
    public static boolean before(long s1, long e1, long s2, long e2) {
        return e1 < s2;
    }

    public static boolean after(long s1, long e1, long s2, long e2) {
        return before(s2, e2, s1, e1);
    }

    /**
     * The end of interval 1 is the same as the start of interval 2.
     *
     * @param s1
     * @param e1
     * @param s2
     * @param e2
     * @return boolean
     * @see #metBy(Comparable, Comparable, Comparable, Comparable)
     */
    public static boolean meets(long s1, long e1, long s2, long e2) {
        return e1 == s2;
    }

    public static boolean metBy(long s1, long e1, long s2, long e2) {
        return meets(s2, e2, s1, e1);
    }

    /**
     * Something at the end of interval 1 is contained as the beginning of interval 2.
     *
     * @param s1
     * @param e1
     * @param s2
     * @param e2
     * @return boolean
     * @see #overlappedBy(Comparable, Comparable, Comparable, Comparable)
     */
    public static boolean overlaps(long s1, long e1, long s2, long e2) {
        return s1 < s2 && e1 > s2 && e2 > e1;
    }

    public static boolean overlappedBy(long s1, long e1, long s2, long e2) {
        return overlaps(s2, e2, s1, e1);
    }

    /**
     * Something is shared by both interval 1 and interval 2.
     *
     * @param s1
     * @param e1
     * @param s2
     * @param e2
     * @return boolean
     */
    public static boolean overlapping(long s1, long e1, long s2, long e2) {
        return s1 < e2 && e1 > s2;
    }

    /**
     * Anything from interval 1 is contained in the beginning of interval 2.
     *
     * @param s1
     * @param e1
     * @param s2
     * @param e2
     * @return boolean
     * @see #startedBy(Comparable, Comparable, Comparable, Comparable)
     */
    public static boolean starts(long s1, long e1, long s2, long e2) {
        return s1 == s2 && e1 <= e2;
    }

    public static boolean startedBy(long s1, long e1, long s2, long e2) {
        return starts(s2, e2, s1, e1);
    }

    /**
     * Anything from interval 2 is in interval 1.
     *
     * @param s1
     * @param e1
     * @param s2
     * @param e2
     * @return boolean
     * @see #startedBy(Comparable, Comparable, Comparable, Comparable)
     */
    public static boolean covers(long s1, long e1, long s2, long e2) {
        return s1 <= s2 && e1 >= e2;
    }

    public static boolean coveredBy(long s1, long e1, long s2, long e2) {
        return covers(s2, e2, s1, e1);
    }

    /**
     * Anything from interval 1 is from the ending part of interval 2.
     *
     * @param s1
     * @param e1
     * @param s2
     * @param e2
     * @return boolean
     * @see #endedBy(Comparable, Comparable, Comparable, Comparable)
     */
    public static boolean ends(long s1, long e1, long s2, long e2) {
        return s1 >= s2 && e1 == e2;
    }

    public static boolean endedBy(long s1, long e1, long s2, long e2) {
        return ends(s2, e2, s1, e1);
    }

    /**
     * Intervals with the same start and end time.
     *
     * @param s1
     * @param e1
     * @param s2
     * @param e2
     * @return boolean
     */
    public static boolean equals(long s1, long e1, long s2, long e2) {
        return s1 == s1 && e1 == e2;
    }

}
